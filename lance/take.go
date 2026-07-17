package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// namedExprJSON is the wire form of a SQL-projection column (see NamedExpr
// in evolution.go, which this reuses).
type namedExprJSON struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}

// TakeSpec describes a point read. Exactly one of Indices, RowIDs or
// Addresses must be set, at most one of Columns or SQLProjection.
type TakeSpec struct {
	// Indices are row offsets in the dataset (0 = first live row. Deleted
	// rows are skipped).
	Indices []uint64
	// RowIDs are stable row ids (write the dataset with
	// WithStableRowIDs(true) for ids that survive compaction. Without the
	// feature, row ids equal row addresses).
	RowIDs []uint64
	// Addresses are row addresses (fragment_id << 32 | row_offset), e.g.
	// obtained from a scan with WithRowID or a take with WithRowAddress.
	Addresses []uint64
	// Columns restricts the result to the given columns. Empty means all
	// columns.
	Columns []string
	// SQLProjection computes the result columns with SQL expressions
	// instead of a plain column list.
	SQLProjection []NamedExpr
	// WithRowAddress includes the internal _rowaddr column in the result.
	WithRowAddress bool
}

// takeSpecJSON mirrors the spec_json contract of lance_dataset_take.
//
// The active selector (Indices, RowIDs or Addresses) carries no omitempty, so
// a nil slice serializes as JSON null (which the native side reads as
// "absent") while an intentionally-empty selector serializes as [] (Some(vec!
// []) on the Rust side, meaning "fetch zero rows"). Exactly one of the three
// is set non-nil by the caller. Columns/SQLProjection keep omitempty because an
// empty column list means "all columns", not "project zero columns".
type takeSpecJSON struct {
	Indices        []uint64        `json:"indices"`
	RowIDs         []uint64        `json:"row_ids"`
	Addresses      []uint64        `json:"addresses"`
	Columns        []string        `json:"columns,omitempty"`
	SQLProjection  []namedExprJSON `json:"sql_projection,omitempty"`
	WithRowAddress bool            `json:"with_row_address,omitempty"`
}

// takeScanSpecJSON mirrors the spec_json contract of lance_dataset_take_scan.
type takeScanSpecJSON struct {
	Ranges         [][2]uint64 `json:"ranges"`
	Columns        []string    `json:"columns,omitempty"`
	BatchReadahead uint64      `json:"batch_readahead,omitempty"`
}

// sampleSpecJSON mirrors the spec_json contract of lance_dataset_sample.
type sampleSpecJSON struct {
	N           uint64   `json:"n"`
	Columns     []string `json:"columns,omitempty"`
	FragmentIDs []uint32 `json:"fragment_ids,omitempty"`
}

// importRecordReader moves a native-filled Arrow C stream into an
// array.RecordReader. The stream struct need not outlive the call. Errors
// carry the "lance:" prefix; callers inside datasetOp/fragmentOp fn closures
// (which apply the prefix themselves) must use importRecordReaderRaw instead.
func importRecordReader(stream *C.struct_ArrowArrayStream, what string, configs ...streamReaderConfig) (array.RecordReader, error) {
	r, err := importRecordReaderRaw(stream, what, configs...)
	if err != nil {
		return nil, fmt.Errorf("lance: %w", err)
	}
	return r, nil
}

// importRecordReaderRaw is importRecordReader with unprefixed errors.
func importRecordReaderRaw(stream *C.struct_ArrowArrayStream, what string, configs ...streamReaderConfig) (array.RecordReader, error) {
	var cfg streamReaderConfig
	if len(configs) > 0 {
		cfg = configs[0]
	}
	return newOwnedRecordReader(unsafe.Pointer(stream), what, cfg)
}

// singleBatch drains a one-batch reader and returns the batch retained. The
// caller must Release the result.
func singleBatch(reader array.RecordReader, what string) (arrow.RecordBatch, error) {
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("lance: %s: %w", what, err)
		}
		return nil, fmt.Errorf("lance: %s produced no batch: %w", what, ErrInternal)
	}
	rec := reader.RecordBatch()
	rec.Retain()
	return rec, nil
}

// Take returns the rows selected by spec as a single record batch, in
// request order. The caller must Release the result.
func (d *Dataset) Take(ctx context.Context, spec TakeSpec) (arrow.RecordBatch, error) {
	return datasetOp(ctx, d, "Dataset.Take", "take",
		func(ctx context.Context, ptr *C.LanceDataset) (arrow.RecordBatch, error) {
			var sql []namedExprJSON
			for _, e := range spec.SQLProjection {
				sql = append(sql, namedExprJSON{Name: e.Name, Expr: e.SQL})
			}
			// marshalOptions is not used here: it returns an error already
			// prefixed "lance: marshal options:", which would double-wrap
			// under datasetOp's single "lance: take: %w". Marshal directly
			// instead, matching the marshalRef exemplar in version.go.
			data, err := json.Marshal(&takeSpecJSON{
				Indices:        spec.Indices,
				RowIDs:         spec.RowIDs,
				Addresses:      spec.Addresses,
				Columns:        spec.Columns,
				SQLProjection:  sql,
				WithRowAddress: spec.WithRowAddress,
			})
			if err != nil {
				return nil, fmt.Errorf("marshal take spec: %w", err)
			}
			cSpec, freeSpec := cString(string(data))
			defer freeSpec()

			// Must be zero-initialized (Go zeroes it). On success the native
			// side writes a self-contained one-batch stream into it.
			var stream C.struct_ArrowArrayStream
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_take(ptr, cSpec, &stream)
			}); err != nil {
				return nil, err
			}
			reader, err := importRecordReaderRaw(&stream, "take")
			if err != nil {
				return nil, err
			}
			return drainSingleBatch(reader)
		})
}

// drainSingleBatch drains a one-batch reader and returns the batch retained.
// Its errors are unprefixed for use inside datasetOp/datasetDo fn closures
// (see singleBatch for the standalone, already-prefixed variant used by
// hand-rolled callers).
func drainSingleBatch(reader array.RecordReader) (arrow.RecordBatch, error) {
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("produced no batch: %w", ErrInternal)
	}
	rec := reader.RecordBatch()
	rec.Retain()
	return rec, nil
}

// TakeIndices is a convenience for Take by row offsets: it returns the rows
// at the given positions (0 = first live row), projected onto columns (all
// columns when empty). The caller must Release the result.
func (d *Dataset) TakeIndices(ctx context.Context, indices []uint64, columns ...string) (arrow.RecordBatch, error) {
	return d.Take(ctx, TakeSpec{Indices: indices, Columns: columns})
}

// TakeRows is a convenience for Take by stable row ids. The caller must
// Release the result.
func (d *Dataset) TakeRows(ctx context.Context, rowIDs []uint64, columns ...string) (arrow.RecordBatch, error) {
	return d.Take(ctx, TakeSpec{RowIDs: rowIDs, Columns: columns})
}

// TakeScan streams the rows in the given half-open [start, end) ranges of
// row offsets, projected onto columns (all columns when empty).
//
// The returned reader and every record obtained from it must be Released by
// the caller. The reader stays valid after the Dataset is closed.
func (d *Dataset) TakeScan(ctx context.Context, ranges [][2]uint64, columns ...string) (reader array.RecordReader, err error) {
	cfg := observedStream(ctx, d.obs(), "Dataset.TakeScan")
	defer cfg.endOnError(&err)
	ctx = cfg.ctx
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	if ranges == nil {
		ranges = [][2]uint64{}
	}
	cSpec, freeSpec, err := marshalOptions(&takeScanSpecJSON{Ranges: ranges, Columns: columns})
	if err != nil {
		return nil, err
	}
	defer freeSpec()

	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_take_scan(ptr, cSpec, &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: take scan: %w", err)
	}
	return importRecordReader(&stream, "take scan", cfg)
}

// Sample returns n randomly sampled rows (in row-id order, not random
// order) projected onto columns (all columns when empty). When the dataset
// has fewer than n rows, all rows are returned. The caller must Release the
// result.
func (d *Dataset) Sample(ctx context.Context, n uint64, columns ...string) (arrow.RecordBatch, error) {
	return datasetOp(ctx, d, "Dataset.Sample", "sample",
		func(ctx context.Context, ptr *C.LanceDataset) (arrow.RecordBatch, error) {
			// See Take: marshal directly rather than via marshalOptions, whose
			// error is already prefixed.
			data, err := json.Marshal(&sampleSpecJSON{N: n, Columns: columns})
			if err != nil {
				return nil, fmt.Errorf("marshal sample spec: %w", err)
			}
			cSpec, freeSpec := cString(string(data))
			defer freeSpec()

			var stream C.struct_ArrowArrayStream
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_sample(ptr, cSpec, &stream)
			}); err != nil {
				return nil, err
			}
			reader, err := importRecordReaderRaw(&stream, "sample")
			if err != nil {
				return nil, err
			}
			return drainSingleBatch(reader)
		})
}
