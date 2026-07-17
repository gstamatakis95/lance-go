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
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// DeltaBuilder configures a change-data-capture query between two dataset
// versions. Build one with Dataset.Delta, select a range with exactly one
// of ComparedAgainstVersion, FromVersion+ToVersion, or FromDate+ToDate,
// then call one of the terminal methods.
//
// The begin bound is exclusive and the end bound inclusive: a delta from
// version 1 to version 3 covers the changes committed by versions 2 and 3.
type DeltaBuilder struct {
	ds  *Dataset
	cfg deltaConfig
}

// deltaConfig mirrors the spec_json contract of lance_dataset_delta.
type deltaConfig struct {
	ComparedAgainstVersion *uint64 `json:"compared_against_version,omitempty"`
	BeginVersion           *uint64 `json:"begin_version,omitempty"`
	EndVersion             *uint64 `json:"end_version,omitempty"`
	BeginDate              string  `json:"begin_date,omitempty"`
	EndDate                string  `json:"end_date,omitempty"`
}

// TransactionInfo summarizes one transaction between two dataset versions.
type TransactionInfo struct {
	// ReadVersion is the version the transaction was based on.
	ReadVersion uint64 `json:"read_version"`
	// UUID uniquely identifies the transaction.
	UUID string `json:"uuid"`
	// Operation is the operation kind, e.g. "Append", "Delete", "Update".
	Operation string `json:"operation"`
	// Tag is the transaction's tag, if any.
	Tag string `json:"tag"`
	// Properties holds the transaction properties recorded at write time.
	Properties map[string]string `json:"properties"`
}

// Delta starts building a change-data-capture query over the dataset.
// Tracking inserted/updated rows requires the dataset to be written with
// stable row ids (WithStableRowIDs(true)).
func (d *Dataset) Delta() *DeltaBuilder {
	return &DeltaBuilder{ds: d}
}

// obs returns the instrumentation handle for this builder, inherited from its
// dataset (nil-safe).
func (b *DeltaBuilder) obs() *obs { return b.ds.obs() }

// ComparedAgainstVersion compares the checked-out version against version v
// (the bounds are ordered automatically). Mutually exclusive with the
// explicit ranges.
func (b *DeltaBuilder) ComparedAgainstVersion(v uint64) *DeltaBuilder {
	b.cfg.ComparedAgainstVersion = &v
	return b
}

// FromVersion sets the exclusive begin version. Use together with
// ToVersion.
func (b *DeltaBuilder) FromVersion(v uint64) *DeltaBuilder {
	b.cfg.BeginVersion = &v
	return b
}

// ToVersion sets the inclusive end version. Use together with FromVersion.
func (b *DeltaBuilder) ToVersion(v uint64) *DeltaBuilder {
	b.cfg.EndVersion = &v
	return b
}

// FromDate sets the exclusive begin timestamp. Use together with ToDate.
func (b *DeltaBuilder) FromDate(t time.Time) *DeltaBuilder {
	b.cfg.BeginDate = t.UTC().Format(time.RFC3339Nano)
	return b
}

// ToDate sets the inclusive end timestamp. Use together with FromDate.
func (b *DeltaBuilder) ToDate(t time.Time) *DeltaBuilder {
	b.cfg.EndDate = t.UTC().Format(time.RFC3339Nano)
	return b
}

// rows runs the delta row query of the given kind (matching the kind values
// of lance_dataset_delta).
func (b *DeltaBuilder) rows(ctx context.Context, kind int32, what, operation string) (reader array.RecordReader, err error) {
	cfg := observedStream(ctx, b.obs(), operation)
	defer cfg.endOnError(&err)
	ctx = cfg.ctx
	b.ds.mu.RLock()
	defer b.ds.mu.RUnlock()
	ptr, err := b.ds.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	cSpec, freeSpec, err := marshalOptions(&b.cfg)
	if err != nil {
		return nil, err
	}
	defer freeSpec()

	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_delta(ptr, cSpec, C.int32_t(kind), &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: delta %s: %w", what, err)
	}
	return importRecordReader(&stream, "delta "+what, cfg)
}

// InsertedRows streams the rows created within the delta's version range.
// Results include the _rowid, _row_created_at_version and
// _row_last_updated_at_version columns. The returned reader and its records
// must be Released by the caller.
func (b *DeltaBuilder) InsertedRows(ctx context.Context) (array.RecordReader, error) {
	return b.rows(ctx, 0, "inserted rows", "DeltaBuilder.InsertedRows")
}

// UpdatedRows streams the rows that existed before the delta's version
// range and were updated within it. See InsertedRows for the result shape.
func (b *DeltaBuilder) UpdatedRows(ctx context.Context) (array.RecordReader, error) {
	return b.rows(ctx, 1, "updated rows", "DeltaBuilder.UpdatedRows")
}

// UpsertedRows streams both the inserted and the updated rows of the
// delta's version range. See InsertedRows for the result shape.
func (b *DeltaBuilder) UpsertedRows(ctx context.Context) (array.RecordReader, error) {
	return b.rows(ctx, 2, "upserted rows", "DeltaBuilder.UpsertedRows")
}

// Transactions lists the transactions committed within the delta's version
// range, oldest first.
func (b *DeltaBuilder) Transactions(ctx context.Context) ([]TransactionInfo, error) {
	return datasetOp(ctx, b.ds, "DeltaBuilder.Transactions", "delta transactions",
		func(ctx context.Context, ptr *C.LanceDataset) ([]TransactionInfo, error) {
			// marshalOptions is not used here: its error is already prefixed
			// "lance: marshal options:", which would double-wrap under
			// datasetOp's single "lance: delta transactions: %w".
			data, err := json.Marshal(&b.cfg)
			if err != nil {
				return nil, fmt.Errorf("marshal delta spec: %w", err)
			}
			cSpec, freeSpec := cString(string(data))
			defer freeSpec()

			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_delta_transactions(ptr, cSpec, &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var transactions []TransactionInfo
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &transactions); err != nil {
				return nil, fmt.Errorf("decode delta transactions: %w", err)
			}
			return transactions, nil
		})
}
