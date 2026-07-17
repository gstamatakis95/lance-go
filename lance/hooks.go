package lance

/*
#include <stdint.h>
#include <stdlib.h>
#include "lance_go.h"

// Reinterprets an integer address carried in a callback payload as a pointer.
// Done in C so the Go `vet` unsafeptr check is not tripped: these addresses
// point at native (Rust) Arrow C-Data structs, never at Go memory.
static void *lance_go_addr_to_ptr(uint64_t addr) { return (void *)(uintptr_t)addr; }
*/
import "C"

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// WriteStats reports cumulative progress of a write, delivered to the callback
// registered with WriteWithProgress / Session.WriteWithProgress after each
// batch is written.
type WriteStats struct {
	// BytesWritten is the cumulative number of data-file bytes written so far.
	BytesWritten uint64
	// RowsWritten is the cumulative number of rows written so far.
	RowsWritten uint64
	// FilesWritten is the cumulative number of data files written so far.
	FilesWritten uint32
}

// WithWriteProgress reports cumulative WriteStats to progress after each batch
// is written. It composes with the other Write options, including
// WithWriteSession.
// A nil progress is a no-op.
//
// progress runs synchronously on the write path and MUST NOT re-enter lance-go
// (opening/scanning a dataset, CountRows, etc.). A re-entrant call is rejected
// with ErrReentrantCall rather than crashing the process. Because progress is
// best-effort, that error is ignored and the write still completes.
func WithWriteProgress(progress func(WriteStats)) WriteOption {
	return func(cfg *writeConfig) { cfg.progress = progress }
}

// writeProgressAdapter bridges the native write-progress callback to a Go
// func. The payload is a 20-byte little-endian WriteStats (see
// rust/src/session.rs).
type writeProgressAdapter struct {
	fn func(WriteStats)
}

func (a writeProgressAdapter) Invoke(method int32, payload []byte) ([]byte, error) {
	if method == 0 && len(payload) >= 20 {
		a.fn(WriteStats{
			BytesWritten: binary.LittleEndian.Uint64(payload[0:8]),
			RowsWritten:  binary.LittleEndian.Uint64(payload[8:16]),
			FilesWritten: binary.LittleEndian.Uint32(payload[16:20]),
		})
	}
	return nil, nil
}

// UDFCheckpointStore persists intermediate AddColumnsUDF results so an
// interrupted run can resume without recomputing. It is optional (see
// WithUDFCheckpoint). Fragments cross the boundary as opaque JSON bytes.
// Treat them as opaque tokens keyed by fragment id. Batches passed to
// InsertBatch are only valid during the call. Retain or copy to keep them.
// Implementations must be safe for concurrent use.
//
// Store methods run synchronously inside the native add-columns operation and
// MUST NOT re-enter lance-go (opening/scanning a dataset, CountRows, etc.). A
// re-entrant call is rejected with ErrReentrantCall rather than crashing the
// process, and that error surfaces as a failure of the enclosing operation.
type UDFCheckpointStore interface {
	// GetBatch returns a previously checkpointed batch for (fragmentID,
	// batchIndex), if present.
	GetBatch(fragmentID uint32, batchIndex int) (batch arrow.RecordBatch, found bool, err error)
	// InsertBatch checkpoints batch for (fragmentID, batchIndex).
	InsertBatch(fragmentID uint32, batchIndex int, batch arrow.RecordBatch) error
	// GetFragment returns the opaque fragment JSON previously stored for
	// fragmentID, if present.
	GetFragment(fragmentID uint32) (fragmentJSON []byte, found bool, err error)
	// InsertFragment stores the opaque fragment JSON. The fragment id is
	// embedded in the JSON.
	InsertFragment(fragmentJSON []byte) error
}

// udfOptions collects AddColumnsUDF options.
type udfOptions struct {
	readColumns []string
	batchSize   uint32
	checkpoint  UDFCheckpointStore
}

// UDFOption configures AddColumnsUDF.
type UDFOption func(*udfOptions)

// WithUDFReadColumns restricts the input batches passed to the mapper to the
// named columns (default: all columns).
func WithUDFReadColumns(columns ...string) UDFOption {
	return func(o *udfOptions) { o.readColumns = columns }
}

// WithUDFBatchSize sets the number of rows per mapper batch (default: engine
// default).
func WithUDFBatchSize(n uint32) UDFOption {
	return func(o *udfOptions) { o.batchSize = n }
}

// WithUDFCheckpoint attaches a checkpoint store consulted before recomputing
// each batch/fragment.
func WithUDFCheckpoint(store UDFCheckpointStore) UDFOption {
	return func(o *udfOptions) { o.checkpoint = store }
}

// AddColumnsUDF adds new columns whose values are computed by fn, one input
// RecordBatch at a time, committing a new version. outSchema is the schema of
// the columns fn returns, and fn's output must match it row-for-row with the
// input batch.
//
// The result batch fn returns is exported across the Arrow C Data Interface,
// so its buffers must live outside the Go heap: build it with a C-backed
// allocator returned by Allocator, as required under
// GOEXPERIMENT=cgocheck2. The input batch fn receives is valid only during
// the call.
//
// fn runs synchronously inside the native add-columns operation and MUST NOT
// re-enter lance-go (for example, opening or scanning a dataset, or calling
// CountRows). A re-entrant call is rejected with ErrReentrantCall rather than
// crashing the process, and that error propagates out of AddColumnsUDF.
func (d *Dataset) AddColumnsUDF(ctx context.Context, outSchema *arrow.Schema, fn func(in arrow.RecordBatch) (arrow.RecordBatch, error), opts ...UDFOption) error {
	if fn == nil {
		return fmt.Errorf("lance: udf mapper must not be nil: %w", ErrInvalidArgument)
	}
	var o udfOptions
	for _, opt := range opts {
		opt(&o)
	}

	udfHandle := registerPlugin(&udfAdapter{fn: fn})
	defer releasePlugin(udfHandle)
	var ckptHandle uintptr
	if o.checkpoint != nil {
		ckptHandle = registerPlugin(&checkpointAdapter{store: o.checkpoint})
		defer releasePlugin(ckptHandle)
	}

	return datasetDo(ctx, d, "Dataset.AddColumnsUDF", "add columns udf",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			var cOpts *C.char
			if len(o.readColumns) > 0 {
				// marshalOptions (dataset.go) returns an already-prefixed
				// "lance: marshal options: %w" error, which would double-prefix
				// if returned from inside this fn, so marshal inline instead.
				data, err := json.Marshal(map[string]any{"read_columns": o.readColumns})
				if err != nil {
					return fmt.Errorf("marshal options: %w", err)
				}
				var freeOpts func()
				cOpts, freeOpts = cString(string(data))
				defer freeOpts()
			}

			// Export the output schema. The native side takes ownership.
			var cSchema C.struct_ArrowSchema
			cdata.ExportArrowSchema(outSchema, (*cdata.CArrowSchema)(unsafe.Pointer(&cSchema)))

			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_add_columns_udf(
					ptr,
					&cSchema,
					C.size_t(udfHandle),
					cOpts,
					C.uint32_t(o.batchSize),
					C.size_t(ckptHandle),
				)
			})
		})
}

// addrToPtr reinterprets the little-endian address in b[:8] as a pointer.
func addrToPtr(b []byte) unsafe.Pointer {
	return C.lance_go_addr_to_ptr(C.uint64_t(binary.LittleEndian.Uint64(b)))
}

// udfAdapter bridges the native BatchUDF mapper to a Go func. Payload: four
// little-endian u64 C-Data pointers (in_array, in_schema, out_array,
// out_schema). See rust/src/hooks.rs.
type udfAdapter struct {
	fn func(in arrow.RecordBatch) (arrow.RecordBatch, error)
}

func (a *udfAdapter) Invoke(method int32, payload []byte) ([]byte, error) {
	if method != 0 {
		return nil, fmt.Errorf("lance: unknown udf method %d", method)
	}
	if len(payload) < 32 {
		return nil, fmt.Errorf("lance: short udf payload (%d bytes)", len(payload))
	}
	inArr := (*cdata.CArrowArray)(addrToPtr(payload[0:8]))
	inSch := (*cdata.CArrowSchema)(addrToPtr(payload[8:16]))
	outArr := (*cdata.CArrowArray)(addrToPtr(payload[16:24]))
	outSch := (*cdata.CArrowSchema)(addrToPtr(payload[24:32]))

	rec, err := cdata.ImportCRecordBatch(inArr, inSch)
	if err != nil {
		return nil, fmt.Errorf("lance: import udf input: %w", err)
	}
	defer rec.Release()
	out, err := a.fn(rec)
	if err != nil {
		return nil, err
	}
	defer out.Release()
	cdata.ExportArrowRecordBatch(out, outArr, outSch)
	return nil, nil
}

// Method discriminators that must match rust/src/hooks.rs.
const (
	ckptGetBatch       int32 = 0
	ckptInsertBatch    int32 = 1
	ckptGetFragment    int32 = 2
	ckptInsertFragment int32 = 3
)

// checkpointAdapter bridges the native UDFCheckpointStore to a Go store.
type checkpointAdapter struct {
	store UDFCheckpointStore
}

func (a *checkpointAdapter) Invoke(method int32, payload []byte) ([]byte, error) {
	switch method {
	case ckptGetBatch:
		if len(payload) < 28 {
			return nil, fmt.Errorf("lance: short checkpoint get_batch payload")
		}
		frag := binary.LittleEndian.Uint32(payload[0:4])
		batchIdx := int(binary.LittleEndian.Uint64(payload[4:12]))
		outArr := (*cdata.CArrowArray)(addrToPtr(payload[12:20]))
		outSch := (*cdata.CArrowSchema)(addrToPtr(payload[20:28]))
		batch, found, err := a.store.GetBatch(frag, batchIdx)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errPluginMiss
		}
		defer batch.Release()
		cdata.ExportArrowRecordBatch(batch, outArr, outSch)
		return nil, nil
	case ckptInsertBatch:
		if len(payload) < 28 {
			return nil, fmt.Errorf("lance: short checkpoint insert_batch payload")
		}
		frag := binary.LittleEndian.Uint32(payload[0:4])
		batchIdx := int(binary.LittleEndian.Uint64(payload[4:12]))
		inArr := (*cdata.CArrowArray)(addrToPtr(payload[12:20]))
		inSch := (*cdata.CArrowSchema)(addrToPtr(payload[20:28]))
		rec, err := cdata.ImportCRecordBatch(inArr, inSch)
		if err != nil {
			return nil, fmt.Errorf("lance: import checkpoint batch: %w", err)
		}
		defer rec.Release()
		return nil, a.store.InsertBatch(frag, batchIdx, rec)
	case ckptGetFragment:
		if len(payload) < 4 {
			return nil, fmt.Errorf("lance: short checkpoint get_fragment payload")
		}
		frag := binary.LittleEndian.Uint32(payload[0:4])
		j, found, err := a.store.GetFragment(frag)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errPluginMiss
		}
		return j, nil
	case ckptInsertFragment:
		return nil, a.store.InsertFragment(payload)
	default:
		return nil, fmt.Errorf("lance: unknown checkpoint method %d", method)
	}
}
