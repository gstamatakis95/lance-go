package lance

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Records returns an array.RecordReader over the given batches, all of which
// must match schema. It is a thin, validated wrapper over
// array.NewRecordReader: schema must not be nil, and every batch's schema
// must equal schema exactly, or the offending batch's index is named in the
// returned error. Zero batches is allowed and yields a reader that is
// immediately exhausted (Next returns false, Err returns nil); passing that
// reader to Write succeeds and produces a valid, zero-row dataset.
//
// Per array.NewRecordReader's semantics (arrow-go v18), the returned reader
// takes its own reference on each batch (it calls Retain on every batch
// before returning). Callers keep their own reference to each batch and may
// Release it immediately after this call returns; the reader remains valid
// until it is itself Released, at which point it releases its own references
// in turn. This call does not change where the underlying buffers must live:
// data destined for lance.Write (or any other entry point that crosses the
// Arrow C Data Interface) must still be allocated with Allocator, not
// memory.DefaultAllocator, regardless of how it is wrapped here.
func Records(schema *arrow.Schema, batches ...arrow.RecordBatch) (array.RecordReader, error) {
	if schema == nil {
		return nil, fmt.Errorf("lance: records: schema must not be nil: %w", ErrInvalidArgument)
	}
	for i, b := range batches {
		if b == nil {
			return nil, fmt.Errorf("lance: records: batch %d: must not be nil: %w", i, ErrInvalidArgument)
		}
		if !b.Schema().Equal(schema) {
			return nil, fmt.Errorf("lance: records: batch %d: schema does not match: %w", i, ErrInvalidArgument)
		}
	}
	// The schema check above already rejects every case array.NewRecordReader
	// itself rejects, so this should be unreachable; keep it as a defensive
	// fallback that still wraps a sentinel rather than a bare error.
	rdr, err := array.NewRecordReader(schema, batches)
	if err != nil {
		return nil, fmt.Errorf("lance: records: %v: %w", err, ErrInternal)
	}
	return rdr, nil
}
