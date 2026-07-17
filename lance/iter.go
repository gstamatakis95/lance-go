package lance

import (
	"context"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// batches adapts a reader-opening function into an iter.Seq2 over record
// batches, as a thin wrapper over the array.RecordReader contract used
// throughout this package (see ownedRecordReader in reader.go).
//
// Release semantics: open is called exactly once, when the returned sequence
// starts running. If it fails, (nil, err) is yielded exactly once and the
// sequence ends; there is nothing to release. Otherwise the resulting reader
// is released exactly once, unconditionally, when the sequence stops for any
// reason (normal exhaustion, the yield func returning false on an early
// break, or a mid-stream error) via a deferred call inside the sequence
// closure.
//
// Each yielded batch is reader.RecordBatch(): the same batch
// ownedRecordReader.Next would hand back from a manual Next/RecordBatch loop.
// It is valid only for the duration of that iteration step - the reader
// releases it on the next Next call (advancing to the following batch) or
// when the reader itself is released at sequence end. The iterator never
// calls Retain or Release on a yielded batch itself, so callers who need a
// batch to outlive its step must call Retain on it before returning to the
// loop (and Release it themselves later); this matches the retain-to-keep
// contract documented on every Reader-returning terminal (e.g. Scanner.Reader).
//
// A mid-stream error, including a context cancellation surfaced by the
// reader (ownedRecordReader.Next observes ctx itself), is yielded exactly
// once as (nil, err) after the loop that produced batches stops, and no
// further values are yielded.
func batches(ctx context.Context, open func(context.Context) (array.RecordReader, error)) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		reader, err := open(ctx)
		if err != nil {
			yield(nil, err)
			return
		}
		defer reader.Release()

		for reader.Next() {
			if !yield(reader.RecordBatch(), nil) {
				return
			}
		}
		if err := reader.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// All executes the scan and returns an iterator over the result batches, as
// a range-over-func alternative to Reader.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the scan, or a mid-stream error (including context cancellation),
// is yielded exactly once as (nil, err), after which the sequence ends.
func (s *Scanner) All(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, s.Reader)
}

// All executes the SQL query and returns an iterator over the result
// batches, as a range-over-func alternative to Reader.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the query, or a mid-stream error (including context cancellation),
// is yielded exactly once as (nil, err), after which the sequence ends.
func (b *SQLBuilder) All(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, b.Reader)
}

// All executes the fragment scan and returns an iterator over the result
// batches, as a range-over-func alternative to Reader.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the scan, or a mid-stream error (including context cancellation),
// is yielded exactly once as (nil, err), after which the sequence ends.
func (s *FragmentScanner) All(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, s.Reader)
}

// AllInserted streams the rows created within the delta's version range as
// an iterator, a range-over-func alternative to InsertedRows. See
// InsertedRows for the result shape.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the query, or a mid-stream error (including context cancellation),
// is yielded exactly once as (nil, err), after which the sequence ends.
func (b *DeltaBuilder) AllInserted(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, b.InsertedRows)
}

// AllUpdated streams the rows updated within the delta's version range as an
// iterator, a range-over-func alternative to UpdatedRows. See InsertedRows
// for the result shape.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the query, or a mid-stream error (including context cancellation),
// is yielded exactly once as (nil, err), after which the sequence ends.
func (b *DeltaBuilder) AllUpdated(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, b.UpdatedRows)
}

// AllUpserted streams both the inserted and the updated rows of the delta's
// version range as an iterator, a range-over-func alternative to
// UpsertedRows. See InsertedRows for the result shape.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the query, or a mid-stream error (including context cancellation),
// is yielded exactly once as (nil, err), after which the sequence ends.
func (b *DeltaBuilder) AllUpserted(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, b.UpsertedRows)
}

// TakeScanAll streams the rows in the given half-open [start, end) ranges of
// row offsets, projected onto columns (all columns when empty), as an
// iterator - a range-over-func alternative to TakeScan.
//
// Each yielded batch is valid only for that iteration step: the underlying
// reader releases it as soon as the loop body returns (advancing to the next
// batch) or the sequence ends. Call Retain on a batch to keep it past its
// step, and Release it yourself when done. The underlying reader is always
// released when iteration ends, whether by exhaustion, an early break, or an
// error - callers must not and need not call Release themselves. A failure
// opening the take scan, or a mid-stream error (including context
// cancellation), is yielded exactly once as (nil, err), after which the
// sequence ends.
func (d *Dataset) TakeScanAll(ctx context.Context, ranges [][2]uint64, columns ...string) iter.Seq2[arrow.RecordBatch, error] {
	return batches(ctx, func(ctx context.Context) (array.RecordReader, error) {
		return d.TakeScan(ctx, ranges, columns...)
	})
}
