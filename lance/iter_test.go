package lance

// Tests for the range-over-func iterators in iter.go. This file is package
// lance (not lance_test) so it can both reuse writeReaderTestDataset from
// reader_test.go and assert on ownedRecordReader's unexported release state,
// mirroring the leak-detection style already used there.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
)

// iterBatchIDs reads the "id" column of a record batch into a slice.
func iterBatchIDs(t *testing.T, rec arrow.RecordBatch) []int64 {
	t.Helper()
	idx := rec.Schema().FieldIndices("id")
	if len(idx) == 0 {
		t.Fatalf("record has no id column (schema=%v)", rec.Schema())
	}
	col := rec.Column(idx[0]).(*array.Int64)
	ids := make([]int64, col.Len())
	for i := range ids {
		ids[i] = col.Value(i)
	}
	return ids
}

// drainAll runs seq to completion, returning the ids observed (in order) and
// the final error, if any. It does not Retain any yielded batch, matching
// the "valid only for this step" contract.
func drainAll(t *testing.T, seq func(func(arrow.RecordBatch, error) bool)) (ids []int64, calls int, finalErr error) {
	t.Helper()
	for rec, err := range seq {
		calls++
		if err != nil {
			finalErr = err
			continue
		}
		ids = append(ids, iterBatchIDs(t, rec)...)
	}
	return ids, calls, finalErr
}

func TestScannerAllFullDrainMatchesReader(t *testing.T) {
	ds := writeReaderTestDataset(t) // 8 rows, ids 0..7.
	ctx := t.Context()

	ids, calls, err := drainAll(t, ds.Scan().BatchSize(2).All(ctx))
	if err != nil {
		t.Fatalf("All: unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("All: no batches yielded")
	}

	rdr, rerr := ds.Scan().BatchSize(2).Reader(ctx)
	if rerr != nil {
		t.Fatalf("Reader: %v", rerr)
	}
	defer rdr.Release()
	recs, cerr := testutil.Collect(rdr)
	if cerr != nil {
		t.Fatalf("collect: %v", cerr)
	}
	defer testutil.ReleaseAll(recs)
	var want []int64
	for _, r := range recs {
		want = append(want, iterBatchIDs(t, r)...)
	}

	if len(ids) != len(want) {
		t.Fatalf("All ids = %v, want %v", ids, want)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Fatalf("All ids = %v, want %v", ids, want)
		}
	}
}

func TestScannerAllEarlyBreakReleasesReader(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx := t.Context()

	var captured *ownedRecordReader
	open := func(ctx context.Context) (array.RecordReader, error) {
		r, err := ds.Scan().BatchSize(2).Reader(ctx)
		if err != nil {
			return nil, err
		}
		captured = r.(*ownedRecordReader)
		return r, nil
	}

	var calls int
	for rec, err := range batches(ctx, open) {
		calls++
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rec == nil {
			t.Fatal("nil batch on first yield")
		}
		break
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (break after first batch)", calls)
	}
	if captured == nil {
		t.Fatal("open was never called")
	}

	captured.mu.Lock()
	released := captured.released
	streamNil := captured.stream == nil
	captured.mu.Unlock()
	if !released || !streamNil {
		t.Fatalf("reader not released after early break: released=%v streamNil=%v", released, streamNil)
	}
}

func TestScannerAllOpenErrorYieldsOnce(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx := t.Context()

	// MaterializationStyle("bogus") fails scanner construction (an "open"
	// failure), never reaching the native stream.
	seq := ds.Scan().MaterializationStyle("bogus").All(ctx)

	var calls int
	var gotErr error
	var gotRec arrow.RecordBatch
	for rec, err := range seq {
		calls++
		gotRec, gotErr = rec, err
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want exactly 1", calls)
	}
	if gotRec != nil {
		t.Fatalf("rec = %v, want nil", gotRec)
	}
	if !errors.Is(gotErr, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", gotErr)
	}
}

func TestScannerAllContextCancelMidIteration(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var captured *ownedRecordReader
	open := func(ctx context.Context) (array.RecordReader, error) {
		r, err := ds.Scan().BatchSize(1).Reader(ctx)
		if err != nil {
			return nil, err
		}
		captured = r.(*ownedRecordReader)
		return r, nil
	}

	var calls int
	var errCalls int
	var lastErr error
	for rec, err := range batches(ctx, open) {
		calls++
		if err != nil {
			errCalls++
			lastErr = err
			if rec != nil {
				t.Fatalf("error yield rec = %v, want nil", rec)
			}
			continue
		}
		if calls == 1 {
			// One batch observed; cancel before the next Next() call.
			cancel()
		}
	}
	if errCalls != 1 {
		t.Fatalf("error yields = %d, want exactly 1", errCalls)
	}
	if !errors.Is(lastErr, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", lastErr)
	}
	if captured == nil {
		t.Fatal("open was never called")
	}
	captured.mu.Lock()
	released := captured.released
	captured.mu.Unlock()
	// Release is always deferred inside batches, so by the time the range
	// loop above has finished, the reader must be released regardless of the
	// error path taken.
	if !released {
		t.Fatal("reader not released after mid-stream cancellation")
	}
}

func TestSQLBuilderAllFullDrainMatchesReader(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx := t.Context()

	ids, calls, err := drainAll(t, ds.SQL("SELECT id FROM dataset ORDER BY id").All(ctx))
	if err != nil {
		t.Fatalf("All: unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("All: no batches yielded")
	}

	rdr, rerr := ds.SQL("SELECT id FROM dataset ORDER BY id").Reader(ctx)
	if rerr != nil {
		t.Fatalf("Reader: %v", rerr)
	}
	defer rdr.Release()
	recs, cerr := testutil.Collect(rdr)
	if cerr != nil {
		t.Fatalf("collect: %v", cerr)
	}
	defer testutil.ReleaseAll(recs)
	var want []int64
	for _, r := range recs {
		want = append(want, iterBatchIDs(t, r)...)
	}
	if len(ids) != len(want) {
		t.Fatalf("All ids = %v, want %v", ids, want)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Fatalf("All ids = %v, want %v", ids, want)
		}
	}
}

func TestFragmentScannerAllFullDrainMatchesReader(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx := t.Context()

	frag, err := ds.Fragment(ctx, 0)
	if err != nil {
		t.Fatalf("Fragment(0): %v", err)
	}
	defer frag.Close()

	ids, calls, aerr := drainAll(t, frag.Scan().BatchSize(2).All(ctx))
	if aerr != nil {
		t.Fatalf("All: unexpected error: %v", aerr)
	}
	if calls == 0 {
		t.Fatal("All: no batches yielded")
	}

	rdr, rerr := frag.Scan().BatchSize(2).Reader(ctx)
	if rerr != nil {
		t.Fatalf("Reader: %v", rerr)
	}
	defer rdr.Release()
	recs, cerr := testutil.Collect(rdr)
	if cerr != nil {
		t.Fatalf("collect: %v", cerr)
	}
	defer testutil.ReleaseAll(recs)
	var want []int64
	for _, r := range recs {
		want = append(want, iterBatchIDs(t, r)...)
	}
	if len(ids) != len(want) {
		t.Fatalf("All ids = %v, want %v", ids, want)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Fatalf("All ids = %v, want %v", ids, want)
		}
	}
}

func TestTakeScanAllHappyPath(t *testing.T) {
	ds := writeReaderTestDataset(t) // 8 rows, ids 0..7.
	ctx := t.Context()

	ranges := [][2]uint64{{0, 3}, {5, 8}}
	ids, calls, err := drainAll(t, ds.TakeScanAll(ctx, ranges, "id"))
	if err != nil {
		t.Fatalf("TakeScanAll: unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("TakeScanAll: no batches yielded")
	}

	want := []int64{0, 1, 2, 5, 6, 7}
	if len(ids) != len(want) {
		t.Fatalf("TakeScanAll ids = %v, want %v", ids, want)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Fatalf("TakeScanAll ids = %v, want %v", ids, want)
		}
	}
}

// writeIterDeltaDataset builds a dataset with stable row ids and two extra
// versions: v1 creates rows 0..7, v2 updates rows 0..2, v3 appends rows
// 8..11. It mirrors setupDeltaDataset in delta_test.go (package lance_test),
// reimplemented here since that helper is unexported in a different test
// package.
func writeIterDeltaDataset(t *testing.T) (ds *Dataset, v1, v3 uint64) {
	t.Helper()
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "iter_delta.lance")

	rdr := testutil.NewReader(testutil.Allocator(), 0, 8, 4)
	defer rdr.Release()
	ds, err := Write(ctx, uri, rdr, WithStableRowIDs(true))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { _ = ds.Close() })

	v1, err = ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v1: %v", err)
	}

	if _, err := ds.Update(ctx, UpdateSpec{Set: map[string]string{"score": "score + 100"}, Where: "id < 3"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	appendRdr := testutil.NewReader(testutil.Allocator(), 8, 4, 4)
	defer appendRdr.Release()
	appended, err := Write(ctx, uri, appendRdr, WithMode(WriteModeAppend))
	if err != nil {
		t.Fatalf("Write(append): %v", err)
	}
	t.Cleanup(func() { _ = appended.Close() })
	if err := ds.CheckoutLatest(ctx); err != nil {
		t.Fatalf("CheckoutLatest: %v", err)
	}
	v3, err = ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v3: %v", err)
	}
	return ds, v1, v3
}

func TestDeltaAllInsertedUpdatedUpsertedHappyPath(t *testing.T) {
	ctx := t.Context()
	ds, v1, v3 := writeIterDeltaDataset(t)

	insertedIDs, calls, err := drainAll(t, ds.Delta().FromVersion(v1).ToVersion(v3).AllInserted(ctx))
	if err != nil {
		t.Fatalf("AllInserted: unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("AllInserted: no batches yielded")
	}
	if len(insertedIDs) != 4 {
		t.Fatalf("inserted rows = %d, want 4", len(insertedIDs))
	}
	for _, id := range insertedIDs {
		if id < 8 {
			t.Fatalf("unexpected inserted id %d (want >= 8)", id)
		}
	}

	updatedIDs, calls, err := drainAll(t, ds.Delta().FromVersion(v1).ToVersion(v3).AllUpdated(ctx))
	if err != nil {
		t.Fatalf("AllUpdated: unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("AllUpdated: no batches yielded")
	}
	if len(updatedIDs) != 3 {
		t.Fatalf("updated rows = %d, want 3", len(updatedIDs))
	}
	for _, id := range updatedIDs {
		if id >= 3 {
			t.Fatalf("unexpected updated id %d (want < 3)", id)
		}
	}

	upsertedIDs, calls, err := drainAll(t, ds.Delta().FromVersion(v1).ToVersion(v3).AllUpserted(ctx))
	if err != nil {
		t.Fatalf("AllUpserted: unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("AllUpserted: no batches yielded")
	}
	if len(upsertedIDs) != 7 {
		t.Fatalf("upserted rows = %d, want 7 (3 updated + 4 inserted)", len(upsertedIDs))
	}
}

// TestBatchesFullDrainReleasesReader ensures the reader is released exactly
// once at normal exhaustion, matching the "always released" contract even
// when no error occurs.
func TestBatchesFullDrainReleasesReader(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx := t.Context()

	var captured *ownedRecordReader
	open := func(ctx context.Context) (array.RecordReader, error) {
		r, err := ds.Scan().BatchSize(2).Reader(ctx)
		if err != nil {
			return nil, err
		}
		captured = r.(*ownedRecordReader)
		return r, nil
	}

	_, calls, err := drainAll(t, batches(ctx, open))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls == 0 {
		t.Fatal("no batches yielded")
	}
	if captured == nil {
		t.Fatal("open was never called")
	}
	captured.mu.Lock()
	released := captured.released
	captured.mu.Unlock()
	if !released {
		t.Fatal("reader not released after full drain")
	}
}

// TestScannerAllRespectsDeadline is a lightweight extra check that a
// deadline-based cancellation (not just an explicit cancel) also surfaces
// through All exactly once.
func TestScannerAllRespectsDeadline(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	time.Sleep(30 * time.Millisecond)

	var calls int
	var lastErr error
	for rec, err := range ds.Scan().BatchSize(1).All(ctx) {
		calls++
		lastErr = err
		if rec != nil {
			t.Fatalf("rec = %v, want nil on a pre-expired deadline", rec)
		}
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want exactly 1", calls)
	}
	if !errors.Is(lastErr, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", lastErr)
	}
}
