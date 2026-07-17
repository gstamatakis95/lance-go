package lance_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestWriteProgress(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "wp.lance")

	var mu sync.Mutex
	var last lance.WriteStats
	calls := 0
	progress := func(s lance.WriteStats) {
		mu.Lock()
		last = s
		calls++
		mu.Unlock()
	}

	rdr := testutil.NewReader(testutil.Allocator(), 0, 500, 100)
	defer rdr.Release()
	ds, err := lance.WriteWithProgress(ctx, uri, rdr, progress, lance.WithMaxRowsPerFile(200))
	if err != nil {
		t.Fatalf("WriteWithProgress: %v", err)
	}
	defer ds.Close()

	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("write progress callback was never invoked")
	}
	if last.RowsWritten == 0 {
		t.Fatalf("final RowsWritten = 0, want > 0 (calls=%d)", calls)
	}
	if last.BytesWritten == 0 {
		t.Fatalf("final BytesWritten = 0, want > 0")
	}
}

// TestWriteProgressOption verifies the composable WithWriteProgress option on
// the plain package-level Write invokes the callback.
func TestWriteProgressOption(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "wpo.lance")

	var mu sync.Mutex
	var last lance.WriteStats
	calls := 0
	progress := func(s lance.WriteStats) {
		mu.Lock()
		last = s
		calls++
		mu.Unlock()
	}

	rdr := testutil.NewReader(testutil.Allocator(), 0, 500, 100)
	defer rdr.Release()
	ds, err := lance.Write(ctx, uri, rdr, lance.WithWriteProgress(progress), lance.WithMaxRowsPerFile(200))
	if err != nil {
		t.Fatalf("Write with WithWriteProgress: %v", err)
	}
	defer ds.Close()

	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("write progress callback was never invoked")
	}
	if last.RowsWritten == 0 || last.BytesWritten == 0 {
		t.Fatalf("final stats not populated: %+v (calls=%d)", last, calls)
	}
}

// TestWriteProgressWithSession verifies WithWriteSession and WithWriteProgress
// combine on a single Write: the session's caches are attached AND progress is
// reported.
func TestWriteProgressWithSession(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "wps.lance")

	sess, err := lance.NewSession(lance.SessionConfig{IndexCacheBytes: 16 << 20, MetadataCacheBytes: 16 << 20})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	var mu sync.Mutex
	calls := 0
	progress := func(lance.WriteStats) {
		mu.Lock()
		calls++
		mu.Unlock()
	}

	rdr := testutil.NewReader(testutil.Allocator(), 0, 400, 64)
	defer rdr.Release()
	ds, err := lance.Write(ctx, uri, rdr,
		lance.WithWriteSession(sess), lance.WithWriteProgress(progress), lance.WithMaxRowsPerFile(150))
	if err != nil {
		t.Fatalf("Write with session + progress: %v", err)
	}
	defer ds.Close()

	n, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if n != 400 {
		t.Fatalf("CountRows = %d, want 400", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("progress callback not invoked on a session + progress write")
	}
}

// doubledSchema is the single-column output of the doubling UDF.
func doubledSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "doubled", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
}

// doublingUDF returns a mapper that computes doubled = 2*score, building the
// result with a C-backed allocator (required for the Arrow C export).
func doublingUDF(t *testing.T) func(arrow.RecordBatch) (arrow.RecordBatch, error) {
	out := doubledSchema()
	return func(in arrow.RecordBatch) (arrow.RecordBatch, error) {
		idx := in.Schema().FieldIndices("score")
		if len(idx) == 0 {
			return nil, errors.New("input batch missing score column")
		}
		score, ok := in.Column(idx[0]).(*array.Float64)
		if !ok {
			return nil, errors.New("score column is not float64")
		}
		b := array.NewFloat64Builder(testutil.Allocator())
		defer b.Release()
		for i := 0; i < int(in.NumRows()); i++ {
			b.Append(score.Value(i) * 2)
		}
		arr := b.NewArray()
		defer arr.Release()
		return array.NewRecordBatch(out, []arrow.Array{arr}, in.NumRows()), nil
	}
}

func TestAddColumnsUDF(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)

	if err := ds.AddColumnsUDF(ctx, doubledSchema(), doublingUDF(t),
		lance.WithUDFReadColumns("score")); err != nil {
		t.Fatalf("AddColumnsUDF: %v", err)
	}

	// Verify doubled == 2*score for every row.
	recs := scanAll(t, ds.Scan().Columns("score", "doubled"))
	var rows int
	for _, rec := range recs {
		si := rec.Schema().FieldIndices("score")
		di := rec.Schema().FieldIndices("doubled")
		if len(si) == 0 || len(di) == 0 {
			t.Fatalf("result schema missing columns: %v", rec.Schema())
		}
		score := rec.Column(si[0]).(*array.Float64)
		doubled := rec.Column(di[0]).(*array.Float64)
		for i := 0; i < int(rec.NumRows()); i++ {
			if got, want := doubled.Value(i), score.Value(i)*2; got != want {
				t.Fatalf("row %d: doubled = %v, want %v", rows+i, got, want)
			}
		}
		rows += int(rec.NumRows())
	}
	if rows != 256 {
		t.Fatalf("scanned %d rows, want 256", rows)
	}
}

// recordingCheckpoint counts the calls the UDF machinery makes to it.
type recordingCheckpoint struct {
	mu                      sync.Mutex
	getFragment, getBatch   int
	insertFragment, insertB int
}

func (c *recordingCheckpoint) GetBatch(uint32, int) (arrow.RecordBatch, bool, error) {
	c.mu.Lock()
	c.getBatch++
	c.mu.Unlock()
	return nil, false, nil
}

func (c *recordingCheckpoint) InsertBatch(uint32, int, arrow.RecordBatch) error {
	c.mu.Lock()
	c.insertB++
	c.mu.Unlock()
	return nil
}

func (c *recordingCheckpoint) GetFragment(uint32) ([]byte, bool, error) {
	c.mu.Lock()
	c.getFragment++
	c.mu.Unlock()
	return nil, false, nil
}

func (c *recordingCheckpoint) InsertFragment([]byte) error {
	c.mu.Lock()
	c.insertFragment++
	c.mu.Unlock()
	return nil
}

func TestAddColumnsUDFCheckpoint(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)

	ckpt := &recordingCheckpoint{}
	if err := ds.AddColumnsUDF(ctx, doubledSchema(), doublingUDF(t),
		lance.WithUDFReadColumns("score"),
		lance.WithUDFCheckpoint(ckpt)); err != nil {
		t.Fatalf("AddColumnsUDF with checkpoint: %v", err)
	}

	ckpt.mu.Lock()
	defer ckpt.mu.Unlock()
	t.Logf("checkpoint calls: getFragment=%d getBatch=%d insertBatch=%d insertFragment=%d",
		ckpt.getFragment, ckpt.getBatch, ckpt.insertB, ckpt.insertFragment)
	if ckpt.getFragment == 0 && ckpt.getBatch == 0 {
		t.Fatal("checkpoint store was never consulted")
	}
	if ckpt.insertB == 0 && ckpt.insertFragment == 0 {
		t.Fatal("checkpoint store received no inserts")
	}
}

// TestAddColumnsUDFReentrantCall verifies that a mapper which re-enters
// lance-go (here, ds.CountRows) fails with ErrReentrantCall instead of
// aborting the process: a nested call would drive the shared tokio runtime
// from within itself, which the public boundary rejects deterministically.
func TestAddColumnsUDFReentrantCall(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 64)

	var once sync.Once
	var innerErr error
	reentrant := func(arrow.RecordBatch) (arrow.RecordBatch, error) {
		// Re-enter lance-go from inside the mapper. This must fail cleanly.
		_, err := ds.CountRows(ctx, "")
		once.Do(func() { innerErr = err })
		if err != nil {
			return nil, err
		}
		return nil, errors.New("expected the re-entrant CountRows to fail")
	}

	err := ds.AddColumnsUDF(ctx, doubledSchema(), reentrant, lance.WithUDFReadColumns("score"))
	if err == nil {
		t.Fatal("expected AddColumnsUDF to fail when the mapper re-enters lance-go")
	}
	if innerErr == nil {
		t.Fatal("mapper never observed the re-entrant CountRows result")
	}
	if !errors.Is(innerErr, lance.ErrReentrantCall) {
		t.Fatalf("inner re-entrant error = %v, want ErrReentrantCall", innerErr)
	}

	// The process survived. A normal UDF still works afterwards.
	if err := ds.AddColumnsUDF(ctx, doubledSchema(), doublingUDF(t),
		lance.WithUDFReadColumns("score")); err != nil {
		t.Fatalf("UDF after a re-entrant attempt failed: %v", err)
	}
}

// TestWriteProgressReentrantCall verifies that a re-entrant write-progress
// callback observes ErrReentrantCall (instead of crashing the process) and,
// because progress reporting is best-effort, the write itself still succeeds.
func TestWriteProgressReentrantCall(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "wp_reentrant.lance")

	// A separate dataset the progress callback will try to re-enter.
	_, other := writeDataset(t, 32)

	var mu sync.Mutex
	var observed error
	progress := func(lance.WriteStats) {
		_, err := other.CountRows(ctx, "")
		mu.Lock()
		if err != nil && observed == nil {
			observed = err
		}
		mu.Unlock()
	}

	rdr := testutil.NewReader(testutil.Allocator(), 0, 500, 100)
	defer rdr.Release()
	ds, err := lance.WriteWithProgress(ctx, uri, rdr, progress, lance.WithMaxRowsPerFile(200))
	if err != nil {
		t.Fatalf("WriteWithProgress with a re-entrant callback should still succeed: %v", err)
	}
	defer ds.Close()

	mu.Lock()
	defer mu.Unlock()
	if observed == nil {
		t.Fatal("progress callback never observed a re-entrant call result")
	}
	if !errors.Is(observed, lance.ErrReentrantCall) {
		t.Fatalf("observed error = %v, want ErrReentrantCall", observed)
	}
}

func TestAddColumnsUDFPanicContained(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 64)

	panicky := func(arrow.RecordBatch) (arrow.RecordBatch, error) {
		panic("udf exploded")
	}
	err := ds.AddColumnsUDF(ctx, doubledSchema(), panicky, lance.WithUDFReadColumns("score"))
	if err == nil {
		t.Fatal("expected an error from a panicking UDF, got nil")
	}
	// The process survived. A normal UDF still works afterwards.
	if err := ds.AddColumnsUDF(ctx, doubledSchema(), doublingUDF(t),
		lance.WithUDFReadColumns("score")); err != nil {
		t.Fatalf("UDF after contained panic failed: %v", err)
	}
}
