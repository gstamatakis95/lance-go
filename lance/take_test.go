package lance_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// batchIDs reads the "id" column of a record batch into a slice.
func batchIDs(t *testing.T, rec arrow.RecordBatch) []int64 {
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

func TestTakeIndices(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	rec, err := ds.TakeIndices(ctx, []uint64{0, 5, 42, 99}, "id", "name")
	if err != nil {
		t.Fatalf("TakeIndices: %v", err)
	}
	defer rec.Release()

	if got := batchIDs(t, rec); !equalInt64(got, []int64{0, 5, 42, 99}) {
		t.Fatalf("take ids = %v, want [0 5 42 99]", got)
	}
	// Projection honored: only id and name requested.
	if n := rec.Schema().NumFields(); n != 2 {
		t.Fatalf("take returned %d columns, want 2", n)
	}
}

func TestTakeIndicesMatchesScanFilter(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 50)

	// Take rows 10..19 by offset, then compare against a scan with the
	// equivalent predicate.
	indices := make([]uint64, 10)
	for i := range indices {
		indices[i] = uint64(10 + i)
	}
	rec, err := ds.TakeIndices(ctx, indices)
	if err != nil {
		t.Fatalf("TakeIndices: %v", err)
	}
	defer rec.Release()

	recs := scanAll(t, ds.Scan().Filter("id >= 10 AND id < 20"))
	var want []int64
	for _, r := range recs {
		want = append(want, batchIDs(t, r)...)
	}
	if got := batchIDs(t, rec); !equalInt64(got, want) {
		t.Fatalf("take ids = %v, want %v", got, want)
	}
}

func TestTakeRowsStableIDs(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 30, lance.WithStableRowIDs(true))

	// With stable row ids on a single fresh write, row ids equal offsets.
	rec, err := ds.TakeRows(ctx, []uint64{1, 2, 3}, "id")
	if err != nil {
		t.Fatalf("TakeRows: %v", err)
	}
	defer rec.Release()
	if got := batchIDs(t, rec); !equalInt64(got, []int64{1, 2, 3}) {
		t.Fatalf("take rows ids = %v, want [1 2 3]", got)
	}
}

func TestTakeEmptySelector(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)

	// An intentionally-empty selector means "fetch zero rows" and must return
	// an empty batch, not a spurious invalid-argument error (the empty slice
	// has to reach the native side as [] rather than being omitted).
	rec, err := ds.TakeIndices(ctx, []uint64{}, "id")
	if err != nil {
		t.Fatalf("TakeIndices(empty): %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 0 {
		t.Fatalf("TakeIndices(empty) returned %d rows, want 0", rec.NumRows())
	}

	rec2, err := ds.TakeRows(ctx, []uint64{}, "id")
	if err != nil {
		t.Fatalf("TakeRows(empty): %v", err)
	}
	defer rec2.Release()
	if rec2.NumRows() != 0 {
		t.Fatalf("TakeRows(empty) returned %d rows, want 0", rec2.NumRows())
	}
}

func TestTakeSQLProjection(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)

	rec, err := ds.Take(ctx, lance.TakeSpec{
		Indices:       []uint64{2, 4},
		SQLProjection: []lance.NamedExpr{{Name: "doubled", SQL: "id * 2"}},
	})
	if err != nil {
		t.Fatalf("Take(sql): %v", err)
	}
	defer rec.Release()

	idx := rec.Schema().FieldIndices("doubled")
	if len(idx) == 0 {
		t.Fatalf("no doubled column (schema=%v)", rec.Schema())
	}
	col := rec.Column(idx[0]).(*array.Int64)
	if col.Value(0) != 4 || col.Value(1) != 8 {
		t.Fatalf("doubled = [%d %d], want [4 8]", col.Value(0), col.Value(1))
	}
}

func TestTakeWithRowAddress(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10, lance.WithStableRowIDs(true))

	rec, err := ds.Take(ctx, lance.TakeSpec{
		RowIDs:         []uint64{0, 1},
		Columns:        []string{"id"},
		WithRowAddress: true,
	})
	if err != nil {
		t.Fatalf("Take(with row address): %v", err)
	}
	defer rec.Release()
	if len(rec.Schema().FieldIndices("_rowaddr")) == 0 {
		t.Fatalf("expected _rowaddr column (schema=%v)", rec.Schema())
	}
}

func TestTakeInvalidSelectors(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 5)

	// No selector at all.
	if _, err := ds.Take(ctx, lance.TakeSpec{Columns: []string{"id"}}); err == nil {
		t.Fatal("expected error with no selector")
	}
	// Two selectors.
	if _, err := ds.Take(ctx, lance.TakeSpec{Indices: []uint64{0}, RowIDs: []uint64{0}}); err == nil {
		t.Fatal("expected error with two selectors")
	}
	// with_row_address with indices is unsupported.
	if _, err := ds.Take(ctx, lance.TakeSpec{Indices: []uint64{0}, WithRowAddress: true}); err == nil {
		t.Fatal("expected error combining indices with WithRowAddress")
	}
}

func TestTakeScan(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	rdr, err := ds.TakeScan(ctx, [][2]uint64{{0, 5}, {90, 100}}, "id")
	if err != nil {
		t.Fatalf("TakeScan: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect take scan: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	var got []int64
	for _, r := range recs {
		got = append(got, batchIDs(t, r)...)
	}
	want := []int64{0, 1, 2, 3, 4, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}
	if !equalInt64(got, want) {
		t.Fatalf("take scan ids = %v, want %v", got, want)
	}
}

func TestSample(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	rec, err := ds.Sample(ctx, 10, "id")
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 10 {
		t.Fatalf("sample returned %d rows, want 10", rec.NumRows())
	}
	// Sampled ids must be valid (in range) and distinct.
	seen := map[int64]bool{}
	for _, id := range batchIDs(t, rec) {
		if id < 0 || id >= 100 {
			t.Fatalf("sampled id %d out of range", id)
		}
		if seen[id] {
			t.Fatalf("duplicate sampled id %d", id)
		}
		seen[id] = true
	}
}

func equalInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
