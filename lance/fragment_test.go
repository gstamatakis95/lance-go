package lance_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestFragmentsListing(t *testing.T) {
	ctx := t.Context()
	// 96 rows, 32 per file -> 3 fragments (ids 0, 1, 2).
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	infos, err := ds.Fragments(ctx)
	if err != nil {
		t.Fatalf("Fragments: %v", err)
	}
	if len(infos) != 3 {
		t.Fatalf("got %d fragments, want 3", len(infos))
	}
	var total uint64
	for _, fi := range infos {
		if fi.NumRows != 32 {
			t.Errorf("fragment %d NumRows = %d, want 32", fi.ID, fi.NumRows)
		}
		if fi.PhysicalRows != 32 {
			t.Errorf("fragment %d PhysicalRows = %d, want 32", fi.ID, fi.PhysicalRows)
		}
		if fi.NumDeletions != 0 {
			t.Errorf("fragment %d NumDeletions = %d, want 0", fi.ID, fi.NumDeletions)
		}
		if fi.NumDataFiles == 0 || len(fi.DataFiles) == 0 {
			t.Errorf("fragment %d has no data files", fi.ID)
		}
		total += fi.NumRows
	}
	if total != 96 {
		t.Fatalf("sum of NumRows = %d, want 96", total)
	}
}

func TestFragmentsAfterDelete(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 64, lance.WithMaxRowsPerFile(32))
	if _, err := ds.Delete(ctx, "id < 10"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Reopen to observe the committed deletions.
	ds2, err := lance.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ds2.Close() })

	infos, err := ds2.Fragments(ctx)
	if err != nil {
		t.Fatalf("Fragments: %v", err)
	}
	var live, deletions uint64
	for _, fi := range infos {
		live += fi.NumRows
		deletions += fi.NumDeletions
	}
	if live != 54 {
		t.Fatalf("live rows = %d, want 54", live)
	}
	if deletions != 10 {
		t.Fatalf("deletions = %d, want 10", deletions)
	}
}

func TestFragmentCounts(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	frag, err := ds.Fragment(ctx, 1)
	if err != nil {
		t.Fatalf("Fragment(1): %v", err)
	}
	t.Cleanup(func() { frag.Close() })

	if n, err := frag.CountRows(ctx, ""); err != nil || n != 32 {
		t.Fatalf("CountRows = %d, %v (want 32)", n, err)
	}
	if n, err := frag.PhysicalRows(ctx); err != nil || n != 32 {
		t.Fatalf("PhysicalRows = %d, %v (want 32)", n, err)
	}
	if n, err := frag.CountDeletions(ctx); err != nil || n != 0 {
		t.Fatalf("CountDeletions = %d, %v (want 0)", n, err)
	}
	// Fragment 1 holds ids 32..63. Filter to a subrange.
	if n, err := frag.CountRows(ctx, "id >= 40 AND id < 50"); err != nil || n != 10 {
		t.Fatalf("CountRows(filter) = %d, %v (want 10)", n, err)
	}
}

func TestFragmentNotFound(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 32, lance.WithMaxRowsPerFile(32))
	if _, err := ds.Fragment(ctx, 99); !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("Fragment(99) err = %v, want ErrNotFound", err)
	}
}

func TestFragmentMetadata(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	frag, err := ds.Fragment(ctx, 2)
	if err != nil {
		t.Fatalf("Fragment(2): %v", err)
	}
	t.Cleanup(func() { frag.Close() })

	meta, err := frag.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if meta.ID != 2 {
		t.Fatalf("metadata ID = %d, want 2", meta.ID)
	}
	if len(meta.Files) == 0 || meta.Files[0].Path == "" {
		t.Fatalf("metadata has no data file path: %+v", meta)
	}
}

func TestFragmentScan(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	frag, err := ds.Fragment(ctx, 1)
	if err != nil {
		t.Fatalf("Fragment(1): %v", err)
	}
	t.Cleanup(func() { frag.Close() })

	rdr, err := frag.Scan().ScanInOrder(true).Reader(ctx)
	if err != nil {
		t.Fatalf("fragment Scan.Reader: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect fragment scan: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	var got []int64
	for _, r := range recs {
		got = append(got, batchIDs(t, r)...)
	}
	// Cross-check against a dataset scan restricted to the same fragment.
	want := scanFragmentIDs(t, ds, 1)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !equalInt64(got, want) {
		t.Fatalf("fragment scan ids = %v, want %v", got, want)
	}
	if len(got) != 32 {
		t.Fatalf("fragment scan returned %d rows, want 32", len(got))
	}
}

func TestFragmentScanColumnsFilter(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	frag, err := ds.Fragment(ctx, 2)
	if err != nil {
		t.Fatalf("Fragment(2): %v", err)
	}
	t.Cleanup(func() { frag.Close() })

	// Fragment 2 holds ids 64..95.
	rdr, err := frag.Scan().Columns("id").Filter("id >= 70").ScanInOrder(true).Reader(ctx)
	if err != nil {
		t.Fatalf("fragment Scan.Reader: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	var got []int64
	for _, r := range recs {
		if n := r.Schema().NumFields(); n != 1 {
			t.Fatalf("projection not honored: %d columns", n)
		}
		got = append(got, batchIDs(t, r)...)
	}
	if len(got) != 26 { // 70..95 inclusive
		t.Fatalf("got %d rows, want 26", len(got))
	}
}

func TestFragmentTake(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	frag, err := ds.Fragment(ctx, 1)
	if err != nil {
		t.Fatalf("Fragment(1): %v", err)
	}
	t.Cleanup(func() { frag.Close() })

	// In-fragment offsets 0, 5, 31 map to global ids 32, 37, 63.
	rec, err := frag.Take(ctx, []uint32{0, 5, 31}, "id")
	if err != nil {
		t.Fatalf("fragment Take: %v", err)
	}
	defer rec.Release()
	if got := batchIDs(t, rec); !equalInt64(got, []int64{32, 37, 63}) {
		t.Fatalf("fragment take ids = %v, want [32 37 63]", got)
	}
}

func TestFragmentScanAfterDatasetClose(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	frag, err := ds.Fragment(ctx, 0)
	if err != nil {
		t.Fatalf("Fragment(0): %v", err)
	}
	t.Cleanup(func() { frag.Close() })

	// Close the originating dataset. The fragment handle is self-contained.
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = uri

	if n, err := frag.CountRows(ctx, ""); err != nil || n != 32 {
		t.Fatalf("CountRows after ds close = %d, %v (want 32)", n, err)
	}
	rdr, err := frag.Scan().Reader(ctx)
	if err != nil {
		t.Fatalf("fragment scan after ds close: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	defer testutil.ReleaseAll(recs)
	if testutil.TotalRows(recs) != 32 {
		t.Fatalf("fragment scan after ds close returned %d rows, want 32", testutil.TotalRows(recs))
	}
}

// scanFragmentIDs returns the sorted ids in a fragment via a dataset scan
// restricted to that fragment (the B2 ScanFragments knob).
func scanFragmentIDs(t *testing.T, ds *lance.Dataset, fragID uint32) []int64 {
	t.Helper()
	recs := scanAll(t, ds.Scan().ScanFragments(fragID).ScanInOrder(true))
	var ids []int64
	for _, r := range recs {
		ids = append(ids, batchIDs(t, r)...)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
