package lance_test

import (
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// setupDeltaDataset builds a dataset with three versions using stable row
// ids: v1 creates rows 0..9, v2 updates rows 0..4, v3 appends rows 10..14.
// It returns the open handle plus the version numbers.
func setupDeltaDataset(t *testing.T) (*lance.Dataset, uint64, uint64, uint64) {
	t.Helper()
	ctx := t.Context()
	uri, ds := writeDataset(t, 10, lance.WithStableRowIDs(true))
	v1, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v1: %v", err)
	}

	if _, err := ds.Update(ctx, lance.UpdateSpec{Set: map[string]string{"score": "score + 100"}, Where: "id < 5"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	v2, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v2: %v", err)
	}

	rdr := testutil.NewReader(testutil.Allocator(), 10, 5, 32)
	defer rdr.Release()
	appended, err := lance.Write(ctx, uri, rdr, lance.WithMode(lance.WriteModeAppend))
	if err != nil {
		t.Fatalf("Write(append): %v", err)
	}
	t.Cleanup(func() { appended.Close() })
	if err := ds.CheckoutLatest(ctx); err != nil {
		t.Fatalf("CheckoutLatest: %v", err)
	}
	v3, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v3: %v", err)
	}
	return ds, v1, v2, v3
}

func TestDeltaInsertedRows(t *testing.T) {
	ctx := t.Context()
	ds, v1, _, v3 := setupDeltaDataset(t)

	// Rows inserted between v1 and v3: the 5 appended rows (10..14).
	rdr, err := ds.Delta().FromVersion(v1).ToVersion(v3).InsertedRows(ctx)
	if err != nil {
		t.Fatalf("InsertedRows: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect inserted: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	got := testutil.TotalRows(recs)
	if got != 5 {
		t.Fatalf("inserted rows = %d, want 5", got)
	}
	for _, r := range recs {
		for _, id := range batchIDs(t, r) {
			if id < 10 {
				t.Fatalf("unexpected inserted id %d (want >= 10)", id)
			}
		}
	}
}

func TestDeltaUpdatedRows(t *testing.T) {
	ctx := t.Context()
	ds, v1, _, v3 := setupDeltaDataset(t)

	// Rows updated between v1 and v3: the 5 updated rows (0..4).
	rdr, err := ds.Delta().FromVersion(v1).ToVersion(v3).UpdatedRows(ctx)
	if err != nil {
		t.Fatalf("UpdatedRows: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect updated: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	got := testutil.TotalRows(recs)
	if got != 5 {
		t.Fatalf("updated rows = %d, want 5", got)
	}
	for _, r := range recs {
		for _, id := range batchIDs(t, r) {
			if id >= 5 {
				t.Fatalf("unexpected updated id %d (want < 5)", id)
			}
		}
	}
}

func TestDeltaUpsertedRows(t *testing.T) {
	ctx := t.Context()
	ds, v1, _, v3 := setupDeltaDataset(t)

	rdr, err := ds.Delta().FromVersion(v1).ToVersion(v3).UpsertedRows(ctx)
	if err != nil {
		t.Fatalf("UpsertedRows: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect upserted: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	// 5 updated + 5 inserted = 10.
	got := testutil.TotalRows(recs)
	if got != 10 {
		t.Fatalf("upserted rows = %d, want 10", got)
	}
}

func TestDeltaTransactions(t *testing.T) {
	ctx := t.Context()
	ds, v1, _, v3 := setupDeltaDataset(t)

	txns, err := ds.Delta().FromVersion(v1).ToVersion(v3).Transactions(ctx)
	if err != nil {
		t.Fatalf("Transactions: %v", err)
	}
	// Between v1 (exclusive) and v3 (inclusive): the update and the append.
	if len(txns) != 2 {
		t.Fatalf("transactions = %d, want 2", len(txns))
	}
	for _, tx := range txns {
		if tx.UUID == "" {
			t.Fatalf("transaction has empty uuid: %+v", tx)
		}
		if tx.Operation == "" {
			t.Fatalf("transaction has empty operation: %+v", tx)
		}
	}
}

func TestDeltaComparedAgainstVersion(t *testing.T) {
	ctx := t.Context()
	ds, v1, _, _ := setupDeltaDataset(t)

	// Compare current (v3) against v1: 5 inserted rows.
	rdr, err := ds.Delta().ComparedAgainstVersion(v1).InsertedRows(ctx)
	if err != nil {
		t.Fatalf("InsertedRows: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect inserted: %v", err)
	}
	defer testutil.ReleaseAll(recs)
	if got := testutil.TotalRows(recs); got != 5 {
		t.Fatalf("inserted rows = %d, want 5", got)
	}
}
