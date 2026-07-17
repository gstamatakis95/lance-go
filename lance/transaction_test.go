package lance_test

import (
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// writeFragmentsSlice writes rows [startID, startID+rows) as uncommitted
// fragments at uri and returns the resulting Transaction.
func writeFragmentsSlice(t *testing.T, uri string, startID, rows int64, opts ...lance.WriteFragmentsOption) *lance.Transaction {
	t.Helper()
	rdr := testutil.NewReader(testutil.Allocator(), startID, rows, 25)
	defer rdr.Release()
	txn, err := lance.WriteFragments(t.Context(), uri, rdr, opts...)
	if err != nil {
		t.Fatalf("WriteFragments(%d..%d): %v", startID, startID+rows, err)
	}
	if txn == nil {
		t.Fatal("WriteFragments returned nil transaction")
	}
	return txn
}

// TestTwoWorkerDistributedWriteBatch simulates two workers appending disjoint
// data to an existing dataset, then a driver committing both transactions in a
// single batch commit.
func TestTwoWorkerDistributedWriteBatch(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "dist.lance")

	// Establish the dataset (schema + v1) with a small base write.
	base := testutil.NewReader(testutil.Allocator(), 0, 10, 10)
	defer base.Release()
	bds, err := lance.Write(ctx, uri, base)
	if err != nil {
		t.Fatalf("Write(base): %v", err)
	}
	t.Cleanup(func() { bds.Close() })

	// Two workers append disjoint slices as uncommitted Append transactions.
	txn1 := writeFragmentsSlice(t, uri, 10, 45, lance.WithFragmentsMode(lance.WriteModeAppend))
	txn2 := writeFragmentsSlice(t, uri, 55, 45, lance.WithFragmentsMode(lance.WriteModeAppend))
	if txn1.Operation().Type != "Append" {
		t.Fatalf("worker 1 op = %q, want Append", txn1.Operation().Type)
	}

	versions, err := lance.NewCommit(uri).ExecuteBatch(ctx, []*lance.Transaction{txn1, txn2})
	if err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("ExecuteBatch returned %d versions, want 1", len(versions))
	}

	ds, err := lance.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ds.Close() })

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 100 {
		t.Fatalf("distributed write has %d rows, want 100", count)
	}

	// Compare to a single-shot write of the union.
	singleURI := filepath.Join(t.TempDir(), "single.lance")
	single := testutil.NewReader(testutil.Allocator(), 0, 100, 25)
	defer single.Release()
	sds, err := lance.Write(ctx, singleURI, single)
	if err != nil {
		t.Fatalf("Write(union): %v", err)
	}
	t.Cleanup(func() { sds.Close() })
	sCount, err := sds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("single CountRows: %v", err)
	}
	if sCount != count {
		t.Fatalf("row count mismatch: distributed %d, single %d", count, sCount)
	}

	// Contents match: scan both ordered by id.
	distIDs := idsOf(t, scanAll(t, ds.Scan().OrderBy("id")))
	singleIDs := idsOf(t, scanAll(t, sds.Scan().OrderBy("id")))
	if len(distIDs) != len(singleIDs) {
		t.Fatalf("id count mismatch: %d vs %d", len(distIDs), len(singleIDs))
	}
	for i := range distIDs {
		if distIDs[i] != singleIDs[i] {
			t.Fatalf("id[%d] mismatch: distributed %d, single %d", i, distIDs[i], singleIDs[i])
		}
	}
}

// TestTwoWorkerSequentialCommit commits two worker transactions sequentially,
// yielding one dataset version per commit.
func TestTwoWorkerSequentialCommit(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "seq.lance")

	// Worker 1 creates the dataset (Overwrite transaction).
	txn1 := writeFragmentsSlice(t, uri, 0, 40)
	if txn1.Operation().Type != "Overwrite" {
		t.Fatalf("worker 1 op = %q, want Overwrite", txn1.Operation().Type)
	}
	ds1, err := lance.NewCommit(uri).Execute(ctx, txn1)
	if err != nil {
		t.Fatalf("Execute(txn1): %v", err)
	}
	t.Cleanup(func() { ds1.Close() })
	v1, err := ds1.Version(ctx)
	if err != nil {
		t.Fatalf("Version after txn1: %v", err)
	}

	// Worker 2 appends more disjoint data, then commits as a second version.
	txn2 := writeFragmentsSlice(t, uri, 40, 60, lance.WithFragmentsMode(lance.WriteModeAppend))
	ds2, err := lance.NewCommit(uri).Execute(ctx, txn2)
	if err != nil {
		t.Fatalf("Execute(txn2): %v", err)
	}
	t.Cleanup(func() { ds2.Close() })
	v2, err := ds2.Version(ctx)
	if err != nil {
		t.Fatalf("Version after txn2: %v", err)
	}
	if v2.Version <= v1.Version {
		t.Fatalf("second commit version %d not newer than first %d", v2.Version, v1.Version)
	}

	count, err := ds2.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 100 {
		t.Fatalf("sequential distributed write has %d rows, want 100", count)
	}
}

// TestReadTransaction verifies the committed transaction is readable and its
// operation surfaces as an Append.
func TestReadTransaction(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "rt.lance")

	// Base dataset, then an append whose transaction we read back.
	base := testutil.NewReader(testutil.Allocator(), 0, 10, 10)
	defer base.Release()
	bds, err := lance.Write(ctx, uri, base)
	if err != nil {
		t.Fatalf("Write(base): %v", err)
	}
	t.Cleanup(func() { bds.Close() })

	txn := writeFragmentsSlice(t, uri, 10, 30, lance.WithFragmentsMode(lance.WriteModeAppend))
	ds, err := lance.NewCommit(uri).Execute(ctx, txn)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	t.Cleanup(func() { ds.Close() })

	got, err := ds.ReadTransaction(ctx)
	if err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if got == nil {
		t.Fatal("ReadTransaction returned nil")
	}
	if got.Operation().Type != "Append" {
		t.Fatalf("read transaction op = %q, want Append", got.Operation().Type)
	}
	if len(got.Operation().FragmentIDs) == 0 {
		t.Fatal("Append operation has no fragment ids")
	}
	if len(got.Raw) == 0 {
		t.Fatal("transaction Raw view is empty")
	}
}

// TestTransactionsAndManifest checks the recent-transactions list and the
// manifest summary.
func TestTransactionsAndManifest(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "tm.lance")
	txn := writeFragmentsSlice(t, uri, 0, 20)
	ds, err := lance.NewCommit(uri).Execute(ctx, txn)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	t.Cleanup(func() { ds.Close() })

	txns, err := ds.Transactions(ctx, 5)
	if err != nil {
		t.Fatalf("Transactions: %v", err)
	}
	found := false
	for _, s := range txns {
		if s != nil && s.Operation().Type == "Overwrite" {
			found = true
		}
	}
	if !found {
		t.Fatal("recent transactions do not include the Overwrite")
	}

	manifest, err := ds.Manifest(ctx)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if manifest.Version == 0 {
		t.Fatalf("manifest version is 0")
	}
	if len(manifest.Fields) == 0 {
		t.Fatal("manifest has no fields")
	}
	hasID := false
	for _, f := range manifest.Fields {
		if f.Name == "id" {
			hasID = true
		}
	}
	if !hasID {
		t.Fatalf("manifest fields missing 'id': %+v", manifest.Fields)
	}

	// No detached manifests for a normal write.
	if _, err := ds.ListDetachedManifests(ctx); err != nil {
		t.Fatalf("ListDetachedManifests: %v", err)
	}
}

// TestCommitDetached commits an append as a detached version via
// CommitBuilder.Detached().Execute and verifies (a) the returned handle is
// checked out at the detached version (Version carries the detached bit,
// ManifestLocation points at the detached manifest), (b) full transaction
// fidelity survives (uuid and transaction properties are readable back via
// ReadTransaction), and (c) the main lineage is untouched while
// ListDetachedManifests reports the detached version.
func TestCommitDetached(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "detached.lance")

	// Create the dataset via a normal commit (new commits default to V2
	// manifest paths, which detached commits require).
	txn1 := writeFragmentsSlice(t, uri, 0, 20)
	ds, err := lance.NewCommit(uri).Execute(ctx, txn1)
	if err != nil {
		t.Fatalf("Execute(create): %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	mainV, err := ds.Version(ctx)
	if err != nil {
		t.Fatalf("Version(main): %v", err)
	}

	// An append carrying transaction properties, committed DETACHED.
	txn2 := writeFragmentsSlice(t, uri, 20, 30,
		lance.WithFragmentsMode(lance.WriteModeAppend),
		lance.WithFragmentsTransactionProperties(map[string]string{"engine": "lance-go-test"}))
	if got := txn2.Properties()["engine"]; got != "lance-go-test" {
		t.Fatalf("transaction properties not attached: %q", got)
	}

	dds, err := lance.NewCommit(uri).Detached().Execute(ctx, txn2)
	if err != nil {
		t.Fatalf("Detached().Execute: %v", err)
	}
	t.Cleanup(func() { dds.Close() })

	const detachedBit = uint64(1) << 63
	dv, err := dds.Version(ctx)
	if err != nil {
		t.Fatalf("Version(detached): %v", err)
	}
	if dv.Version&detachedBit == 0 {
		t.Fatalf("version %d does not carry the detached bit", dv.Version)
	}
	loc, err := dds.ManifestLocation(ctx)
	if err != nil {
		t.Fatalf("ManifestLocation(detached): %v", err)
	}
	if loc.Version != dv.Version || loc.Path == "" {
		t.Fatalf("manifest location %+v does not match detached version %d", loc, dv.Version)
	}

	// The detached handle sees the appended rows.
	count, err := dds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows(detached): %v", err)
	}
	if count != 50 {
		t.Fatalf("detached version has %d rows, want 50", count)
	}

	// Transaction fidelity: the detached transaction reads back with its
	// uuid and properties intact.
	got, err := dds.ReadTransaction(ctx)
	if err != nil {
		t.Fatalf("ReadTransaction(detached): %v", err)
	}
	if got == nil {
		t.Fatal("ReadTransaction(detached) returned nil")
	}
	if got.UUID() != txn2.UUID() {
		t.Fatalf("detached transaction uuid = %q, want %q", got.UUID(), txn2.UUID())
	}
	if p := got.Properties()["engine"]; p != "lance-go-test" {
		t.Fatalf("detached transaction properties = %v, want engine=lance-go-test", got.Properties())
	}
	if got.Operation().Type != "Append" {
		t.Fatalf("detached transaction op = %q, want Append", got.Operation().Type)
	}

	// The main lineage is untouched...
	mds, err := lance.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open(main): %v", err)
	}
	t.Cleanup(func() { mds.Close() })
	mv, err := mds.Version(ctx)
	if err != nil {
		t.Fatalf("Version(main after detached): %v", err)
	}
	if mv.Version != mainV.Version {
		t.Fatalf("main lineage moved from %d to %d after detached commit", mainV.Version, mv.Version)
	}
	mcount, err := mds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows(main): %v", err)
	}
	if mcount != 20 {
		t.Fatalf("main version has %d rows, want 20", mcount)
	}

	// ...but the detached manifest is discoverable.
	list, err := mds.ListDetachedManifests(ctx)
	if err != nil {
		t.Fatalf("ListDetachedManifests: %v", err)
	}
	found := false
	for _, m := range list {
		if m.Version == dv.Version {
			found = true
			if m.Path == "" {
				t.Fatal("detached manifest has empty path")
			}
		}
	}
	if !found {
		t.Fatalf("detached version %d not in ListDetachedManifests result %+v", dv.Version, list)
	}
}

// TestCommitOperationUpdateConfig builds a config-update operation from JSON,
// commits it, and verifies it applied.
func TestCommitOperationUpdateConfig(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 32)

	v, err := ds.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}

	op := lance.UpdateConfigOperation{
		ConfigUpserts: map[string]string{"greeting": "hello", "team": "lance"},
	}
	nds, err := lance.CommitOperation(ctx, uri, op, v.Version)
	if err != nil {
		t.Fatalf("CommitOperation(UpdateConfig): %v", err)
	}
	t.Cleanup(func() { nds.Close() })

	cfg, err := nds.Config(ctx)
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg["greeting"] != "hello" || cfg["team"] != "lance" {
		t.Fatalf("config not applied: %+v", cfg)
	}
}
