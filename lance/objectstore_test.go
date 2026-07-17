package lance_test

// Object-store integration tests. They exercise the full dataset lifecycle
// (write, open, scan, index, search, mutate) against local emulators of the
// three major object stores:
//
//   - SeaweedFS S3 gateway (s3://)
//   - Azurite             (az://)
//   - fake-gcs-server     (gs://)
//
// The tests skip unless LANCE_GO_OBJECT_STORE_TESTS is set. With the gate
// set they FAIL (not skip) when an emulator is unreachable. Start the
// emulators with `make object-store-up` (or run `make test-object-store`).

import (
	"fmt"
	"testing"
	"time"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestObjectStoreS3(t *testing.T) { runObjectStoreSuite(t, testutil.S3Fixture) }

func TestObjectStoreAzure(t *testing.T) { runObjectStoreSuite(t, testutil.AzureFixture) }

func TestObjectStoreGCS(t *testing.T) { runObjectStoreSuite(t, testutil.GCSFixture) }

// runObjectStoreSuite drives one provider through the whole surface:
// create -> open -> count -> scan equality -> append -> scalar index ->
// filtered scan -> vector index -> nearest -> delete -> count.
func runObjectStoreSuite(t *testing.T, fixture func(path string) testutil.ObjectStoreFixture) {
	if !testutil.ObjectStoreEnabled() {
		t.Skipf("object-store integration tests disabled (set %s=1 and run `make object-store-up`)",
			testutil.ObjectStoreGateEnv)
	}
	// A unique path per run keeps reruns (-count>1, retries) independent.
	fix := fixture(fmt.Sprintf("run-%d/data.lance", time.Now().UnixNano()))
	if err := fix.CheckReachable(5 * time.Second); err != nil {
		t.Fatalf("%s emulator unreachable: %v (run `make object-store-up`)", fix.Provider, err)
	}
	ctx := t.Context()

	const (
		createRows  = 200
		appendCount = 56
		totalRows   = createRows + appendCount // 256
	)

	// Create.
	rdr := testutil.NewReader(testutil.Allocator(), 0, createRows, 64)
	created, err := lance.Write(ctx, fix.URI, rdr, lance.WithWriteStorageOptions(fix.StorageOptions))
	rdr.Release()
	if err != nil {
		t.Fatalf("Write(create) to %s: %v", fix.URI, err)
	}
	created.Close()

	// Open + count.
	ds, err := lance.Open(ctx, fix.URI, lance.WithStorageOptions(fix.StorageOptions))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer ds.Close()
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != createRows {
		t.Fatalf("CountRows = %d, want %d", count, createRows)
	}

	// Scan equality: every row round-trips through the object store.
	assertRows(t, scanAll(t, ds.Scan().ScanInOrder(true)), seq(0, createRows))

	// Append.
	rdr = testutil.NewReader(testutil.Allocator(), createRows, appendCount, 64)
	appended, err := lance.Write(ctx, fix.URI, rdr,
		lance.WithMode(lance.WriteModeAppend), lance.WithWriteStorageOptions(fix.StorageOptions))
	rdr.Release()
	if err != nil {
		t.Fatalf("Write(append): %v", err)
	}
	defer appended.Close()
	count, err = appended.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows after append: %v", err)
	}
	if count != totalRows {
		t.Fatalf("CountRows after append = %d, want %d", count, totalRows)
	}

	// BTree index + filtered scan.
	if err := appended.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex(BTree): %v", err)
	}
	assertRows(t, scanAll(t, appended.Scan().Filter(fmt.Sprintf("id >= %d", createRows)).ScanInOrder(true)),
		seq(createRows, appendCount))

	// Vector index + nearest.
	if err := appended.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: 2},
		lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex(IvfFlat): %v", err)
	}
	indices, err := appended.ListIndices(ctx)
	if err != nil {
		t.Fatalf("ListIndices: %v", err)
	}
	if len(indices) != 2 {
		t.Fatalf("ListIndices returned %d indices, want 2: %+v", len(indices), indices)
	}
	recs := scanAll(t, appended.Scan().Nearest("vec", vecOf(42), 5).Nprobes(2))
	ids := idsOf(t, recs)
	if len(ids) != 5 {
		t.Fatalf("Nearest returned %d rows, want 5", len(ids))
	}
	if ids[0] != 42 {
		t.Fatalf("nearest row to vec(42) is id %d, want 42 (ids: %v)", ids[0], ids)
	}
	if dists := distancesOf(t, recs); dists[0] != 0 {
		t.Fatalf("self-query distance = %v, want 0", dists[0])
	}

	// Delete + count.
	res, err := appended.Delete(ctx, "id >= 250")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if res.NumDeletedRows != totalRows-250 {
		t.Fatalf("Delete removed %d rows, want %d", res.NumDeletedRows, totalRows-250)
	}
	count, err = appended.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows after delete: %v", err)
	}
	if count != 250 {
		t.Fatalf("CountRows after delete = %d, want 250", count)
	}

	// The deletion is visible to a fresh open.
	reopened, err := lance.Open(ctx, fix.URI, lance.WithStorageOptions(fix.StorageOptions))
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer reopened.Close()
	count, err = reopened.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on reopened dataset: %v", err)
	}
	if count != 250 {
		t.Fatalf("reopened CountRows = %d, want 250", count)
	}
}
