package lance_test

import (
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestCreateAndListBranches(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)

	v1, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}

	branch, err := ds.CreateBranch(ctx, "dev", lance.VersionRef(v1))
	if err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	defer branch.Close()

	branches, err := ds.ListBranches(ctx)
	if err != nil {
		t.Fatalf("ListBranches: %v", err)
	}
	if _, ok := branches["dev"]; !ok {
		t.Fatalf("branch dev not listed: %v", branches)
	}
}

func TestCheckoutBranchAndWrite(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 20)

	branchDS, err := ds.CreateBranch(ctx, "dev", lance.Ref{})
	if err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	defer branchDS.Close()

	// Append to the branch (via the branch handle) and confirm the main
	// branch is unaffected.
	rdr := testutil.NewReader(testutil.Allocator(), 20, 10, 32)
	defer rdr.Release()
	if err := branchDS.Append(ctx, rdr); err != nil {
		t.Fatalf("Append(branch): %v", err)
	}

	branchCount, err := branchDS.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows(branch): %v", err)
	}
	if branchCount != 30 {
		t.Fatalf("branch rows = %d, want 30", branchCount)
	}

	// Re-open the branch via WithBranch and verify it reads the branch data.
	reopened, err := lance.Open(ctx, uri, lance.WithBranch("dev"))
	if err != nil {
		t.Fatalf("Open(WithBranch): %v", err)
	}
	defer reopened.Close()
	if n, err := reopened.CountRows(ctx, ""); err != nil || n != 30 {
		t.Fatalf("reopened branch rows = %d (err=%v), want 30", n, err)
	}

	// Main branch still has 20 rows.
	mainCount, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows(main): %v", err)
	}
	if mainCount != 20 {
		t.Fatalf("main rows = %d, want 20", mainCount)
	}
}

func TestDeleteBranch(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	branch, err := ds.CreateBranch(ctx, "tmp", lance.Ref{})
	if err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	branch.Close()

	if err := ds.DeleteBranch(ctx, "tmp", false); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}
	branches, err := ds.ListBranches(ctx)
	if err != nil {
		t.Fatalf("ListBranches: %v", err)
	}
	if _, ok := branches["tmp"]; ok {
		t.Fatalf("branch tmp still present after delete: %v", branches)
	}
}

func TestShallowClone(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 40)

	target := filepath.Join(t.TempDir(), "clone.lance")
	clone, err := ds.ShallowClone(ctx, target, lance.Ref{}, nil)
	if err != nil {
		t.Fatalf("ShallowClone: %v", err)
	}
	defer clone.Close()

	n, err := clone.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows(clone): %v", err)
	}
	if n != 40 {
		t.Fatalf("clone rows = %d, want 40", n)
	}

	// Data reads equal the source.
	rec, err := clone.TakeIndices(ctx, []uint64{0, 39}, "id")
	if err != nil {
		t.Fatalf("TakeIndices(clone): %v", err)
	}
	defer rec.Release()
	if got := batchIDs(t, rec); !equalInt64(got, []int64{0, 39}) {
		t.Fatalf("clone take ids = %v, want [0 39]", got)
	}
}

func TestDeepClone(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 25)

	target := filepath.Join(t.TempDir(), "deep.lance")
	clone, err := ds.DeepClone(ctx, target, lance.Ref{}, nil)
	if err != nil {
		t.Fatalf("DeepClone: %v", err)
	}
	defer clone.Close()

	n, err := clone.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows(clone): %v", err)
	}
	if n != 25 {
		t.Fatalf("deep clone rows = %d, want 25", n)
	}

	// Re-open independently to confirm files were copied.
	reopened, err := lance.Open(ctx, target)
	if err != nil {
		t.Fatalf("Open(deep clone): %v", err)
	}
	defer reopened.Close()
	if n, err := reopened.CountRows(ctx, ""); err != nil || n != 25 {
		t.Fatalf("reopened deep clone rows = %d (err=%v), want 25", n, err)
	}
}

func TestTagsListOrderedAndReplaceMetadata(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 10)
	v1, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v1: %v", err)
	}
	appendRows(t, uri, 10, 5)
	if err := ds.CheckoutLatest(ctx); err != nil {
		t.Fatalf("CheckoutLatest: %v", err)
	}
	v2, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion v2: %v", err)
	}

	tags := ds.Tags()
	if err := tags.Create(ctx, "old", lance.VersionRef(v1)); err != nil {
		t.Fatalf("Create tag old: %v", err)
	}
	if err := tags.Create(ctx, "new", lance.VersionRef(v2)); err != nil {
		t.Fatalf("Create tag new: %v", err)
	}

	ordered, err := tags.ListOrdered(ctx, false)
	if err != nil {
		t.Fatalf("ListOrdered: %v", err)
	}
	if len(ordered) != 2 {
		t.Fatalf("ordered tags = %d, want 2", len(ordered))
	}
	// Descending: newest version first.
	if ordered[0].Name != "new" || ordered[1].Name != "old" {
		t.Fatalf("order = [%s %s], want [new old]", ordered[0].Name, ordered[1].Name)
	}

	if err := tags.ReplaceMetadata(ctx, "new", map[string]string{"note": "release"}); err != nil {
		t.Fatalf("ReplaceMetadata: %v", err)
	}
	all, err := tags.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if all["new"].Metadata["note"] != "release" {
		t.Fatalf("tag metadata = %v, want note=release", all["new"].Metadata)
	}
}
