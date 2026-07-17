package lance_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/gstamatakis95/lance-go/lance"
)

// threeVersions writes a dataset with three committed versions: v1 has rows
// 0..99, v2 adds 100..199, v3 adds 200..299. Returns the URI and a handle at
// the latest version.
func threeVersions(t *testing.T) (string, *lance.Dataset) {
	t.Helper()
	uri, _ := writeDataset(t, 100)
	appendRows(t, uri, 100, 100)
	ds := appendRows(t, uri, 200, 100)
	return uri, ds
}

func TestVersionsAndCheckout(t *testing.T) {
	ctx := t.Context()
	_, ds := threeVersions(t)

	versions, err := ds.Versions(ctx)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("Versions returned %d entries, want 3", len(versions))
	}
	for i, v := range versions {
		if v.Version != uint64(i+1) {
			t.Fatalf("versions[%d].Version = %d, want %d", i, v.Version, i+1)
		}
		if v.Timestamp.IsZero() {
			t.Errorf("versions[%d].Timestamp is zero", i)
		}
	}

	old, err := ds.Checkout(ctx, lance.VersionRef(1))
	if err != nil {
		t.Fatalf("Checkout(version 1): %v", err)
	}
	defer old.Close()

	count, err := old.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on checkout: %v", err)
	}
	if count != 100 {
		t.Fatalf("checkout handle CountRows = %d, want 100", count)
	}
	recs := scanAll(t, old.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(0, 100))

	// The original handle is unaffected and still at the latest version.
	count, err = ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on original: %v", err)
	}
	if count != 300 {
		t.Fatalf("original handle CountRows = %d, want 300", count)
	}

	if _, err := ds.Checkout(ctx, lance.Ref{}); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Checkout(zero Ref) error = %v, want ErrInvalidArgument", err)
	}

	_, err = ds.Checkout(ctx, lance.VersionRef(999))
	if !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("Checkout(missing version) error = %v, want errors.Is(_, ErrNotFound)", err)
	}
	const wantCheckoutHint = "version or tag does not exist; list with Versions or Tags().List"
	if !strings.Contains(err.Error(), wantCheckoutHint) {
		t.Fatalf("error = %q, want it to contain hint %q", err.Error(), wantCheckoutHint)
	}
}

func TestRestore(t *testing.T) {
	ctx := t.Context()
	_, ds := threeVersions(t)

	// Check out v1 on a new handle and restore it as the new latest (v4).
	old, err := ds.Checkout(ctx, lance.VersionRef(1))
	if err != nil {
		t.Fatalf("Checkout(version 1): %v", err)
	}
	defer old.Close()
	if err := old.Restore(ctx); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// The main handle picks the restored version up via CheckoutLatest.
	if err := ds.CheckoutLatest(ctx); err != nil {
		t.Fatalf("CheckoutLatest: %v", err)
	}
	info, err := ds.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if info.Version != 4 {
		t.Fatalf("version after restore = %d, want 4", info.Version)
	}
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 100 {
		t.Fatalf("CountRows after restore = %d, want 100 (v1 content)", count)
	}
	recs := scanAll(t, ds.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(0, 100))
}

func TestTags(t *testing.T) {
	ctx := t.Context()
	uri, ds := threeVersions(t)
	tags := ds.Tags()

	if err := tags.Create(ctx, "v1", lance.VersionRef(1)); err != nil {
		t.Fatalf("Tags.Create: %v", err)
	}

	list, err := tags.List(ctx)
	if err != nil {
		t.Fatalf("Tags.List: %v", err)
	}
	info, found := list["v1"]
	if !found {
		t.Fatalf("Tags.List = %v, want tag v1 present", list)
	}
	if info.Version != 1 {
		t.Fatalf("tag v1 points at version %d, want 1", info.Version)
	}

	version, err := tags.GetVersion(ctx, "v1")
	if err != nil {
		t.Fatalf("Tags.GetVersion: %v", err)
	}
	if version != 1 {
		t.Fatalf("Tags.GetVersion = %d, want 1", version)
	}

	// The existing open path resolves tags.
	tagged, err := lance.Open(ctx, uri, lance.WithTag("v1"))
	if err != nil {
		t.Fatalf("Open(WithTag): %v", err)
	}
	defer tagged.Close()
	count, err := tagged.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on tagged handle: %v", err)
	}
	if count != 100 {
		t.Fatalf("tagged handle CountRows = %d, want 100", count)
	}

	// Checkout by tag works too.
	byTag, err := ds.Checkout(ctx, lance.TagRef("v1"))
	if err != nil {
		t.Fatalf("Checkout(TagRef): %v", err)
	}
	defer byTag.Close()
	count, err = byTag.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on checkout-by-tag handle: %v", err)
	}
	if count != 100 {
		t.Fatalf("checkout-by-tag CountRows = %d, want 100", count)
	}

	// Update moves the tag.
	if err := tags.Update(ctx, "v1", lance.VersionRef(2)); err != nil {
		t.Fatalf("Tags.Update: %v", err)
	}
	version, err = tags.GetVersion(ctx, "v1")
	if err != nil {
		t.Fatalf("Tags.GetVersion after update: %v", err)
	}
	if version != 2 {
		t.Fatalf("Tags.GetVersion after update = %d, want 2", version)
	}
}

func TestTagsMissingHints(t *testing.T) {
	ctx := t.Context()
	_, ds := threeVersions(t)
	tags := ds.Tags()
	const wantHint = "tag does not exist"

	if _, err := tags.Get(ctx, "no-such-tag"); !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("Tags.Get(missing) error = %v, want errors.Is(_, ErrNotFound)", err)
	} else if !strings.Contains(err.Error(), wantHint) {
		t.Fatalf("error = %q, want it to contain hint %q", err.Error(), wantHint)
	}

	if _, err := tags.GetVersion(ctx, "no-such-tag"); !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("Tags.GetVersion(missing) error = %v, want errors.Is(_, ErrNotFound)", err)
	} else if !strings.Contains(err.Error(), wantHint) {
		t.Fatalf("error = %q, want it to contain hint %q", err.Error(), wantHint)
	}
}

func TestTagsBlockCleanup(t *testing.T) {
	ctx := t.Context()
	_, ds := threeVersions(t)
	tags := ds.Tags()

	if err := tags.Create(ctx, "keep", lance.VersionRef(1)); err != nil {
		t.Fatalf("Tags.Create: %v", err)
	}

	// A tagged old version blocks cleanup when errors are requested.
	_, err := ds.CleanupOldVersions(ctx, 0,
		lance.WithDeleteUnverified(true),
		lance.WithErrorIfTaggedOldVersions(true))
	if err == nil {
		t.Fatalf("CleanupOldVersions with tagged old version succeeded, want error")
	}

	// After deleting the tag, cleanup succeeds and removes old versions.
	if err := tags.Delete(ctx, "keep"); err != nil {
		t.Fatalf("Tags.Delete: %v", err)
	}
	stats, err := ds.CleanupOldVersions(ctx, 0,
		lance.WithDeleteUnverified(true),
		lance.WithErrorIfTaggedOldVersions(true))
	if err != nil {
		t.Fatalf("CleanupOldVersions after tag delete: %v", err)
	}
	if stats.OldVersions == 0 {
		t.Errorf("OldVersions = 0, want > 0")
	}

	recs := scanAll(t, ds.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(0, 300))
}
