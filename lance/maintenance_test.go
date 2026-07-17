package lance_test

import (
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestCompactFiles(t *testing.T) {
	ctx := t.Context()
	uri, _ := writeDataset(t, 20)
	var ds *lance.Dataset
	for i := int64(1); i < 10; i++ {
		ds = appendRows(t, uri, i*20, 20)
	}

	metrics, err := ds.CompactFiles(ctx, lance.CompactionOptions{})
	if err != nil {
		t.Fatalf("CompactFiles: %v", err)
	}
	if metrics.FragmentsRemoved == 0 {
		t.Errorf("FragmentsRemoved = 0, want > 0")
	}
	if metrics.FragmentsAdded < 1 {
		t.Errorf("FragmentsAdded = %d, want >= 1", metrics.FragmentsAdded)
	}
	if metrics.FilesRemoved == 0 || metrics.FilesAdded == 0 {
		t.Errorf("FilesRemoved/FilesAdded = %d/%d, want both > 0", metrics.FilesRemoved, metrics.FilesAdded)
	}

	// Data must be intact after compaction (count + full spot-check).
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 200 {
		t.Fatalf("CountRows after compaction = %d, want 200", count)
	}
	recs := scanAll(t, ds.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(0, 200))
}

func TestCompactFilesWithOptions(t *testing.T) {
	ctx := t.Context()
	uri, _ := writeDataset(t, 10)
	var ds *lance.Dataset
	for i := int64(1); i < 5; i++ {
		ds = appendRows(t, uri, i*10, 10)
	}

	threshold := float32(0.5)
	metrics, err := ds.CompactFiles(ctx, lance.CompactionOptions{
		TargetRowsPerFragment:         1000,
		MaterializeDeletionsThreshold: &threshold,
	})
	if err != nil {
		t.Fatalf("CompactFiles: %v", err)
	}
	if metrics.FragmentsRemoved == 0 {
		t.Errorf("FragmentsRemoved = 0, want > 0")
	}
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 50 {
		t.Fatalf("CountRows after compaction = %d, want 50", count)
	}
}

func TestCleanupOldVersions(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "cleanup.lance")

	// Create the dataset, then overwrite it a few times so old versions
	// hold data files no longer referenced by the latest version.
	var ds *lance.Dataset
	for i := 0; i < 3; i++ {
		rdr := testutil.NewReader(testutil.Allocator(), int64(i*100), 50, 32)
		mode := lance.WriteModeCreate
		if i > 0 {
			mode = lance.WriteModeOverwrite
		}
		h, err := lance.Write(ctx, uri, rdr, lance.WithMode(mode))
		rdr.Release()
		if err != nil {
			t.Fatalf("Write #%d: %v", i, err)
		}
		t.Cleanup(func() { h.Close() })
		ds = h
	}

	stats, err := ds.CleanupOldVersions(ctx, 0, lance.WithDeleteUnverified(true))
	if err != nil {
		t.Fatalf("CleanupOldVersions: %v", err)
	}
	if stats.BytesRemoved == 0 {
		t.Errorf("BytesRemoved = 0, want > 0")
	}
	if stats.OldVersions == 0 {
		t.Errorf("OldVersions = 0, want > 0")
	}

	// Old versions are gone: checking one out must fail.
	if _, err := ds.Checkout(ctx, lance.VersionRef(1)); err == nil {
		t.Errorf("Checkout(version 1) after cleanup succeeded, want error")
	}

	// The latest version's data is intact.
	recs := scanAll(t, ds.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(200, 50))
}
