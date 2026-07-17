package lance_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestSessionSharedMetadataCacheHits(t *testing.T) {
	ctx := t.Context()
	uri := writeVecDataset(t, 256)

	sess, err := lance.NewSession(lance.SessionConfig{
		IndexCacheBytes:    32 << 20,
		MetadataCacheBytes: 32 << 20,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	ds1, err := sess.Open(ctx, uri)
	if err != nil {
		t.Fatalf("session.Open #1: %v", err)
	}
	// A full scan reads column/file metadata into the shared metadata cache.
	_ = scanAll(t, ds1.Scan())
	s1, err := sess.Stats()
	if err != nil {
		t.Fatalf("Stats #1: %v", err)
	}
	ds1.Close()

	// Reopening the same dataset on the shared session serves that file
	// metadata from the metadata cache.
	ds2, err := sess.Open(ctx, uri)
	if err != nil {
		t.Fatalf("session.Open #2: %v", err)
	}
	_ = scanAll(t, ds2.Scan())
	s2, err := sess.Stats()
	if err != nil {
		t.Fatalf("Stats #2: %v", err)
	}
	ds2.Close()

	if s2.MetadataCache.Hits <= s1.MetadataCache.Hits {
		t.Fatalf("metadata cache hits did not grow across a shared-session reopen: %d -> %d",
			s1.MetadataCache.Hits, s2.MetadataCache.Hits)
	}
}

func TestSessionWriteAndClose(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "sw.lance")

	sess, err := lance.NewSession(lance.SessionConfig{IndexCacheBytes: 16 << 20, MetadataCacheBytes: 16 << 20})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	rdr := testutil.NewReader(testutil.Allocator(), 0, 128, 64)
	defer rdr.Release()
	ds, err := sess.Write(ctx, uri, rdr)
	if err != nil {
		t.Fatalf("session.Write: %v", err)
	}
	n, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if n != 128 {
		t.Fatalf("CountRows = %d, want 128", n)
	}
	ds.Close()

	// Close is idempotent. Stats after close errors cleanly.
	if err := sess.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := sess.Stats(); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Stats after close = %v, want ErrInvalidArgument", err)
	}
}
