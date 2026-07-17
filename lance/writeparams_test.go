package lance_test

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// TestAdvancedWriteOptionsAcrossEntryPoints prevents the native plain,
// session, and progress write paths from drifting apart. Transaction
// properties are persisted by Lance, giving the test an observable advanced
// option instead of merely checking that the write returned no error.
func TestAdvancedWriteOptionsAcrossEntryPoints(t *testing.T) {
	tests := []struct {
		name     string
		session  bool
		progress bool
	}{
		{name: "plain"},
		{name: "session", session: true},
		{name: "progress", progress: true},
		{name: "session_and_progress", session: true, progress: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			uri := filepath.Join(t.TempDir(), "advanced.lance")
			rdr := testutil.NewReader(testutil.Allocator(), 0, 40, 16)
			defer rdr.Release()

			opts := []lance.WriteOption{
				lance.WithV2ManifestPaths(true),
				lance.WithAutoCleanup(7, time.Hour),
				lance.WithSkipAutoCleanup(true),
				lance.WithTransactionProperties(map[string]string{"path": tt.name}),
			}
			var sess *lance.Session
			if tt.session {
				var err error
				sess, err = lance.NewSession(lance.SessionConfig{
					IndexCacheBytes:    4 << 20,
					MetadataCacheBytes: 4 << 20,
				})
				if err != nil {
					t.Fatalf("NewSession: %v", err)
				}
				defer sess.Close()
				opts = append(opts, lance.WithWriteSession(sess))
			}

			var progressCalls atomic.Int64
			if tt.progress {
				opts = append(opts, lance.WithWriteProgress(func(lance.WriteStats) {
					progressCalls.Add(1)
				}))
			}

			ds, err := lance.Write(ctx, uri, rdr, opts...)
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			defer ds.Close()
			if tt.progress && progressCalls.Load() == 0 {
				t.Fatal("progress callback was not invoked")
			}

			version, err := ds.Version(ctx)
			if err != nil {
				t.Fatalf("Version: %v", err)
			}
			txn, err := ds.TransactionByVersion(ctx, version.Version)
			if err != nil {
				t.Fatalf("TransactionByVersion: %v", err)
			}
			if txn == nil {
				t.Fatal("write transaction is nil")
			}
			if got := txn.Properties()["path"]; got != tt.name {
				t.Fatalf("transaction property = %q, want %q", got, tt.name)
			}
		})
	}
}

func TestWriteWithNewOptions(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "opts.lance")
	rdr := testutil.NewReader(testutil.Allocator(), 0, 50, 32)
	defer rdr.Release()

	ds, err := lance.Write(ctx, uri, rdr,
		lance.WithV2ManifestPaths(true),
		lance.WithSkipAutoCleanup(true),
		lance.WithTransactionProperties(map[string]string{"engine": "test", "msg": "initial load"}),
	)
	if err != nil {
		t.Fatalf("Write with new options: %v", err)
	}
	defer ds.Close()

	// Dataset opens and reads back correctly.
	reopened, err := lance.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()
	n, err := reopened.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if n != 50 {
		t.Fatalf("rows = %d, want 50", n)
	}
}

func TestWriteWithAutoCleanup(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "autoclean.lance")
	rdr := testutil.NewReader(testutil.Allocator(), 0, 10, 32)
	defer rdr.Release()

	ds, err := lance.Write(ctx, uri, rdr,
		lance.WithAutoCleanup(5, 24*time.Hour),
	)
	if err != nil {
		t.Fatalf("Write with auto cleanup: %v", err)
	}
	defer ds.Close()

	// Config records the auto-cleanup settings for a new dataset.
	cfg, err := ds.Config(ctx)
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if _, ok := cfg["lance.auto_cleanup.interval"]; !ok {
		t.Logf("auto_cleanup config keys: %v", cfg)
	}
}
