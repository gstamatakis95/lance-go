package lance_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// TestScanStatsCallback pins the observed lance 8.0.0 behavior of
// WithScanStats: the report is delivered only by the streaming terminals
// (Reader, and therefore the All iterator) and by Batch. CountRows accepts
// the option but delivers no report, because upstream does not propagate
// the callback on that code path.
//
// If this test starts failing after a lance upgrade (e.g. CountRows starts
// reporting), the docs (docs/observability.md, docs/indexes.md) and the
// WithScanStats doc comment in lance/scanner.go should be updated to
// re-include the newly-supported terminal(s).
func TestScanStatsCallback(t *testing.T) {
	ctx := context.Background()
	uri := filepath.Join(t.TempDir(), "stats.lance")
	reader := testutil.NewReader(testutil.Allocator(), 0, 192, 64)
	ds, err := lance.Write(ctx, uri, reader)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	var reports atomic.Int64
	var mu sync.Mutex
	var last lance.ScanStats
	sc := ds.Scan().WithScanStats(func(s lance.ScanStats) {
		reports.Add(1)
		mu.Lock()
		last = s
		mu.Unlock()
	})

	// CountRows delivers no report in lance 8.0.0.
	if _, err := sc.CountRows(ctx); err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if got := reports.Load(); got != 0 {
		t.Fatalf("reports after CountRows = %d, want 0 (lance 8.0.0 does not propagate the callback on this path)", got)
	}

	// Streaming terminal: the report fires when the stream is exhausted.
	rdr, err := sc.Reader(ctx)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	rows := int64(0)
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	rdr.Release()
	if rows != 192 {
		t.Fatalf("rows = %d, want 192", rows)
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("reports after full drain = %d, want 1", got)
	}
	// The report must carry real execution data: a full-table scan performs
	// I/O and the payload includes the unstable debug counters map.
	mu.Lock()
	if last.BytesRead == 0 && last.IOPS == 0 && len(last.AllCounts) == 0 {
		mu.Unlock()
		t.Fatalf("last report carries no counters: %+v", last)
	}
	mu.Unlock()

	// Batch: the report fires before the call returns.
	rec, err := sc.Batch(ctx)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	rec.Release()
	if got := reports.Load(); got != 2 {
		t.Fatalf("reports after Batch = %d, want 2", got)
	}
}

// TestScanStatsConcurrentTerminals is a regression test for a data race:
// newNative used to write the plugin handle into the shared Scanner config,
// racing concurrent terminal calls (and cross-wiring their stats plugins).
// Uses Batch (which delivers a report per call, unlike CountRows in lance
// 8.0.0) so the report-count assertion is meaningful. Run under -race.
func TestScanStatsConcurrentTerminals(t *testing.T) {
	ctx := context.Background()
	uri := filepath.Join(t.TempDir(), "stats-race.lance")
	reader := testutil.NewReader(testutil.Allocator(), 0, 1000, 250)
	ds, err := lance.Write(ctx, uri, reader)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	var reports atomic.Int64
	sc := ds.Scan().Filter("id < 500").WithScanStats(func(lance.ScanStats) {
		reports.Add(1)
	})

	const goroutines, iters = 8, 5
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				rec, err := sc.Batch(ctx)
				if err != nil {
					t.Errorf("Batch: %v", err)
					return
				}
				n := rec.NumRows()
				rec.Release()
				if n != 500 {
					t.Errorf("rows = %d, want 500", n)
				}
			}
		}()
	}
	wg.Wait()
	if got := reports.Load(); got != goroutines*iters {
		t.Fatalf("reports = %d, want %d (each terminal call reports exactly once)", got, goroutines*iters)
	}
}
