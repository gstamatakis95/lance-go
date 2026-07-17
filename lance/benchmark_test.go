package lance_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// benchRows is the size of the fixture dataset used by the scan/take
// benchmarks (BenchmarkWrite additionally exercises the write path itself at
// the same scale). It is large enough to be realistic while still running
// quickly under the default -benchtime.
const benchRows = 100_000

// benchBatchRows is the batch size used when generating fixture data: large
// enough to keep per-batch overhead low, small enough to exercise multiple
// record batches per write/scan.
const benchBatchRows = 10_000

// buildBenchDataset writes a benchRows-row dataset to a fresh temp dir and
// returns an open handle to it. The caller must not call b.ResetTimer before
// this returns; buildBenchDataset is meant to run entirely inside benchmark
// setup, before the timed loop starts.
func buildBenchDataset(b *testing.B, ctx context.Context) *lance.Dataset {
	b.Helper()
	mem := testutil.Allocator()
	rdr := testutil.NewReader(mem, 0, benchRows, benchBatchRows)
	uri := filepath.Join(b.TempDir(), "bench.lance")
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		b.Fatalf("Write (fixture): %v", err)
	}
	b.Cleanup(func() { ds.Close() })
	return ds
}

// BenchmarkWrite measures writing a benchRows-row dataset to a fresh
// destination on the local filesystem. Fixture generation (record building)
// is excluded from the timed portion; only the FFI write call is measured.
func BenchmarkWrite(b *testing.B) {
	ctx := context.Background()
	mem := testutil.Allocator()
	dir := b.TempDir()

	b.ResetTimer()
	var written uint64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rdr := testutil.NewReader(mem, 0, benchRows, benchBatchRows)
		uri := filepath.Join(dir, fmt.Sprintf("write-%d.lance", i))
		b.StartTimer()

		ds, err := lance.Write(ctx, uri, rdr)

		b.StopTimer()
		rdr.Release()
		if err != nil {
			b.Fatalf("Write: %v", err)
		}
		ds.Close()
		written += benchRows
		b.StartTimer()
	}
	b.StopTimer()
	if b.Elapsed().Seconds() > 0 {
		b.ReportMetric(float64(written)/b.Elapsed().Seconds(), "rows/sec")
	}
}

// BenchmarkScanAll measures a full, in-order scan of every row and column in
// a pre-built benchRows-row dataset.
func BenchmarkScanAll(b *testing.B) {
	ctx := context.Background()
	ds := buildBenchDataset(b, ctx)

	b.ResetTimer()
	var scanned uint64
	for i := 0; i < b.N; i++ {
		rdr, err := ds.Scan().ScanInOrder(true).Reader(ctx)
		if err != nil {
			b.Fatalf("Scan.Reader: %v", err)
		}
		var rows uint64
		for rdr.Next() {
			rec := rdr.RecordBatch()
			rows += uint64(rec.NumRows())
		}
		if err := rdr.Err(); err != nil {
			b.Fatalf("scan: %v", err)
		}
		rdr.Release()
		scanned += rows
	}
	b.StopTimer()
	if scanned != uint64(b.N)*benchRows {
		b.Fatalf("scanned %d rows over %d iterations, want %d", scanned, b.N, uint64(b.N)*benchRows)
	}
	if b.Elapsed().Seconds() > 0 {
		b.ReportMetric(float64(scanned)/b.Elapsed().Seconds(), "rows/sec")
	}
}

// BenchmarkScanFiltered measures a filtered scan selecting roughly 1% of the
// rows in a pre-built benchRows-row dataset ("id < benchRows/100").
func BenchmarkScanFiltered(b *testing.B) {
	ctx := context.Background()
	ds := buildBenchDataset(b, ctx)
	filter := fmt.Sprintf("id < %d", benchRows/100)

	b.ResetTimer()
	var scanned uint64
	for i := 0; i < b.N; i++ {
		rdr, err := ds.Scan().Filter(filter).ScanInOrder(true).Reader(ctx)
		if err != nil {
			b.Fatalf("Scan.Reader: %v", err)
		}
		var rows uint64
		for rdr.Next() {
			rec := rdr.RecordBatch()
			rows += uint64(rec.NumRows())
		}
		if err := rdr.Err(); err != nil {
			b.Fatalf("scan: %v", err)
		}
		rdr.Release()
		scanned += rows
	}
	b.StopTimer()
	if scanned != uint64(b.N)*(benchRows/100) {
		b.Fatalf("scanned %d rows over %d iterations, want %d", scanned, b.N, uint64(b.N)*(benchRows/100))
	}
	if b.Elapsed().Seconds() > 0 {
		b.ReportMetric(float64(scanned)/b.Elapsed().Seconds(), "rows/sec")
	}
}

// BenchmarkTake measures a point read of 100 random row offsets from a
// pre-built benchRows-row dataset.
func BenchmarkTake(b *testing.B) {
	ctx := context.Background()
	ds := buildBenchDataset(b, ctx)

	const takeN = 100
	rng := rand.New(rand.NewSource(1))
	indices := make([]uint64, takeN)
	for i := range indices {
		indices[i] = uint64(rng.Int63n(benchRows))
	}

	b.ResetTimer()
	var taken uint64
	for i := 0; i < b.N; i++ {
		rec, err := ds.TakeIndices(ctx, indices)
		if err != nil {
			b.Fatalf("TakeIndices: %v", err)
		}
		taken += uint64(rec.NumRows())
		rec.Release()
	}
	b.StopTimer()
	if b.Elapsed().Seconds() > 0 {
		b.ReportMetric(float64(taken)/b.Elapsed().Seconds(), "rows/sec")
	}
}
