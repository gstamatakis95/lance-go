// Command maintenance demonstrates dataset maintenance: creating a dataset
// with several small appends (which leaves many small fragments), compacting
// them into fewer, larger fragments with CompactFiles, then reclaiming the
// storage held by old versions with CleanupOldVersions. It prints fragment
// counts and dataset versions before and after each step.
//
// Usage: go run ./examples/maintenance
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/lance"
)

// newRecordReader builds rows records (id int64, name utf8) starting at
// startID. Buffers are allocated with lance.Allocator, as required by
// lance.Write.
func newRecordReader(startID, rows int64) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(startID + i)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", startID+i))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

// report prints step's label plus the dataset's current fragment count and
// latest version number.
func report(ctx context.Context, ds *lance.Dataset, step string) {
	frags, err := ds.Fragments(ctx)
	if err != nil {
		log.Fatalf("Fragments: %v", err)
	}
	versions, err := ds.Versions(ctx)
	if err != nil {
		log.Fatalf("Versions: %v", err)
	}
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatalf("CountRows: %v", err)
	}
	fmt.Printf("[%s] fragments=%d versions=%d rows=%d\n", step, len(frags), len(versions), count)
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-maintenance-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "fragmented.lance")

	// Create the dataset, then append several small batches. Every write
	// (create or append) commits a new version and, for appends, adds a new
	// fragment, so this leaves the dataset with many small fragments.
	const batches = 8
	const rowsPerBatch = 25

	rdr := newRecordReader(0, rowsPerBatch)
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write(create): %v", err)
	}
	defer ds.Close()

	for i := int64(1); i < batches; i++ {
		rdr := newRecordReader(i*rowsPerBatch, rowsPerBatch)
		appended, err := lance.Write(ctx, uri, rdr, lance.WithMode(lance.WriteModeAppend))
		rdr.Release()
		if err != nil {
			log.Fatalf("Write(append %d): %v", i, err)
		}
		ds.Close()
		ds = appended
	}
	report(ctx, ds, "before compaction")

	// CompactFiles rewrites small fragments into fewer, larger ones and
	// commits the result as a new version.
	metrics, err := ds.CompactFiles(ctx, lance.CompactionOptions{})
	if err != nil {
		log.Fatalf("CompactFiles: %v", err)
	}
	fmt.Printf("CompactFiles: removed %d fragments (%d files), added %d fragments (%d files)\n",
		metrics.FragmentsRemoved, metrics.FilesRemoved, metrics.FragmentsAdded, metrics.FilesAdded)
	report(ctx, ds, "after compaction")

	// CleanupOldVersions reclaims storage held by versions superseded by the
	// compaction (and every earlier append). olderThan=0 makes every eligible
	// old version a candidate; WithDeleteUnverified is safe here because no
	// other writer is running concurrently.
	stats, err := ds.CleanupOldVersions(ctx, 0, lance.WithDeleteUnverified(true))
	if err != nil {
		log.Fatalf("CleanupOldVersions: %v", err)
	}
	fmt.Printf("CleanupOldVersions: removed %d old versions, %d data files, %d bytes\n",
		stats.OldVersions, stats.DataFilesRemoved, stats.BytesRemoved)
	report(ctx, ds, "after cleanup")
}
