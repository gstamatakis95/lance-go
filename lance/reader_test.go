package lance

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
)

func writeReaderTestDataset(t *testing.T) *Dataset {
	t.Helper()
	rdr := testutil.NewReader(testutil.Allocator(), 0, 8, 2)
	defer rdr.Release()
	ds, err := Write(t.Context(), filepath.Join(t.TempDir(), "reader.lance"), rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

func TestRecordReaderReleaseClosesNativeStream(t *testing.T) {
	ds := writeReaderTestDataset(t)
	rdr, err := ds.Scan().BatchSize(1).Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	owned, ok := rdr.(*ownedRecordReader)
	if !ok {
		t.Fatalf("Reader type = %T, want *ownedRecordReader", rdr)
	}

	owned.mu.Lock()
	openBefore := owned.stream != nil && !owned.released
	owned.mu.Unlock()
	if !openBefore {
		t.Fatal("native stream was not open before Release")
	}

	rdr.Release()
	owned.mu.Lock()
	closedAfter := owned.stream == nil && owned.released
	owned.mu.Unlock()
	if !closedAfter {
		t.Fatal("Release did not synchronously close the native stream")
	}
}

func TestRecordReaderHonorsContextBetweenBatches(t *testing.T) {
	ds := writeReaderTestDataset(t)
	ctx, cancel := context.WithCancel(t.Context())
	rdr, err := ds.Scan().BatchSize(1).Reader(ctx)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rdr.Release()

	cancel()
	if rdr.Next() {
		t.Fatal("Next succeeded after context cancellation")
	}
	if !errors.Is(rdr.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", rdr.Err())
	}
	owned := rdr.(*ownedRecordReader)
	owned.mu.Lock()
	streamClosed := owned.stream == nil
	owned.mu.Unlock()
	if !streamClosed {
		t.Fatal("context cancellation did not close the native stream")
	}
}

func TestRecordReaderEndOfStreamClosesNativeStream(t *testing.T) {
	ds := writeReaderTestDataset(t)
	rdr, err := ds.Scan().BatchSize(2).Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rdr.Release()

	var rows int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if rows != 8 {
		t.Fatalf("rows = %d, want 8", rows)
	}
	owned := rdr.(*ownedRecordReader)
	owned.mu.Lock()
	streamClosed := owned.stream == nil
	owned.mu.Unlock()
	if !streamClosed {
		t.Fatal("end of stream did not close the native stream")
	}
}
