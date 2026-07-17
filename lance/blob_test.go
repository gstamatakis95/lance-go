package lance_test

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// errBlobMismatch flags a payload mismatch inside a goroutine.
var errBlobMismatch = errors.New("blob payload mismatch")

// blobPayload returns the deterministic bytes stored in blob row i.
func blobPayload(i int) []byte {
	n := 1000 + i*500
	b := make([]byte, n)
	for j := range b {
		b[j] = byte((i*31 + j) % 251)
	}
	return b
}

// blobSchema is {id: int64, blob: large_binary (blob-tagged)}.
func blobSchema() *arrow.Schema {
	md := arrow.NewMetadata([]string{"lance-encoding:blob"}, []string{"true"})
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "blob", Type: arrow.BinaryTypes.LargeBinary, Nullable: true, Metadata: md},
	}, nil)
}

// writeBlobDataset writes rows rows of {id, blob} (blob row i = blobPayload(i))
// with C-allocated buffers and returns the uri and an open handle.
func writeBlobDataset(t *testing.T, rows int) (string, *lance.Dataset) {
	t.Helper()
	mem := testutil.Allocator()
	schema := blobSchema()

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	blobB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer blobB.Release()
	for i := 0; i < rows; i++ {
		idB.Append(int64(i))
		blobB.Append(blobPayload(i))
	}
	idArr := idB.NewArray()
	defer idArr.Release()
	blobArr := blobB.NewArray()
	defer blobArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{idArr, blobArr}, int64(rows))
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	uri := filepath.Join(t.TempDir(), "blobs.lance")
	ds, err := lance.Write(t.Context(), uri, rdr, lance.WithStableRowIDs(true))
	if err != nil {
		t.Fatalf("Write blob dataset: %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return uri, ds
}

func TestTakeBlobsRead(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 5)

	list, err := ds.TakeBlobs(ctx, "blob", []uint64{0, 2, 4})
	if err != nil {
		t.Fatalf("TakeBlobs: %v", err)
	}
	defer list.Close()

	if list.Len() != 3 {
		t.Fatalf("BlobList.Len = %d, want 3", list.Len())
	}
	wantRows := []int{0, 2, 4}
	for i, row := range wantRows {
		bf := list.Get(i)
		if bf == nil {
			t.Fatalf("Get(%d) = nil", i)
		}
		want := blobPayload(row)
		if got := bf.Size(); got != uint64(len(want)) {
			t.Errorf("blob %d Size = %d, want %d", row, got, len(want))
		}
		got, err := bf.Read(ctx)
		if err != nil {
			t.Fatalf("blob %d Read: %v", row, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("blob %d Read mismatch: got %d bytes, want %d bytes", row, len(got), len(want))
		}
	}
}

func TestBlobReadRange(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 3)

	list, err := ds.TakeBlobs(ctx, "blob", []uint64{1})
	if err != nil {
		t.Fatalf("TakeBlobs: %v", err)
	}
	defer list.Close()

	bf := list.Get(0)
	want := blobPayload(1)

	// Sub-slice [100, 350).
	got, err := bf.ReadRange(ctx, 100, 250)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if !bytes.Equal(got, want[100:350]) {
		t.Fatalf("ReadRange mismatch: got %d bytes", len(got))
	}
}

func TestBlobReader(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 3)
	list, err := ds.TakeBlobs(ctx, "blob", []uint64{2})
	if err != nil {
		t.Fatalf("TakeBlobs: %v", err)
	}
	defer list.Close()

	want := blobPayload(2)
	r := list.Get(0).Reader(ctx)
	first := make([]byte, 127)
	if _, err := io.ReadFull(r, first); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(first, want[:len(first)]) {
		t.Fatal("sequential chunk mismatch")
	}

	random := make([]byte, 83)
	if n, err := r.ReadAt(random, 401); err != nil || n != len(random) {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if !bytes.Equal(random, want[401:401+len(random)]) {
		t.Fatal("random chunk mismatch")
	}

	if pos, err := r.Seek(-50, io.SeekEnd); err != nil || pos != int64(len(want)-50) {
		t.Fatalf("Seek = %d, %v", pos, err)
	}
	tail, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll(reader): %v", err)
	}
	if !bytes.Equal(tail, want[len(want)-50:]) {
		t.Fatal("seeked tail mismatch")
	}

	if _, err := r.Seek(-1, io.SeekStart); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("negative seek error = %v", err)
	}
	if err := list.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := r.ReadAt(make([]byte, 1), 0); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("read after list close error = %v", err)
	}
}

func TestBlobSeekAndPartialRead(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 3)

	list, err := ds.TakeBlobs(ctx, "blob", []uint64{2})
	if err != nil {
		t.Fatalf("TakeBlobs: %v", err)
	}
	defer list.Close()

	bf := list.Get(0)
	want := blobPayload(2)

	// Seek to an offset and read to the end (cursor-based).
	if err := bf.Seek(ctx, 500); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if pos, err := bf.Tell(ctx); err != nil || pos != 500 {
		t.Fatalf("Tell = %d, %v (want 500)", pos, err)
	}
	got, err := bf.Read(ctx)
	if err != nil {
		t.Fatalf("Read after seek: %v", err)
	}
	if !bytes.Equal(got, want[500:]) {
		t.Fatalf("seek+read mismatch: got %d bytes, want %d", len(got), len(want)-500)
	}
}

func TestBlobReadAfterDatasetClose(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 3)

	list, err := ds.TakeBlobs(ctx, "blob", []uint64{0, 1})
	if err != nil {
		t.Fatalf("TakeBlobs: %v", err)
	}
	defer list.Close()

	// Close the originating dataset. Blob reads must still work (the blob
	// files are self-contained, like scanner streams).
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bf := list.Get(1)
	got, err := bf.Read(ctx)
	if err != nil {
		t.Fatalf("Read after ds close: %v", err)
	}
	if !bytes.Equal(got, blobPayload(1)) {
		t.Fatalf("blob read after ds close mismatch: got %d bytes", len(got))
	}
}

func TestTakeBlobsByIndices(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 4)

	list, err := ds.TakeBlobsByIndices(ctx, "blob", []uint64{3})
	if err != nil {
		t.Fatalf("TakeBlobsByIndices: %v", err)
	}
	defer list.Close()

	got, err := list.Get(0).Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, blobPayload(3)) {
		t.Fatalf("by-indices blob mismatch: got %d bytes", len(got))
	}
}

func TestBlobConcurrentReads(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 6)

	list, err := ds.TakeBlobs(ctx, "blob", []uint64{0, 1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("TakeBlobs: %v", err)
	}
	defer list.Close()

	// Concurrent reads across distinct blobs must all succeed (they take the
	// list read lock, so they run in parallel without freeing the list).
	var wg sync.WaitGroup
	errs := make([]error, list.Len())
	for i := 0; i < list.Len(); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bf := list.Get(i)
			got, err := bf.Read(ctx)
			if err != nil {
				errs[i] = err
				return
			}
			if !bytes.Equal(got, blobPayload(i)) {
				errs[i] = errBlobMismatch
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent read blob %d: %v", i, err)
		}
	}
}

func TestTakeBlobsNotBlobColumn(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 2)
	// "id" is not a blob column.
	if _, err := ds.TakeBlobs(ctx, "id", []uint64{0}); err == nil {
		t.Fatal("expected error taking blobs on a non-blob column")
	}
}

func TestTakeBlobsInvalidSpec(t *testing.T) {
	ctx := t.Context()
	_, ds := writeBlobDataset(t, 2)
	list, err := ds.TakeBlobs(ctx, "blob", nil)
	if err != nil {
		t.Fatalf("TakeBlobs(empty): %v", err)
	}
	defer list.Close()
	if list.Len() != 0 {
		t.Fatalf("empty take returned %d blobs", list.Len())
	}
}
