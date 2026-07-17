// Command take_blobs writes a dataset with a Lance blob column (a
// LargeBinary column tagged lance-encoding:blob=true), then reads blob bytes
// back with Dataset.TakeBlobs (whole blobs, sub-ranges, and cursor seeks)
// without materializing them into a scan.
//
// Blob columns store large values out-of-line. TakeBlobs returns file-like
// handles you read on demand, which is how you serve large binary payloads
// (images, tensors, documents) without loading them during ordinary scans.
//
// Usage: go run ./examples/take_blobs
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

// blobPayload is the deterministic content of blob row i.
func blobPayload(i int) []byte {
	n := 256 + i*128
	b := make([]byte, n)
	for j := range b {
		b[j] = byte((i*31 + j) % 251)
	}
	return b
}

// newBlobReader builds {id int64, blob large_binary(blob-tagged)}. The blob
// column is marked with lance.MarkBlobColumn (sets lance-encoding:blob=true).
// Buffers crossing into the native side must be C-allocated.
func newBlobReader(rows int) array.RecordReader {
	mem := lance.Allocator()
	blobField := lance.MarkBlobColumn(arrow.Field{
		Name: "blob", Type: arrow.BinaryTypes.LargeBinary, Nullable: true,
	})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		blobField,
	}, nil)

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
		log.Fatalf("new record reader: %v", err)
	}
	return rdr
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "take_blobs")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "blobs.lance")

	// WithStableRowIDs makes TakeBlobs' row ids survive compaction/updates.
	rdr := newBlobReader(5)
	ds, err := lance.Write(ctx, uri, rdr, lance.WithStableRowIDs(true))
	rdr.Release()
	if err != nil {
		log.Fatalf("write: %v", err)
	}
	defer ds.Close()

	// Take blobs for stable row ids 0, 2, 4.
	list, err := ds.TakeBlobs(ctx, "blob", []uint64{0, 2, 4})
	if err != nil {
		log.Fatalf("take blobs: %v", err)
	}
	defer list.Close()
	fmt.Printf("took %d blobs\n", list.Len())

	// Whole-blob read of the first handle.
	bf := list.Get(0)
	whole, err := bf.Read(ctx)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	fmt.Printf("blob[row 0]: size=%d bytes, read=%d bytes, matches=%v\n",
		bf.Size(), len(whole), string(whole) == string(blobPayload(0)))

	// Random-access sub-range read (does not move the cursor).
	part, err := bf.ReadRange(ctx, 10, 16)
	if err != nil {
		log.Fatalf("read range: %v", err)
	}
	fmt.Printf("blob[row 0] bytes [10,26): %d bytes, matches=%v\n",
		len(part), string(part) == string(blobPayload(0)[10:26]))

	// Cursor seek then read-to-end on the third handle (row 4).
	last := list.Get(2)
	if err := last.Seek(ctx, 100); err != nil {
		log.Fatalf("seek: %v", err)
	}
	tail, err := last.Read(ctx)
	if err != nil {
		log.Fatalf("read after seek: %v", err)
	}
	fmt.Printf("blob[row 4] from offset 100: %d bytes, matches=%v\n",
		len(tail), string(tail) == string(blobPayload(4)[100:]))
}
