// Command distributed_index demonstrates the v0.2 distributed write and
// distributed index-build workflow:
//
//  1. A coordinator seeds the dataset schema with a small base write.
//  2. Two "workers" each write disjoint data as uncommitted fragments with
//     WriteFragments, returning opaque Transactions.
//  3. A driver commits both transactions in one batch (CommitBuilder.
//     ExecuteBatch merges the Append transactions into a single new version).
//  4. Each worker builds a per-fragment IVF index segment pinned to shared
//     centroids (CreateIndexUncommitted with WithFragments(...)). The driver
//     merges the segments (MergeIndexSegments) and commits one logical index
//     (CommitIndexSegments).
//  5. A vector search runs against the committed distributed index.
//
// In production the workers run on separate machines/processes. The
// Transaction and IndexMetadata objects are shipped to the driver as bytes
// (see their Bytes() accessors).
//
// Usage: go run ./examples/distributed_index
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

const (
	dim = 8 // vector dimension
	k   = 4 // IVF partitions / shared centroids
)

// newVecReader builds {id int64, vec fixed_size_list<float32, dim>} for ids in
// [start, start+rows). Buffers crossing into the native side must be
// C-allocated with lance.Allocator, never the Go heap.
func newVecReader(start, rows int64) array.RecordReader {
	mem := lance.Allocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	ids := b.Field(0).(*array.Int64Builder)
	lb := b.Field(1).(*array.FixedSizeListBuilder)
	vb := lb.ValueBuilder().(*array.Float32Builder)
	for i := start; i < start+rows; i++ {
		ids.Append(i)
		lb.Append(true)
		for j := 0; j < dim; j++ {
			vb.Append(float32(i) + float32(j))
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("new record reader: %v", err)
	}
	return rdr
}

// sharedCentroids builds k IVF centroids as a fixed_size_list<float32, dim>.
// Every worker pins the index to the same centroids so the per-fragment
// segments are comparable and mergeable. The caller must Release the array.
func sharedCentroids() arrow.Array {
	mem := lance.Allocator()
	bld := array.NewFixedSizeListBuilder(mem, dim, arrow.PrimitiveTypes.Float32)
	defer bld.Release()
	vb := bld.ValueBuilder().(*array.Float32Builder)
	for c := 0; c < k; c++ {
		bld.Append(true)
		base := float32(c * 25)
		for j := 0; j < dim; j++ {
			vb.Append(base + float32(j))
		}
	}
	return bld.NewArray()
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "distributed_index")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "vectors.lance")

	// (1) Coordinator seeds the schema + v1 with a small base write.
	base := newVecReader(0, 20)
	bds, err := lance.Write(ctx, uri, base)
	base.Release()
	if err != nil {
		log.Fatalf("base write: %v", err)
	}
	bds.Close()

	// (2) Two workers write disjoint slices as uncommitted Append fragments.
	txn1, err := workerWrite(ctx, uri, 20, 40)
	if err != nil {
		log.Fatalf("worker 1 write: %v", err)
	}
	txn2, err := workerWrite(ctx, uri, 60, 40)
	if err != nil {
		log.Fatalf("worker 2 write: %v", err)
	}
	fmt.Printf("worker 1 txn: op=%s bytes=%d\n", txn1.Operation().Type, len(txn1.Bytes()))
	fmt.Printf("worker 2 txn: op=%s bytes=%d\n", txn2.Operation().Type, len(txn2.Bytes()))

	// (3) Driver commits both in one batch -> a single new version.
	versions, err := lance.NewCommit(uri).ExecuteBatch(ctx, []*lance.Transaction{txn1, txn2})
	if err != nil {
		log.Fatalf("execute batch: %v", err)
	}
	fmt.Printf("committed distributed write as version %d\n", versions[0])

	ds, err := lance.Open(ctx, uri)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer ds.Close()
	total, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatalf("count: %v", err)
	}
	fmt.Printf("dataset has %d rows\n", total)

	// (4) Distributed index build: one uncommitted segment per fragment,
	// pinned to shared centroids, then merge + commit one logical index.
	frags, err := ds.Fragments(ctx)
	if err != nil {
		log.Fatalf("fragments: %v", err)
	}
	fmt.Printf("building distributed index over %d fragments\n", len(frags))

	centroids := sharedCentroids()
	defer centroids.Release()

	segments := make([]*lance.IndexMetadata, 0, len(frags))
	for _, f := range frags {
		seg, err := ds.CreateIndexUncommitted(ctx, "vec",
			lance.IvfFlat{
				Partitions:    k,
				VectorOptions: lance.VectorOptions{Centroids: centroids},
			},
			lance.WithFragments(f.ID),
			lance.WithUncommittedName("vec_idx"),
		)
		if err != nil {
			log.Fatalf("build segment for fragment %d: %v", f.ID, err)
		}
		segments = append(segments, seg)
	}

	merged, err := ds.MergeIndexSegments(ctx, segments)
	if err != nil {
		log.Fatalf("merge segments: %v", err)
	}
	if err := ds.CommitIndexSegments(ctx, "vec_idx", "vec", []*lance.IndexMetadata{merged}); err != nil {
		log.Fatalf("commit index: %v", err)
	}
	fmt.Println("committed one logical index from the merged segments")

	// Prove the committed index actually covers all rows: if a per-fragment
	// segment were lost in the merge/commit round-trip, its rows would be
	// unindexed. (Vector search alone can't prove this, since Lance brute-forces
	// unindexed fragments, so results would still look correct.)
	descs, err := ds.DescribeIndices(ctx, &lance.IndexCriteria{HasName: "vec_idx"})
	if err != nil {
		log.Fatalf("describe indices: %v", err)
	}
	if len(descs) > 0 {
		fmt.Printf("index %q covers %d of %d rows\n", descs[0].Name, descs[0].RowsIndexed, total)
	}

	// (5) Vector search against the committed distributed index.
	query := make([]float32, dim)
	for j := range query {
		query[j] = 42 + float32(j) // nearest neighbor should be id 42
	}
	rdr, err := ds.Scan().Columns("id").Nearest("vec", query, 5).Nprobes(k).Reader(ctx)
	if err != nil {
		log.Fatalf("search: %v", err)
	}
	defer rdr.Release()
	fmt.Println("5 nearest neighbors of the id-42 vector:")
	for rdr.Next() {
		fmt.Println(rdr.RecordBatch())
	}
	if err := rdr.Err(); err != nil {
		log.Fatalf("search read: %v", err)
	}
}

// workerWrite is what a distributed worker runs: it writes its slice as
// uncommitted Append fragments and returns the Transaction (shipped to the
// driver as txn.Bytes() in a real deployment).
func workerWrite(ctx context.Context, uri string, start, rows int64) (*lance.Transaction, error) {
	rdr := newVecReader(start, rows)
	defer rdr.Release()
	return lance.WriteFragments(ctx, uri, rdr, lance.WithFragmentsMode(lance.WriteModeAppend))
}
