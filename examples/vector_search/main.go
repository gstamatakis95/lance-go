// Command vector_search builds an IVF_PQ vector index over a small dataset
// and runs nearest-neighbor searches, including a prefiltered one.
//
// Usage: go run ./examples/vector_search
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
	dim  = 16
	rows = 512
)

// vecOf is the deterministic embedding of row g: [g, g+1, ..., g+dim-1].
func vecOf(g int64) []float32 {
	v := make([]float32, dim)
	for j := range v {
		v[j] = float32(g) + float32(j)
	}
	return v
}

// newRecordReader builds rows records (id int64, vec fixed_size_list<f32,16>)
// using lance.Allocator, as required by lance.Write.
func newRecordReader() array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for g := int64(0); g < rows; g++ {
		b.Field(0).(*array.Int64Builder).Append(g)
		vecB.Append(true)
		valB.AppendValues(vecOf(g), nil)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

// printMatches prints the id and _distance columns of a search result.
func printMatches(rdr array.RecordReader) {
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.RecordBatch()
		ids := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		dists := rec.Column(rec.Schema().FieldIndices("_distance")[0]).(*array.Float32)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  id=%d distance=%.1f\n", ids.Value(i), dists.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		log.Fatalf("search: %v", err)
	}
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-vector-search-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "vectors.lance")

	rdr := newRecordReader()
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	// Build an IVF_PQ index: 4 IVF partitions, 4 PQ sub-vectors of 8 bits.
	if err := ds.CreateIndex(ctx, "vec",
		lance.IvfPq{Partitions: 4, SubVectors: 4, Bits: 8},
		lance.WithIndexName("vec_idx")); err != nil {
		log.Fatalf("CreateIndex: %v", err)
	}
	fmt.Println("built IVF_PQ index on \"vec\"")

	// Approximate top-5 for the embedding of row 42. A refine step re-ranks
	// 4*k candidates with exact distances to compensate for PQ loss.
	fmt.Println("top-5 nearest to vec(42):")
	res, err := ds.Scan().
		Nearest("vec", vecOf(42), 5).
		Nprobes(4).
		Refine(4).
		Reader(ctx)
	if err != nil {
		log.Fatalf("Nearest: %v", err)
	}
	printMatches(res)

	// Prefiltered search: restrict candidates to id < 100 BEFORE the vector
	// stage, so the top-k comes from the filtered subset.
	fmt.Println("top-5 nearest to vec(250) among id < 100:")
	res, err = ds.Scan().
		Nearest("vec", vecOf(250), 5).
		Nprobes(4).
		Refine(4).
		Filter("id < 100").
		Prefilter(true).
		Reader(ctx)
	if err != nil {
		log.Fatalf("prefiltered Nearest: %v", err)
	}
	printMatches(res)
}
