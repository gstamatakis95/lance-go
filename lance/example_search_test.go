package lance_test

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

// ExampleDataset_CreateIndex builds an IVF_PQ vector index over 1024 rows of
// an 8-dimensional vector column and runs an exact nearest-neighbor lookup
// against it.
//
// Row id's vector is a scaled one-hot vector: all zero except at index
// id%8, which holds 1+id*0.001. Vectors sharing that basis index are only
// ever a few thousandths apart, while vectors on different basis indices
// are always more than 1.0 apart, so querying with the exact vector of a
// given id (distance 0 to itself, and to no other row) always yields that
// id as the unambiguous nearest match. Nprobes matches the partition count
// and Refine covers the whole dataset, so the search result is exact
// despite PQ quantization.
func ExampleDataset_CreateIndex() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-example-create-index-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	const (
		dim  = 8
		rows = 1024
	)
	vecOf := func(id int64) []float32 {
		v := make([]float32, dim)
		v[id%dim] = 1 + float32(id)*0.001
		return v
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for id := int64(0); id < rows; id++ {
		idB.Append(id)
		vecB.Append(true)
		valB.AppendValues(vecOf(id), nil)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatal(err)
	}

	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatal(err)
	}
	defer ds.Close()

	if err := ds.CreateIndex(ctx, "vec",
		lance.IvfPq{Partitions: 4, SubVectors: 4, Bits: 8},
		lance.WithIndexName("vec_idx")); err != nil {
		log.Fatal(err)
	}

	res, err := ds.Scan().
		Nearest("vec", vecOf(500), 1).
		Nprobes(4).
		Refine(rows).
		Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Release()
	for res.Next() {
		batch := res.RecordBatch()
		ids := batch.Column(batch.Schema().FieldIndices("id")[0]).(*array.Int64)
		for i := 0; i < int(batch.NumRows()); i++ {
			fmt.Println(ids.Value(i))
		}
	}
	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
	// Output: 500
}

// ExampleScanner_FullTextSearch builds an inverted (full-text) index over a
// text column and runs a MatchQuery against it. Results are ordered by
// descending BM25 relevance, so the document matching both query terms
// ("database" and "search") ranks above the one matching only "search".
func ExampleScanner_FullTextSearch() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-example-fts-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	docs := []string{
		"the quick brown fox jumps over the lazy dog",
		"lance is a columnar data format built for machine learning",
		"a vector database stores embeddings for similarity search",
		"full text search uses an inverted index over tokens",
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "text", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	textB := b.Field(1).(*array.StringBuilder)
	for i, doc := range docs {
		idB.Append(int64(i))
		textB.Append(doc)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatal(err)
	}

	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatal(err)
	}
	defer ds.Close()

	if err := ds.CreateIndex(ctx, "text", lance.Inverted{}, lance.WithIndexName("text_idx")); err != nil {
		log.Fatal(err)
	}

	res, err := ds.Scan().
		FullTextSearch(lance.MatchQuery{Column: "text", Terms: "database search"}).
		Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Release()
	for res.Next() {
		batch := res.RecordBatch()
		ids := batch.Column(batch.Schema().FieldIndices("id")[0]).(*array.Int64)
		for i := 0; i < int(batch.NumRows()); i++ {
			fmt.Println(ids.Value(i))
		}
	}
	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// 2
	// 3
}
