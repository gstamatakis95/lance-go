// Command fts builds an inverted (full-text search) index over a small
// document corpus and runs match and phrase queries against it.
//
// Usage: go run ./examples/fts
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

var docs = []string{
	"the quick brown fox jumps over the lazy dog",
	"lance is a modern columnar data format for machine learning",
	"a vector database stores embeddings for similarity search",
	"postgres is a battle tested relational database",
	"full text search uses an inverted index over tokens",
	"machine learning models turn documents into vector embeddings",
	"sqlite is a small embedded relational database engine",
}

// newRecordReader builds the corpus (id int64, text utf8) using the C
// lance.Allocator, as required by lance.Write.
func newRecordReader() array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "text", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i, doc := range docs {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(doc)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

// runQuery executes q and prints the matching documents with their BM25
// scores (results are ordered by descending relevance).
func runQuery(ctx context.Context, ds *lance.Dataset, q lance.FtsQuery) {
	rdr, err := ds.Scan().FullTextSearch(q).Reader(ctx)
	if err != nil {
		log.Fatalf("FullTextSearch: %v", err)
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.RecordBatch()
		ids := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		scores := rec.Column(rec.Schema().FieldIndices("_score")[0]).(*array.Float32)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  score=%.3f  %q\n", scores.Value(i), docs[ids.Value(i)])
		}
	}
	if err := rdr.Err(); err != nil {
		log.Fatalf("fts scan: %v", err)
	}
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-fts-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "docs.lance")

	rdr := newRecordReader()
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	// WithPosition stores token positions, enabling phrase queries.
	if err := ds.CreateIndex(ctx, "text", lance.Inverted{WithPosition: true},
		lance.WithIndexName("text_idx")); err != nil {
		log.Fatalf("CreateIndex(Inverted): %v", err)
	}
	fmt.Println("built inverted index on \"text\"")

	fmt.Println("match \"database\":")
	runQuery(ctx, ds, lance.MatchQuery{Column: "text", Terms: "database"})

	fmt.Println("match \"vector embeddings\" (all terms required):")
	runQuery(ctx, ds, lance.MatchQuery{
		Column:   "text",
		Terms:    "vector embeddings",
		Operator: lance.FtsOperatorAnd,
	})

	fmt.Println("phrase \"machine learning\":")
	runQuery(ctx, ds, lance.PhraseQuery{Column: "text", Terms: "machine learning"})
}
