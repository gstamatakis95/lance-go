// Command sql_query writes a small dataset and runs SQL queries over it with
// Dataset.SQL (DataFusion under the hood). The dataset is registered as the
// table "dataset" unless renamed with TableName.
//
// Usage: go run ./examples/sql_query
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

// newReader builds a {id int64, category utf8, score float64} dataset. Buffers
// exported to the native side must be C-allocated with lance.Allocator, never the Go
// heap.
func newReader() array.RecordReader {
	mem := lance.Allocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	ids := b.Field(0).(*array.Int64Builder)
	cats := b.Field(1).(*array.StringBuilder)
	scores := b.Field(2).(*array.Float64Builder)

	categories := []string{"news", "sports", "tech"}
	for i := 0; i < 30; i++ {
		ids.Append(int64(i))
		cats.Append(categories[i%len(categories)])
		scores.Append(float64(i) * 0.1)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("new record reader: %v", err)
	}
	return rdr
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sql_query")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "events.lance")

	rdr := newReader()
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("write: %v", err)
	}
	defer ds.Close()

	// A filtered projection.
	runQuery(ctx, ds, "SELECT id, score FROM dataset WHERE score > 2.0 ORDER BY score DESC LIMIT 3")

	// An aggregate: rows and mean score per category.
	runQuery(ctx, ds,
		"SELECT category, COUNT(*) AS n, ROUND(AVG(score), 3) AS avg_score "+
			"FROM dataset GROUP BY category ORDER BY category")
}

// runQuery executes q and prints every result batch. The reader and its
// records must be Released by the caller.
func runQuery(ctx context.Context, ds *lance.Dataset, q string) {
	fmt.Printf("\n> %s\n", q)
	rdr, err := ds.SQL(q).Reader(ctx)
	if err != nil {
		log.Fatalf("sql: %v", err)
	}
	defer rdr.Release()
	for rdr.Next() {
		fmt.Println(rdr.RecordBatch())
	}
	if err := rdr.Err(); err != nil {
		log.Fatalf("sql read: %v", err)
	}
}
