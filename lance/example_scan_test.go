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

// ExampleDataset_Scan writes a small dataset and then ranges over a filtered,
// ordered scan using Scanner.All, the range-over-func alternative to Reader.
func ExampleDataset_Scan() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-example-scan-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	scoreB := b.Field(1).(*array.Float64Builder)
	for i := int64(0); i < 10; i++ {
		idB.Append(i)
		scoreB.Append(float64(i) * 1.5)
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

	for batch, err := range ds.Scan().Filter("id >= 7").OrderBy("id").All(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		ids := batch.Column(0).(*array.Int64)
		scores := batch.Column(1).(*array.Float64)
		for i := 0; i < int(batch.NumRows()); i++ {
			fmt.Printf("id=%d score=%.1f\n", ids.Value(i), scores.Value(i))
		}
	}
	// Output:
	// id=7 score=10.5
	// id=8 score=12.0
	// id=9 score=13.5
}
