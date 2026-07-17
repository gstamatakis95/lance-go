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

// ExampleDataset_MergeInsert upserts source rows into a dataset, matching on
// "id": rows present in the source update the matching target row, and
// unmatched source rows are inserted.
func ExampleDataset_MergeInsert() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-example-merge-insert-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// buildReader builds a one-batch reader over the given id/value pairs
	// using the C-backed Allocator, as required to cross the Arrow C Data
	// Interface into lance.Write / MergeInsertBuilder.Execute.
	buildReader := func(ids, values []int64) array.RecordReader {
		b := array.NewRecordBuilder(lance.Allocator(), schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.Int64Builder).AppendValues(values, nil)
		rec := b.NewRecordBatch()
		defer rec.Release()
		rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
		if err != nil {
			log.Fatal(err)
		}
		return rdr
	}

	rdr := buildReader([]int64{0, 1, 2}, []int64{10, 20, 30})
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatal(err)
	}
	defer ds.Close()

	before, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatal(err)
	}

	// id=1 already exists (updated); id=3 does not (inserted).
	src := buildReader([]int64{1, 3}, []int64{200, 300})
	stats, err := ds.MergeInsert("id").
		WhenMatchedUpdateAll().
		WhenNotMatchedInsertAll().
		Execute(ctx, src)
	src.Release()
	if err != nil {
		log.Fatal(err)
	}

	after, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("before=%d after=%d inserted=%d updated=%d\n",
		before, after, stats.NumInsertedRows, stats.NumUpdatedRows)
	// Output: before=3 after=4 inserted=1 updated=1
}
