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

// ExampleWrite creates a new dataset from an Arrow record reader and reports
// how many rows were written. Records exported across the Arrow C Data
// Interface must live outside the Go heap, so the builder uses
// lance.Allocator (a C-backed allocator) rather than the Arrow default.
func ExampleWrite() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-example-write-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i, name := range []string{"alice", "bob", "carol"} {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(name)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Release()

	ds, err := lance.Write(ctx, uri, rdr)
	if err != nil {
		log.Fatal(err)
	}
	defer ds.Close()

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(count)
	// Output: 3
}

// ExampleOpen writes a small dataset and then reopens it by URI with Open,
// printing the field names of its schema.
func ExampleOpen() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-example-open-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StringBuilder).Append("alice")
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
	ds.Close()

	opened, err := lance.Open(ctx, uri)
	if err != nil {
		log.Fatal(err)
	}
	defer opened.Close()

	openedSchema, err := opened.Schema(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range openedSchema.Fields() {
		fmt.Println(f.Name)
	}
	// Output:
	// id
	// name
}
