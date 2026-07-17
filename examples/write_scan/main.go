// Command write_scan writes a Lance dataset to the local filesystem, scans
// it back in full, and runs a filtered + projected scan.
//
// Usage: go run ./examples/write_scan
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

// newRecordReader builds rows records (id int64, name utf8, score float64)
// starting at startID. The buffers are allocated with lance.Allocator, as
// required by lance.Write.
func newRecordReader(startID, rows int64) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(startID + i)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", startID+i))
		b.Field(2).(*array.Float64Builder).Append(float64(startID+i) / 2.0)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-write-scan-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	// Write 100 rows.
	rdr := newRecordReader(0, 100)
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write: %v", err)
	}
	defer ds.Close()
	fmt.Printf("wrote dataset at %s\n", uri)

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatalf("CountRows: %v", err)
	}
	fmt.Printf("total rows: %d\n", count)

	// Filtered + projected scan: names of the rows with score >= 45.
	scan, err := ds.Scan().
		Filter("score >= 45").
		Columns("id", "name").
		ScanInOrder(true).
		Reader(ctx)
	if err != nil {
		log.Fatalf("Scan: %v", err)
	}
	defer scan.Release()

	fmt.Println("rows with score >= 45:")
	for scan.Next() {
		rec := scan.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		names := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  id=%d name=%s\n", ids.Value(i), names.Value(i))
		}
	}
	if err := scan.Err(); err != nil {
		log.Fatalf("scan: %v", err)
	}
}
