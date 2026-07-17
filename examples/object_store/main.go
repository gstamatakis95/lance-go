// Command object_store writes and scans a Lance dataset on any object
// store. The destination and credentials come from the environment:
//
//	LANCE_URI              dataset URI, e.g. s3://bucket/path/data.lance,
//	                       az://container/path/data.lance,
//	                       gs://bucket/path/data.lance (or a local path)
//	LANCE_STORAGE_OPTIONS  comma-separated key=value storage options passed
//	                       to the object store, e.g.
//	                       "access_key_id=...,secret_access_key=...,endpoint=http://localhost:8333,region=us-east-1,allow_http=true"
//
// Example against the SeaweedFS emulator from `make object-store-up`:
//
//	LANCE_URI=s3://lance-test/example/data.lance \
//	LANCE_STORAGE_OPTIONS="access_key_id=lance-access-key,secret_access_key=lance-secret-key,endpoint=http://localhost:8333,region=us-east-1,allow_http=true,virtual_hosted_style_request=false" \
//	go run ./examples/object_store
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/lance"
)

// parseStorageOptions parses "k1=v1,k2=v2" into a map.
func parseStorageOptions(s string) map[string]string {
	options := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, found := strings.Cut(pair, "=")
		if !found {
			log.Fatalf("invalid LANCE_STORAGE_OPTIONS entry %q (want key=value)", pair)
		}
		options[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return options
}

// newRecordReader builds rows records (id int64, name utf8, score float64)
// starting at startID, using lance.Allocator as required by lance.Write.
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
	uri := os.Getenv("LANCE_URI")
	if uri == "" {
		fmt.Fprintln(os.Stderr, "LANCE_URI is not set, see the comment at the top of examples/object_store/main.go")
		os.Exit(2)
	}
	options := parseStorageOptions(os.Getenv("LANCE_STORAGE_OPTIONS"))
	ctx := context.Background()

	// Create the dataset on the first run, append on subsequent runs.
	var ds *lance.Dataset
	existing, err := lance.Open(ctx, uri, lance.WithStorageOptions(options))
	switch {
	case err == nil:
		count, err := existing.CountRows(ctx, "")
		if err != nil {
			log.Fatalf("CountRows: %v", err)
		}
		existing.Close()
		rdr := newRecordReader(int64(count), 50)
		ds, err = lance.Write(ctx, uri, rdr,
			lance.WithMode(lance.WriteModeAppend), lance.WithWriteStorageOptions(options))
		rdr.Release()
		if err != nil {
			log.Fatalf("Write(append): %v", err)
		}
		fmt.Printf("appended 50 rows to %s\n", uri)
	case errors.Is(err, lance.ErrNotFound):
		rdr := newRecordReader(0, 100)
		ds, err = lance.Write(ctx, uri, rdr, lance.WithWriteStorageOptions(options))
		rdr.Release()
		if err != nil {
			log.Fatalf("Write(create): %v", err)
		}
		fmt.Printf("created %s with 100 rows\n", uri)
	default:
		log.Fatalf("Open: %v", err)
	}
	defer ds.Close()

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatalf("CountRows: %v", err)
	}
	version, err := ds.Version(ctx)
	if err != nil {
		log.Fatalf("Version: %v", err)
	}
	fmt.Printf("dataset now has %d rows at version %d\n", count, version.Version)

	// Scan a few rows back.
	rdr, err := ds.Scan().Filter("id < 5").ScanInOrder(true).Reader(ctx)
	if err != nil {
		log.Fatalf("Scan: %v", err)
	}
	defer rdr.Release()
	fmt.Println("first rows:")
	for rdr.Next() {
		rec := rdr.RecordBatch()
		ids := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		names := rec.Column(rec.Schema().FieldIndices("name")[0]).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  id=%d name=%s\n", ids.Value(i), names.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		log.Fatalf("scan: %v", err)
	}
}
