// Command versioning demonstrates Lance's versioning features: every write
// commits a new version, old versions stay readable (time travel), and tags
// give versions stable names.
//
// Usage: go run ./examples/versioning
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

// newRecordReader builds rows records (id int64, name utf8) starting at
// startID, using lance.Allocator as required by lance.Write.
func newRecordReader(startID, rows int64) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(startID + i)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", startID+i))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

func mustCount(ctx context.Context, ds *lance.Dataset) uint64 {
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatalf("CountRows: %v", err)
	}
	return count
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-versioning-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "versioned.lance")

	// Version 1: create with 100 rows.
	rdr := newRecordReader(0, 100)
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write(create): %v", err)
	}
	defer ds.Close()
	v1, err := ds.Version(ctx)
	if err != nil {
		log.Fatalf("Version: %v", err)
	}
	fmt.Printf("created version %d with %d rows\n", v1.Version, mustCount(ctx, ds))

	// Version 2: append 50 more rows.
	rdr = newRecordReader(100, 50)
	appended, err := lance.Write(ctx, uri, rdr, lance.WithMode(lance.WriteModeAppend))
	rdr.Release()
	if err != nil {
		log.Fatalf("Write(append): %v", err)
	}
	defer appended.Close()
	fmt.Printf("appended, dataset now has %d rows\n", mustCount(ctx, appended))

	// List the commit history.
	versions, err := appended.Versions(ctx)
	if err != nil {
		log.Fatalf("Versions: %v", err)
	}
	fmt.Println("versions:")
	for _, v := range versions {
		fmt.Printf("  version %d at %s\n", v.Version, v.Timestamp.Format("15:04:05.000"))
	}

	// Time travel: check out the first version. The handle is read-only and
	// fixed at that version, while the dataset itself keeps moving forward.
	old, err := appended.Checkout(ctx, lance.VersionRef(v1.Version))
	if err != nil {
		log.Fatalf("Checkout: %v", err)
	}
	defer old.Close()
	fmt.Printf("checked out version %d: %d rows\n", v1.Version, mustCount(ctx, old))

	// Tags: name the first version and open the dataset by tag.
	if err := appended.Tags().Create(ctx, "before-append", lance.VersionRef(v1.Version)); err != nil {
		log.Fatalf("Tags.Create: %v", err)
	}
	tagged, err := lance.Open(ctx, uri, lance.WithTag("before-append"))
	if err != nil {
		log.Fatalf("Open(WithTag): %v", err)
	}
	defer tagged.Close()
	fmt.Printf("opened tag %q: %d rows\n", "before-append", mustCount(ctx, tagged))

	tags, err := appended.Tags().List(ctx)
	if err != nil {
		log.Fatalf("Tags.List: %v", err)
	}
	for name, info := range tags {
		fmt.Printf("tag %q -> version %d\n", name, info.Version)
	}
}
