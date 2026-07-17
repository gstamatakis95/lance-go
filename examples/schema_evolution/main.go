// Command schema_evolution walks through Lance's schema evolution
// operations: adding a computed column with AddColumnsSQL, renaming and
// casting columns with AlterColumns, dropping a column with DropColumns, and
// joining in new columns with Merge. It prints the dataset schema after each
// step so the effect of each operation is visible.
//
// Usage: go run ./examples/schema_evolution
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
// starting at startID. Buffers are allocated with lance.Allocator, as
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

// categoryReader builds the right-hand side of the Merge step: (id,
// category) for every id in [0, rows), joined on id.
func categoryReader(rows int64) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
		category := "even"
		if i%2 != 0 {
			category = "odd"
		}
		b.Field(1).(*array.StringBuilder).Append(category)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

// printSchema prints step's label and the dataset's current field names.
func printSchema(ctx context.Context, ds *lance.Dataset, step string) {
	schema, err := ds.Schema(ctx)
	if err != nil {
		log.Fatalf("Schema: %v", err)
	}
	names := make([]string, schema.NumFields())
	for i := range names {
		f := schema.Field(i)
		names[i] = fmt.Sprintf("%s:%s", f.Name, f.Type)
	}
	fmt.Printf("[%s] schema: %v\n", step, names)
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lance-schema-evolution-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "evolving.lance")

	const rows = 20

	// Step 1: create the dataset with (id, name, score).
	rdr := newRecordReader(0, rows)
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write: %v", err)
	}
	defer ds.Close()
	printSchema(ctx, ds, "1. created")

	// Step 2: AddColumnsSQL computes a new column from existing ones.
	err = ds.AddColumnsSQL(ctx, []lance.NamedExpr{
		{Name: "score_doubled", SQL: "score * 2"},
	}, nil, 0)
	if err != nil {
		log.Fatalf("AddColumnsSQL: %v", err)
	}
	printSchema(ctx, ds, "2. AddColumnsSQL(score_doubled = score * 2)")

	// Step 3: AlterColumns renames "name" and casts "score_doubled" to
	// float32. Renames and nullability changes are zero-copy; casts rewrite
	// the column data.
	err = ds.AlterColumns(ctx,
		lance.ColumnAlteration{Path: "name", Rename: "full_name"},
		lance.ColumnAlteration{Path: "score_doubled", DataType: "float32"},
	)
	if err != nil {
		log.Fatalf("AlterColumns: %v", err)
	}
	printSchema(ctx, ds, "3. AlterColumns(name->full_name, score_doubled->float32)")

	// Step 4: DropColumns removes score_doubled again (metadata-only; the
	// underlying column data is reclaimed later by CompactFiles).
	if err := ds.DropColumns(ctx, "score_doubled"); err != nil {
		log.Fatalf("DropColumns: %v", err)
	}
	printSchema(ctx, ds, "4. DropColumns(score_doubled)")

	// Step 5: Merge joins a new "category" column in from a separate reader,
	// matched by id.
	catRdr := categoryReader(rows)
	if err := ds.Merge(ctx, catRdr, "id", "id"); err != nil {
		log.Fatalf("Merge: %v", err)
	}
	catRdr.Release()
	printSchema(ctx, ds, "5. Merge(category, on id)")

	// Spot-check the final data: full_name and category for the first few rows.
	scan, err := ds.Scan().
		Columns("id", "full_name", "category").
		ScanInOrder(true).
		Limit(5).
		Reader(ctx)
	if err != nil {
		log.Fatalf("Scan: %v", err)
	}
	defer scan.Release()

	fmt.Println("first 5 rows after evolution:")
	for scan.Next() {
		rec := scan.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		names := rec.Column(1).(*array.String)
		categories := rec.Column(2).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("  id=%d full_name=%s category=%s\n", ids.Value(i), names.Value(i), categories.Value(i))
		}
	}
	if err := scan.Err(); err != nil {
		log.Fatalf("scan: %v", err)
	}
}
