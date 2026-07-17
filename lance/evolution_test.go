package lance_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// singleColumnReader builds a RecordReader over one batch with a single
// column named name, holding int64 values start..start+rows-1 mapped by fn.
// Buffers are C-allocated (see testutil.Allocator).
func singleColumnReader(t *testing.T, name string, dt arrow.DataType, rows int64, appendVal func(b array.Builder, i int64)) array.RecordReader {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: dt}}, nil)
	b := array.NewRecordBuilder(testutil.Allocator(), schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		appendVal(b.Field(0), i)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

// fieldNames returns the names of all top-level schema fields.
func fieldNames(schema *arrow.Schema) []string {
	names := make([]string, schema.NumFields())
	for i := range names {
		names[i] = schema.Field(i).Name
	}
	return names
}

func hasField(schema *arrow.Schema, name string) bool {
	return len(schema.FieldIndices(name)) > 0
}

func TestAddColumnsSQL(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 50)

	err := ds.AddColumnsSQL(ctx, []lance.NamedExpr{{Name: "double_score", SQL: "score * 2"}}, nil, 0)
	if err != nil {
		t.Fatalf("AddColumnsSQL: %v", err)
	}

	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !hasField(schema, "double_score") {
		t.Fatalf("schema fields = %v, want double_score present", fieldNames(schema))
	}

	// score for global id g is g/2, so double_score must equal float64(g).
	recs := scanAll(t, ds.Scan().Columns("id", "double_score").ScanInOrder(true))
	row := 0
	for _, rec := range recs {
		idCol := rec.Column(0).(*array.Int64)
		dblCol := rec.Column(1).(*array.Float64)
		for i := 0; i < int(rec.NumRows()); i++ {
			if want := float64(idCol.Value(i)); dblCol.Value(i) != want {
				t.Fatalf("row %d: double_score = %v, want %v", row, dblCol.Value(i), want)
			}
			row++
		}
	}
	if row != 50 {
		t.Fatalf("scanned %d rows, want 50", row)
	}
}

func TestAddColumnsAllNulls(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	newCols := arrow.NewSchema([]arrow.Field{
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	if err := ds.AddColumnsAllNulls(ctx, newCols); err != nil {
		t.Fatalf("AddColumnsAllNulls: %v", err)
	}

	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !hasField(schema, "note") {
		t.Fatalf("schema fields = %v, want note present", fieldNames(schema))
	}

	recs := scanAll(t, ds.Scan().Columns("note").ScanInOrder(true))
	rows := 0
	for _, rec := range recs {
		col := rec.Column(0)
		for i := 0; i < int(rec.NumRows()); i++ {
			if !col.IsNull(i) {
				t.Fatalf("row %d: note is not null", rows)
			}
			rows++
		}
	}
	if rows != 10 {
		t.Fatalf("scanned %d rows, want 10", rows)
	}
}

func TestAddColumnsFromReader(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 25)

	rdr := singleColumnReader(t, "flag", arrow.PrimitiveTypes.Int64, 25, func(b array.Builder, i int64) {
		b.(*array.Int64Builder).Append(i * 10)
	})
	defer rdr.Release()
	if err := ds.AddColumnsFromReader(ctx, rdr, 0); err != nil {
		t.Fatalf("AddColumnsFromReader: %v", err)
	}

	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !hasField(schema, "flag") {
		t.Fatalf("schema fields = %v, want flag present", fieldNames(schema))
	}

	recs := scanAll(t, ds.Scan().Columns("id", "flag").ScanInOrder(true))
	row := 0
	for _, rec := range recs {
		idCol := rec.Column(0).(*array.Int64)
		flagCol := rec.Column(1).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			if want := idCol.Value(i) * 10; flagCol.Value(i) != want {
				t.Fatalf("row %d: flag = %d, want %d", row, flagCol.Value(i), want)
			}
			row++
		}
	}
	if row != 25 {
		t.Fatalf("scanned %d rows, want 25", row)
	}
}

func TestDropColumns(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	if err := ds.DropColumns(ctx, "vec", "name"); err != nil {
		t.Fatalf("DropColumns: %v", err)
	}
	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if schema.NumFields() != 2 {
		t.Fatalf("schema has %d fields (%v), want 2", schema.NumFields(), fieldNames(schema))
	}
	if hasField(schema, "vec") || hasField(schema, "name") {
		t.Fatalf("schema fields = %v, want vec and name dropped", fieldNames(schema))
	}

	if err := ds.DropColumns(ctx); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("DropColumns() error = %v, want ErrInvalidArgument", err)
	}
}

func TestAlterColumns(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)

	nullable := true
	err := ds.AlterColumns(ctx,
		lance.ColumnAlteration{Path: "name", Rename: "label"},
		lance.ColumnAlteration{Path: "score", DataType: "float32"},
		lance.ColumnAlteration{Path: "id", Nullable: &nullable},
	)
	if err != nil {
		t.Fatalf("AlterColumns: %v", err)
	}

	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !hasField(schema, "label") || hasField(schema, "name") {
		t.Fatalf("schema fields = %v, want name renamed to label", fieldNames(schema))
	}
	scoreField := schema.Field(schema.FieldIndices("score")[0])
	if scoreField.Type.ID() != arrow.FLOAT32 {
		t.Fatalf("score type = %v, want float32", scoreField.Type)
	}
	idField := schema.Field(schema.FieldIndices("id")[0])
	if !idField.Nullable {
		t.Fatalf("id field is not nullable after AlterColumns")
	}

	// Values survive the rename and cast.
	recs := scanAll(t, ds.Scan().Columns("id", "label", "score").ScanInOrder(true))
	row := 0
	for _, rec := range recs {
		idCol := rec.Column(0).(*array.Int64)
		labelCol := rec.Column(1).(*array.String)
		scoreCol := rec.Column(2).(*array.Float32)
		for i := 0; i < int(rec.NumRows()); i++ {
			g := idCol.Value(i)
			if want := fmt.Sprintf("row-%d", g); labelCol.Value(i) != want {
				t.Fatalf("row %d: label = %q, want %q", row, labelCol.Value(i), want)
			}
			if want := float32(g) / 2.0; scoreCol.Value(i) != want {
				t.Fatalf("row %d: score = %v, want %v", row, scoreCol.Value(i), want)
			}
			row++
		}
	}
	if row != 20 {
		t.Fatalf("scanned %d rows, want 20", row)
	}
}

func TestAlterColumnsRejectsUnknownType(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 5)
	err := ds.AlterColumns(ctx, lance.ColumnAlteration{Path: "score", DataType: "decimal128"})
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("AlterColumns(decimal128) error = %v, want ErrInvalidArgument", err)
	}
}

func TestMerge(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 30)

	// Right-hand side: (id, extra) for every id, joined on id.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(testutil.Allocator(), schema)
	for i := int64(0); i < 30; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("extra-%d", i))
	}
	rec := b.NewRecordBatch()
	b.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	rec.Release()
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	if err := ds.Merge(ctx, rdr, "id", "id"); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	got, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !hasField(got, "extra") {
		t.Fatalf("schema fields = %v, want extra present", fieldNames(got))
	}

	recs := scanAll(t, ds.Scan().Columns("id", "extra").ScanInOrder(true))
	row := 0
	for _, r := range recs {
		idCol := r.Column(0).(*array.Int64)
		extraCol := r.Column(1).(*array.String)
		for i := 0; i < int(r.NumRows()); i++ {
			if want := fmt.Sprintf("extra-%d", idCol.Value(i)); extraCol.Value(i) != want {
				t.Fatalf("row %d: extra = %q, want %q", row, extraCol.Value(i), want)
			}
			row++
		}
	}
	if row != 30 {
		t.Fatalf("scanned %d rows, want 30", row)
	}
}
