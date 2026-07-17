package lance_test

import (
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

func TestSetFieldCompressionMetadata(t *testing.T) {
	base := arrow.Field{Name: "name", Type: arrow.BinaryTypes.String}
	f := lance.SetFieldCompression(base, lance.CompressionZstd, lance.WithCompressionLevel(3))
	md := f.Metadata
	got := map[string]string{}
	for i, k := range md.Keys() {
		got[k] = md.Values()[i]
	}
	if got[lance.CompressionMetaKey] != lance.CompressionZstd {
		t.Fatalf("compression key = %q, want %q", got[lance.CompressionMetaKey], lance.CompressionZstd)
	}
	if got[lance.CompressionLevelMetaKey] != "3" {
		t.Fatalf("compression level = %q, want 3", got[lance.CompressionLevelMetaKey])
	}

	// MarkBlobColumn and SetFieldMetadata merge, preserving prior keys.
	blob := lance.MarkBlobColumn(f)
	bmd := blob.Metadata
	found := map[string]string{}
	for i, k := range bmd.Keys() {
		found[k] = bmd.Values()[i]
	}
	if found[lance.BlobMetaKey] != "true" {
		t.Fatalf("blob key = %q, want true", found[lance.BlobMetaKey])
	}
	if found[lance.CompressionMetaKey] != lance.CompressionZstd {
		t.Fatal("MarkBlobColumn dropped the compression metadata")
	}
}

func TestEncodingRoundTrip(t *testing.T) {
	ctx := t.Context()
	uri := filepath.Join(t.TempDir(), "enc.lance")
	mem := testutil.Allocator()

	nameField := lance.SetFieldCompression(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		lance.CompressionZstd,
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		nameField,
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	const n = 300
	names := make([]string, n)
	for i := 0; i < n; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		names[i] = "row-" + string(rune('a'+i%26))
		b.Field(1).(*array.StringBuilder).Append(names[i])
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	ds, err := lance.Write(ctx, uri, rdr, lance.WithDataStorageVersion("2.1"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	// Values round-trip regardless of on-disk codec.
	recs := scanAll(t, ds.Scan())
	var rows int
	for _, r := range recs {
		idCol := r.Column(r.Schema().FieldIndices("id")[0]).(*array.Int64)
		nameCol := r.Column(r.Schema().FieldIndices("name")[0]).(*array.String)
		for i := 0; i < int(r.NumRows()); i++ {
			id := idCol.Value(i)
			if nameCol.Value(i) != names[id] {
				t.Fatalf("row %d: name = %q, want %q", id, nameCol.Value(i), names[id])
			}
		}
		rows += int(r.NumRows())
	}
	if rows != n {
		t.Fatalf("scanned %d rows, want %d", rows, n)
	}

	// Report whether the encoding metadata survives on the read-back schema.
	rschema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	nmeta := rschema.Field(rschema.FieldIndices("name")[0]).Metadata
	survived := map[string]string{}
	for i, k := range nmeta.Keys() {
		survived[k] = nmeta.Values()[i]
	}
	t.Logf("name field metadata on read-back: %v", survived)
	if survived[lance.CompressionMetaKey] != lance.CompressionZstd {
		t.Errorf("compression metadata did not survive the round-trip: got %v", survived)
	}
}
