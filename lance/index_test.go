package lance_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// vecOf returns the deterministic test vector of row g: [g, g+1, ..., g+15]
// (see testutil.NewRecord).
func vecOf(g int64) []float32 {
	v := make([]float32, testutil.VecDim)
	for j := range v {
		v[j] = float32(g) + float32(j)
	}
	return v
}

// idsOf extracts the id column values from recs, in order.
func idsOf(t *testing.T, recs []arrow.RecordBatch) []int64 {
	t.Helper()
	var ids []int64
	for _, rec := range recs {
		idx := rec.Schema().FieldIndices("id")
		if len(idx) == 0 {
			t.Fatalf("result batch has no id column: %v", rec.Schema())
		}
		col := rec.Column(idx[0]).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
	}
	return ids
}

// distancesOf extracts the _distance column values from recs, in order.
func distancesOf(t *testing.T, recs []arrow.RecordBatch) []float32 {
	t.Helper()
	var out []float32
	for _, rec := range recs {
		idx := rec.Schema().FieldIndices("_distance")
		if len(idx) == 0 {
			t.Fatalf("result batch has no _distance column: %v", rec.Schema())
		}
		col := rec.Column(idx[0]).(*array.Float32)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
	}
	return out
}

func contains(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func TestBTreeIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)

	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex(BTree): %v", err)
	}

	infos, err := ds.ListIndices(ctx)
	if err != nil {
		t.Fatalf("ListIndices: %v", err)
	}
	if len(infos) != 1 || infos[0].Name != "id_idx" {
		t.Fatalf("ListIndices = %+v, want one index named id_idx", infos)
	}
	if infos[0].UUID == "" || len(infos[0].Fields) == 0 {
		t.Fatalf("ListIndices entry missing uuid/fields: %+v", infos[0])
	}

	plan, err := ds.Scan().Filter("id = 7").Explain(ctx, false)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !strings.Contains(plan, "ScalarIndexQuery") && !strings.Contains(plan, "MaterializeIndex") {
		t.Fatalf("explain plan does not use the scalar index:\n%s", plan)
	}

	recs := scanAll(t, ds.Scan().Filter("id = 7"))
	assertRows(t, recs, []int64{7})

	// Same name again: fails without replace, succeeds with it.
	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err == nil {
		t.Fatal("CreateIndex with duplicate name should fail without WithReplace")
	}
	if err := ds.CreateIndex(ctx, "id", lance.BTree{},
		lance.WithIndexName("id_idx"), lance.WithReplace(true)); err != nil {
		t.Fatalf("CreateIndex(replace): %v", err)
	}
}

// writeCategoryDataset writes a dataset with a low-cardinality "category"
// column: id 0..rows-1, category cycling through red/green/blue.
func writeCategoryDataset(t *testing.T, rows int64) *lance.Dataset {
	t.Helper()
	mem := testutil.Allocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	categories := []string{"red", "green", "blue"}
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
		b.Field(1).(*array.StringBuilder).Append(categories[i%3])
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	uri := filepath.Join(t.TempDir(), "categories.lance")
	ds, err := lance.Write(t.Context(), uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return ds
}

func TestBitmapIndex(t *testing.T) {
	ctx := t.Context()
	ds := writeCategoryDataset(t, 99)

	if err := ds.CreateIndex(ctx, "category", lance.Bitmap{}, lance.WithIndexName("cat_idx")); err != nil {
		t.Fatalf("CreateIndex(Bitmap): %v", err)
	}

	plan, err := ds.Scan().Filter("category = 'green'").Explain(ctx, false)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !strings.Contains(plan, "ScalarIndexQuery") && !strings.Contains(plan, "MaterializeIndex") {
		t.Fatalf("explain plan does not use the bitmap index:\n%s", plan)
	}

	recs := scanAll(t, ds.Scan().Filter("category = 'green'"))
	ids := idsOf(t, recs)
	if len(ids) != 33 {
		t.Fatalf("filter matched %d rows, want 33", len(ids))
	}
	for _, id := range ids {
		if id%3 != 1 {
			t.Fatalf("id %d is not a 'green' row", id)
		}
	}

	stats, err := ds.IndexStatistics(ctx, "cat_idx")
	if err != nil {
		t.Fatalf("IndexStatistics: %v", err)
	}
	if !json.Valid([]byte(stats)) {
		t.Fatalf("IndexStatistics is not valid JSON: %q", stats)
	}
	if !strings.Contains(strings.ToLower(stats), "bitmap") {
		t.Fatalf("IndexStatistics does not mention the index type: %s", stats)
	}
}

func TestNearestWithoutIndex(t *testing.T) {
	_, ds := writeDataset(t, 128)

	recs := scanAll(t, ds.Scan().Nearest("vec", vecOf(42), 5))
	ids := idsOf(t, recs)
	if len(ids) != 5 {
		t.Fatalf("Nearest returned %d rows, want 5", len(ids))
	}
	if ids[0] != 42 {
		t.Fatalf("nearest row to vec(42) is id %d, want 42", ids[0])
	}
	dists := distancesOf(t, recs)
	if dists[0] != 0 {
		t.Fatalf("self-query distance = %v, want 0", dists[0])
	}
	for i := 1; i < len(dists); i++ {
		if dists[i] < dists[i-1] {
			t.Fatalf("distances not ascending: %v", dists)
		}
	}
}

func TestIvfFlatIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 512)

	if err := ds.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: 4},
		lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex(IvfFlat): %v", err)
	}

	plan, err := ds.Scan().Nearest("vec", vecOf(42), 5).Nprobes(4).Explain(ctx, false)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !strings.Contains(plan, "ANN") {
		t.Fatalf("explain plan does not use the vector index:\n%s", plan)
	}

	recs := scanAll(t, ds.Scan().Nearest("vec", vecOf(42), 5).Nprobes(4))
	ids := idsOf(t, recs)
	if len(ids) != 5 {
		t.Fatalf("Nearest returned %d rows, want 5", len(ids))
	}
	if ids[0] != 42 {
		t.Fatalf("nearest row to vec(42) is id %d, want 42 (ids: %v)", ids[0], ids)
	}
	if dists := distancesOf(t, recs); dists[0] != 0 {
		t.Fatalf("self-query distance = %v, want 0", dists[0])
	}
}

func TestIvfPqIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 512)

	if err := ds.CreateIndex(ctx, "vec",
		lance.IvfPq{Partitions: 4, SubVectors: 4, Bits: 8}); err != nil {
		t.Fatalf("CreateIndex(IvfPq): %v", err)
	}

	// PQ is lossy: probe everything and refine with exact distances, then
	// expect the self-match within the top 10.
	recs := scanAll(t, ds.Scan().Nearest("vec", vecOf(42), 10).Nprobes(4).Refine(4))
	ids := idsOf(t, recs)
	if len(ids) != 10 {
		t.Fatalf("Nearest returned %d rows, want 10", len(ids))
	}
	if !contains(ids, 42) {
		t.Fatalf("top-10 for vec(42) misses id 42: %v", ids)
	}
	dists := distancesOf(t, recs)
	for i := 1; i < len(dists); i++ {
		if dists[i] < dists[i-1] {
			t.Fatalf("distances not ascending: %v", dists)
		}
	}
}

func TestIvfRqIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 512)

	if err := ds.CreateIndex(ctx, "vec", lance.IvfRq{Partitions: 4}); err != nil {
		t.Fatalf("CreateIndex(IvfRq): %v", err)
	}

	recs := scanAll(t, ds.Scan().Nearest("vec", vecOf(42), 10).Nprobes(4).Refine(8))
	ids := idsOf(t, recs)
	if len(ids) != 10 {
		t.Fatalf("Nearest returned %d rows, want 10", len(ids))
	}
	// RQ is very lossy (1 bit/dim), so with a refine pass the self-match must
	// still surface in the top 10.
	if !contains(ids, 42) {
		t.Fatalf("top-10 for vec(42) misses id 42: %v", ids)
	}
	if dists := distancesOf(t, recs); len(dists) != 10 {
		t.Fatalf("expected 10 distances, got %d", len(dists))
	}
}

func TestNearestPrefilter(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 512)

	if err := ds.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: 4}); err != nil {
		t.Fatalf("CreateIndex(IvfFlat): %v", err)
	}

	// The nearest rows to vec(250) are ids around 250, all filtered out, so
	// with a prefilter the search must return the best rows with id < 100.
	recs := scanAll(t, ds.Scan().
		Nearest("vec", vecOf(250), 10).
		Nprobes(4).
		Filter("id < 100").
		Prefilter(true))
	ids := idsOf(t, recs)
	if len(ids) != 10 {
		t.Fatalf("Nearest returned %d rows, want 10", len(ids))
	}
	for _, id := range ids {
		if id >= 100 {
			t.Fatalf("prefiltered search returned id %d >= 100: %v", id, ids)
		}
	}
	if ids[0] != 99 {
		t.Fatalf("best prefiltered match is id %d, want 99 (ids: %v)", ids[0], ids)
	}
}

func TestListDropIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)

	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := ds.CreateIndex(ctx, "name", lance.Bitmap{}, lance.WithIndexName("name_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	infos, err := ds.ListIndices(ctx)
	if err != nil {
		t.Fatalf("ListIndices: %v", err)
	}
	names := map[string]bool{}
	for _, info := range infos {
		names[info.Name] = true
	}
	if len(infos) != 2 || !names["id_idx"] || !names["name_idx"] {
		t.Fatalf("ListIndices = %+v, want id_idx and name_idx", infos)
	}

	stats, err := ds.IndexStatistics(ctx, "id_idx")
	if err != nil {
		t.Fatalf("IndexStatistics: %v", err)
	}
	if !json.Valid([]byte(stats)) {
		t.Fatalf("IndexStatistics is not valid JSON: %q", stats)
	}

	if err := ds.DropIndex(ctx, "id_idx"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	infos, err = ds.ListIndices(ctx)
	if err != nil {
		t.Fatalf("ListIndices after drop: %v", err)
	}
	if len(infos) != 1 || infos[0].Name != "name_idx" {
		t.Fatalf("ListIndices after drop = %+v, want only name_idx", infos)
	}
	if _, err := ds.IndexStatistics(ctx, "id_idx"); err == nil {
		t.Fatal("IndexStatistics of dropped index should fail")
	}
}

func TestOptimizeIndices(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 512)

	if err := ds.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: 4},
		lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex(IvfFlat): %v", err)
	}
	ds.Close()

	// Append unindexed rows, then fold them into the index.
	appended := appendRows(t, uri, 512, 64)
	if err := appended.OptimizeIndices(ctx, lance.WithIndexNames("vec_idx"), lance.WithMergeIndices(1)); err != nil {
		t.Fatalf("OptimizeIndices: %v", err)
	}

	// FastSearch only sees indexed data, so finding an appended row proves
	// the optimize covered it.
	recs := scanAll(t, appended.Scan().Nearest("vec", vecOf(550), 3).Nprobes(4).FastSearch())
	ids := idsOf(t, recs)
	if len(ids) != 3 {
		t.Fatalf("Nearest returned %d rows, want 3", len(ids))
	}
	if ids[0] != 550 {
		t.Fatalf("nearest row to vec(550) is id %d, want 550 (ids: %v)", ids[0], ids)
	}
}

func TestPrewarmIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)

	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := ds.PrewarmIndex(ctx, "id_idx"); err != nil {
		t.Fatalf("PrewarmIndex: %v", err)
	}
	if err := ds.PrewarmIndex(ctx, "no_such_idx"); err == nil {
		t.Fatal("PrewarmIndex of unknown index should fail")
	}
}

func TestNearestArrow(t *testing.T) {
	_, ds := writeDataset(t, 128)

	mem := testutil.Allocator()
	bld := array.NewFloat32Builder(mem)
	defer bld.Release()
	bld.AppendValues(vecOf(17), nil)
	query := bld.NewArray()
	defer query.Release()

	recs := scanAll(t, ds.Scan().NearestArrow("vec", query, 3))
	ids := idsOf(t, recs)
	if len(ids) != 3 || ids[0] != 17 {
		t.Fatalf("NearestArrow top ids = %v, want leading 17", ids)
	}
}

func TestNearestInvalidColumn(t *testing.T) {
	_, ds := writeDataset(t, 64)
	if _, err := ds.Scan().Nearest("no_such_column", vecOf(1), 3).Reader(t.Context()); err == nil {
		t.Fatal("Nearest on unknown column should fail")
	}
	if _, err := ds.Scan().Nearest("vec", []float32{1, 2, 3}, 3).Reader(t.Context()); err == nil {
		t.Fatal("Nearest with wrong dimension should fail")
	}
}

func TestIndexOnMissingColumn(t *testing.T) {
	_, ds := writeDataset(t, 64)
	if err := ds.CreateIndex(t.Context(), "no_such_column", lance.BTree{}); err == nil {
		t.Fatal("CreateIndex on unknown column should fail")
	}
}
