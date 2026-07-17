package lance_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// usesScalarIndex reports whether an explain plan routes through a scalar
// index (the node name differs by Lance version / index type).
func usesScalarIndex(plan string) bool {
	return strings.Contains(plan, "ScalarIndexQuery") || strings.Contains(plan, "MaterializeIndex")
}

// TestFMIndex builds an FM-index on the default dataset's text "name" column
// and checks that a contains() filter routes through it and returns the right
// rows.
func TestFMIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 64)

	if err := ds.CreateIndex(ctx, "name", lance.FM{}, lance.WithIndexName("name_idx")); err != nil {
		t.Fatalf("CreateIndex(FM): %v", err)
	}

	plan, err := ds.Scan().Filter("contains(name, 'row-1')").Explain(ctx, false)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !usesScalarIndex(plan) {
		t.Fatalf("contains() filter does not route through the FM index:\n%s", plan)
	}

	// row-1, row-10..row-19 all contain "row-1".
	recs := scanAll(t, ds.Scan().Filter("contains(name, 'row-1')"))
	got := map[int64]bool{}
	for _, id := range idsOf(t, recs) {
		got[id] = true
	}
	want := []int64{1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	if len(got) != len(want) {
		t.Fatalf("contains('row-1') returned %d rows, want %d: %v", len(got), len(want), got)
	}
	for _, id := range want {
		if !got[id] {
			t.Fatalf("contains('row-1') missing id %d (got %v)", id, got)
		}
	}
}

// writeJSONDataset writes a dataset with an id column and a JSON column
// ("jsons") holding the given JSON strings. The JSON column is tagged with
// the arrow.json extension so Lance stores it as JSONB (LargeBinary) and it
// is indexable by a JSON index.
func writeJSONDataset(t *testing.T, jsons []string) (string, *lance.Dataset) {
	t.Helper()
	mem := testutil.Allocator()
	jsonMeta := arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"arrow.json"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "jsons", Type: arrow.BinaryTypes.String, Metadata: jsonMeta},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i, v := range jsons {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(v)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()
	uri := filepath.Join(t.TempDir(), "json.lance")
	ds, err := lance.Write(t.Context(), uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return uri, ds
}

// TestJSONIndex builds a JSON index on a path with a btree target and checks
// that it is created (via describe) and that a json_get_int filter on the
// path routes through it.
func TestJSONIndex(t *testing.T) {
	ctx := t.Context()
	_, ds := writeJSONDataset(t, []string{
		`{"x": 7, "y": 10}`,
		`{"x": 11, "y": 22}`,
		`{"y": 0}`,
		`{"x": 10}`,
		`{"x": 10, "y": 5}`,
	})

	err := ds.CreateIndex(ctx, "jsons",
		lance.JSONIndex{Path: "x", TargetIndexType: "btree"},
		lance.WithIndexName("jsons_idx"))
	if err != nil {
		t.Fatalf("CreateIndex(JSONIndex): %v", err)
	}

	// The index must be created and described.
	descs, err := ds.DescribeIndices(ctx, nil)
	if err != nil {
		t.Fatalf("DescribeIndices: %v", err)
	}
	if len(descs) != 1 || descs[0].Name != "jsons_idx" {
		t.Fatalf("DescribeIndices = %+v, want one index named jsons_idx", descs)
	}
	if descs[0].RowsIndexed != 5 {
		t.Fatalf("JSON index rows_indexed = %d, want 5", descs[0].RowsIndexed)
	}

	// The json_get_int filter on the indexed path should route through the
	// index and return rows where x == 10 (ids 3 and 4).
	filter := "json_get_int(jsons, 'x') = 10"
	plan, err := ds.Scan().Filter(filter).Explain(ctx, false)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !usesScalarIndex(plan) {
		t.Fatalf("json_get_int filter does not route through the JSON index:\n%s", plan)
	}
	recs := scanAll(t, ds.Scan().Filter(filter))
	got := map[int64]bool{}
	for _, id := range idsOf(t, recs) {
		got[id] = true
	}
	if len(got) != 2 || !got[3] || !got[4] {
		t.Fatalf("json_get_int filter returned %v, want {3, 4}", got)
	}
}

// TestBloomFilterTypedParams builds a BloomFilter index with custom sizing
// and checks it builds and its stats reflect the type.
func TestBloomFilterTypedParams(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 128)

	// Use values distinct from the Lance defaults (8192 / 0.00057) so the
	// stats fingerprint proves the params round-tripped rather than silently
	// falling back to defaults.
	cfg := lance.BloomFilter{NumberOfItems: 4096, Probability: 0.001}
	if err := ds.CreateIndex(ctx, "id", cfg, lance.WithIndexName("id_bf")); err != nil {
		t.Fatalf("CreateIndex(BloomFilter typed): %v", err)
	}
	stats := indexStats(t, ds, "id_bf")
	if !strings.Contains(strings.ToLower(stats["index_type"].(string)), "bloom") {
		t.Fatalf("bloom filter stats do not report the type: %v", stats)
	}
	seg := firstIndexEntry(t, stats)
	if got := seg["number_of_items"].(float64); got != 4096 {
		t.Errorf("bloom filter number_of_items = %v, want 4096 (params dropped?)", got)
	}
	if got := seg["probability"].(float64); got != 0.001 {
		t.Errorf("bloom filter probability = %v, want 0.001 (params dropped?)", got)
	}
}

// firstIndexEntry returns indices[0] of an IndexStatistics document.
func firstIndexEntry(t *testing.T, stats map[string]any) map[string]any {
	t.Helper()
	indices, ok := stats["indices"].([]any)
	if !ok || len(indices) == 0 {
		t.Fatalf("stats have no indices array: %v", stats)
	}
	entry, ok := indices[0].(map[string]any)
	if !ok {
		t.Fatalf("stats indices[0] not an object: %v", indices[0])
	}
	return entry
}

// TestBTreeZoneSize checks the typed BTree ZoneSize param round-trips: 256
// rows with a 128-row zone size yields 2 BTree pages (the default zone size
// is far larger and would produce a single page).
func TestBTreeZoneSize(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)
	if err := ds.CreateIndex(ctx, "id", lance.BTree{ZoneSize: 128}, lance.WithIndexName("id_bt")); err != nil {
		t.Fatalf("CreateIndex(BTree ZoneSize): %v", err)
	}
	recs := scanAll(t, ds.Scan().Filter("id = 42"))
	assertRows(t, recs, []int64{42})

	stats := indexStats(t, ds, "id_bt")
	seg := firstIndexEntry(t, stats)
	if got := seg["num_pages"].(float64); got != 2 {
		t.Errorf("BTree num_pages = %v, want 2 (ZoneSize=128 over 256 rows dropped?)", got)
	}
}

// TestZoneMapTypedParams checks the typed ZoneMap RowsPerZone param
// round-trips: 256 rows with a 64-row zone yields rows_per_zone=64 and 4
// zones in the stats fingerprint.
func TestZoneMapTypedParams(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)
	if err := ds.CreateIndex(ctx, "score", lance.ZoneMap{RowsPerZone: 64}, lance.WithIndexName("score_zm")); err != nil {
		t.Fatalf("CreateIndex(ZoneMap RowsPerZone): %v", err)
	}
	stats := indexStats(t, ds, "score_zm")
	seg := firstIndexEntry(t, stats)
	if got := seg["rows_per_zone"].(float64); got != 64 {
		t.Errorf("ZoneMap rows_per_zone = %v, want 64 (param dropped?)", got)
	}
	if got := seg["num_zones"].(float64); got != 4 {
		t.Errorf("ZoneMap num_zones = %v, want 4", got)
	}
}

// indexStats returns the parsed IndexStatistics JSON for name.
func indexStats(t *testing.T, ds *lance.Dataset, name string) map[string]any {
	t.Helper()
	stats, err := ds.IndexStatistics(t.Context(), name)
	if err != nil {
		t.Fatalf("IndexStatistics(%q): %v", name, err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(stats), &parsed); err != nil {
		t.Fatalf("stats not valid JSON: %q", stats)
	}
	return parsed
}

// numPartitions extracts indices[0].num_partitions from IVF stats.
func numPartitions(t *testing.T, stats map[string]any) int {
	t.Helper()
	indices, ok := stats["indices"].([]any)
	if !ok || len(indices) == 0 {
		t.Fatalf("stats have no indices array: %v", stats)
	}
	first, ok := indices[0].(map[string]any)
	if !ok {
		t.Fatalf("stats indices[0] not an object: %v", indices[0])
	}
	n, ok := first["num_partitions"].(float64)
	if !ok {
		t.Fatalf("stats indices[0] has no num_partitions: %v", first)
	}
	return int(n)
}

// TestTargetPartitionSize builds an IVF_PQ index with target_partition_size
// set and verifies it builds.
func TestTargetPartitionSize(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 512)
	cfg := lance.IvfPq{
		SubVectors:    4,
		VectorOptions: lance.VectorOptions{TargetPartitionSize: 128},
	}
	if err := ds.CreateIndex(ctx, "vec", cfg, lance.WithIndexName("vec_tps")); err != nil {
		t.Fatalf("CreateIndex(IvfPq target_partition_size): %v", err)
	}
	stats := indexStats(t, ds, "vec_tps")
	if numPartitions(t, stats) < 1 {
		t.Fatalf("expected >=1 partition, stats: %v", stats)
	}
}

// TestKmeansRedosAndPrefetch smoke-tests kmeans_redos (PQ) and prefetch
// distance (HNSW) knobs.
func TestKmeansRedosAndPrefetch(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)
	pq := lance.IvfPq{
		Partitions:    4,
		SubVectors:    4,
		VectorOptions: lance.VectorOptions{KmeansRedos: 2},
	}
	if err := ds.CreateIndex(ctx, "vec", pq, lance.WithIndexName("vec_redos")); err != nil {
		t.Fatalf("CreateIndex(IvfPq kmeans_redos): %v", err)
	}
	hnsw := lance.IvfHnswFlat{
		Partitions:    4,
		VectorOptions: lance.VectorOptions{PrefetchDistance: 4},
	}
	if err := ds.CreateIndex(ctx, "vec", hnsw, lance.WithIndexName("vec_hnsw"), lance.WithReplace(true)); err != nil {
		t.Fatalf("CreateIndex(IvfHnswFlat prefetch_distance): %v", err)
	}
}

// TestIndexFileVersionLegacy builds an IVF_PQ index with the legacy index
// file version.
func TestIndexFileVersionLegacy(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)
	cfg := lance.IvfPq{
		Partitions:    4,
		SubVectors:    4,
		VectorOptions: lance.VectorOptions{IndexFileVersion: "legacy"},
	}
	if err := ds.CreateIndex(ctx, "vec", cfg, lance.WithIndexName("vec_legacy")); err != nil {
		t.Fatalf("CreateIndex(IvfPq legacy): %v", err)
	}
	stats := indexStats(t, ds, "vec_legacy")
	if numPartitions(t, stats) != 4 {
		t.Fatalf("legacy IVF_PQ num_partitions = %d, want 4", numPartitions(t, stats))
	}
}

// TestCentroids precomputes k centroids in Go, passes them to build an
// IVF_FLAT index, and verifies the resulting partition count equals k.
func TestCentroids(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 512)

	const k = 6
	mem := testutil.Allocator()
	// Centroids is a FixedSizeList<float32, VecDim> with k rows. Use a
	// spread of the deterministic test vectors as centroids.
	bld := array.NewFixedSizeListBuilder(mem, testutil.VecDim, arrow.PrimitiveTypes.Float32)
	defer bld.Release()
	valB := bld.ValueBuilder().(*array.Float32Builder)
	for c := 0; c < k; c++ {
		bld.Append(true)
		base := float32(c * 80)
		for j := 0; j < testutil.VecDim; j++ {
			valB.Append(base + float32(j))
		}
	}
	centroids := bld.NewArray()
	defer centroids.Release()

	cfg := lance.IvfFlat{
		Partitions:    k,
		VectorOptions: lance.VectorOptions{Centroids: centroids},
	}
	if err := ds.CreateIndex(ctx, "vec", cfg, lance.WithIndexName("vec_cent")); err != nil {
		t.Fatalf("CreateIndex(IvfFlat centroids): %v", err)
	}
	stats := indexStats(t, ds, "vec_cent")
	if got := numPartitions(t, stats); got != k {
		t.Fatalf("IVF_FLAT with %d centroids has num_partitions = %d, want %d", k, got, k)
	}
}

// TestDescribeIndices builds two indices and checks describe surfaces
// rows_indexed == row count and sane details.
func TestDescribeIndices(t *testing.T) {
	ctx := t.Context()
	const rows = 256
	_, ds := writeDataset(t, rows)

	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex(BTree): %v", err)
	}
	if err := ds.CreateIndex(ctx, "vec", lance.IvfPq{Partitions: 4, SubVectors: 4},
		lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex(IvfPq): %v", err)
	}

	descs, err := ds.DescribeIndices(ctx, nil)
	if err != nil {
		t.Fatalf("DescribeIndices: %v", err)
	}
	if len(descs) != 2 {
		t.Fatalf("DescribeIndices returned %d indices, want 2: %+v", len(descs), descs)
	}
	byName := map[string]lance.IndexDescription{}
	for _, d := range descs {
		byName[d.Name] = d
	}
	for _, name := range []string{"id_idx", "vec_idx"} {
		d, ok := byName[name]
		if !ok {
			t.Fatalf("DescribeIndices missing %q: %+v", name, descs)
		}
		if d.RowsIndexed != rows {
			t.Errorf("%s rows_indexed = %d, want %d", name, d.RowsIndexed, rows)
		}
		if d.IndexType == "" || d.IndexType == "Unknown" {
			t.Errorf("%s has unhelpful index_type %q", name, d.IndexType)
		}
		if len(d.FieldIDs) == 0 {
			t.Errorf("%s has no field ids", name)
		}
		if len(d.Segments) == 0 {
			t.Errorf("%s has no segments", name)
		}
	}

	// Criteria filter by name.
	only, err := ds.DescribeIndices(ctx, &lance.IndexCriteria{HasName: "id_idx"})
	if err != nil {
		t.Fatalf("DescribeIndices(criteria): %v", err)
	}
	if len(only) != 1 || only[0].Name != "id_idx" {
		t.Fatalf("DescribeIndices(HasName id_idx) = %+v, want just id_idx", only)
	}
}

// TestLoadIndexByName checks load-by-name and load-by-uuid.
func TestLoadIndexByName(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 128)
	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex(BTree): %v", err)
	}

	info, err := ds.LoadIndexByName(ctx, "id_idx")
	if err != nil {
		t.Fatalf("LoadIndexByName: %v", err)
	}
	if info == nil || info.Name != "id_idx" || info.UUID == "" {
		t.Fatalf("LoadIndexByName = %+v, want id_idx with a uuid", info)
	}

	// Missing name -> nil, no error.
	missing, err := ds.LoadIndexByName(ctx, "nope")
	if err != nil {
		t.Fatalf("LoadIndexByName(missing): %v", err)
	}
	if missing != nil {
		t.Fatalf("LoadIndexByName(missing) = %+v, want nil", missing)
	}

	// Load by uuid round-trips.
	byUUID, err := ds.LoadIndex(ctx, info.UUID)
	if err != nil {
		t.Fatalf("LoadIndex(uuid): %v", err)
	}
	if byUUID == nil || byUUID.Name != "id_idx" {
		t.Fatalf("LoadIndex(uuid) = %+v, want id_idx", byUUID)
	}
}

// TestPrewarmFTS builds an inverted index with positions and prewarms it with
// FTS position options.
func TestPrewarmFTS(t *testing.T) {
	ctx := t.Context()
	ds := writeTextDataset(t)
	if err := ds.CreateIndex(ctx, "text", lance.Inverted{WithPosition: true},
		lance.WithIndexName("text_idx")); err != nil {
		t.Fatalf("CreateIndex(Inverted): %v", err)
	}
	if err := ds.PrewarmIndexWithOptions(ctx, "text_idx", lance.PrewarmFTS{WithPosition: true}); err != nil {
		t.Fatalf("PrewarmIndexWithOptions(FTS): %v", err)
	}
}

// TestOptimizeWithTransactionProperties runs optimize with transaction
// properties and asserts success.
func TestOptimizeWithTransactionProperties(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 128)
	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex(BTree): %v", err)
	}
	// Append more rows so optimize has delta data to fold in.
	ds2 := appendRows(t, uri, 128, 64)
	_ = ds2
	if err := ds.OptimizeIndices(ctx,
		lance.WithOptimizeTransactionProperties(map[string]string{"job_id": "wave-b3"})); err != nil {
		t.Fatalf("OptimizeIndices(transaction_properties): %v", err)
	}
}

// TestRemapIndexNoFragReuse exercises the RemapIndex FFI binding at runtime:
// with no prior deferred-remap compaction there is no fragment reuse index,
// so the call returns a clean wrapped error. This proves the binding links
// and marshals its arguments correctly (there is no other test that executes
// it).
func TestRemapIndexNoFragReuse(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 128)
	if err := ds.CreateIndex(ctx, "id", lance.BTree{}, lance.WithIndexName("id_idx")); err != nil {
		t.Fatalf("CreateIndex(BTree): %v", err)
	}
	err := ds.RemapIndex(ctx, "id", "id_idx")
	if err == nil {
		t.Fatal("RemapIndex without a prior deferred-remap compaction should fail")
	}
	// Wraps a sentinel so errors.Is classification keeps working.
	if !errors.Is(err, lance.ErrNotImplemented) && !errors.Is(err, lance.ErrIndex) &&
		!errors.Is(err, lance.ErrInvalidArgument) && !errors.Is(err, lance.ErrInternal) {
		t.Fatalf("RemapIndex error %v wraps no known sentinel", err)
	}
}

// TestRTreeErrorPath verifies RTree creation fails cleanly on a non-geometry
// column. Building a valid geoarrow column from arrow-go is disproportionate,
// so we exercise the creation error path (item 1 fallback). geoarrow support
// is compiled in (geo is in Lance's default features), so the failure is a
// schema/geometry error, not a "feature not enabled" error.
func TestRTreeErrorPath(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 32)
	err := ds.CreateIndex(ctx, "score", lance.RTree{}, lance.WithIndexName("score_rt"))
	if err == nil {
		t.Fatal("RTree index on a non-geometry column should fail")
	}
	if !errors.Is(err, lance.ErrInvalidArgument) && !errors.Is(err, lance.ErrIndex) {
		t.Fatalf("RTree error = %v, want ErrInvalidArgument or ErrIndex", err)
	}
}
