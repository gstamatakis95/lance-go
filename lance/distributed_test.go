package lance_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// sharedCentroids builds k IVF centroids as a FixedSizeList<float32, VecDim>,
// spread across the deterministic test-vector range. The caller must Release.
func sharedCentroids(mem memory.Allocator, k int) arrow.Array {
	bld := array.NewFixedSizeListBuilder(mem, testutil.VecDim, arrow.PrimitiveTypes.Float32)
	defer bld.Release()
	valB := bld.ValueBuilder().(*array.Float32Builder)
	for c := 0; c < k; c++ {
		bld.Append(true)
		base := float32(c * 40)
		for j := 0; j < testutil.VecDim; j++ {
			valB.Append(base + float32(j))
		}
	}
	return bld.NewArray()
}

// TestDistributedIVFIndex builds an IVF_FLAT index across two fragment subsets
// as separate uncommitted segments pinned to shared centroids, merges them,
// commits one logical index, and verifies vector search matches a single-shot
// build.
func TestDistributedIVFIndex(t *testing.T) {
	ctx := t.Context()
	const rows = 128
	const k = 4
	mem := testutil.Allocator()

	uri := filepath.Join(t.TempDir(), "dist_idx.lance")
	rdr := testutil.NewReader(mem, 0, rows, 64) // 64 rows/file -> 2 fragments (0, 1)
	defer rdr.Release()
	ds, err := lance.Write(ctx, uri, rdr, lance.WithMaxRowsPerFile(64))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })

	centroids := sharedCentroids(mem, k)
	defer centroids.Release()

	// Two workers each build a per-fragment segment with the shared centroids.
	cfg := func() lance.IvfFlat {
		return lance.IvfFlat{
			Partitions:    k,
			VectorOptions: lance.VectorOptions{Centroids: centroids},
		}
	}
	seg1, err := ds.CreateIndexUncommitted(ctx, "vec", cfg(),
		lance.WithFragments(0), lance.WithUncommittedName("vec_idx"))
	if err != nil {
		t.Fatalf("CreateIndexUncommitted(frag 0): %v", err)
	}
	seg2, err := ds.CreateIndexUncommitted(ctx, "vec", cfg(),
		lance.WithFragments(1), lance.WithUncommittedName("vec_idx"))
	if err != nil {
		t.Fatalf("CreateIndexUncommitted(frag 1): %v", err)
	}
	if seg1.Name() != "vec_idx" {
		t.Fatalf("segment name = %q, want vec_idx", seg1.Name())
	}
	if len(seg1.Bytes()) == 0 {
		t.Fatal("segment 1 has empty protobuf bytes")
	}

	// Driver merges the two segments and commits a single logical index.
	merged, err := ds.MergeIndexSegments(ctx, []*lance.IndexMetadata{seg1, seg2})
	if err != nil {
		t.Fatalf("MergeIndexSegments: %v", err)
	}
	if err := ds.CommitIndexSegments(ctx, "vec_idx", "vec", []*lance.IndexMetadata{merged}); err != nil {
		t.Fatalf("CommitIndexSegments: %v", err)
	}

	// The committed index covers all rows. This is the assertion that forces
	// BOTH per-fragment segments through pb-decode + merge + commit: if either
	// segment were dropped, its fragment's rows would be unindexed. (Vector
	// search alone can't prove this, since Lance brute-forces unindexed fragments,
	// and the 5-NN of vecOf(42) all live in fragment 0.)
	indices, err := ds.ListIndices(ctx)
	if err != nil {
		t.Fatalf("ListIndices: %v", err)
	}
	if len(indices) == 0 {
		t.Fatal("no indices after commit")
	}
	stats := indexStats(t, ds, "vec_idx")
	if got := statInt(t, stats, "num_indexed_rows"); got != rows {
		t.Fatalf("committed distributed index covers %d rows, want %d (a segment was lost in the round-trip)", got, rows)
	}
	if got := statInt(t, stats, "num_unindexed_rows"); got != 0 {
		t.Fatalf("committed distributed index has %d unindexed rows, want 0", got)
	}

	// Vector search via the distributed index matches a single-shot build.
	distIDs := idsOf(t, scanAll(t, ds.Scan().Nearest("vec", vecOf(42), 5).Nprobes(k)))
	if len(distIDs) != 5 {
		t.Fatalf("distributed search returned %d rows, want 5", len(distIDs))
	}

	singleURI := filepath.Join(t.TempDir(), "single_idx.lance")
	single := testutil.NewReader(mem, 0, rows, 64)
	defer single.Release()
	sds, err := lance.Write(ctx, singleURI, single, lance.WithMaxRowsPerFile(64))
	if err != nil {
		t.Fatalf("Write(single): %v", err)
	}
	t.Cleanup(func() { sds.Close() })
	if err := sds.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: k}, lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex(single): %v", err)
	}
	singleIDs := idsOf(t, scanAll(t, sds.Scan().Nearest("vec", vecOf(42), 5).Nprobes(k)))

	if !equalIDSet(distIDs, singleIDs) {
		t.Fatalf("distributed search %v != single-shot search %v", distIDs, singleIDs)
	}
	// The nearest neighbor to vecOf(42) is row 42.
	if !containsID(distIDs, 42) {
		t.Fatalf("distributed search %v does not contain the query row 42", distIDs)
	}
}

// TestReadIndexPartition reads one partition of a committed IVF index.
func TestReadIndexPartition(t *testing.T) {
	ctx := t.Context()
	const rows = 128
	const k = 4
	mem := testutil.Allocator()

	uri := filepath.Join(t.TempDir(), "part.lance")
	rdr := testutil.NewReader(mem, 0, rows, 64)
	defer rdr.Release()
	ds, err := lance.Write(ctx, uri, rdr, lance.WithMaxRowsPerFile(64))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })

	if err := ds.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: k}, lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Every partition read must succeed and together cover all rows.
	total := 0
	for p := uint64(0); p < uint64(k); p++ {
		reader, err := ds.ReadIndexPartition(ctx, "vec_idx", p, false)
		if err != nil {
			t.Fatalf("ReadIndexPartition(%d): %v", p, err)
		}
		recs, err := testutil.Collect(reader)
		if err != nil {
			reader.Release()
			t.Fatalf("collect partition %d: %v", p, err)
		}
		total += int(testutil.TotalRows(recs))
		testutil.ReleaseAll(recs)
		reader.Release()
	}
	if total != rows {
		t.Fatalf("index partitions cover %d rows, want %d", total, rows)
	}
}

// TestPlanCompaction plans a compaction over a fragmented dataset and checks
// the plan is valid JSON with tasks.
func TestPlanCompaction(t *testing.T) {
	ctx := t.Context()
	mem := testutil.Allocator()
	uri := filepath.Join(t.TempDir(), "compact.lance")

	// Many small fragments -> compaction candidates.
	rdr := testutil.NewReader(mem, 0, 200, 20)
	defer rdr.Release()
	ds, err := lance.Write(ctx, uri, rdr, lance.WithMaxRowsPerFile(20))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })

	plan, err := ds.PlanCompaction(ctx, lance.CompactionOptions{TargetRowsPerFragment: 200})
	if err != nil {
		t.Fatalf("PlanCompaction: %v", err)
	}
	if len(plan.Tasks) == 0 {
		t.Fatalf("compaction plan has no tasks: %s", string(plan.Raw))
	}
	if plan.ReadVersion == 0 {
		t.Fatalf("compaction plan read version is 0: %s", string(plan.Raw))
	}
	if len(plan.Raw) == 0 {
		t.Fatal("compaction plan Raw view is empty")
	}

	// Each task's opaque payload round-trips through JSON verbatim (the
	// worker round-trip contract).
	for i, task := range plan.Tasks {
		if len(task.Payload) == 0 {
			t.Fatalf("task %d has empty payload", i)
		}
		data, err := json.Marshal(task)
		if err != nil {
			t.Fatalf("marshal task %d: %v", i, err)
		}
		var roundTrip lance.CompactionTask
		if err := json.Unmarshal(data, &roundTrip); err != nil {
			t.Fatalf("unmarshal task %d: %v", i, err)
		}
		if string(roundTrip.Payload) != string(task.Payload) {
			t.Fatalf("task %d payload did not round-trip: got %s, want %s", i, roundTrip.Payload, task.Payload)
		}
	}
}

func equalIDSet(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[int64]int, len(a))
	for _, v := range a {
		m[v]++
	}
	for _, v := range b {
		m[v]--
	}
	for _, c := range m {
		if c != 0 {
			return false
		}
	}
	return true
}

// statInt reads an integer-valued index-statistics field (JSON numbers decode
// to float64).
func statInt(t *testing.T, stats map[string]any, key string) int {
	t.Helper()
	v, ok := stats[key]
	if !ok {
		t.Fatalf("index stats missing %q: %+v", key, stats)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("index stat %q is %T, want number", key, v)
	}
	return int(f)
}

func containsID(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
