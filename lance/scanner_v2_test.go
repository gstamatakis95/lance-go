package lance_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// writeGroupedDataset writes a small dataset with columns id (int64), cat
// (utf8), val (int64, NULL where id%5 == 4) for the ordering / aggregation
// tests. cat is "even" or "odd" by id parity. val is id*10.
func writeGroupedDataset(t *testing.T, rows int64) *lance.Dataset {
	t.Helper()
	mem := testutil.Allocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cat", Type: arrow.BinaryTypes.String},
		{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
		if i%2 == 0 {
			b.Field(1).(*array.StringBuilder).Append("even")
		} else {
			b.Field(1).(*array.StringBuilder).Append("odd")
		}
		if i%5 == 4 {
			b.Field(2).(*array.Int64Builder).AppendNull()
		} else {
			b.Field(2).(*array.Int64Builder).Append(i * 10)
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()
	uri := filepath.Join(t.TempDir(), "grouped.lance")
	ds, err := lance.Write(t.Context(), uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return ds
}

// int64Column collects column name from recs as values plus a null mask.
func int64Column(t *testing.T, recs []arrow.RecordBatch, name string) (vals []int64, valid []bool) {
	t.Helper()
	for _, rec := range recs {
		idxs := rec.Schema().FieldIndices(name)
		if len(idxs) == 0 {
			t.Fatalf("column %q not in result schema %v", name, rec.Schema())
		}
		col := rec.Column(idxs[0]).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			vals = append(vals, col.Value(i))
			valid = append(valid, col.IsValid(i))
		}
	}
	return vals, valid
}

func TestOrderBy(t *testing.T) {
	ds := writeGroupedDataset(t, 20)

	// Ascending on val: non-NULL values ascending, NULLs last by default.
	recs := scanAll(t, ds.Scan().OrderBy("val"))
	vals, valid := int64Column(t, recs, "val")
	if len(vals) != 20 {
		t.Fatalf("order_by returned %d rows, want 20", len(vals))
	}
	nulls := 0
	prev := int64(-1)
	for i := range vals {
		if !valid[i] {
			nulls++
			continue
		}
		if nulls > 0 {
			t.Fatalf("row %d: non-NULL value after NULLs (nulls must sort last)", i)
		}
		if vals[i] < prev {
			t.Fatalf("row %d: val %d < previous %d (not ascending)", i, vals[i], prev)
		}
		prev = vals[i]
	}
	if nulls != 4 {
		t.Fatalf("saw %d NULLs, want 4", nulls)
	}

	// Descending with NULLs first.
	recs = scanAll(t, ds.Scan().OrderByDesc("val", lance.NullsFirst(true)))
	vals, valid = int64Column(t, recs, "val")
	for i := 0; i < 4; i++ {
		if valid[i] {
			t.Fatalf("row %d: expected NULL first in desc/nulls-first order", i)
		}
	}
	prev = int64(1 << 62)
	for i := 4; i < len(vals); i++ {
		if !valid[i] {
			t.Fatalf("row %d: NULL after the leading NULL block", i)
		}
		if vals[i] > prev {
			t.Fatalf("row %d: val %d > previous %d (not descending)", i, vals[i], prev)
		}
		prev = vals[i]
	}

	// Ordering by an unknown column fails cleanly.
	if _, err := ds.Scan().OrderBy("nope").Reader(t.Context()); err == nil {
		t.Fatal("order_by on unknown column should fail")
	}
}

func TestAggregate(t *testing.T) {
	ds := writeGroupedDataset(t, 20)

	// Group by cat, count all rows and sum val. Compare with the manually
	// computed values. ids 0..19: 10 even / 10 odd. val = id*10 with NULLs
	// at id%5==4 (ids 4, 9, 14, 19 -> two per group).
	rec, err := ds.Scan().Aggregate(lance.AggSpec{
		GroupBy: []string{"cat"},
		Aggregates: []lance.AggFunc{
			{Func: lance.AggCount, Alias: "n"},
			{Func: lance.AggCount, Column: "val", Alias: "n_val"},
			{Func: lance.AggSum, Column: "val", Alias: "total"},
		},
	}).Batch(t.Context())
	if err != nil {
		t.Fatalf("aggregate scan: %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 2 {
		t.Fatalf("aggregate returned %d groups, want 2", rec.NumRows())
	}
	catCol := rec.Column(rec.Schema().FieldIndices("cat")[0]).(*array.String)
	nCol := rec.Column(rec.Schema().FieldIndices("n")[0]).(*array.Int64)
	nValCol := rec.Column(rec.Schema().FieldIndices("n_val")[0]).(*array.Int64)
	totalCol := rec.Column(rec.Schema().FieldIndices("total")[0]).(*array.Int64)
	// even ids: 0,2,..,18 sum*10 = 900, minus NULLs at 4 and 14 -> 900-180 = 720
	// odd ids: 1,3,..,19 sum*10 = 1000, minus NULLs at 9 and 19 -> 1000-280 = 720
	want := map[string]struct{ n, nVal, total int64 }{
		"even": {10, 8, 720},
		"odd":  {10, 8, 720},
	}
	for i := 0; i < int(rec.NumRows()); i++ {
		w, ok := want[catCol.Value(i)]
		if !ok {
			t.Fatalf("unexpected group %q", catCol.Value(i))
		}
		if nCol.Value(i) != w.n || nValCol.Value(i) != w.nVal || totalCol.Value(i) != w.total {
			t.Fatalf("group %q = (n=%d, n_val=%d, total=%d), want %+v",
				catCol.Value(i), nCol.Value(i), nValCol.Value(i), totalCol.Value(i), w)
		}
	}

	// Global (ungrouped) min/max/avg over id with a filter applied first.
	rec2, err := ds.Scan().Filter("id >= 10").Aggregate(lance.AggSpec{
		Aggregates: []lance.AggFunc{
			{Func: lance.AggMin, Column: "id", Alias: "lo"},
			{Func: lance.AggMax, Column: "id", Alias: "hi"},
			{Func: lance.AggAvg, Column: "id", Alias: "mean"},
		},
	}).Batch(t.Context())
	if err != nil {
		t.Fatalf("global aggregate: %v", err)
	}
	defer rec2.Release()
	if rec2.NumRows() != 1 {
		t.Fatalf("global aggregate returned %d rows, want 1", rec2.NumRows())
	}
	lo := rec2.Column(rec2.Schema().FieldIndices("lo")[0]).(*array.Int64).Value(0)
	hi := rec2.Column(rec2.Schema().FieldIndices("hi")[0]).(*array.Int64).Value(0)
	mean := rec2.Column(rec2.Schema().FieldIndices("mean")[0]).(*array.Float64).Value(0)
	if lo != 10 || hi != 19 || mean != 14.5 {
		t.Fatalf("min/max/avg = %d/%d/%v, want 10/19/14.5", lo, hi, mean)
	}

	// Aggregates that need a column reject specs without one.
	if _, err := ds.Scan().Aggregate(lance.AggSpec{
		Aggregates: []lance.AggFunc{{Func: lance.AggSum}},
	}).Batch(t.Context()); err == nil {
		t.Fatal("sum without a column should fail")
	}

	// Without an alias the DataFusion default output name is kept.
	rec3, err := ds.Scan().Aggregate(lance.AggSpec{
		Aggregates: []lance.AggFunc{{Func: lance.AggSum, Column: "val"}},
	}).Batch(t.Context())
	if err != nil {
		t.Fatalf("unaliased aggregate: %v", err)
	}
	defer rec3.Release()
	if len(rec3.Schema().FieldIndices("sum(val)")) == 0 {
		t.Fatalf("unaliased sum output column not named \"sum(val)\": %v", rec3.Schema())
	}
}

func TestProjectExpr(t *testing.T) {
	_, ds := writeDataset(t, 30)

	recs := scanAll(t, ds.Scan().
		ProjectExpr("id", "id").
		ProjectExpr("double_id", "id * 2").
		ScanInOrder(true))
	ids, _ := int64Column(t, recs, "id")
	doubled, _ := int64Column(t, recs, "double_id")
	if len(ids) != 30 {
		t.Fatalf("projected scan returned %d rows, want 30", len(ids))
	}
	for i := range ids {
		if doubled[i] != 2*ids[i] {
			t.Fatalf("row %d: double_id = %d, want %d", i, doubled[i], 2*ids[i])
		}
	}
	// Only the projected columns come back.
	if got := len(recs[0].Schema().Fields()); got != 2 {
		t.Fatalf("projected schema has %d fields, want 2: %v", got, recs[0].Schema())
	}

	// Columns and ProjectExpr are mutually exclusive.
	if _, err := ds.Scan().Columns("id").ProjectExpr("d", "id * 2").Reader(t.Context()); err == nil {
		t.Fatal("columns + projection_exprs should fail")
	}
}

func TestWithRowAddress(t *testing.T) {
	_, ds := writeDataset(t, 10)
	recs := scanAll(t, ds.Scan().WithRowAddress())
	for _, rec := range recs {
		if len(rec.Schema().FieldIndices("_rowaddr")) == 0 {
			t.Fatalf("no _rowaddr column in schema %v", rec.Schema())
		}
	}
}

func TestDistanceRange(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 256)
	if err := ds.CreateIndex(ctx, "vec", lance.IvfFlat{Partitions: 4}); err != nil {
		t.Fatalf("CreateIndex(IvfFlat): %v", err)
	}

	// Query at row 10's vector. Row g's vector is [g, g+1, ...], so the L2
	// distance to row g is 16*(g-10)^2: 0, 16, 64, 144, ... for |g-10| =
	// 0, 1, 2, 3, ... (rows 10 / 9,11 / 8,12 / 7,13 / ...). lance's range is
	// [lower, upper): lower inclusive, upper exclusive.
	query := make([]float32, testutil.VecDim)
	for j := range query {
		query[j] = float32(10 + j)
	}
	base := ds.Scan().Nearest("vec", query, 10).Nprobes(4)

	recs := scanAll(t, ds.Scan().Nearest("vec", query, 10).Nprobes(4))
	if n := testutil.TotalRows(recs); n != 10 {
		t.Fatalf("unrestricted ANN returned %d rows, want 10", n)
	}

	// Upper bound 100 (exclusive) keeps dist < 100, i.e. |g-10| <= 2:
	// rows 8..12 (distances 0, 16, 16, 64, 64).
	recs = scanAll(t, base.DistanceRange(nil, ptr(float32(100))))
	ids, _ := int64Column(t, recs, "id")
	if len(ids) != 5 {
		t.Fatalf("distance < 100 returned ids %v, want 5 rows (8..12)", ids)
	}
	for _, id := range ids {
		if id < 8 || id > 12 {
			t.Fatalf("distance < 100 returned id %d outside 8..12", id)
		}
	}

	// Lower bound restricts the near side: [20, 100) drops distances 0 and
	// 16 (rows 10, 9, 11), keeping only distance 64 (rows 8, 12).
	recs = scanAll(t, base.DistanceRange(ptr(float32(20)), ptr(float32(100))))
	ids, _ = int64Column(t, recs, "id")
	if len(ids) != 2 {
		t.Fatalf("20 <= distance < 100 returned ids %v, want 2 rows (8, 12)", ids)
	}
	for _, id := range ids {
		if id != 8 && id != 12 {
			t.Fatalf("20 <= distance < 100 returned id %d, want 8 or 12", id)
		}
	}
}

func TestScanFragments(t *testing.T) {
	// 96 rows with at most 32 per file -> fragments 0, 1, 2.
	_, ds := writeDataset(t, 96, lance.WithMaxRowsPerFile(32))

	recs := scanAll(t, ds.Scan().ScanFragments(1).ScanInOrder(true))
	assertRows(t, recs, seq(32, 32))

	recs = scanAll(t, ds.Scan().ScanFragments(2, 0).ScanInOrder(true))
	if n := testutil.TotalRows(recs); n != 64 {
		t.Fatalf("two-fragment scan returned %d rows, want 64", n)
	}

	_, err := ds.Scan().ScanFragments(99).Reader(t.Context())
	if !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("unknown fragment id error = %v, want ErrNotFound", err)
	}
}

func TestIncludeDeletedRows(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)
	if _, err := ds.Delete(ctx, "id < 10"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	recs := scanAll(t, ds.Scan().Columns("id"))
	if n := testutil.TotalRows(recs); n != 90 {
		t.Fatalf("normal scan returned %d rows, want 90", n)
	}

	recs = scanAll(t, ds.Scan().Columns("id").WithRowID().IncludeDeletedRows())
	if n := testutil.TotalRows(recs); n != 100 {
		t.Fatalf("include_deleted_rows scan returned %d rows, want 100", n)
	}
	nullRowIDs := 0
	for _, rec := range recs {
		rowIDCol := rec.Column(rec.Schema().FieldIndices("_rowid")[0])
		nullRowIDs += rowIDCol.NullN()
	}
	if nullRowIDs != 10 {
		t.Fatalf("deleted rows with NULL _rowid = %d, want 10", nullRowIDs)
	}
}

func TestStrictBatchSize(t *testing.T) {
	_, ds := writeDataset(t, 100)
	rdr, err := ds.Scan().Columns("id").BatchSize(7).StrictBatchSize(true).Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rdr.Release()
	var sizes []int64
	for rdr.Next() {
		sizes = append(sizes, rdr.RecordBatch().NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	var total int64
	for i, n := range sizes {
		total += n
		if i < len(sizes)-1 && n != 7 {
			t.Fatalf("batch %d has %d rows, want exactly 7: %v", i, n, sizes)
		}
	}
	if total != 100 {
		t.Fatalf("strict batches sum to %d rows, want 100", total)
	}
	if last := sizes[len(sizes)-1]; last != 100%7 {
		t.Fatalf("last batch has %d rows, want %d", last, 100%7)
	}
}

func TestBatchTerminal(t *testing.T) {
	_, ds := writeDataset(t, 100)
	rec, err := ds.Scan().ScanInOrder(true).Batch(t.Context())
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 100 {
		t.Fatalf("Batch returned %d rows, want 100", rec.NumRows())
	}
	assertRows(t, []arrow.RecordBatch{rec}, seq(0, 100))
}

func TestAnalyzePlan(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 50)

	plan, err := ds.Scan().Filter("id < 25").AnalyzePlan(ctx)
	if err != nil {
		t.Fatalf("AnalyzePlan: %v", err)
	}
	if plan == "" || !strings.Contains(plan, "metrics") {
		t.Fatalf("AnalyzePlan output has no execution metrics: %q", plan)
	}

	countPlan, err := ds.Scan().Filter("id < 25").AnalyzeCountPlan(ctx)
	if err != nil {
		t.Fatalf("AnalyzeCountPlan: %v", err)
	}
	if countPlan == "" || !strings.Contains(countPlan, "metrics") {
		t.Fatalf("AnalyzeCountPlan output has no execution metrics: %q", countPlan)
	}
	if !strings.Contains(strings.ToLower(countPlan), "aggregate") {
		t.Fatalf("AnalyzeCountPlan output has no aggregate node: %q", countPlan)
	}
}

func TestFilterSubstrait(t *testing.T) {
	// Generating a Substrait ExtendedExpression in-test needs a full
	// Substrait producer, so only the error path is exercised: invalid
	// bytes must fail cleanly (proving the substrait feature is compiled
	// in rather than returning "not supported").
	_, ds := writeDataset(t, 10)
	_, err := ds.Scan().FilterSubstrait([]byte("not a substrait message")).Reader(t.Context())
	if err == nil {
		t.Fatal("invalid substrait filter should fail")
	}
	if strings.Contains(err.Error(), "not supported in this build") {
		t.Fatalf("substrait feature not compiled in: %v", err)
	}

	// Filter and FilterSubstrait are mutually exclusive.
	_, err = ds.Scan().Filter("id < 5").FilterSubstrait([]byte{1}).Reader(t.Context())
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("filter + filter_substrait error = %v, want ErrInvalidArgument", err)
	}
}

func TestPerfKnobsSmoke(t *testing.T) {
	// The performance/tuning knobs must be accepted and leave scan results
	// intact.
	_, ds := writeDataset(t, 100)
	recs := scanAll(t, ds.Scan().
		IOBufferSize(64<<20).
		BatchReadahead(4).
		FragmentReadahead(2).
		TargetParallelism(1).
		BatchSizeBytes(1<<20).
		UseStats(true).
		UseScalarIndex(true).
		MaterializationStyle(lance.MaterializationAllEarly).
		BlobHandling(lance.BlobsDescriptions).
		ScanInOrder(true))
	assertRows(t, recs, seq(0, 100))

	// Vector-search-scoped knobs (query_parallelism, approx_mode) smoke.
	query := make([]float32, testutil.VecDim)
	recs = scanAll(t, ds.Scan().
		Nearest("vec", query, 5).
		QueryParallelism(1).
		ApproxMode(lance.ApproxModeNormal))
	if n := testutil.TotalRows(recs); n != 5 {
		t.Fatalf("ANN with perf knobs returned %d rows, want 5", n)
	}

	// Invalid enum values fail cleanly.
	if _, err := ds.Scan().MaterializationStyle("bogus").Reader(t.Context()); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("bogus materialization_style error = %v, want ErrInvalidArgument", err)
	}
	if _, err := ds.Scan().BlobHandling("bogus").Reader(t.Context()); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("bogus blob_handling error = %v, want ErrInvalidArgument", err)
	}
	if _, err := ds.Scan().Nearest("vec", query, 5).ApproxMode("bogus").Reader(t.Context()); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("bogus approx_mode error = %v, want ErrInvalidArgument", err)
	}
	if _, err := ds.Scan().IndexSegments("not-a-uuid").Reader(t.Context()); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("bad index segment uuid error = %v, want ErrInvalidArgument", err)
	}
}

func TestDisableScoringAutoprojection(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 50)
	query := make([]float32, testutil.VecDim)

	// By default an explicit projection still gains _distance.
	recs := scanAll(t, ds.Scan().Nearest("vec", query, 5).Columns("id"))
	if len(recs[0].Schema().FieldIndices("_distance")) == 0 {
		t.Fatalf("expected autoprojected _distance column: %v", recs[0].Schema())
	}

	rec, err := ds.Scan().Nearest("vec", query, 5).Columns("id").
		DisableScoringAutoprojection().Batch(ctx)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	defer rec.Release()
	if len(rec.Schema().FieldIndices("_distance")) != 0 {
		t.Fatalf("_distance still projected after DisableScoringAutoprojection: %v", rec.Schema())
	}
}
