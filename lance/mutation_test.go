package lance_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// newSourceReader builds merge-insert source rows with ids
// startID..startID+rows-1 whose values differ from the deterministic
// testutil rows: name "src-g", score g*10, vec filled with -g.
func newSourceReader(t *testing.T, startID, rows int64) array.RecordReader {
	t.Helper()
	rec := newSourceRecord(testutil.Allocator(), startID, rows)
	defer rec.Release()
	rdr, err := array.NewRecordReader(testutil.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

func newSourceRecord(mem memory.Allocator, startID, rows int64) arrow.RecordBatch {
	b := array.NewRecordBuilder(mem, testutil.Schema())
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	nameB := b.Field(1).(*array.StringBuilder)
	scoreB := b.Field(2).(*array.Float64Builder)
	vecB := b.Field(3).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i := int64(0); i < rows; i++ {
		g := startID + i
		idB.Append(g)
		nameB.Append(fmt.Sprintf("src-%d", g))
		scoreB.Append(float64(g) * 10)
		vecB.Append(true)
		for j := 0; j < testutil.VecDim; j++ {
			valB.Append(float32(-g))
		}
	}
	return b.NewRecordBatch()
}

type rowValues struct {
	name  string
	score float64
}

// rowsByID scans ds and indexes name/score by id.
func rowsByID(t *testing.T, ds *lance.Dataset) map[int64]rowValues {
	t.Helper()
	recs := scanAll(t, ds.Scan().Columns("id", "name", "score"))
	rows := make(map[int64]rowValues)
	for _, rec := range recs {
		schema := rec.Schema()
		idCol := rec.Column(schema.FieldIndices("id")[0]).(*array.Int64)
		nameCol := rec.Column(schema.FieldIndices("name")[0]).(*array.String)
		scoreCol := rec.Column(schema.FieldIndices("score")[0]).(*array.Float64)
		for i := 0; i < int(rec.NumRows()); i++ {
			id := idCol.Value(i)
			if _, dup := rows[id]; dup {
				t.Fatalf("duplicate id %d in scan results", id)
			}
			rows[id] = rowValues{name: nameCol.Value(i), score: scoreCol.Value(i)}
		}
	}
	return rows
}

// version returns the current version number of ds.
func version(t *testing.T, ds *lance.Dataset) uint64 {
	t.Helper()
	info, err := ds.Version(t.Context())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	return info.Version
}

// mustCount counts all rows through the same handle.
func mustCount(t *testing.T, ds *lance.Dataset) uint64 {
	t.Helper()
	count, err := ds.CountRows(t.Context(), "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	return count
}

func TestDelete(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)
	v0 := version(t, ds)

	res, err := ds.Delete(ctx, "id < 50")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if res.NumDeletedRows != 50 {
		t.Fatalf("NumDeletedRows = %d, want 50", res.NumDeletedRows)
	}
	// The SAME handle must observe the new version without reopening.
	if got := mustCount(t, ds); got != 50 {
		t.Fatalf("CountRows after delete = %d, want 50", got)
	}
	if v1 := version(t, ds); v1 <= v0 {
		t.Fatalf("version after delete = %d, want > %d", v1, v0)
	}
	assertRows(t, scanAll(t, ds.Scan()), seq(50, 50))

	// An always-false predicate deletes nothing.
	res, err = ds.Delete(ctx, "id > 1000000")
	if err != nil {
		t.Fatalf("Delete (always-false): %v", err)
	}
	if res.NumDeletedRows != 0 {
		t.Fatalf("NumDeletedRows = %d, want 0", res.NumDeletedRows)
	}
	if got := mustCount(t, ds); got != 50 {
		t.Fatalf("CountRows after no-op delete = %d, want 50", got)
	}
}

func TestDeleteErrors(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	if _, err := ds.Delete(ctx, ""); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Delete(empty) error = %v, want ErrInvalidArgument", err)
	}

	_, err := ds.Delete(ctx, "no_such_column < 5")
	if err == nil {
		t.Fatal("Delete with bad predicate succeeded, want error")
	}
	if !strings.Contains(err.Error(), "no_such_column") {
		t.Fatalf("Delete error %q does not mention the bad column", err)
	}
	// A failed delete must not change the data.
	if got := mustCount(t, ds); got != 10 {
		t.Fatalf("CountRows after failed delete = %d, want 10", got)
	}
}

func TestUpdateWhere(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)
	v0 := version(t, ds)

	res, err := ds.Update(ctx, lance.UpdateSpec{
		Set:   map[string]string{"score": "score * 2"},
		Where: "id < 10",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res.RowsUpdated != 10 {
		t.Fatalf("RowsUpdated = %d, want 10", res.RowsUpdated)
	}
	if v1 := version(t, ds); v1 <= v0 {
		t.Fatalf("version after update = %d, want > %d", v1, v0)
	}

	rows := rowsByID(t, ds)
	if len(rows) != 100 {
		t.Fatalf("scan returned %d distinct ids, want 100", len(rows))
	}
	for id := int64(0); id < 100; id++ {
		want := float64(id) / 2.0 // original testutil score
		if id < 10 {
			want = float64(id) // doubled
		}
		if got := rows[id].score; got != want {
			t.Fatalf("id %d: score = %v, want %v", id, got, want)
		}
		if want := fmt.Sprintf("row-%d", id); rows[id].name != want {
			t.Fatalf("id %d: name = %q, want %q (must be untouched)", id, rows[id].name, want)
		}
	}
}

func TestUpdateAllRows(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	res, err := ds.Update(ctx, lance.UpdateSpec{
		Set: map[string]string{"score": "score + 1"},
	})
	if err != nil {
		t.Fatalf("Update (all rows): %v", err)
	}
	if res.RowsUpdated != 100 {
		t.Fatalf("RowsUpdated = %d, want 100", res.RowsUpdated)
	}
	rows := rowsByID(t, ds)
	for id := int64(0); id < 100; id++ {
		if want := float64(id)/2.0 + 1; rows[id].score != want {
			t.Fatalf("id %d: score = %v, want %v", id, rows[id].score, want)
		}
	}
}

func TestUpdateErrors(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	if _, err := ds.Update(ctx, lance.UpdateSpec{}); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Update(empty Set) error = %v, want ErrInvalidArgument", err)
	}

	_, err := ds.Update(ctx, lance.UpdateSpec{Set: map[string]string{"no_such_column": "1"}})
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Update(bad column) error = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "no_such_column") {
		t.Fatalf("Update error %q does not mention the bad column", err)
	}

	_, err = ds.Update(ctx, lance.UpdateSpec{
		Set:   map[string]string{"score": "score + 1"},
		Where: "no_such_column < 5",
	})
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Update(bad where) error = %v, want ErrInvalidArgument", err)
	}
	if got := mustCount(t, ds); got != 10 {
		t.Fatalf("CountRows after failed updates = %d, want 10", got)
	}
}

func TestMergeInsertUpsert(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)
	v0 := version(t, ds)

	src := newSourceReader(t, 50, 100) // ids 50..149
	defer src.Release()
	stats, err := ds.MergeInsert("id").
		WhenMatchedUpdateAll().
		WhenNotMatchedInsertAll().
		Execute(ctx, src)
	if err != nil {
		t.Fatalf("MergeInsert upsert: %v", err)
	}
	if stats.NumUpdatedRows != 50 {
		t.Fatalf("NumUpdatedRows = %d, want 50", stats.NumUpdatedRows)
	}
	if stats.NumInsertedRows != 50 {
		t.Fatalf("NumInsertedRows = %d, want 50", stats.NumInsertedRows)
	}
	if stats.NumDeletedRows != 0 {
		t.Fatalf("NumDeletedRows = %d, want 0", stats.NumDeletedRows)
	}
	if stats.NumAttempts < 1 {
		t.Fatalf("NumAttempts = %d, want >= 1", stats.NumAttempts)
	}
	if stats.BytesWritten == 0 || stats.NumFilesWritten == 0 {
		t.Fatalf("BytesWritten = %d / NumFilesWritten = %d, want > 0", stats.BytesWritten, stats.NumFilesWritten)
	}

	if got := mustCount(t, ds); got != 150 {
		t.Fatalf("CountRows after upsert = %d, want 150", got)
	}
	if v1 := version(t, ds); v1 <= v0 {
		t.Fatalf("version after merge-insert = %d, want > %d", v1, v0)
	}

	rows := rowsByID(t, ds)
	if len(rows) != 150 {
		t.Fatalf("scan returned %d distinct ids, want 150", len(rows))
	}
	for id := int64(0); id < 150; id++ {
		got := rows[id]
		if id < 50 {
			// Untouched original rows.
			if want := fmt.Sprintf("row-%d", id); got.name != want || got.score != float64(id)/2.0 {
				t.Fatalf("id %d = %+v, want original row", id, got)
			}
		} else {
			// Updated (50..99) and inserted (100..149) source rows.
			if want := fmt.Sprintf("src-%d", id); got.name != want || got.score != float64(id)*10 {
				t.Fatalf("id %d = %+v, want source row", id, got)
			}
		}
	}
}

func TestMergeInsertDeleteNotMatchedBySource(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	src := newSourceReader(t, 0, 50) // ids 0..49
	defer src.Release()
	stats, err := ds.MergeInsert("id").
		WhenMatchedDoNothing().
		WhenNotMatchedDoNothing().
		WhenNotMatchedBySourceDelete().
		Execute(ctx, src)
	if err != nil {
		t.Fatalf("MergeInsert delete-not-matched-by-source: %v", err)
	}
	if stats.NumDeletedRows != 50 {
		t.Fatalf("NumDeletedRows = %d, want 50", stats.NumDeletedRows)
	}
	if stats.NumUpdatedRows != 0 || stats.NumInsertedRows != 0 {
		t.Fatalf("NumUpdatedRows = %d / NumInsertedRows = %d, want 0/0", stats.NumUpdatedRows, stats.NumInsertedRows)
	}
	if got := mustCount(t, ds); got != 50 {
		t.Fatalf("CountRows = %d, want 50", got)
	}
	// The kept rows must be the untouched originals (matched rows were
	// DoNothing).
	assertRows(t, scanAll(t, ds.Scan()), seq(0, 50))
}

func TestMergeInsertUpdateIf(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	src := newSourceReader(t, 50, 100) // ids 50..149
	defer src.Release()
	stats, err := ds.MergeInsert("id").
		WhenMatchedUpdateIf("target.id >= 75").
		WhenNotMatchedInsertAll().
		Execute(ctx, src)
	if err != nil {
		t.Fatalf("MergeInsert update-if: %v", err)
	}
	if stats.NumUpdatedRows != 25 {
		t.Fatalf("NumUpdatedRows = %d, want 25", stats.NumUpdatedRows)
	}
	if stats.NumInsertedRows != 50 {
		t.Fatalf("NumInsertedRows = %d, want 50", stats.NumInsertedRows)
	}
	if got := mustCount(t, ds); got != 150 {
		t.Fatalf("CountRows = %d, want 150", got)
	}

	rows := rowsByID(t, ds)
	for id := int64(0); id < 150; id++ {
		wantSource := id >= 75 // 75..99 updated, 100..149 inserted
		wantName := fmt.Sprintf("row-%d", id)
		if wantSource {
			wantName = fmt.Sprintf("src-%d", id)
		}
		if rows[id].name != wantName {
			t.Fatalf("id %d: name = %q, want %q", id, rows[id].name, wantName)
		}
	}
}

func TestMergeInsertErrors(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	// Join key that does not exist in the schema.
	src := newSourceReader(t, 0, 5)
	defer src.Release()
	_, err := ds.MergeInsert("no_such_column").
		WhenMatchedUpdateAll().
		Execute(ctx, src)
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("MergeInsert(bad key) error = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "no_such_column") {
		t.Fatalf("MergeInsert error %q does not mention the bad column", err)
	}

	// A configuration that cannot change anything is rejected.
	src2 := newSourceReader(t, 0, 5)
	defer src2.Release()
	_, err = ds.MergeInsert("id").
		WhenMatchedDoNothing().
		WhenNotMatchedDoNothing().
		WhenNotMatchedBySourceKeep().
		Execute(ctx, src2)
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("MergeInsert(no-op config) error = %v, want ErrInvalidArgument", err)
	}

	if got := mustCount(t, ds); got != 10 {
		t.Fatalf("CountRows after failed merges = %d, want 10", got)
	}
}

// newKeyedSourceReader builds a merge-insert source with the given ids and
// scores (one row per element, ids and scores must be the same length). The
// name column is "src-<id>" and the vec column is filled with -id. Buffers use
// the C mallocator so they may cross the Arrow C Data Interface.
func newKeyedSourceReader(t *testing.T, ids []int64, scores []float64) array.RecordReader {
	t.Helper()
	if len(ids) != len(scores) {
		t.Fatalf("ids/scores length mismatch: %d vs %d", len(ids), len(scores))
	}
	b := array.NewRecordBuilder(testutil.Allocator(), testutil.Schema())
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	nameB := b.Field(1).(*array.StringBuilder)
	scoreB := b.Field(2).(*array.Float64Builder)
	vecB := b.Field(3).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i, id := range ids {
		idB.Append(id)
		nameB.Append(fmt.Sprintf("src-%d", id))
		scoreB.Append(scores[i])
		vecB.Append(true)
		for j := 0; j < testutil.VecDim; j++ {
			valB.Append(float32(-id))
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(testutil.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

// TestMergeInsertSourceDedupeBehavior exercises the correctness knob that
// controls how duplicate source keys matching the same target row are handled.
// The dedupe check only fires on rows that MATCH an existing target row, so the
// target must already contain the duplicated keys and WhenMatchedUpdateAll is
// used to reach the dedupe path.
func TestMergeInsertSourceDedupeBehavior(t *testing.T) {
	// Source mirrors Lance's own first-seen test: key 2 appears three times
	// (scores 100, 200, 300), key 3 twice (400, 500), key 5 once (600, a new
	// insert). Keys 2 and 3 match the target (writeDataset gives ids 0..4).
	dupIDs := []int64{2, 2, 2, 3, 3, 5}
	dupScores := []float64{100, 200, 300, 400, 500, 600}

	t.Run("fail_default_errors", func(t *testing.T) {
		ctx := t.Context()
		_, ds := writeDataset(t, 5)
		src := newKeyedSourceReader(t, dupIDs, dupScores)
		defer src.Release()
		// Default (no SourceDedupeBehavior set) is Fail.
		_, err := ds.MergeInsert("id").
			WhenMatchedUpdateAll().
			WhenNotMatchedInsertAll().
			Execute(ctx, src)
		if err == nil {
			t.Fatal("MergeInsert with default (Fail) dedupe succeeded, want error on duplicate source keys")
		}
		// The failed merge must not have changed the data.
		if got := mustCount(t, ds); got != 5 {
			t.Fatalf("CountRows after failed merge = %d, want 5", got)
		}
	})

	t.Run("fail_explicit_errors", func(t *testing.T) {
		ctx := t.Context()
		_, ds := writeDataset(t, 5)
		src := newKeyedSourceReader(t, dupIDs, dupScores)
		defer src.Release()
		_, err := ds.MergeInsert("id").
			WhenMatchedUpdateAll().
			WhenNotMatchedInsertAll().
			SourceDedupeBehavior(lance.SourceDedupeFail).
			Execute(ctx, src)
		if err == nil {
			t.Fatal("MergeInsert with explicit Fail dedupe succeeded, want error")
		}
	})

	t.Run("first_seen_skips_duplicates", func(t *testing.T) {
		ctx := t.Context()
		_, ds := writeDataset(t, 5)
		src := newKeyedSourceReader(t, dupIDs, dupScores)
		defer src.Release()
		stats, err := ds.MergeInsert("id").
			WhenMatchedUpdateAll().
			WhenNotMatchedInsertAll().
			SourceDedupeBehavior(lance.SourceDedupeFirstSeen).
			Execute(ctx, src)
		if err != nil {
			t.Fatalf("MergeInsert first-seen: %v", err)
		}
		// 2 extra rows for key 2 and 1 extra for key 3 are skipped.
		if stats.NumSkippedDuplicates != 3 {
			t.Fatalf("NumSkippedDuplicates = %d, want 3", stats.NumSkippedDuplicates)
		}
		if stats.NumUpdatedRows != 2 {
			t.Fatalf("NumUpdatedRows = %d, want 2 (keys 2 and 3)", stats.NumUpdatedRows)
		}
		if stats.NumInsertedRows != 1 {
			t.Fatalf("NumInsertedRows = %d, want 1 (key 5)", stats.NumInsertedRows)
		}
		if got := mustCount(t, ds); got != 6 {
			t.Fatalf("CountRows after first-seen merge = %d, want 6", got)
		}
		// The first-seen source value must have landed for the matched keys,
		// and untouched keys must keep their original testutil score (id/2).
		rows := rowsByID(t, ds)
		want := map[int64]float64{
			0: 0.0 / 2, 1: 1.0 / 2, 4: 4.0 / 2, // untouched originals
			2: 100, // first seen for key 2 (not 200 or 300)
			3: 400, // first seen for key 3 (not 500)
			5: 600, // inserted
		}
		for id, wantScore := range want {
			if got := rows[id].score; got != wantScore {
				t.Fatalf("id %d: score = %v, want %v", id, got, wantScore)
			}
		}
	})
}

// TestMergeInsertParityOptions smoke-tests the newly bound options
// (UseIndex, CommitRetries, SkipAutoCleanup): each must be accepted and a
// normal upsert must still succeed with them set. There is no scalar index on
// the join key here, so UseIndex(false) and UseIndex(true) both take the full
// scan path and must behave identically.
func TestMergeInsertParityOptions(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*lance.MergeInsertBuilder) *lance.MergeInsertBuilder
	}{
		{"use_index_true", func(b *lance.MergeInsertBuilder) *lance.MergeInsertBuilder { return b.UseIndex(true) }},
		{"use_index_false", func(b *lance.MergeInsertBuilder) *lance.MergeInsertBuilder { return b.UseIndex(false) }},
		{"commit_retries", func(b *lance.MergeInsertBuilder) *lance.MergeInsertBuilder { return b.CommitRetries(5) }},
		{"skip_auto_cleanup", func(b *lance.MergeInsertBuilder) *lance.MergeInsertBuilder { return b.SkipAutoCleanup(true) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			_, ds := writeDataset(t, 100)
			src := newSourceReader(t, 50, 100) // ids 50..149: 50 matched, 50 new
			defer src.Release()
			b := ds.MergeInsert("id").WhenMatchedUpdateAll().WhenNotMatchedInsertAll()
			stats, err := tc.apply(b).Execute(ctx, src)
			if err != nil {
				t.Fatalf("MergeInsert with %s: %v", tc.name, err)
			}
			if stats.NumUpdatedRows != 50 || stats.NumInsertedRows != 50 {
				t.Fatalf("%s: updated/inserted = %d/%d, want 50/50", tc.name, stats.NumUpdatedRows, stats.NumInsertedRows)
			}
			if got := mustCount(t, ds); got != 150 {
				t.Fatalf("%s: CountRows = %d, want 150", tc.name, got)
			}
		})
	}
}

// TestMutationVersionChain applies delete, update and merge-insert through
// one handle and verifies each commits a new version visible to that handle
// as well as to a freshly opened one.
func TestMutationVersionChain(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 100)
	versions := []uint64{version(t, ds)}

	if _, err := ds.Delete(ctx, "id >= 90"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	versions = append(versions, version(t, ds))

	if _, err := ds.Update(ctx, lance.UpdateSpec{Set: map[string]string{"score": "0.0"}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	versions = append(versions, version(t, ds))

	src := newSourceReader(t, 80, 20) // ids 80..99: 10 matched, 10 new
	defer src.Release()
	if _, err := ds.MergeInsert("id").WhenMatchedUpdateAll().Execute(ctx, src); err != nil {
		t.Fatalf("MergeInsert: %v", err)
	}
	versions = append(versions, version(t, ds))

	for i := 1; i < len(versions); i++ {
		if versions[i] <= versions[i-1] {
			t.Fatalf("versions %v are not strictly increasing", versions)
		}
	}

	if got := mustCount(t, ds); got != 100 {
		t.Fatalf("CountRows through mutating handle = %d, want 100", got)
	}
	latest, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	if latest != versions[len(versions)-1] {
		t.Fatalf("LatestVersion = %d, want %d", latest, versions[len(versions)-1])
	}

	// A fresh handle sees the same final state.
	reopened, err := lance.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()
	if got := mustCount(t, reopened); got != 100 {
		t.Fatalf("CountRows on reopened dataset = %d, want 100", got)
	}
	if got := version(t, reopened); got != versions[len(versions)-1] {
		t.Fatalf("reopened version = %d, want %d", got, versions[len(versions)-1])
	}
}
