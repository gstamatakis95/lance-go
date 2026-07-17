package lance_test

import (
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// ftsDocs is the corpus indexed by the full-text search tests. The row ids
// are the slice indices.
var ftsDocs = []string{
	0: "the quick brown fox jumps over the lazy dog",
	1: "lance is a modern columnar data format for machine learning",
	2: "a vector database stores embeddings for similarity search",
	3: "postgres is a battle tested relational database",
	4: "full text search uses an inverted index over tokens",
	5: "the database vector is stored in reverse order here",
	6: "machine learning models turn documents into vector embeddings",
	7: "sqlite is a small embedded relational database engine",
}

// writeTextDataset writes ftsDocs as a dataset with columns id, text and
// title, and returns an open handle (closed via t.Cleanup).
func writeTextDataset(t *testing.T) *lance.Dataset {
	t.Helper()
	mem := testutil.Allocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "text", Type: arrow.BinaryTypes.String},
		{Name: "title", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	titles := []string{"animals", "lance", "vectors", "postgres", "fts", "reversed", "ml", "sqlite"}
	for i, doc := range ftsDocs {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(doc)
		b.Field(2).(*array.StringBuilder).Append(titles[i])
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	uri := filepath.Join(t.TempDir(), "docs.lance")
	ds, err := lance.Write(t.Context(), uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return ds
}

// idSet converts the ids in recs into a set.
func idSet(t *testing.T, recs []arrow.RecordBatch) map[int64]bool {
	t.Helper()
	set := map[int64]bool{}
	for _, id := range idsOf(t, recs) {
		set[id] = true
	}
	return set
}

// assertScoreColumn verifies FTS results carry a _score column.
func assertScoreColumn(t *testing.T, recs []arrow.RecordBatch) {
	t.Helper()
	for _, rec := range recs {
		if len(rec.Schema().FieldIndices("_score")) == 0 {
			t.Fatalf("FTS result batch has no _score column: %v", rec.Schema())
		}
	}
}

func TestMatchQuery(t *testing.T) {
	ctx := t.Context()
	ds := writeTextDataset(t)

	if err := ds.CreateIndex(ctx, "text", lance.Inverted{}, lance.WithIndexName("text_idx")); err != nil {
		t.Fatalf("CreateIndex(Inverted): %v", err)
	}

	recs := scanAll(t, ds.Scan().FullTextSearch(lance.MatchQuery{Column: "text", Terms: "database"}))
	got := idSet(t, recs)
	want := map[int64]bool{2: true, 3: true, 5: true, 7: true}
	if len(got) != len(want) {
		t.Fatalf("match 'database' returned ids %v, want %v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("match 'database' misses id %d (got %v)", id, got)
		}
	}
	assertScoreColumn(t, recs)

	// Operator And: all terms must match.
	recs = scanAll(t, ds.Scan().FullTextSearch(lance.MatchQuery{
		Column:   "text",
		Terms:    "relational database",
		Operator: lance.FtsOperatorAnd,
	}))
	got = idSet(t, recs)
	if len(got) != 2 || !got[3] || !got[7] {
		t.Fatalf("match And 'relational database' = %v, want {3, 7}", got)
	}

	// Column via option instead of the query. Stemming folds "embeddings"
	// and "embedded" (doc 7) onto the same token.
	recs = scanAll(t, ds.Scan().FullTextSearch(
		lance.MatchQuery{Terms: "embeddings"}, lance.WithFtsColumns("text")))
	got = idSet(t, recs)
	if len(got) != 3 || !got[2] || !got[6] || !got[7] {
		t.Fatalf("match 'embeddings' = %v, want {2, 6, 7}", got)
	}

	// Limit.
	recs = scanAll(t, ds.Scan().FullTextSearch(
		lance.MatchQuery{Column: "text", Terms: "database"}, lance.WithFtsLimit(2)))
	if n := testutil.TotalRows(recs); n != 2 {
		t.Fatalf("limited FTS returned %d rows, want 2", n)
	}
}

func TestPhraseQuery(t *testing.T) {
	ctx := t.Context()
	ds := writeTextDataset(t)

	// Phrase queries need positions. Disable stop-word removal so word
	// positions stay contiguous.
	if err := ds.CreateIndex(ctx, "text", lance.Inverted{
		WithPosition:    true,
		RemoveStopWords: ptr(false),
	}, lance.WithIndexName("text_idx")); err != nil {
		t.Fatalf("CreateIndex(Inverted with positions): %v", err)
	}

	// "vector database" appears as a phrase only in doc 2. doc 5 has the
	// words in the opposite order.
	recs := scanAll(t, ds.Scan().FullTextSearch(lance.PhraseQuery{Column: "text", Terms: "vector database"}))
	got := idSet(t, recs)
	if len(got) != 1 || !got[2] {
		t.Fatalf("phrase 'vector database' = %v, want {2}", got)
	}
	assertScoreColumn(t, recs)

	// The reversed order matches doc 5 instead.
	recs = scanAll(t, ds.Scan().FullTextSearch(lance.PhraseQuery{Column: "text", Terms: "database vector"}))
	got = idSet(t, recs)
	if len(got) != 1 || !got[5] {
		t.Fatalf("phrase 'database vector' = %v, want {5}", got)
	}
}

func TestBooleanQuery(t *testing.T) {
	ctx := t.Context()
	ds := writeTextDataset(t)

	if err := ds.CreateIndex(ctx, "text", lance.Inverted{}); err != nil {
		t.Fatalf("CreateIndex(Inverted): %v", err)
	}

	// database AND NOT relational -> the vector database and reversed docs.
	recs := scanAll(t, ds.Scan().FullTextSearch(lance.BooleanQuery{
		Must:    []lance.FtsQuery{lance.MatchQuery{Column: "text", Terms: "database"}},
		MustNot: []lance.FtsQuery{lance.MatchQuery{Column: "text", Terms: "relational"}},
	}))
	got := idSet(t, recs)
	if len(got) != 2 || !got[2] || !got[5] {
		t.Fatalf("boolean must/must_not = %v, want {2, 5}", got)
	}

	// must database AND must embeddings -> docs 2 and 7 ("embedded" stems
	// to the same token as "embeddings").
	recs = scanAll(t, ds.Scan().FullTextSearch(lance.BooleanQuery{
		Must: []lance.FtsQuery{
			lance.MatchQuery{Column: "text", Terms: "database"},
			lance.MatchQuery{Column: "text", Terms: "embeddings"},
		},
	}))
	got = idSet(t, recs)
	if len(got) != 2 || !got[2] || !got[7] {
		t.Fatalf("boolean must+must = %v, want {2, 7}", got)
	}
}

func TestBoostQuery(t *testing.T) {
	ctx := t.Context()
	ds := writeTextDataset(t)

	if err := ds.CreateIndex(ctx, "text", lance.Inverted{}); err != nil {
		t.Fatalf("CreateIndex(Inverted): %v", err)
	}

	// Boost database docs down when they mention "relational": the purely
	// vector-ish docs must outrank the relational ones.
	recs := scanAll(t, ds.Scan().FullTextSearch(lance.BoostQuery{
		Positive:      lance.MatchQuery{Column: "text", Terms: "database"},
		Negative:      lance.MatchQuery{Column: "text", Terms: "relational"},
		NegativeBoost: 0.2,
	}))
	ids := idsOf(t, recs)
	if len(ids) != 4 {
		t.Fatalf("boost query returned ids %v, want 4 database docs", ids)
	}
	rank := map[int64]int{}
	for i, id := range ids {
		rank[id] = i
	}
	if rank[3] < rank[2] || rank[3] < rank[5] || rank[7] < rank[2] || rank[7] < rank[5] {
		t.Fatalf("relational docs outrank boosted docs: %v", ids)
	}
}

func TestMultiMatchQuery(t *testing.T) {
	ctx := t.Context()
	ds := writeTextDataset(t)

	if err := ds.CreateIndex(ctx, "text", lance.Inverted{}); err != nil {
		t.Fatalf("CreateIndex(text): %v", err)
	}
	if err := ds.CreateIndex(ctx, "title", lance.Inverted{}); err != nil {
		t.Fatalf("CreateIndex(title): %v", err)
	}

	// "postgres" appears in doc 3's text and title only.
	recs := scanAll(t, ds.Scan().FullTextSearch(lance.MultiMatchQuery{
		Queries: []lance.MatchQuery{
			{Column: "text", Terms: "postgres"},
			{Column: "title", Terms: "postgres"},
		},
	}))
	got := idSet(t, recs)
	if len(got) != 1 || !got[3] {
		t.Fatalf("multi-match 'postgres' = %v, want {3}", got)
	}
}

func TestFtsQueryValidation(t *testing.T) {
	ds := writeTextDataset(t)
	if _, err := ds.Scan().FullTextSearch(lance.MatchQuery{Column: "text"}).Reader(t.Context()); err == nil {
		t.Fatal("empty match terms should fail")
	}
	if _, err := ds.Scan().FullTextSearch(nil).Reader(t.Context()); err == nil {
		t.Fatal("nil FTS query should fail")
	}
	if _, err := ds.Scan().FullTextSearch(lance.BooleanQuery{
		Must: []lance.FtsQuery{nil},
	}).Reader(t.Context()); err == nil {
		t.Fatal("nil boolean sub-query should fail")
	}
}

// ptr returns a pointer to v.
func ptr[T any](v T) *T {
	return &v
}
