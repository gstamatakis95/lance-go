package lance_test

import (
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
)

func TestSQLEqualsScanFilter(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	rdr, err := ds.SQL("SELECT id FROM dataset WHERE id > 90 ORDER BY id").Reader(ctx)
	if err != nil {
		t.Fatalf("SQL.Reader: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect sql: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	var got []int64
	for _, r := range recs {
		got = append(got, batchIDs(t, r)...)
	}

	// Equivalent scan+filter.
	scanRecs := scanAll(t, ds.Scan().Columns("id").Filter("id > 90"))
	var want []int64
	for _, r := range scanRecs {
		want = append(want, batchIDs(t, r)...)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !equalInt64(got, want) {
		t.Fatalf("sql ids = %v, want %v", got, want)
	}
	if len(got) != 9 {
		t.Fatalf("sql returned %d rows, want 9", len(got))
	}
}

func TestSQLTableNameAndRowID(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)

	rdr, err := ds.SQL("SELECT _rowid, id FROM tbl WHERE id = 5").
		TableName("tbl").
		WithRowID().
		Reader(ctx)
	if err != nil {
		t.Fatalf("SQL.Reader: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect sql: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	total := testutil.TotalRows(recs)
	if total != 1 {
		t.Fatalf("sql returned %d rows, want 1", total)
	}
	if len(recs[0].Schema().FieldIndices("_rowid")) == 0 {
		t.Fatalf("expected _rowid column (schema=%v)", recs[0].Schema())
	}
}

func TestSQLAggregate(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)

	rdr, err := ds.SQL("SELECT COUNT(*) AS n FROM dataset WHERE id < 30").Reader(ctx)
	if err != nil {
		t.Fatalf("SQL.Reader: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect sql: %v", err)
	}
	defer testutil.ReleaseAll(recs)

	if len(recs) == 0 || recs[0].NumRows() != 1 {
		t.Fatalf("expected a single count row")
	}
	col := recs[0].Column(recs[0].Schema().FieldIndices("n")[0]).(*array.Int64)
	if col.Value(0) != 30 {
		t.Fatalf("count = %d, want 30", col.Value(0))
	}
}
