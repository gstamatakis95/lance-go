package lance_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// writeDataset creates a fresh dataset at a temp URI with rows deterministic
// rows (ids 0..rows-1) and returns the URI and an open handle (closed via
// t.Cleanup).
func writeDataset(t *testing.T, rows int64, opts ...lance.WriteOption) (string, *lance.Dataset) {
	t.Helper()
	uri := filepath.Join(t.TempDir(), "test.lance")
	rdr := testutil.NewReader(testutil.Allocator(), 0, rows, 32)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr, opts...)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return uri, ds
}

// appendRows appends rows records starting at startID to the dataset at uri.
func appendRows(t *testing.T, uri string, startID, rows int64) *lance.Dataset {
	t.Helper()
	rdr := testutil.NewReader(testutil.Allocator(), startID, rows, 32)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr, lance.WithMode(lance.WriteModeAppend))
	if err != nil {
		t.Fatalf("Write(append): %v", err)
	}
	t.Cleanup(func() { ds.Close() })
	return ds
}

// scanAll runs sc and returns all records (released via t.Cleanup).
func scanAll(t *testing.T, sc *lance.Scanner) []arrow.RecordBatch {
	t.Helper()
	rdr, err := sc.Reader(t.Context())
	if err != nil {
		t.Fatalf("Scanner.Reader: %v", err)
	}
	defer rdr.Release()
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect scan results: %v", err)
	}
	t.Cleanup(func() { testutil.ReleaseAll(recs) })
	return recs
}

// assertRows verifies that recs contain exactly the deterministic rows with
// the given global ids, in order, across all four generated columns.
func assertRows(t *testing.T, recs []arrow.RecordBatch, wantIDs []int64) {
	t.Helper()
	if got := testutil.TotalRows(recs); got != int64(len(wantIDs)) {
		t.Fatalf("scan returned %d rows, want %d", got, len(wantIDs))
	}
	row := 0
	for _, rec := range recs {
		schema := rec.Schema()
		idCol := rec.Column(schema.FieldIndices("id")[0]).(*array.Int64)
		nameCol := rec.Column(schema.FieldIndices("name")[0]).(*array.String)
		scoreCol := rec.Column(schema.FieldIndices("score")[0]).(*array.Float64)
		vecCol := rec.Column(schema.FieldIndices("vec")[0]).(*array.FixedSizeList)
		vecVals := vecCol.ListValues().(*array.Float32)
		for i := 0; i < int(rec.NumRows()); i++ {
			g := wantIDs[row]
			if idCol.Value(i) != g {
				t.Fatalf("row %d: id = %d, want %d", row, idCol.Value(i), g)
			}
			if want := fmt.Sprintf("row-%d", g); nameCol.Value(i) != want {
				t.Fatalf("row %d: name = %q, want %q", row, nameCol.Value(i), want)
			}
			if want := float64(g) / 2.0; scoreCol.Value(i) != want {
				t.Fatalf("row %d: score = %v, want %v", row, scoreCol.Value(i), want)
			}
			start := (vecCol.Data().Offset() + i) * testutil.VecDim
			for j := 0; j < testutil.VecDim; j++ {
				if want := float32(g) + float32(j); vecVals.Value(start+j) != want {
					t.Fatalf("row %d: vec[%d] = %v, want %v", row, j, vecVals.Value(start+j), want)
				}
			}
			row++
		}
	}
}

func seq(start, n int64) []int64 {
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = start + int64(i)
	}
	return ids
}

func TestCreateOpenCount(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 100)

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on write handle: %v", err)
	}
	if count != 100 {
		t.Fatalf("CountRows = %d, want 100", count)
	}

	opened, err := lance.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer opened.Close()
	count, err = opened.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows on opened handle: %v", err)
	}
	if count != 100 {
		t.Fatalf("CountRows = %d, want 100", count)
	}
}

func TestWriteScanRoundTrip(t *testing.T) {
	_, ds := writeDataset(t, 100)
	recs := scanAll(t, ds.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(0, 100))
}

func TestSchema(t *testing.T) {
	_, ds := writeDataset(t, 10)
	schema, err := ds.Schema(t.Context())
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	want := testutil.Schema()
	if schema.NumFields() != want.NumFields() {
		t.Fatalf("schema has %d fields, want %d", schema.NumFields(), want.NumFields())
	}
	for i := 0; i < want.NumFields(); i++ {
		got, exp := schema.Field(i), want.Field(i)
		if got.Name != exp.Name {
			t.Errorf("field %d name = %q, want %q", i, got.Name, exp.Name)
		}
		if !arrow.TypeEqual(got.Type, exp.Type) {
			t.Errorf("field %q type = %s, want %s", got.Name, got.Type, exp.Type)
		}
	}
}

func TestAppendGrowsCountAndVersion(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 100)
	v1, err := ds.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}

	appended := appendRows(t, uri, 100, 50)
	count, err := appended.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 150 {
		t.Fatalf("CountRows after append = %d, want 150", count)
	}
	v2, err := appended.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if v2.Version <= v1.Version {
		t.Fatalf("version after append = %d, want > %d", v2.Version, v1.Version)
	}
	if v2.Timestamp.IsZero() {
		t.Error("version timestamp is zero")
	}

	latest, err := appended.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	if latest != v2.Version {
		t.Fatalf("LatestVersion = %d, want %d", latest, v2.Version)
	}

	// The full appended contents scan back in order.
	recs := scanAll(t, appended.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(0, 150))
}

func TestOverwriteResets(t *testing.T) {
	ctx := t.Context()
	uri, _ := writeDataset(t, 100)

	rdr := testutil.NewReader(testutil.Allocator(), 500, 20, 32)
	defer rdr.Release()
	ds, err := lance.Write(ctx, uri, rdr, lance.WithMode(lance.WriteModeOverwrite))
	if err != nil {
		t.Fatalf("Write(overwrite): %v", err)
	}
	defer ds.Close()

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 20 {
		t.Fatalf("CountRows after overwrite = %d, want 20", count)
	}
	recs := scanAll(t, ds.Scan().ScanInOrder(true))
	assertRows(t, recs, seq(500, 20))
}

func TestCreateExistingFails(t *testing.T) {
	uri, _ := writeDataset(t, 10)
	rdr := testutil.NewReader(testutil.Allocator(), 0, 10, 32)
	defer rdr.Release()
	_, err := lance.Write(t.Context(), uri, rdr, lance.WithMode(lance.WriteModeCreate))
	if err == nil {
		t.Fatal("Write(create) over an existing dataset succeeded, want error")
	}
	if !errors.Is(err, lance.ErrAlreadyExists) {
		t.Fatalf("error = %v, want errors.Is(_, ErrAlreadyExists)", err)
	}
	const wantHint = "dataset exists; use WithMode(WriteModeAppend) or WithMode(WriteModeOverwrite)"
	if !strings.Contains(err.Error(), wantHint) {
		t.Fatalf("error = %q, want it to contain hint %q", err.Error(), wantHint)
	}
}

func TestFilterScan(t *testing.T) {
	_, ds := writeDataset(t, 100)
	recs := scanAll(t, ds.Scan().Filter("id < 50").ScanInOrder(true))
	assertRows(t, recs, seq(0, 50))
}

func TestProjection(t *testing.T) {
	_, ds := writeDataset(t, 10)
	rdr, err := ds.Scan().Columns("name", "id").Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rdr.Release()
	schema := rdr.Schema()
	if schema.NumFields() != 2 {
		t.Fatalf("projected schema has %d fields (%s), want 2", schema.NumFields(), schema)
	}
	if schema.Field(0).Name != "name" || schema.Field(1).Name != "id" {
		t.Fatalf("projected fields = [%s, %s], want [name, id]",
			schema.Field(0).Name, schema.Field(1).Name)
	}
	var rows int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if rows != 10 {
		t.Fatalf("projected scan returned %d rows, want 10", rows)
	}
}

func TestLimitOffset(t *testing.T) {
	_, ds := writeDataset(t, 100)
	recs := scanAll(t, ds.Scan().Limit(10).Offset(20).ScanInOrder(true))
	assertRows(t, recs, seq(20, 10))
}

func TestCountRowsWithFilter(t *testing.T) {
	_, ds := writeDataset(t, 100)
	count, err := ds.CountRows(t.Context(), "id >= 90")
	if err != nil {
		t.Fatalf("CountRows(filter): %v", err)
	}
	if count != 10 {
		t.Fatalf("CountRows(id >= 90) = %d, want 10", count)
	}

	count, err = ds.Scan().Filter("id < 25").CountRows(t.Context())
	if err != nil {
		t.Fatalf("Scanner.CountRows: %v", err)
	}
	if count != 25 {
		t.Fatalf("Scanner.CountRows(id < 25) = %d, want 25", count)
	}
}

func TestWithRowID(t *testing.T) {
	_, ds := writeDataset(t, 10)
	rdr, err := ds.Scan().WithRowID().Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rdr.Release()
	if len(rdr.Schema().FieldIndices("_rowid")) == 0 {
		t.Fatalf("schema %s does not contain _rowid", rdr.Schema())
	}
	for rdr.Next() {
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
}

func TestExplain(t *testing.T) {
	_, ds := writeDataset(t, 10)
	plan, err := ds.Scan().Filter("id < 5").Explain(t.Context(), false)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if strings.TrimSpace(plan) == "" {
		t.Fatal("Explain returned an empty plan")
	}
	t.Logf("plan:\n%s", plan)
}

func TestTimeTravel(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 100)
	v1, err := ds.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	appendRows(t, uri, 100, 50)

	old, err := lance.Open(ctx, uri, lance.WithVersion(v1.Version))
	if err != nil {
		t.Fatalf("Open(WithVersion): %v", err)
	}
	defer old.Close()
	count, err := old.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 100 {
		t.Fatalf("time-travel CountRows = %d, want 100", count)
	}
	latest, err := old.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	if latest <= v1.Version {
		t.Fatalf("LatestVersion = %d, want > %d", latest, v1.Version)
	}
}

func TestOpenNonexistent(t *testing.T) {
	const wantHint = "check the URI, version/tag, and storage options"

	_, err := lance.Open(t.Context(), filepath.Join(t.TempDir(), "missing.lance"))
	if err == nil {
		t.Fatal("Open of a nonexistent dataset succeeded, want error")
	}
	if !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("error = %v, want errors.Is(_, ErrNotFound)", err)
	}
	if !strings.Contains(err.Error(), wantHint) {
		t.Fatalf("error = %q, want it to contain hint %q", err.Error(), wantHint)
	}

	// Session.Open (openWithSession) must carry the same ErrNotFound hint as
	// the plain Open path.
	sess, err := lance.NewSession(lance.SessionConfig{})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()
	_, err = sess.Open(t.Context(), filepath.Join(t.TempDir(), "missing.lance"))
	if err == nil {
		t.Fatal("Session.Open of a nonexistent dataset succeeded, want error")
	}
	if !errors.Is(err, lance.ErrNotFound) {
		t.Fatalf("Session.Open error = %v, want errors.Is(_, ErrNotFound)", err)
	}
	if !strings.Contains(err.Error(), wantHint) {
		t.Fatalf("Session.Open error = %q, want it to contain hint %q", err.Error(), wantHint)
	}
}

func TestBadFilter(t *testing.T) {
	_, ds := writeDataset(t, 10)
	_, err := ds.Scan().Filter("no_such_column ==== 1").Reader(t.Context())
	if err == nil {
		t.Fatal("scan with a bad filter succeeded, want error")
	}
	if !strings.Contains(err.Error(), "no_such_column") && !strings.Contains(err.Error(), "filter") {
		t.Fatalf("error %q does not mention the bad filter", err)
	}
}

func TestClosedDataset(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := ds.CountRows(ctx, ""); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("CountRows on closed dataset = %v, want ErrInvalidArgument", err)
	}
	if _, err := ds.Scan().Reader(ctx); !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Scan on closed dataset = %v, want ErrInvalidArgument", err)
	}
}

func TestReaderOutlivesDataset(t *testing.T) {
	_, ds := writeDataset(t, 50)
	rdr, err := ds.Scan().ScanInOrder(true).Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rdr.Release()
	// Close the dataset while the stream is still unread: the stream is
	// self-contained and must keep working.
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	recs, err := testutil.Collect(rdr)
	if err != nil {
		t.Fatalf("collect after close: %v", err)
	}
	defer testutil.ReleaseAll(recs)
	assertRows(t, recs, seq(0, 50))
}

func TestCanceledContext(t *testing.T) {
	uri, ds := writeDataset(t, 10)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ds.CountRows(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("CountRows with canceled ctx = %v, want context.Canceled", err)
	}
	if _, err := lance.Open(ctx, uri); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with canceled ctx = %v, want context.Canceled", err)
	}
}

func TestOpenRejectsAmbiguousOrEmptyRefs(t *testing.T) {
	tests := []struct {
		name string
		opts []lance.OpenOption
	}{
		{name: "tag and version", opts: []lance.OpenOption{lance.WithTag("stable"), lance.WithVersion(1)}},
		{name: "tag and branch", opts: []lance.OpenOption{lance.WithTag("stable"), lance.WithBranch("main")}},
		{name: "empty tag", opts: []lance.OpenOption{lance.WithTag("")}},
		{name: "empty branch", opts: []lance.OpenOption{lance.WithBranch("")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := lance.Open(t.Context(), filepath.Join(t.TempDir(), "missing.lance"), tt.opts...)
			if !errors.Is(err, lance.ErrInvalidArgument) {
				t.Fatalf("Open error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestLeakLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping leak loop in -short mode")
	}
	ctx := t.Context()
	uri, seed := writeDataset(t, 100, lance.WithMaxRowsPerFile(64))
	seed.Close()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		ds, err := lance.Open(ctx, uri)
		if err != nil {
			t.Fatalf("iteration %d: Open: %v", i, err)
		}
		rdr, err := ds.Scan().Filter("id < 50").Columns("id", "vec").Reader(ctx)
		if err != nil {
			ds.Close()
			t.Fatalf("iteration %d: Reader: %v", i, err)
		}
		var rows int64
		for rdr.Next() {
			rows += rdr.RecordBatch().NumRows()
		}
		if err := rdr.Err(); err != nil {
			t.Fatalf("iteration %d: reader error: %v", i, err)
		}
		rdr.Release()
		if err := ds.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
		if rows != 50 {
			t.Fatalf("iteration %d: scanned %d rows, want 50", i, rows)
		}
	}
}
