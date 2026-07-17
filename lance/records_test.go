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

// mismatchSchema is deliberately incompatible with testutil.Schema().
var mismatchSchema = arrow.NewSchema([]arrow.Field{
	{Name: "only", Type: arrow.PrimitiveTypes.Int32},
}, nil)

// TestRecordsHappyPath builds two batches with lance.Allocator, wraps them
// with Records, feeds the reader straight to lance.Write, and checks the row
// count on read-back.
func TestRecordsHappyPath(t *testing.T) {
	ctx := t.Context()
	mem := lance.Allocator()
	schema := testutil.Schema()

	b1 := testutil.NewRecord(mem, 0, 5)
	b2 := testutil.NewRecord(mem, 5, 7)
	defer b1.Release()
	defer b2.Release()

	rdr, err := lance.Records(schema, b1, b2)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	defer rdr.Release()

	uri := filepath.Join(t.TempDir(), "records_happy.lance")
	ds, err := lance.Write(ctx, uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 12 {
		t.Fatalf("CountRows = %d, want 12", count)
	}
}

// TestRecordsZeroBatches checks that no batches yields an immediately
// exhausted reader (documented behavior), and exercises what lance.Write
// actually does when handed one.
func TestRecordsZeroBatches(t *testing.T) {
	ctx := t.Context()
	schema := testutil.Schema()

	rdr, err := lance.Records(schema)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	defer rdr.Release()

	if got := rdr.Schema(); got != schema {
		t.Fatalf("Schema() = %v, want the same schema value", got)
	}
	if rdr.Next() {
		t.Fatal("Next() on a zero-batch reader = true, want false")
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("Err() on a zero-batch reader = %v, want nil", err)
	}

	// Feed a fresh zero-batch reader (Write fully consumes its reader) to
	// lance.Write. Verified behavior: Write succeeds on an empty reader and
	// produces a valid, zero-row dataset (documented on Records).
	rdr2, err := lance.Records(schema)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	defer rdr2.Release()

	uri := filepath.Join(t.TempDir(), "records_empty.lance")
	ds, err := lance.Write(ctx, uri, rdr2)
	if err != nil {
		t.Fatalf("Write with a zero-batch reader: %v, want success", err)
	}
	defer ds.Close()
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 0 {
		t.Fatalf("CountRows on empty-reader write = %d, want 0", count)
	}
}

// TestRecordsNilSchema checks that a nil schema is rejected with
// ErrInvalidArgument.
func TestRecordsNilSchema(t *testing.T) {
	_, err := lance.Records(nil)
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Records(nil): err = %v, want ErrInvalidArgument", err)
	}
	if !strings.HasPrefix(err.Error(), "lance: records:") {
		t.Fatalf("Records(nil): err = %q, want lance: records: prefix", err.Error())
	}
}

// TestRecordsSchemaMismatch checks that a batch whose schema does not match
// is rejected with ErrInvalidArgument, and that the error names the
// offending batch's index.
func TestRecordsSchemaMismatch(t *testing.T) {
	mem := lance.Allocator()
	schema := testutil.Schema()

	good := testutil.NewRecord(mem, 0, 1)
	defer good.Release()

	badBuilder := array.NewRecordBuilder(mem, mismatchSchema)
	badBuilder.Field(0).(*array.Int32Builder).Append(1)
	bad := badBuilder.NewRecordBatch()
	badBuilder.Release()
	defer bad.Release()

	// bad is at index 1: schema mismatches testutil.Schema().
	_, err := lance.Records(schema, good, bad)
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Records(mismatch at index 1): err = %v, want ErrInvalidArgument", err)
	}
	if !strings.HasPrefix(err.Error(), "lance: records:") {
		t.Fatalf("Records(mismatch at index 1): err = %q, want lance: records: prefix", err.Error())
	}
	if !strings.Contains(err.Error(), "batch 1") {
		t.Fatalf("Records(mismatch at index 1): err = %q, want it to name batch index 1", err.Error())
	}

	// Swap order: bad is now at index 0.
	_, err = lance.Records(schema, bad, good)
	if !errors.Is(err, lance.ErrInvalidArgument) {
		t.Fatalf("Records(mismatch at index 0): err = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "batch 0") {
		t.Fatalf("Records(mismatch at index 0): err = %q, want it to name batch index 0", err.Error())
	}
}

// TestRecordsReleasedInputSafety verifies the documented retention
// semantics: Records takes its own reference on each batch, so the caller
// may Release its batches immediately after the call returns and the reader
// remains fully usable. Run with GOEXPERIMENT=cgocheck2 to also catch any
// use-after-free of the underlying native buffers.
func TestRecordsReleasedInputSafety(t *testing.T) {
	ctx := t.Context()
	mem := lance.Allocator()
	schema := testutil.Schema()

	b1 := testutil.NewRecord(mem, 0, 3)
	b2 := testutil.NewRecord(mem, 3, 4)

	rdr, err := lance.Records(schema, b1, b2)
	if err != nil {
		b1.Release()
		b2.Release()
		t.Fatalf("Records: %v", err)
	}

	// The caller's own references are no longer needed: Records (via
	// array.NewRecordReader) retained its own before returning.
	b1.Release()
	b2.Release()

	uri := filepath.Join(t.TempDir(), "records_released.lance")
	ds, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		t.Fatalf("Write after releasing caller batches: %v", err)
	}
	defer ds.Close()

	count, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != 7 {
		t.Fatalf("CountRows = %d, want 7", count)
	}
}
