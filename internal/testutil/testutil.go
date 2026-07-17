// Package testutil holds test helpers shared across lance-go packages.
package testutil

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// VecDim is the fixed-size-list length of the "vec" column produced by
// NewRecords.
const VecDim = 16

// Allocator returns the Arrow allocator used by tests. It is backed by C
// malloc so that buffers exported across the C Data Interface (lance.Write)
// live outside the Go heap, as required by the cgo pointer-passing rules
// (enforced under GOEXPERIMENT=cgocheck2).
func Allocator() memory.Allocator {
	return mallocator.NewMallocator()
}

// Schema returns the canonical test schema:
// id int64, name utf8, score float64, vec fixed_size_list<float32, 16>.
func Schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		{Name: "vec", Type: arrow.FixedSizeListOf(VecDim, arrow.PrimitiveTypes.Float32)},
	}, nil)
}

// NewRecord builds one deterministic record with rows rows, whose id column
// starts at startID. Row i (global id g = startID+i) has name "row-g",
// score g/2.0 and vec [g, g+1, ..., g+15]. The caller must Release the
// record.
func NewRecord(mem memory.Allocator, startID, rows int64) arrow.RecordBatch {
	b := array.NewRecordBuilder(mem, Schema())
	defer b.Release()

	idB := b.Field(0).(*array.Int64Builder)
	nameB := b.Field(1).(*array.StringBuilder)
	scoreB := b.Field(2).(*array.Float64Builder)
	vecB := b.Field(3).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	for i := int64(0); i < rows; i++ {
		g := startID + i
		idB.Append(g)
		nameB.Append(fmt.Sprintf("row-%d", g))
		scoreB.Append(float64(g) / 2.0)
		vecB.Append(true)
		for j := 0; j < VecDim; j++ {
			valB.Append(float32(g) + float32(j))
		}
	}
	return b.NewRecordBatch()
}

// NewRecords builds a deterministic dataset of rows rows starting at id
// startID, split into batches of at most batchRows rows. The caller must
// Release every returned record.
func NewRecords(mem memory.Allocator, startID, rows, batchRows int64) []arrow.RecordBatch {
	var records []arrow.RecordBatch
	for off := int64(0); off < rows; off += batchRows {
		n := min(batchRows, rows-off)
		records = append(records, NewRecord(mem, startID+off, n))
	}
	return records
}

// NewReader wraps deterministic test data (see NewRecords) in a
// RecordReader. The reader owns the records; the caller must Release the
// reader (or hand it to a consumer that does).
func NewReader(mem memory.Allocator, startID, rows, batchRows int64) array.RecordReader {
	records := NewRecords(mem, startID, rows, batchRows)
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()
	rdr, err := array.NewRecordReader(Schema(), records)
	if err != nil {
		panic(fmt.Sprintf("testutil: NewRecordReader: %v", err))
	}
	return rdr
}

// Collect drains rdr, returning all its records retained. The caller must
// Release every returned record. Collect does not release the reader.
func Collect(rdr array.RecordReader) ([]arrow.RecordBatch, error) {
	var out []arrow.RecordBatch
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		out = append(out, rec)
	}
	if err := rdr.Err(); err != nil {
		for _, rec := range out {
			rec.Release()
		}
		return nil, err
	}
	return out, nil
}

// ReleaseAll releases every record in recs.
func ReleaseAll(recs []arrow.RecordBatch) {
	for _, rec := range recs {
		rec.Release()
	}
}

// TotalRows sums the row counts of recs.
func TotalRows(recs []arrow.RecordBatch) int64 {
	var n int64
	for _, rec := range recs {
		n += rec.NumRows()
	}
	return n
}
