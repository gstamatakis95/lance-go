package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// SQLBuilder configures a SQL query over a dataset. Build one with
// Dataset.SQL, chain the configuration methods, then call Reader.
//
// The underlying engine is DataFusion, and the dataset is registered as a table
// named "dataset" unless overridden with TableName.
type SQLBuilder struct {
	ds    *Dataset
	query string
	cfg   sqlConfig
}

// sqlConfig mirrors the options_json contract of lance_dataset_sql.
type sqlConfig struct {
	TableName   string `json:"table_name,omitempty"`
	WithRowID   bool   `json:"with_row_id,omitempty"`
	WithRowAddr bool   `json:"with_row_addr,omitempty"`
}

// SQL starts building a SQL query over the dataset, e.g.
// ds.SQL("SELECT id FROM dataset WHERE id > 10").Reader(ctx).
func (d *Dataset) SQL(query string) *SQLBuilder {
	return &SQLBuilder{ds: d, query: query}
}

// obs returns the instrumentation handle for this builder, inherited from its
// dataset (nil-safe).
func (b *SQLBuilder) obs() *obs { return b.ds.obs() }

// TableName sets the table name the dataset is registered under in the SQL
// context (default "dataset").
func (b *SQLBuilder) TableName(name string) *SQLBuilder {
	b.cfg.TableName = name
	return b
}

// WithRowID exposes the internal _rowid column to the query.
func (b *SQLBuilder) WithRowID() *SQLBuilder {
	b.cfg.WithRowID = true
	return b
}

// WithRowAddr exposes the internal _rowaddr column to the query.
func (b *SQLBuilder) WithRowAddr() *SQLBuilder {
	b.cfg.WithRowAddr = true
	return b
}

// Reader executes the query and returns a reader over the result batches.
//
// The returned reader and every record obtained from it must be Released by
// the caller. Records returned by Next/RecordBatch are only valid until the
// next call to Next. Retain them to keep them longer. The reader stays
// valid after the Dataset is closed.
func (b *SQLBuilder) Reader(ctx context.Context) (reader array.RecordReader, err error) {
	cfg := observedStream(ctx, b.obs(), "SQLBuilder.Reader")
	defer cfg.endOnError(&err)
	ctx = cfg.ctx
	b.ds.mu.RLock()
	defer b.ds.mu.RUnlock()
	ptr, err := b.ds.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	cQuery, freeQuery := cString(b.query)
	defer freeQuery()
	cOpts, freeOpts, err := marshalOptions(&b.cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	// Must be zero-initialized (Go zeroes it). On success the native side
	// writes a self-contained stream into it.
	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_sql(ptr, cQuery, cOpts, &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: sql: %w", err)
	}
	return importRecordReader(&stream, "sql", cfg)
}
