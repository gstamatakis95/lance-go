package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// NamedExpr defines a new column as an SQL expression evaluated over the
// existing columns of the dataset.
type NamedExpr struct {
	// Name is the name of the new column.
	Name string
	// SQL is the expression computing the column's values, e.g. "score * 2".
	SQL string
}

// ColumnAlteration describes a change to one existing column: a rename, a
// nullability change, a type cast, or any combination. Zero-valued fields
// are left unchanged.
type ColumnAlteration struct {
	// Path is the (dot-separated) path of the column to alter. Required.
	Path string
	// Rename is the new name of the column, or "" to keep the name.
	Rename string
	// Nullable sets whether the column is nullable, and nil keeps the current
	// nullability.
	Nullable *bool
	// DataType casts the column to a new type, or "" to keep the type.
	// Supported values: "int32", "int64", "float32", "float64", "utf8",
	// "large_utf8", "binary", "bool", "date32", "date64", "timestamp_us",
	// "timestamp_ns". Casting rewrites the column data and drops any
	// indices covering it.
	DataType string
}

// addColumnsSpec mirrors the spec_json contract of lance_dataset_add_columns.
type addColumnsSpec struct {
	Kind        string      `json:"kind"`
	Expressions [][2]string `json:"expressions,omitempty"`
	ReadColumns []string    `json:"read_columns,omitempty"`
	BatchSize   uint32      `json:"batch_size,omitempty"`
}

// columnAlterationJSON mirrors one entry of the alterations_json contract of
// lance_dataset_alter_columns.
type columnAlterationJSON struct {
	Path     string `json:"path"`
	Rename   string `json:"rename,omitempty"`
	Nullable *bool  `json:"nullable,omitempty"`
	DataType string `json:"data_type,omitempty"`
}

// addColumns invokes lance_dataset_add_columns with an optional exported
// stream and/or schema. Both may be nil.
func (d *Dataset) addColumns(ctx context.Context, name string, spec addColumnsSpec, rdr array.RecordReader, schema *arrow.Schema) error {
	cSpec, freeSpec, err := marshalOptions(&spec)
	if err != nil {
		return err
	}
	defer freeSpec()

	return datasetDo(ctx, d, name, "add columns",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			// Both structs must be zero-initialized (Go zeroes them). The native
			// side always takes ownership of exported resources, even on error, so
			// they are released exactly once.
			var cStream *C.struct_ArrowArrayStream
			if rdr != nil {
				var stream C.struct_ArrowArrayStream
				cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&stream)))
				cStream = &stream
			}
			var cSchema *C.struct_ArrowSchema
			if schema != nil {
				var s C.struct_ArrowSchema
				cdata.ExportArrowSchema(schema, (*cdata.CArrowSchema)(unsafe.Pointer(&s)))
				cSchema = &s
			}

			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_add_columns(ptr, cSpec, cStream, cSchema)
			})
		})
}

// AddColumnsSQL adds new columns computed by SQL expressions over the
// existing rows, committing a new version. readColumns optionally restricts
// which existing columns are read to evaluate the expressions (nil reads
// whatever the expressions reference), and batchSize optionally overrides the
// scan batch size (0 uses the default).
func (d *Dataset) AddColumnsSQL(ctx context.Context, exprs []NamedExpr, readColumns []string, batchSize uint32) error {
	if len(exprs) == 0 {
		return fmt.Errorf("lance: add columns: no expressions given: %w", ErrInvalidArgument)
	}
	spec := addColumnsSpec{
		Kind:        "sql",
		ReadColumns: readColumns,
		BatchSize:   batchSize,
	}
	for _, e := range exprs {
		spec.Expressions = append(spec.Expressions, [2]string{e.Name, e.SQL})
	}
	return d.addColumns(ctx, "Dataset.AddColumnsSQL", spec, nil, nil)
}

// AddColumnsFromReader adds new columns whose values are supplied by rdr,
// which must produce exactly one value per existing row, in order. The
// reader's batches cross the Arrow C Data Interface, so their buffers must
// live outside the Go heap (see Write). batchSize is currently unused by the
// engine for this transform. Pass 0.
func (d *Dataset) AddColumnsFromReader(ctx context.Context, rdr array.RecordReader, batchSize uint32) error {
	if rdr == nil {
		return fmt.Errorf("lance: add columns: reader must not be nil: %w", ErrInvalidArgument)
	}
	spec := addColumnsSpec{Kind: "reader", BatchSize: batchSize}
	return d.addColumns(ctx, "Dataset.AddColumnsFromReader", spec, rdr, nil)
}

// AddColumnsAllNulls adds new all-null columns described by schema,
// committing a new version. This is a metadata-only operation. All fields in
// schema must be nullable.
func (d *Dataset) AddColumnsAllNulls(ctx context.Context, schema *arrow.Schema) error {
	if schema == nil {
		return fmt.Errorf("lance: add columns: schema must not be nil: %w", ErrInvalidArgument)
	}
	spec := addColumnsSpec{Kind: "all_nulls"}
	return d.addColumns(ctx, "Dataset.AddColumnsAllNulls", spec, nil, schema)
}

// DropColumns removes columns from the dataset, committing a new version.
// This is a metadata-only operation. The column data is reclaimed later by
// CompactFiles and CleanupOldVersions.
func (d *Dataset) DropColumns(ctx context.Context, cols ...string) error {
	if len(cols) == 0 {
		return fmt.Errorf("lance: drop columns: no columns given: %w", ErrInvalidArgument)
	}

	// NULL-terminated array of C strings, like cStorageKV but single-valued.
	n := len(cols) + 1
	arr := (**C.char)(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	slots := unsafe.Slice(arr, n)
	for i, col := range cols {
		slots[i] = C.CString(col)
	}
	defer func() {
		for _, p := range slots[:n-1] {
			C.free(unsafe.Pointer(p))
		}
		C.free(unsafe.Pointer(arr))
	}()

	return datasetDo(ctx, d, "Dataset.DropColumns", "drop columns",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_drop_columns(ptr, arr)
			})
		})
}

// AlterColumns renames columns, changes their nullability and/or casts them
// to new types, committing a new version. Renames and nullability changes
// are zero-copy. Type casts rewrite the column data and drop any indices
// covering it.
func (d *Dataset) AlterColumns(ctx context.Context, alts ...ColumnAlteration) error {
	if len(alts) == 0 {
		return fmt.Errorf("lance: alter columns: no alterations given: %w", ErrInvalidArgument)
	}
	payload := make([]columnAlterationJSON, 0, len(alts))
	for _, a := range alts {
		payload = append(payload, columnAlterationJSON{
			Path:     a.Path,
			Rename:   a.Rename,
			Nullable: a.Nullable,
			DataType: a.DataType,
		})
	}

	cJSON, freeJSON, err := marshalOptions(payload)
	if err != nil {
		return err
	}
	defer freeJSON()

	return datasetDo(ctx, d, "Dataset.AlterColumns", "alter columns",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_alter_columns(ptr, cJSON)
			})
		})
}

// Merge joins new columns onto the dataset, committing a new version: for
// each dataset row whose leftOn value matches a rightOn value in rdr, the
// remaining columns of rdr are appended to that row. Unmatched rows get
// nulls. The reader's batches cross the Arrow C Data Interface, so their
// buffers must live outside the Go heap (see Write).
func (d *Dataset) Merge(ctx context.Context, rdr array.RecordReader, leftOn, rightOn string) error {
	if rdr == nil {
		return fmt.Errorf("lance: merge: reader must not be nil: %w", ErrInvalidArgument)
	}
	cLeft, freeLeft := cString(leftOn)
	defer freeLeft()
	cRight, freeRight := cString(rightOn)
	defer freeRight()

	return datasetDo(ctx, d, "Dataset.Merge", "merge",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			// Must be zero-initialized (Go zeroes it). The native side always
			// takes ownership of the exported stream, even on error.
			var stream C.struct_ArrowArrayStream
			cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&stream)))

			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_merge(ptr, &stream, cLeft, cRight)
			})
		})
}
