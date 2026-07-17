//! Schema evolution: adding, altering and dropping columns, plus merging in
//! new columns by join key.

use std::ffi::c_char;
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::{DataType, TimeUnit};
use lance::dataset::{ColumnAlteration, NewColumnTransform};
use serde::Deserialize;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AddColumnsSpec {
    kind: String,
    #[serde(default)]
    expressions: Vec<(String, String)>,
    read_columns: Option<Vec<String>>,
    batch_size: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlterationJson {
    path: String,
    rename: Option<String>,
    nullable: Option<bool>,
    data_type: Option<String>,
}

/// Maps the documented FFI type vocabulary to Arrow data types. Anything
/// else is rejected as an invalid argument.
fn parse_data_type(name: &str) -> Result<DataType, String> {
    match name {
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        "large_utf8" => Ok(DataType::LargeUtf8),
        "binary" => Ok(DataType::Binary),
        "bool" => Ok(DataType::Boolean),
        "date32" => Ok(DataType::Date32),
        "date64" => Ok(DataType::Date64),
        "timestamp_us" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamp_ns" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        other => Err(format!(
            "unsupported data type {other:?}; expected one of int32, int64, float32, float64, \
             utf8, large_utf8, binary, bool, date32, date64, timestamp_us, timestamp_ns"
        )),
    }
}

/// Takes ownership of a caller-provided Arrow C schema and converts it. The
/// caller's struct is moved out (left released), so the producer side is
/// cleaned up exactly once regardless of later errors.
///
/// # Safety
///
/// `schema` must be a valid, non-NULL pointer to an initialized
/// `ArrowSchema` that has not been released or moved.
unsafe fn import_schema(schema: *mut FFI_ArrowSchema) -> Result<arrow_schema::Schema, String> {
    // SAFETY: per the contract, replace moves the contents out, leaving an
    // empty (released) struct behind for the caller.
    let ffi_schema = unsafe { std::ptr::replace(schema, FFI_ArrowSchema::empty()) };
    arrow_schema::Schema::try_from(&ffi_schema)
        .map_err(|e| format!("failed to import Arrow schema: {e}"))
}

/// Adds new columns to the dataset, committing a new version.
///
/// - `spec_json`: JSON object selecting the transform. One of
///   `{"kind": "sql", "expressions": [["name", "sql expr"], ...],
///   "read_columns"?: [string], "batch_size"?: uint}`,
///   `{"kind": "reader", "batch_size"?: uint}` (requires `stream`), or
///   `{"kind": "all_nulls"}` (requires `schema`). Must not be NULL.
/// - `stream`: Arrow C stream providing the new columns' values for
///   `kind = "reader"`, NULL otherwise. Ownership is always taken when
///   non-NULL, even on error, so the producer is released exactly once.
/// - `schema`: Arrow C schema describing the new all-null columns for
///   `kind = "all_nulls"`, NULL otherwise. Ownership is always taken when
///   non-NULL, even on error.
///
/// # Safety
///
/// `ds` must be a valid handle and the other arguments must satisfy the
/// contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_add_columns(
    ds: *mut LanceDataset,
    spec_json: *const c_char,
    stream: *mut FFI_ArrowArrayStream,
    schema: *mut FFI_ArrowSchema,
) -> i32 {
    // Take ownership of the Arrow resources FIRST so every subsequent error
    // path releases them exactly once.
    let reader = if stream.is_null() {
        None
    } else {
        match unsafe { arrow_bridge::import_stream(stream) } {
            Ok(reader) => Some(reader),
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        }
    };
    let imported_schema = if schema.is_null() {
        None
    } else {
        match unsafe { import_schema(schema) } {
            Ok(schema) => Some(schema),
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        }
    };

    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let spec_json = match unsafe { storage::required_str(spec_json, "spec_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let spec: AddColumnsSpec = match serde_json::from_str(spec_json) {
        Ok(spec) => spec,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid add-columns spec JSON: {e}"),
            );
        }
    };

    let transform = match spec.kind.as_str() {
        "sql" => {
            if spec.expressions.is_empty() {
                return set_error(
                    ErrorCode::InvalidArgument,
                    "kind \"sql\" requires a non-empty \"expressions\" list",
                );
            }
            NewColumnTransform::SqlExpressions(spec.expressions)
        }
        "reader" => match reader {
            Some(reader) => NewColumnTransform::Reader(Box::new(reader)),
            None => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    "kind \"reader\" requires a non-NULL stream",
                );
            }
        },
        "all_nulls" => match imported_schema {
            Some(schema) => NewColumnTransform::AllNulls(Arc::new(schema)),
            None => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    "kind \"all_nulls\" requires a non-NULL schema",
                );
            }
        },
        other => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!(
                    "unsupported add-columns kind {other:?}, expected \"sql\", \"reader\" or \"all_nulls\""
                ),
            );
        }
    };

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.add_columns(transform, spec.read_columns, spec.batch_size)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Drops columns from the dataset, committing a new version. This is a
/// metadata-only operation. The column data is removed later by compaction
/// and cleanup.
///
/// - `columns`: NULL-terminated array of column names (`[c1, c2, NULL]`).
///   Must contain at least one column.
///
/// # Safety
///
/// `ds` must be a valid handle and `columns` a valid NULL-terminated array
/// of valid C strings.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_drop_columns(
    ds: *mut LanceDataset,
    columns: *const *const c_char,
) -> i32 {
    if ds.is_null() || columns.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and columns must not be NULL",
        );
    }
    let mut names: Vec<&str> = Vec::new();
    let mut i = 0usize;
    loop {
        // SAFETY: the caller guarantees the array is NULL-terminated.
        let ptr = unsafe { *columns.add(i) };
        if ptr.is_null() {
            break;
        }
        match unsafe { storage::required_str(ptr, "column name") } {
            Ok(name) => names.push(name),
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        }
        i += 1;
    }
    if names.is_empty() {
        return set_error(
            ErrorCode::InvalidArgument,
            "columns must contain at least one column name",
        );
    }

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.drop_columns(&names)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Alters columns of the dataset (rename, nullability, type cast),
/// committing a new version.
///
/// - `alterations_json`: JSON array
///   `[{"path": string, "rename"?: string, "nullable"?: bool,
///   "data_type"?: string}, ...]`. `data_type` accepts: int32, int64,
///   float32, float64, utf8, large_utf8, binary, bool, date32, date64,
///   timestamp_us, timestamp_ns. Must not be NULL or empty.
///
/// # Safety
///
/// `ds` must be a valid handle and `alterations_json` a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_alter_columns(
    ds: *mut LanceDataset,
    alterations_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let json = match unsafe { storage::required_str(alterations_json, "alterations_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let parsed: Vec<AlterationJson> = match serde_json::from_str(json) {
        Ok(parsed) => parsed,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid alterations JSON: {e}"),
            );
        }
    };
    if parsed.is_empty() {
        return set_error(
            ErrorCode::InvalidArgument,
            "alterations must contain at least one entry",
        );
    }

    let mut alterations = Vec::with_capacity(parsed.len());
    for a in parsed {
        let mut alteration = ColumnAlteration::new(a.path);
        if let Some(rename) = a.rename {
            alteration = alteration.rename(rename);
        }
        if let Some(nullable) = a.nullable {
            alteration = alteration.set_nullable(nullable);
        }
        if let Some(name) = a.data_type {
            match parse_data_type(&name) {
                Ok(data_type) => alteration = alteration.cast_to(data_type),
                Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
            }
        }
        alterations.push(alteration);
    }

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.alter_columns(&alterations)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Merges new columns into the dataset by joining an Arrow stream on a key
/// column, committing a new version. Rows without a match get nulls.
///
/// - `stream`: Arrow C stream with the right-hand side of the join.
///   Ownership is always taken, even on error, so the producer is released
///   exactly once. Must not be NULL.
/// - `left_on` / `right_on`: join key column names in the dataset and the
///   stream, respectively. Must not be NULL.
///
/// # Safety
///
/// `ds` must be a valid handle and the other arguments must satisfy the
/// contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_merge(
    ds: *mut LanceDataset,
    stream: *mut FFI_ArrowArrayStream,
    left_on: *const c_char,
    right_on: *const c_char,
) -> i32 {
    // Import the stream FIRST so its producer resources are owned (and thus
    // released) on every subsequent error path.
    let reader = match unsafe { arrow_bridge::import_stream(stream) } {
        Ok(reader) => reader,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (left_on, right_on) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(left_on, "left_on")?,
                storage::required_str(right_on, "right_on")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.merge(reader, left_on, right_on)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
