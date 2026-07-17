//! SQL queries over a dataset (DataFusion-backed).

use std::ffi::c_char;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::scanner::DatasetRecordBatchStream;
use serde::Deserialize;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::maintenance::parse_json_options;
use crate::runtime::block_on_cc;
use crate::storage;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SqlOptions {
    /// Table name the dataset is registered under (default "dataset").
    table_name: Option<String>,
    /// Include the `_rowid` column in the results.
    with_row_id: Option<bool>,
    /// Include the `_rowaddr` column in the results.
    with_row_addr: Option<bool>,
}

/// Runs a SQL query against the dataset and exports the results into `out`
/// as an Arrow C stream. The underlying engine is DataFusion, and the dataset is
/// registered as a table named by `"table_name"` (default `"dataset"`).
///
/// - `sql`: the SQL query text. Must not be NULL.
/// - `options_json`: optional JSON object `{"table_name"?: string,
///   "with_row_id"?: bool, "with_row_addr"?: bool}`, or NULL for defaults.
/// - `out`: receives a self-contained Arrow C stream. The caller owns it and
///   must call its `release` callback. It stays valid after the dataset
///   handle is closed.
///
/// # Safety
///
/// `ds` must be a valid handle, `sql` a valid C string, `options_json` NULL
/// or a valid C string, and `out` valid for writes (previous contents are
/// overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_sql(
    ds: *const LanceDataset,
    sql: *const c_char,
    options_json: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let (sql, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(sql, "sql")?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: SqlOptions = match parse_json_options(options_json, "sql options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let dataset = unsafe { &*ds }.dataset();
    let mut builder = dataset
        .sql(sql)
        .with_row_id(options.with_row_id.unwrap_or(false))
        .with_row_addr(options.with_row_addr.unwrap_or(false));
    if let Some(table_name) = &options.table_name {
        builder = builder.table_name(table_name);
    }

    let stream = match block_on_cc!(async { builder.build().await?.into_stream().await }) {
        Ok(stream) => DatasetRecordBatchStream::new(stream),
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match unsafe { arrow_bridge::export_stream(stream, out) } {
        Ok(()) => ok(),
        Err(msg) => set_error(ErrorCode::Internal, msg),
    }
}
