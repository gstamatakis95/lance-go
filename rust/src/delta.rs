//! Change-data-capture between dataset versions (delta queries).

use std::ffi::c_char;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use chrono::{DateTime, Utc};
use lance::dataset::delta::DatasetDelta;
use serde::Deserialize;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::maintenance::{emit_json, parse_json_options};
use crate::runtime::block_on_cc;
use crate::storage;

/// Delta row kinds accepted by `lance_dataset_delta`.
pub const LANCE_DELTA_INSERTED: i32 = 0;
/// See [`LANCE_DELTA_INSERTED`].
pub const LANCE_DELTA_UPDATED: i32 = 1;
/// See [`LANCE_DELTA_INSERTED`].
pub const LANCE_DELTA_UPSERTED: i32 = 2;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DeltaSpec {
    /// Compare the checked-out version against this version. Mutually
    /// exclusive with the explicit ranges below.
    compared_against_version: Option<u64>,
    /// Explicit version range: (begin, end], both must be set together.
    begin_version: Option<u64>,
    end_version: Option<u64>,
    /// Explicit RFC 3339 timestamp range: (begin, end], both set together.
    begin_date: Option<String>,
    end_date: Option<String>,
}

fn parse_date(value: &str, what: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| format!("invalid {what} (want RFC 3339): {e}"))
}

/// Builds the [`DatasetDelta`] described by `spec_json`.
///
/// # Safety
///
/// `ds` must be a valid handle and `spec_json` NULL or a valid C string.
unsafe fn build_delta(
    ds: *const LanceDataset,
    spec_json: *const c_char,
) -> Result<DatasetDelta, (ErrorCode, String)> {
    let spec_json = unsafe { storage::optional_str(spec_json, "spec_json") }
        .map_err(|msg| (ErrorCode::InvalidArgument, msg))?;
    let spec: DeltaSpec = parse_json_options(spec_json, "delta spec")
        .map_err(|msg| (ErrorCode::InvalidArgument, msg))?;

    let dataset = unsafe { &*ds }.dataset();
    let mut builder = dataset.delta();
    if let Some(version) = spec.compared_against_version {
        builder = builder.compared_against_version(version);
    }
    if let Some(version) = spec.begin_version {
        builder = builder.with_begin_version(version);
    }
    if let Some(version) = spec.end_version {
        builder = builder.with_end_version(version);
    }
    if let Some(date) = &spec.begin_date {
        builder = builder.with_begin_date(
            parse_date(date, "begin_date").map_err(|msg| (ErrorCode::InvalidArgument, msg))?,
        );
    }
    if let Some(date) = &spec.end_date {
        builder = builder.with_end_date(
            parse_date(date, "end_date").map_err(|msg| (ErrorCode::InvalidArgument, msg))?,
        );
    }
    builder
        .build()
        .map_err(|e| (map_lance_error(&e), e.to_string()))
}

/// Streams the rows changed between two dataset versions.
///
/// - `spec_json`: optional JSON object with exactly one of
///   `{"compared_against_version": uint}`, `{"begin_version": uint,
///   "end_version": uint}` or `{"begin_date": rfc3339, "end_date": rfc3339}`
///   (begin exclusive, end inclusive), or NULL/empty (rejected by lance:
///   a range must be specified).
/// - `kind`: 0 = inserted rows, 1 = updated rows, 2 = upserted rows
///   (inserted + updated).
/// - `out`: receives a self-contained Arrow C stream of the changed rows
///   (including the `_rowid`, `_row_created_at_version` and
///   `_row_last_updated_at_version` columns). The caller owns it and must
///   call its `release` callback.
///
/// # Safety
///
/// `ds` must be a valid handle, `spec_json` NULL or a valid C string, and
/// `out` valid for writes (previous contents are overwritten without
/// release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_delta(
    ds: *const LanceDataset,
    spec_json: *const c_char,
    kind: i32,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let delta = match unsafe { build_delta(ds, spec_json) } {
        Ok(delta) => delta,
        Err((code, msg)) => return set_error(code, msg),
    };
    let stream = match kind {
        LANCE_DELTA_INSERTED => block_on_cc!(delta.get_inserted_rows()),
        LANCE_DELTA_UPDATED => block_on_cc!(delta.get_updated_rows()),
        LANCE_DELTA_UPSERTED => block_on_cc!(delta.get_upserted_rows()),
        other => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!(
                    "invalid delta kind {other}, expected 0 (inserted), 1 (updated) or 2 (upserted)"
                ),
            );
        }
    };
    let stream = match stream {
        Ok(stream) => stream,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match unsafe { arrow_bridge::export_stream(stream, out) } {
        Ok(()) => ok(),
        Err(msg) => set_error(ErrorCode::Internal, msg),
    }
}

/// Lists the transactions between two dataset versions as JSON
/// `[{"read_version": uint, "uuid": string, "operation": string,
/// "tag": string|null, "properties": {..}|null}, ...]`. The caller owns
/// `*out_json` and must free it with `lance_string_free`.
///
/// - `spec_json`: same contract as in `lance_dataset_delta`.
///
/// # Safety
///
/// `ds` must be a valid handle, `spec_json` NULL or a valid C string, and
/// `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_delta_transactions(
    ds: *const LanceDataset,
    spec_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let delta = match unsafe { build_delta(ds, spec_json) } {
        Ok(delta) => delta,
        Err((code, msg)) => return set_error(code, msg),
    };
    let transactions = match block_on_cc!(delta.list_transactions()) {
        Ok(transactions) => transactions,
        Err(e) => return set_error(map_lance_error(&e), e),
    };

    // Transaction does not derive Serialize, so emit a summary by hand.
    let json = serde_json::Value::Array(
        transactions
            .iter()
            .map(|t| {
                serde_json::json!({
                    "read_version": t.read_version,
                    "uuid": t.uuid,
                    "operation": t.operation.name(),
                    "tag": t.tag,
                    "properties": t.transaction_properties.as_deref(),
                })
            })
            .collect(),
    );
    unsafe { emit_json(json.to_string(), out_json) }
}
