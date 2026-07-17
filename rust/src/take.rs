//! Point reads: take by offsets / stable row ids / row addresses, range
//! scans over row offsets, and random sampling.

use std::sync::Arc;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator};
use futures::StreamExt;
use lance::dataset::{ProjectionRequest, TakeBuilder};
use serde::Deserialize;
use std::ffi::c_char;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::maintenance::parse_json_options;
use crate::runtime::block_on_cc;
use crate::storage;

/// One `{"name": ..., "expr": ...}` entry of a SQL projection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedExprJson {
    name: String,
    expr: String,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TakeSpec {
    /// Row offsets in the dataset (position 0 = first live row).
    indices: Option<Vec<u64>>,
    /// Stable row ids (requires `enable_stable_row_ids`, without the feature
    /// row ids equal row addresses).
    row_ids: Option<Vec<u64>>,
    /// Row addresses (`fragment_id << 32 | row_offset`).
    addresses: Option<Vec<u64>>,
    /// Plain column projection. Mutually exclusive with `sql_projection`.
    columns: Option<Vec<String>>,
    /// SQL expression projection. Mutually exclusive with `columns`.
    sql_projection: Option<Vec<NamedExprJson>>,
    /// Include the `_rowaddr` column in the result.
    with_row_address: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TakeScanSpec {
    /// Half-open `[start, end)` ranges of row offsets.
    ranges: Vec<[u64; 2]>,
    columns: Option<Vec<String>>,
    batch_readahead: Option<usize>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SampleSpec {
    n: u64,
    columns: Option<Vec<String>>,
    fragment_ids: Option<Vec<u32>>,
}

/// Builds the [`ProjectionRequest`] for a take: SQL expressions win over a
/// plain column list, and neither means "all columns".
fn projection_request(
    dataset: &lance::Dataset,
    columns: Option<&Vec<String>>,
    sql: Option<&Vec<NamedExprJson>>,
) -> Result<ProjectionRequest, (ErrorCode, String)> {
    match (columns, sql) {
        (Some(_), Some(_)) => Err((
            ErrorCode::InvalidArgument,
            "\"columns\" and \"sql_projection\" are mutually exclusive".to_string(),
        )),
        (_, Some(sql)) => Ok(ProjectionRequest::from_sql(
            sql.iter().map(|e| (e.name.as_str(), e.expr.as_str())),
        )),
        (Some(columns), None) => {
            let schema = dataset
                .schema()
                .project(columns)
                .map_err(|e| (map_lance_error(&e), e.to_string()))?;
            Ok(ProjectionRequest::from_schema(schema))
        }
        (None, None) => Ok(ProjectionRequest::from_schema(dataset.schema().clone())),
    }
}

/// Projects the dataset schema by an optional column list (all columns when
/// `None`).
fn projected_schema(
    dataset: &lance::Dataset,
    columns: Option<&Vec<String>>,
) -> Result<lance::datatypes::Schema, (ErrorCode, String)> {
    match columns {
        Some(columns) => dataset
            .schema()
            .project(columns)
            .map_err(|e| (map_lance_error(&e), e.to_string())),
        None => Ok(dataset.schema().clone()),
    }
}

/// Exports a single record batch into `out` as a one-batch Arrow C stream.
/// The caller owns the stream and must call its `release` callback.
///
/// # Safety
///
/// `out` must be a valid, non-NULL pointer to writable memory for an
/// `ArrowArrayStream`, and any previous contents are overwritten without being
/// released.
pub(crate) unsafe fn export_single_batch(batch: RecordBatch, out: *mut FFI_ArrowArrayStream) {
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);
    let stream = FFI_ArrowArrayStream::new(Box::new(reader));
    // SAFETY: `out` is valid for writes per the contract above. Unaligned
    // write because the caller may hand us arbitrarily aligned memory.
    unsafe { std::ptr::write_unaligned(out, stream) };
}

/// Takes specific rows from the dataset and returns them as a one-batch
/// Arrow C stream.
///
/// - `spec_json`: JSON object with exactly one of `"indices"` (row offsets),
///   `"row_ids"` (stable row ids) or `"addresses"` (row addresses), plus
///   optionally `"columns"`: [string] or `"sql_projection"`:
///   `[{"name": string, "expr": string}]` (mutually exclusive) and
///   `"with_row_address"`: bool. Must not be NULL.
/// - `out`: receives a self-contained Arrow C stream producing exactly one
///   batch with the requested rows in request order. The caller owns it and
///   must call its `release` callback.
///
/// # Safety
///
/// `ds` must be a valid handle, `spec_json` a valid C string, and `out`
/// valid for writes (previous contents are overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_take(
    ds: *const LanceDataset,
    spec_json: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let spec_json = match unsafe { storage::required_str(spec_json, "spec_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let spec: TakeSpec = match parse_json_options(Some(spec_json), "take spec") {
        Ok(spec) => spec,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let dataset = Arc::new(unsafe { &*ds }.dataset());
    let projection = match projection_request(
        &dataset,
        spec.columns.as_ref(),
        spec.sql_projection.as_ref(),
    ) {
        Ok(projection) => projection,
        Err((code, msg)) => return set_error(code, msg),
    };

    let selectors = [
        spec.indices.is_some(),
        spec.row_ids.is_some(),
        spec.addresses.is_some(),
    ];
    if selectors.iter().filter(|s| **s).count() != 1 {
        return set_error(
            ErrorCode::InvalidArgument,
            "take spec must contain exactly one of \"indices\", \"row_ids\" or \"addresses\"",
        );
    }
    let with_row_address = spec.with_row_address.unwrap_or(false);

    // The offsets path goes through Dataset::take, which converts offsets to
    // addresses internally (accounting for deletions). That code path has no
    // with_row_address knob. row_ids/addresses use TakeBuilder which does.
    let result = if let Some(indices) = &spec.indices {
        if with_row_address {
            return set_error(
                ErrorCode::InvalidArgument,
                "\"with_row_address\" is only supported with \"row_ids\" or \"addresses\", not \"indices\"",
            );
        }
        block_on_cc!(dataset.take(indices, projection))
    } else {
        let builder = if let Some(row_ids) = &spec.row_ids {
            dataset.take_builder(row_ids, projection)
        } else {
            let addresses = spec.addresses.unwrap_or_default();
            match projection.into_projection_plan(dataset.clone()) {
                Ok(plan) => {
                    TakeBuilder::try_new_from_addresses(dataset.clone(), addresses, Arc::new(plan))
                }
                Err(e) => return set_error(map_lance_error(&e), e),
            }
        };
        let builder = match builder {
            Ok(builder) => builder.with_row_address(with_row_address),
            Err(e) => return set_error(map_lance_error(&e), e),
        };
        block_on_cc!(builder.execute())
    };

    match result {
        Ok(batch) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { export_single_batch(batch, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Streams batches for a list of `[start, end)` row-offset ranges.
///
/// - `spec_json`: JSON object `{"ranges": [[uint, uint], ...],
///   "columns"?: [string], "batch_readahead"?: uint}`. Must not be NULL.
/// - `out`: receives a self-contained Arrow C stream. The caller owns it and
///   must call its `release` callback. It stays valid after the dataset
///   handle is closed.
///
/// # Safety
///
/// `ds` must be a valid handle, `spec_json` a valid C string, and `out`
/// valid for writes (previous contents are overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_take_scan(
    ds: *const LanceDataset,
    spec_json: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let spec_json = match unsafe { storage::required_str(spec_json, "spec_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let spec: TakeScanSpec = match parse_json_options(Some(spec_json), "take scan spec") {
        Ok(spec) => spec,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let dataset = unsafe { &*ds }.dataset();
    let projection = match projected_schema(&dataset, spec.columns.as_ref()) {
        Ok(schema) => Arc::new(schema),
        Err((code, msg)) => return set_error(code, msg),
    };
    let ranges =
        futures::stream::iter(spec.ranges.into_iter().map(|[start, end]| Ok(start..end))).boxed();
    let stream = dataset.take_scan(ranges, projection, spec.batch_readahead.unwrap_or(16));
    match unsafe { arrow_bridge::export_stream(stream, out) } {
        Ok(()) => ok(),
        Err(msg) => set_error(ErrorCode::Internal, msg),
    }
}

/// Randomly samples `n` rows from the dataset and returns them (in row-id
/// order, not random order) as a one-batch Arrow C stream.
///
/// - `spec_json`: JSON object `{"n": uint, "columns"?: [string],
///   "fragment_ids"?: [uint]}`. Must not be NULL.
/// - `out`: receives a self-contained one-batch Arrow C stream. The caller
///   owns it and must call its `release` callback.
///
/// # Safety
///
/// `ds` must be a valid handle, `spec_json` a valid C string, and `out`
/// valid for writes (previous contents are overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_sample(
    ds: *const LanceDataset,
    spec_json: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let spec_json = match unsafe { storage::required_str(spec_json, "spec_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let spec: SampleSpec = match parse_json_options(Some(spec_json), "sample spec") {
        Ok(spec) => spec,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let dataset = unsafe { &*ds }.dataset();
    let projection = match projected_schema(&dataset, spec.columns.as_ref()) {
        Ok(schema) => schema,
        Err((code, msg)) => return set_error(code, msg),
    };
    match block_on_cc!(dataset.sample(spec.n as usize, &projection, spec.fragment_ids.as_deref())) {
        Ok(batch) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { export_single_batch(batch, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
