//! Fragment access: list fragment metadata, open a single fragment as an
//! opaque handle, and run per-fragment counts, takes and scans.
//!
//! A [`LanceFragment`] wraps a [`FileFragment`], which owns an `Arc<Dataset>`
//! snapshot internally, so a fragment handle (and any stream exported from a
//! fragment scan) stays valid even after the originating dataset handle is
//! closed, mirroring the [`crate::scanner::LanceScanner`] contract.

use std::ffi::{CString, c_char};

use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::fragment::FileFragment;
use serde::{Deserialize, Serialize};

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;
use crate::take::export_single_batch;

/// Opaque handle to a single fragment of a Lance dataset. Created by
/// `lance_dataset_get_fragment`. Must be released with `lance_fragment_close`.
/// Self-contained: the wrapped fragment snapshots the dataset, so the handle
/// outlives the originating dataset handle.
pub struct LanceFragment(FileFragment);

/// One entry of the `lance_dataset_fragments` result. `num_rows` is the
/// logical (live) row count (`physical_rows - num_deletions`).
#[derive(Serialize)]
struct FragmentInfoJson {
    id: u32,
    num_rows: u64,
    physical_rows: u64,
    num_deletions: u64,
    num_data_files: u32,
    data_files: Vec<String>,
}

/// Point-read spec for `lance_fragment_take`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FragmentTakeSpec {
    /// Row offsets within the fragment (0 = first live row of the fragment).
    indices: Vec<u32>,
    /// Column projection, all columns when absent.
    columns: Option<Vec<String>>,
}

/// The subset of scan options a fragment scan honors (see
/// `lance_fragment_scan_to_stream`).
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FragmentScanOptions {
    columns: Option<Vec<String>>,
    filter: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    with_row_id: Option<bool>,
    with_row_address: Option<bool>,
    batch_size: Option<u64>,
    scan_in_order: Option<bool>,
}

/// Lists the dataset's fragments as a JSON array of objects
/// `{"id": uint, "num_rows": uint, "physical_rows": uint,
/// "num_deletions": uint, "num_data_files": uint, "data_files": [string]}`.
/// `num_rows` is the live row count. The caller owns `*out_json` and must
/// free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid dataset handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_fragments(
    ds: *const LanceDataset,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let dataset = unsafe { &*ds }.dataset();
    let fragments = dataset.get_fragments();
    let infos: Result<Vec<FragmentInfoJson>, lance::Error> = block_on_cc!(async {
        let mut infos = Vec::with_capacity(fragments.len());
        for frag in &fragments {
            let physical = frag.physical_rows().await?;
            let deletions = frag.count_deletions().await?;
            let meta = frag.metadata();
            infos.push(FragmentInfoJson {
                id: meta.id as u32,
                num_rows: physical.saturating_sub(deletions) as u64,
                physical_rows: physical as u64,
                num_deletions: deletions as u64,
                num_data_files: frag.num_data_files() as u32,
                data_files: meta.files.iter().map(|f| f.path.clone()).collect(),
            });
        }
        Ok(infos)
    });
    let infos = match infos {
        Ok(infos) => infos,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let json = match serde_json::to_string(&infos) {
        Ok(json) => json,
        Err(e) => return set_error(ErrorCode::Internal, e),
    };
    match CString::new(json) {
        Ok(cstr) => {
            // SAFETY: `out_json` is non-NULL and valid for writes.
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Opens fragment `frag_id` as an opaque handle. Fails with `NotFound` when no
/// fragment has that id.
///
/// # Safety
///
/// `ds` must be a valid dataset handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_get_fragment(
    ds: *const LanceDataset,
    frag_id: u32,
    out: *mut *mut LanceFragment,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match dataset.get_fragment(frag_id as usize) {
        Some(fragment) => {
            let handle = Box::into_raw(Box::new(LanceFragment(fragment)));
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = handle };
            ok()
        }
        None => set_error(ErrorCode::NotFound, format!("fragment {frag_id} not found")),
    }
}

/// Releases a fragment handle. NULL is a no-op. Streams previously exported
/// from this fragment remain valid.
///
/// # Safety
///
/// `f` must be NULL or a handle from `lance_dataset_get_fragment` that has not
/// already been closed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_close(f: *mut LanceFragment) {
    if f.is_null() {
        return;
    }
    // SAFETY: per the contract, `f` came from Box::into_raw and is closed at
    // most once.
    drop(unsafe { Box::from_raw(f) });
}

/// Counts the live rows in the fragment, optionally restricted by an SQL
/// `filter` (NULL for "count everything").
///
/// # Safety
///
/// `f` must be a valid fragment handle, `filter` NULL or a valid C string, and
/// `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_count_rows(
    f: *const LanceFragment,
    filter: *const c_char,
    out: *mut u64,
) -> i32 {
    if f.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "f and out must not be NULL");
    }
    let filter = match unsafe { storage::optional_str(filter, "filter") } {
        Ok(filter) => filter.map(str::to_owned),
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let fragment = &unsafe { &*f }.0;
    match block_on_cc!(fragment.count_rows(filter)) {
        Ok(count) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = count as u64 };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Counts the rows deleted from the fragment.
///
/// # Safety
///
/// `f` must be a valid fragment handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_count_deletions(
    f: *const LanceFragment,
    out: *mut u64,
) -> i32 {
    if f.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "f and out must not be NULL");
    }
    let fragment = &unsafe { &*f }.0;
    match block_on_cc!(fragment.count_deletions()) {
        Ok(count) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = count as u64 };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns the number of physical rows stored in the fragment (before
/// deletions).
///
/// # Safety
///
/// `f` must be a valid fragment handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_physical_rows(
    f: *const LanceFragment,
    out: *mut u64,
) -> i32 {
    if f.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "f and out must not be NULL");
    }
    let fragment = &unsafe { &*f }.0;
    match block_on_cc!(fragment.physical_rows()) {
        Ok(count) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = count as u64 };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns the fragment's metadata as a JSON object (the serde form of
/// Lance's `Fragment`: `{"id": uint, "files": [{"path": string, ...}],
/// "physical_rows": uint|null, "deletion_file"?: {..}, ...}`). The caller owns
/// `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `f` must be a valid fragment handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_metadata(
    f: *const LanceFragment,
    out_json: *mut *mut c_char,
) -> i32 {
    if f.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "f and out_json must not be NULL",
        );
    }
    let fragment = &unsafe { &*f }.0;
    let json = match serde_json::to_string(fragment.metadata()) {
        Ok(json) => json,
        Err(e) => return set_error(ErrorCode::Internal, e),
    };
    match CString::new(json) {
        Ok(cstr) => {
            // SAFETY: `out_json` is non-NULL and valid for writes.
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Takes rows from the fragment by in-fragment offset and returns them as a
/// one-batch Arrow C stream.
///
/// - `spec_json`: JSON object `{"indices": [uint], "columns"?: [string]}`.
///   Must not be NULL.
/// - `out`: receives a self-contained one-batch Arrow C stream. The caller
///   owns it and must call its `release` callback.
///
/// # Safety
///
/// `f` must be a valid fragment handle, `spec_json` a valid C string, and
/// `out` valid for writes (previous contents overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_take(
    f: *const LanceFragment,
    spec_json: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if f.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "f and out must not be NULL");
    }
    let spec_json = match unsafe { storage::required_str(spec_json, "spec_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let spec: FragmentTakeSpec = match serde_json::from_str(spec_json) {
        Ok(spec) => spec,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid fragment take spec JSON: {e}"),
            );
        }
    };

    let fragment = &unsafe { &*f }.0;
    let schema = match &spec.columns {
        Some(columns) => match fragment.schema().project(columns) {
            Ok(schema) => schema,
            Err(e) => return set_error(map_lance_error(&e), e),
        },
        None => fragment.schema().clone(),
    };
    match block_on_cc!(fragment.take(&spec.indices, &schema)) {
        Ok(batch) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { export_single_batch(batch, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Scans the fragment and exports the result as a self-contained Arrow C
/// stream. Self-contained (no scanner.rs cross-module edit): builds a
/// `Scanner` from the fragment and applies a documented subset of scan
/// options inline.
///
/// - `scan_json`: optional JSON object `{"columns"?: [string],
///   "filter"?: string, "limit"?: int, "offset"?: int, "with_row_id"?: bool,
///   "with_row_address"?: bool, "batch_size"?: uint, "scan_in_order"?: bool}`,
///   or NULL for a full fragment scan. Unknown fields are rejected.
/// - `out`: receives a self-contained Arrow C stream. The caller owns it and
///   must call its `release` callback. It stays valid after the fragment and
///   dataset handles are closed.
///
/// # Safety
///
/// `f` must be a valid fragment handle, `scan_json` NULL or a valid C string,
/// and `out` valid for writes (previous contents overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_fragment_scan_to_stream(
    f: *const LanceFragment,
    scan_json: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if f.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "f and out must not be NULL");
    }
    let scan_json = match unsafe { storage::optional_str(scan_json, "scan_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: FragmentScanOptions = match scan_json {
        None => FragmentScanOptions::default(),
        Some(s) if s.trim().is_empty() => FragmentScanOptions::default(),
        Some(s) => match serde_json::from_str(s) {
            Ok(options) => options,
            Err(e) => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    format!("invalid fragment scan JSON: {e}"),
                );
            }
        },
    };

    let fragment = &unsafe { &*f }.0;
    let mut scanner = fragment.scan();
    if let Some(columns) = &options.columns
        && let Err(e) = scanner.project(columns)
    {
        return set_error(map_lance_error(&e), e);
    }
    if let Some(filter) = &options.filter
        && let Err(e) = scanner.filter(filter)
    {
        return set_error(map_lance_error(&e), e);
    }
    if (options.limit.is_some() || options.offset.is_some())
        && let Err(e) = scanner.limit(options.limit, options.offset)
    {
        return set_error(map_lance_error(&e), e);
    }
    if options.with_row_id.unwrap_or(false) {
        scanner.with_row_id();
    }
    if options.with_row_address.unwrap_or(false) {
        scanner.with_row_address();
    }
    if let Some(batch_size) = options.batch_size {
        scanner.batch_size(batch_size as usize);
    }
    if let Some(ordered) = options.scan_in_order {
        scanner.scan_in_order(ordered);
    }

    let stream = match block_on_cc!(scanner.try_into_stream()) {
        Ok(stream) => stream,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match unsafe { arrow_bridge::export_stream(stream, out) } {
        Ok(()) => ok(),
        Err(msg) => set_error(ErrorCode::Internal, msg),
    }
}
