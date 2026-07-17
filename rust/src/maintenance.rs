//! Dataset maintenance: file compaction and old-version cleanup.

use std::ffi::{CString, c_char};

use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use serde::Deserialize;

use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// Partial mirror of [`CompactionOptions`]. Every field is optional so
/// callers only send what they want to override. (`CompactionOptions`
/// derives `Deserialize` but not `#[serde(default)]`, so partial JSON cannot
/// be deserialized into it directly.)
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CompactOptionsJson {
    target_rows_per_fragment: Option<usize>,
    max_rows_per_group: Option<usize>,
    max_bytes_per_file: Option<usize>,
    materialize_deletions: Option<bool>,
    materialize_deletions_threshold: Option<f32>,
    num_threads: Option<usize>,
    batch_size: Option<usize>,
    io_buffer_size: Option<u64>,
    defer_index_remap: Option<bool>,
}

impl CompactOptionsJson {
    fn into_options(self) -> CompactionOptions {
        let mut options = CompactionOptions::default();
        if let Some(v) = self.target_rows_per_fragment {
            options.target_rows_per_fragment = v;
        }
        if let Some(v) = self.max_rows_per_group {
            options.max_rows_per_group = v;
        }
        if self.max_bytes_per_file.is_some() {
            options.max_bytes_per_file = self.max_bytes_per_file;
        }
        if let Some(v) = self.materialize_deletions {
            options.materialize_deletions = v;
        }
        if let Some(v) = self.materialize_deletions_threshold {
            options.materialize_deletions_threshold = v;
        }
        if self.num_threads.is_some() {
            options.num_threads = self.num_threads;
        }
        if self.batch_size.is_some() {
            options.batch_size = self.batch_size;
        }
        if self.io_buffer_size.is_some() {
            options.io_buffer_size = self.io_buffer_size;
        }
        if let Some(v) = self.defer_index_remap {
            options.defer_index_remap = v;
        }
        options
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupOptionsJson {
    #[serde(default = "default_older_than_seconds")]
    older_than_seconds: i64,
    delete_unverified: Option<bool>,
    error_if_tagged_old_versions: Option<bool>,
}

impl Default for CleanupOptionsJson {
    fn default() -> Self {
        Self {
            older_than_seconds: default_older_than_seconds(),
            delete_unverified: None,
            error_if_tagged_old_versions: None,
        }
    }
}

/// Two weeks, matching `Dataset::cleanup_old_versions`'s documented default.
fn default_older_than_seconds() -> i64 {
    14 * 24 * 60 * 60
}

/// Parses optional JSON options. NULL/empty maps to `T::default()`.
pub(crate) fn parse_json_options<T: Default + for<'de> Deserialize<'de>>(
    json: Option<&str>,
    what: &str,
) -> Result<T, String> {
    match json {
        None => Ok(T::default()),
        Some(s) if s.trim().is_empty() => Ok(T::default()),
        Some(s) => serde_json::from_str(s).map_err(|e| format!("invalid {what} JSON: {e}")),
    }
}

/// Writes `json` into `out_json` as an owned C string (the caller frees it
/// with `lance_string_free`) and returns Ok.
///
/// # Safety
///
/// `out_json` must be non-NULL and valid for writes.
pub(crate) unsafe fn emit_json(json: String, out_json: *mut *mut c_char) -> i32 {
    match CString::new(json) {
        Ok(cstr) => {
            // SAFETY: `out_json` is non-NULL and valid for writes per the
            // contract above.
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Compacts small data files of the dataset into larger ones, committing the
/// result as a new version.
///
/// - `options_json`: optional JSON object
///   `{"target_rows_per_fragment"?: uint, "max_rows_per_group"?: uint,
///   "max_bytes_per_file"?: uint, "materialize_deletions"?: bool,
///   "materialize_deletions_threshold"?: float, "num_threads"?: uint,
///   "batch_size"?: uint, "io_buffer_size"?: uint,
///   "defer_index_remap"?: bool}`, or NULL for defaults.
/// - `out_json`: receives the compaction metrics as JSON
///   `{"fragments_removed": uint, "fragments_added": uint,
///   "files_removed": uint, "files_added": uint}`. Free it with
///   `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle, `options_json` NULL or a valid C string, and
/// `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_compact_files(
    ds: *mut LanceDataset,
    options_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let options_json = match unsafe { storage::optional_str(options_json, "options_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: CompactOptionsJson = match parse_json_options(options_json, "compaction options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    // compact_files needs `&mut Dataset`: hold the handle's mutex for the
    // duration so the updated dataset is visible to subsequent calls.
    //
    // remap_options is always None: `IndexRemapperOptions` is a trait object
    // with no JSON-constructible implementation, so a custom remap hook is
    // not FFI-expressible. Lance falls back to its default index remapping
    // (or defers it when `defer_index_remap` is set).
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let metrics = match block_on_cc!(compact_files(&mut guard, options.into_options(), None)) {
        Ok(metrics) => metrics,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    drop(guard);

    match serde_json::to_string(&metrics) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Removes old dataset versions (and files unique to them) from storage.
/// Removed versions can no longer be checked out or restored.
///
/// - `options_json`: optional JSON object
///   `{"older_than_seconds"?: int (default 1209600, i.e. two weeks),
///   "delete_unverified"?: bool, "error_if_tagged_old_versions"?: bool}`,
///   or NULL for defaults.
/// - `out_json`: receives the removal statistics as JSON
///   `{"bytes_removed": uint, "old_versions": uint,
///   "data_files_removed": uint, "transaction_files_removed": uint,
///   "index_files_removed": uint, "deletion_files_removed": uint}`. Free it
///   with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle, `options_json` NULL or a valid C string, and
/// `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_cleanup_old_versions(
    ds: *const LanceDataset,
    options_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let options_json = match unsafe { storage::optional_str(options_json, "options_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: CleanupOptionsJson = match parse_json_options(options_json, "cleanup options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    // cleanup_old_versions takes `&self`: clone the (Arc-backed) dataset out
    // of the guard so the mutex is not held across the IO-heavy cleanup.
    let dataset = unsafe { &*ds }.dataset();
    let stats: RemovalStats = match block_on_cc!(dataset.cleanup_old_versions(
        chrono::Duration::seconds(options.older_than_seconds),
        options.delete_unverified,
        options.error_if_tagged_old_versions,
    )) {
        Ok(stats) => stats,
        Err(e) => return set_error(map_lance_error(&e), e),
    };

    // RemovalStats does not derive Serialize, so build the JSON by hand.
    let json = serde_json::json!({
        "bytes_removed": stats.bytes_removed,
        "old_versions": stats.old_versions,
        "data_files_removed": stats.data_files_removed,
        "transaction_files_removed": stats.transaction_files_removed,
        "index_files_removed": stats.index_files_removed,
        "deletion_files_removed": stats.deletion_files_removed,
    });
    unsafe { emit_json(json.to_string(), out_json) }
}
