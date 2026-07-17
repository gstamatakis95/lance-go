//! Dataset lifecycle: open, write, close, and metadata accessors.
//!
//! # Deliberately not exposed
//!
//! - `Dataset::write_into_namespace` / `write_into_namespace_on_branch`:
//!   both require an `Arc<dyn LanceNamespace>` trait object (a live
//!   namespace client with credential vending). There is no
//!   JSON-constructible implementation to build one from FFI parameters, so
//!   these cannot be expressed across the C ABI. Namespace-managed tables
//!   should be resolved to a plain URI + storage options on the Go side.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{ExternalBlobMode, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use serde::Deserialize;

use crate::arrow_bridge;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// Opaque handle to an opened Lance dataset. Created by `lance_dataset_open`
/// or `lance_dataset_write`. Must be released with `lance_dataset_close`.
pub struct LanceDataset(pub(crate) Mutex<lance::Dataset>);

impl LanceDataset {
    /// Returns a cheap clone of the wrapped dataset (Lance datasets are
    /// internally reference-counted).
    pub(crate) fn dataset(&self) -> lance::Dataset {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct OpenOptions {
    version: Option<u64>,
    tag: Option<String>,
    branch: Option<String>,
    index_cache_bytes: Option<u64>,
    metadata_cache_bytes: Option<u64>,
}

pub(crate) fn validate_open_ref(
    version: Option<u64>,
    tag: Option<&str>,
    branch: Option<&str>,
) -> Result<(), String> {
    if tag == Some("") {
        return Err("tag must not be empty".to_string());
    }
    if branch == Some("") {
        return Err("branch must not be empty".to_string());
    }
    if tag.is_some() && (version.is_some() || branch.is_some()) {
        return Err("tag cannot be combined with version or branch".to_string());
    }
    Ok(())
}

/// JSON form of [`lance::dataset::write::AutoCleanupParams`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AutoCleanupJson {
    interval: usize,
    older_than_seconds: i64,
}

/// JSON form of [`lance::table::format::BasePath`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BasePathJson {
    id: u32,
    name: Option<String>,
    #[serde(default)]
    is_dataset_root: bool,
    path: String,
}

/// JSON wire format shared by every dataset write entry point.
///
/// Keep option decoding and [`WriteParams`] construction here so the plain,
/// session-backed, and progress-reporting paths cannot drift apart.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WriteOptions {
    mode: Option<String>,
    max_rows_per_file: Option<usize>,
    max_rows_per_group: Option<usize>,
    max_bytes_per_file: Option<usize>,
    data_storage_version: Option<String>,
    enable_stable_row_ids: Option<bool>,
    enable_v2_manifest_paths: Option<bool>,
    auto_cleanup: Option<AutoCleanupJson>,
    skip_auto_cleanup: Option<bool>,
    transaction_properties: Option<HashMap<String, String>>,
    /// "reference" (default) or "ingest".
    external_blob_mode: Option<String>,
    blob_pack_file_size_threshold: Option<usize>,
    allow_external_blob_outside_bases: Option<bool>,
    initial_bases: Option<Vec<BasePathJson>>,
    target_bases: Option<Vec<u32>>,
    target_base_names_or_paths: Option<Vec<String>>,
    /// Per-base storage options, keyed by base name or path.
    base_store_params: Option<HashMap<String, HashMap<String, String>>>,
}

fn parse_json_options<T: Default + for<'de> Deserialize<'de>>(
    json: Option<&str>,
    what: &str,
) -> Result<T, String> {
    match json {
        None => Ok(T::default()),
        Some(s) if s.trim().is_empty() => Ok(T::default()),
        Some(s) => serde_json::from_str(s).map_err(|e| format!("invalid {what} JSON: {e}")),
    }
}

fn parse_write_mode(mode: Option<&str>) -> Result<WriteMode, String> {
    match mode {
        None | Some("create") => Ok(WriteMode::Create),
        Some("append") => Ok(WriteMode::Append),
        Some("overwrite") => Ok(WriteMode::Overwrite),
        Some(other) => Err(format!(
            "invalid write mode {other:?}, expected \"create\", \"append\" or \"overwrite\""
        )),
    }
}

/// Decodes the common write-options JSON and constructs the complete Lance
/// write parameters. Callers may attach path-specific runtime state (for
/// example a session or progress callback) before invoking `Dataset::write`.
pub(crate) fn build_write_params(
    options_json: Option<&str>,
    storage_options: HashMap<String, String>,
) -> Result<WriteParams, String> {
    let options: WriteOptions = parse_json_options(options_json, "write options")?;
    let mode = parse_write_mode(options.mode.as_deref())?;
    let data_storage_version = options
        .data_storage_version
        .as_deref()
        .map(LanceFileVersion::from_str)
        .transpose()
        .map_err(|e| e.to_string())?;

    let mut params = WriteParams {
        mode,
        data_storage_version,
        store_params: storage::object_store_params(storage_options),
        ..Default::default()
    };
    if let Some(n) = options.max_rows_per_file {
        params.max_rows_per_file = n;
    }
    if let Some(n) = options.max_rows_per_group {
        params.max_rows_per_group = n;
    }
    if let Some(n) = options.max_bytes_per_file {
        params.max_bytes_per_file = n;
    }
    if let Some(enable) = options.enable_stable_row_ids {
        params.enable_stable_row_ids = enable;
    }
    if let Some(enable) = options.enable_v2_manifest_paths {
        params.enable_v2_manifest_paths = enable;
    }
    if let Some(auto_cleanup) = options.auto_cleanup {
        params.auto_cleanup = Some(lance::dataset::AutoCleanupParams {
            interval: auto_cleanup.interval,
            older_than: chrono::TimeDelta::seconds(auto_cleanup.older_than_seconds),
        });
    }
    if let Some(skip) = options.skip_auto_cleanup {
        params.skip_auto_cleanup = skip;
    }
    if let Some(properties) = options.transaction_properties {
        params.transaction_properties = Some(Arc::new(properties));
    }
    if let Some(mode) = options.external_blob_mode.as_deref() {
        params.external_blob_mode = ExternalBlobMode::try_from(mode).map_err(|e| e.to_string())?;
    }
    if options.blob_pack_file_size_threshold.is_some() {
        params.blob_pack_file_size_threshold = options.blob_pack_file_size_threshold;
    }
    if let Some(allow) = options.allow_external_blob_outside_bases {
        params.allow_external_blob_outside_bases = allow;
    }
    if let Some(bases) = options.initial_bases {
        params.initial_bases = Some(
            bases
                .into_iter()
                .map(|b| {
                    lance::table::format::BasePath::new(b.id, b.path, b.name, b.is_dataset_root)
                })
                .collect(),
        );
    }
    if options.target_bases.is_some() {
        params.target_bases = options.target_bases;
    }
    if options.target_base_names_or_paths.is_some() {
        params.target_base_names_or_paths = options.target_base_names_or_paths;
    }
    if let Some(base_store_params) = options.base_store_params {
        let map = base_store_params
            .into_iter()
            .filter_map(|(name, kv)| storage::object_store_params(kv).map(|p| (name, p)))
            .collect::<HashMap<_, _>>();
        if !map.is_empty() {
            params.base_store_params = Some(map);
        }
    }
    Ok(params)
}

#[cfg(test)]
mod write_params_tests {
    use super::*;

    #[test]
    fn complete_options_map_to_write_params() {
        let json = r#"{
            "mode":"overwrite",
            "max_rows_per_file":101,
            "max_rows_per_group":17,
            "max_bytes_per_file":4096,
            "data_storage_version":"2.1",
            "enable_stable_row_ids":true,
            "enable_v2_manifest_paths":false,
            "auto_cleanup":{"interval":7,"older_than_seconds":3600},
            "skip_auto_cleanup":true,
            "transaction_properties":{"engine":"go","job":"parity"},
            "external_blob_mode":"ingest",
            "blob_pack_file_size_threshold":8192,
            "allow_external_blob_outside_bases":false,
            "initial_bases":[{"id":1,"name":"archive","path":"file:///archive"}],
            "target_bases":[1],
            "target_base_names_or_paths":["archive"],
            "base_store_params":{"archive":{"region":"test"}}
        }"#;

        let params = build_write_params(Some(json), HashMap::new()).unwrap();
        assert!(matches!(params.mode, WriteMode::Overwrite));
        assert_eq!(params.max_rows_per_file, 101);
        assert_eq!(params.max_rows_per_group, 17);
        assert_eq!(params.max_bytes_per_file, 4096);
        assert!(params.data_storage_version.is_some());
        assert!(params.enable_stable_row_ids);
        assert!(!params.enable_v2_manifest_paths);
        let cleanup = params.auto_cleanup.as_ref().unwrap();
        assert_eq!(cleanup.interval, 7);
        assert_eq!(cleanup.older_than, chrono::TimeDelta::hours(1));
        assert!(params.skip_auto_cleanup);
        assert_eq!(
            params.transaction_properties.as_ref().unwrap()["job"],
            "parity"
        );
        assert_eq!(params.external_blob_mode, ExternalBlobMode::Ingest);
        assert_eq!(params.blob_pack_file_size_threshold, Some(8192));
        assert!(!params.allow_external_blob_outside_bases);
        let bases = params.initial_bases.as_ref().unwrap();
        assert_eq!(bases.len(), 1);
        assert_eq!(bases[0].id, 1);
        assert_eq!(bases[0].name.as_deref(), Some("archive"));
        assert_eq!(params.target_bases.as_deref(), Some(&[1][..]));
        assert_eq!(
            params.target_base_names_or_paths.as_deref(),
            Some(&["archive".to_owned()][..])
        );
        assert!(
            params
                .base_store_params
                .as_ref()
                .unwrap()
                .contains_key("archive")
        );
    }

    #[test]
    fn invalid_advanced_option_is_rejected() {
        let err = build_write_params(
            Some(r#"{"external_blob_mode":"not-a-mode"}"#),
            HashMap::new(),
        )
        .unwrap_err();
        assert!(err.contains("external blob mode"), "{err}");
    }

    #[test]
    fn unknown_write_option_is_rejected() {
        let err =
            build_write_params(Some(r#"{"future_option":true}"#), HashMap::new()).unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
    }
}

/// Writes `dataset` into `out` as a heap-allocated opaque handle.
fn emit_dataset(dataset: lance::Dataset, out: *mut *mut LanceDataset) {
    let handle = Box::into_raw(Box::new(LanceDataset(Mutex::new(dataset))));
    // SAFETY: callers validated `out` is non-NULL and writable.
    unsafe { *out = handle };
}

/// Opens an existing Lance dataset.
///
/// - `uri`: dataset URI (e.g. a local path). Must not be NULL.
/// - `storage_kv`: NULL-terminated array of alternating storage-option
///   keys/values (`[k1, v1, ..., NULL]`), or NULL for none.
/// - `options_json`: optional JSON object
///   `{"version"?: uint, "tag"?: string, "branch"?: string,
///   "index_cache_bytes"?: uint, "metadata_cache_bytes"?: uint}`, or NULL.
///   `branch` checks out the named branch (at `version` if also given,
///   otherwise its latest version).
/// - `out`: receives the dataset handle on success. Release it with
///   `lance_dataset_close`.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above and `out` must be
/// valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_open(
    uri: *const c_char,
    storage_kv: *const *const c_char,
    options_json: *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    if out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "out must not be NULL");
    }
    let (uri, storage_options, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(uri, "uri")?,
                storage::parse_storage_kv(storage_kv)?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: OpenOptions = match parse_json_options(options_json, "open options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if let Err(msg) = validate_open_ref(
        options.version,
        options.tag.as_deref(),
        options.branch.as_deref(),
    ) {
        return set_error(ErrorCode::InvalidArgument, msg);
    }

    let mut builder = DatasetBuilder::from_uri(uri);
    if !storage_options.is_empty() {
        builder = builder.with_storage_options(storage_options);
    }
    if let Some(branch) = &options.branch {
        builder = builder.with_branch(branch, options.version);
    } else if let Some(version) = options.version {
        builder = builder.with_version(version);
    }
    if let Some(tag) = &options.tag {
        builder = builder.with_tag(tag);
    }
    if let Some(bytes) = options.index_cache_bytes {
        builder = builder.with_index_cache_size_bytes(bytes as usize);
    }
    if let Some(bytes) = options.metadata_cache_bytes {
        builder = builder.with_metadata_cache_size_bytes(bytes as usize);
    }

    match block_on_cc!(builder.load()) {
        Ok(dataset) => {
            emit_dataset(dataset, out);
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Writes an Arrow stream of record batches to a Lance dataset.
///
/// - `stream`: Arrow C stream of record batches. Ownership is always taken,
///   even on error, so the producer is released exactly once. Must not be
///   NULL.
/// - `uri`: destination dataset URI. Must not be NULL.
/// - `options_json`: optional JSON object
///   `{"mode"?: "create"|"append"|"overwrite", "max_rows_per_file"?: uint,
///   "max_rows_per_group"?: uint, "max_bytes_per_file"?: uint,
///   "data_storage_version"?: string, "enable_stable_row_ids"?: bool,
///   "enable_v2_manifest_paths"?: bool,
///   "auto_cleanup"?: {"interval": uint, "older_than_seconds": int},
///   "skip_auto_cleanup"?: bool, "transaction_properties"?: {..},
///   "external_blob_mode"?: "reference"|"ingest",
///   "blob_pack_file_size_threshold"?: uint,
///   "allow_external_blob_outside_bases"?: bool,
///   "initial_bases"?: [{"id": uint, "name"?: string,
///   "is_dataset_root"?: bool, "path": string}],
///   "target_bases"?: [uint], "target_base_names_or_paths"?: [string],
///   "base_store_params"?: {"<base>": {"<key>": "<value>"}}}`,
///   or NULL (defaults to mode "create").
/// - `storage_kv`: same format as in `lance_dataset_open`, or NULL.
/// - `out`: if non-NULL, receives a handle to the resulting dataset (release
///   with `lance_dataset_close`). Pass NULL to discard it.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_write(
    stream: *mut FFI_ArrowArrayStream,
    uri: *const c_char,
    options_json: *const c_char,
    storage_kv: *const *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    // Import the stream FIRST so its producer resources are owned (and thus
    // released) on every subsequent error path.
    let reader = match unsafe { arrow_bridge::import_stream(stream) } {
        Ok(reader) => reader,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let (uri, storage_options, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(uri, "uri")?,
                storage::parse_storage_kv(storage_kv)?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let params = match build_write_params(options_json, storage_options) {
        Ok(params) => params,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    match block_on_cc!(lance::Dataset::write(reader, uri, Some(params))) {
        Ok(dataset) => {
            if !out.is_null() {
                emit_dataset(dataset, out);
            }
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Releases a dataset handle. NULL is a no-op. The handle must not be used
/// again afterwards.
///
/// # Safety
///
/// `ds` must be NULL or a handle obtained from `lance_dataset_open` /
/// `lance_dataset_write` that has not already been closed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_close(ds: *mut LanceDataset) {
    if ds.is_null() {
        return;
    }
    // SAFETY: per the contract, `ds` came from Box::into_raw and is closed
    // at most once.
    drop(unsafe { Box::from_raw(ds) });
}

/// Counts the rows in the dataset, optionally restricted by an SQL filter
/// expression (`filter` may be NULL for "count everything").
///
/// # Safety
///
/// `ds` must be a valid handle, `filter` NULL or a valid C string, and `out`
/// valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_count_rows(
    ds: *const LanceDataset,
    filter: *const c_char,
    out: *mut u64,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let filter = match unsafe { storage::optional_str(filter, "filter") } {
        Ok(filter) => filter.map(str::to_owned),
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.count_rows(filter)) {
        Ok(count) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = count as u64 };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Exports the dataset schema into `out` as an Arrow C schema. The caller
/// owns the result and must call its `release` callback.
///
/// # Safety
///
/// `ds` must be a valid handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_schema(
    ds: *const LanceDataset,
    out: *mut FFI_ArrowSchema,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    let arrow_schema = arrow_schema::Schema::from(dataset.schema());
    match unsafe { arrow_bridge::export_schema(&arrow_schema, out) } {
        Ok(()) => ok(),
        Err(msg) => set_error(ErrorCode::Internal, msg),
    }
}

/// Returns the currently checked-out version of the dataset as a JSON string
/// `{"version": uint, "timestamp": string, "metadata": {..}}`. The caller
/// owns `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_version(
    ds: *const LanceDataset,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let version = unsafe { &*ds }.dataset().version();
    let json = match serde_json::to_string(&version) {
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

/// Returns the latest committed version id of the dataset (which may be newer
/// than the checked-out version).
///
/// # Safety
///
/// `ds` must be a valid handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_latest_version(
    ds: *const LanceDataset,
    out: *mut u64,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.latest_version_id()) {
        Ok(version) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = version };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
