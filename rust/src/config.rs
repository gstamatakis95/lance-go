//! Dataset config / table metadata / schema and field metadata, plus
//! miscellaneous dataset utilities (truncate, validate, stats, paths,
//! policy-based cleanup, multi-base registration).

use std::collections::HashMap;
use std::ffi::c_char;
use std::sync::Arc;

use lance::dataset::cleanup::CleanupPolicy;
use lance::dataset::statistics::DatasetStatisticsExt;
use serde::Deserialize;

use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::maintenance::{emit_json, parse_json_options};
use crate::runtime::{block_on, block_on_cc};
use crate::storage;

/// Which key-value map a metadata update targets.
enum MapKind {
    Config,
    TableMetadata,
    SchemaMetadata,
}

/// Parses a required `{"key": "value"|null, ...}` JSON argument.
///
/// # Safety
///
/// `updates_json` must be a valid C string (NULL is rejected).
unsafe fn parse_updates(
    updates_json: *const c_char,
) -> Result<HashMap<String, Option<String>>, String> {
    let json = unsafe { storage::required_str(updates_json, "updates_json") }?;
    serde_json::from_str(json).map_err(|e| format!("invalid updates JSON: {e}"))
}

/// Shared implementation of the config / table-metadata / schema-metadata
/// update entry points.
///
/// # Safety
///
/// `ds` must be a valid handle, `updates_json` a valid C string, and
/// `out_json` NULL or valid for writes.
unsafe fn update_map(
    ds: *mut LanceDataset,
    updates_json: *const c_char,
    replace: bool,
    kind: MapKind,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let updates = match unsafe { parse_updates(updates_json) } {
        Ok(updates) => updates,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    // The update builders take `&mut Dataset` and commit a new version:
    // hold the handle's mutex for the duration so the updated dataset is
    // visible to subsequent calls.
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let result = block_on_cc!(async {
        let mut builder = match kind {
            MapKind::Config => guard.update_config(updates),
            MapKind::TableMetadata => guard.update_metadata(updates),
            MapKind::SchemaMetadata => guard.update_schema_metadata(updates),
        };
        if replace {
            builder = builder.replace();
        }
        builder.await
    });
    drop(guard);

    match result {
        Ok(map) => {
            if out_json.is_null() {
                return ok();
            }
            match serde_json::to_string(&map) {
                Ok(json) => unsafe { emit_json(json, out_json) },
                Err(e) => set_error(ErrorCode::Internal, e),
            }
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns the dataset config (from the manifest) as a JSON object
/// `{"key": "value", ...}`. The caller owns `*out_json` and must free it
/// with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_config(
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
    match serde_json::to_string(dataset.config()) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Returns the table metadata as a JSON object. The caller owns `*out_json`
/// and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_metadata(
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
    match serde_json::to_string(dataset.metadata()) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Updates the dataset config, committing a new version.
///
/// - `updates_json`: JSON object `{"key": "value"|null, ...}`. A null value
///   removes the key. Must not be NULL.
/// - `replace`: when true the entire config map is replaced by the updates
///   instead of merged.
/// - `out_json`: if non-NULL, receives the post-update config map as JSON
///   (free with `lance_string_free`).
///
/// # Safety
///
/// `ds` must be a valid handle, `updates_json` a valid C string, and
/// `out_json` NULL or valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_update_config(
    ds: *mut LanceDataset,
    updates_json: *const c_char,
    replace: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    unsafe { update_map(ds, updates_json, replace, MapKind::Config, out_json) }
}

/// Updates the table metadata. Same contract as
/// `lance_dataset_update_config`.
///
/// # Safety
///
/// Same contracts as `lance_dataset_update_config`.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_update_metadata(
    ds: *mut LanceDataset,
    updates_json: *const c_char,
    replace: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    unsafe { update_map(ds, updates_json, replace, MapKind::TableMetadata, out_json) }
}

/// Updates the schema metadata. Same contract as
/// `lance_dataset_update_config`.
///
/// # Safety
///
/// Same contracts as `lance_dataset_update_config`.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_update_schema_metadata(
    ds: *mut LanceDataset,
    updates_json: *const c_char,
    replace: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    unsafe { update_map(ds, updates_json, replace, MapKind::SchemaMetadata, out_json) }
}

/// Deletes keys from the dataset config, committing a new version.
///
/// - `keys_json`: JSON array of key names, e.g. `["a", "b"]`. Must not be
///   NULL. Deleting a key that does not exist is a no-op.
///
/// # Safety
///
/// `ds` must be a valid handle and `keys_json` a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_delete_config_keys(
    ds: *mut LanceDataset,
    keys_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let keys: Vec<String> = match unsafe {
        (|| -> Result<_, String> {
            let json = storage::required_str(keys_json, "keys_json")?;
            serde_json::from_str(json).map_err(|e| format!("invalid keys JSON: {e}"))
        })()
    } {
        Ok(keys) => keys,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();

    // delete_config_keys takes `&mut Dataset` and commits a new version:
    // hold the handle's mutex for the duration.
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    // `Dataset::delete_config_keys` is deprecated upstream in favor of
    // `update_config` with None values; it is exposed here for API parity
    // and delegates to exactly that.
    #[allow(deprecated)]
    let result = block_on_cc!(guard.delete_config_keys(&key_refs));
    drop(guard);

    match result {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns the initial storage options this dataset handle was opened with as
/// a JSON object `{"key": "value", ...}` (an empty object when none were
/// given). The caller owns `*out_json` and must free it with
/// `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_storage_options(
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
    // `initial_storage_options` is the non-deprecated accessor behind the
    // upstream `storage_options()` alias: the static options captured at open
    // time, without triggering a refresh.
    let json = match dataset.initial_storage_options() {
        Some(options) => match serde_json::to_string(options) {
            Ok(json) => json,
            Err(e) => return set_error(ErrorCode::Internal, e),
        },
        None => "{}".to_string(),
    };
    unsafe { emit_json(json, out_json) }
}

/// Updates the metadata of a single field, committing a new version.
///
/// - `field`: field name (nested fields use dotted paths). Must not be NULL.
/// - `updates_json`: JSON object `{"key": "value"|null, ...}`. A null value
///   removes the key. Must not be NULL.
/// - `replace`: when true the field's metadata map is replaced instead of
///   merged.
///
/// # Safety
///
/// `ds` must be a valid handle and the strings valid C strings.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_update_field_metadata(
    ds: *mut LanceDataset,
    field: *const c_char,
    updates_json: *const c_char,
    replace: bool,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (field, updates) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(field, "field")?,
                parse_updates(updates_json)?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let result = block_on_cc!(async {
        let builder = guard.update_field_metadata();
        let builder = if replace {
            builder.replace(field, updates)?
        } else {
            builder.update(field, updates)?
        };
        builder.await.map(|_| ())
    });
    drop(guard);

    match result {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Deletes all rows from the dataset (committing a new version).
///
/// # Safety
///
/// `ds` must be a valid handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_truncate(ds: *mut LanceDataset) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.truncate_table()) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Counts the soft-deleted rows still present in the dataset's files.
///
/// # Safety
///
/// `ds` must be a valid handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_count_deleted_rows(
    ds: *const LanceDataset,
    out: *mut u64,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.count_deleted_rows()) {
        Ok(count) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = count as u64 };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Reports whether the dataset has a newer committed version than the one
/// checked out on this handle.
///
/// # Safety
///
/// `ds` must be a valid handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_is_stale(ds: *const LanceDataset, out: *mut bool) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.is_stale()) {
        Ok(stale) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = stale };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Reports whether the immediate successor version's manifest exists (a fast
/// contiguous-history probe, prefer `lance_dataset_is_stale` for a general
/// freshness check).
///
/// # Safety
///
/// `ds` must be a valid handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_has_successor_version(
    ds: *const LanceDataset,
    out: *mut bool,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.has_successor_version()) {
        Ok(has) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = has };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Validates the dataset's internal consistency (fragment ids, field ids,
/// physical rows).
///
/// # Safety
///
/// `ds` must be a valid handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_validate(ds: *const LanceDataset) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.validate()) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Migrates the dataset's manifest naming scheme to V2 (constant-time
/// latest-manifest lookups on object storage). The handle is re-pointed at
/// the migrated latest version. This call is deliberately NOT cancellable:
/// once started it runs to completion regardless of the caller's
/// cancellation token, because an aborted migration would leave a mix of V1
/// and V2 manifest names that Lance refuses to open.
///
/// # Safety
///
/// `ds` must be a valid handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_migrate_manifest_paths_v2(ds: *mut LanceDataset) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    // Deliberately plain `block_on`, NOT `block_on_cc!`: upstream
    // `migrate_manifest_paths_v2` renames every V1 manifest to its V2 name one
    // file at a time. It is not an atomic commit — dropping the future
    // mid-loop leaves the dataset with a mix of V1 and V2 manifest names,
    // which Lance then refuses to open ("multiple manifest naming schemes"),
    // and recovery requires re-running the migration through an already-open
    // handle. Running to completion is always the safe outcome.
    match block_on(guard.migrate_manifest_paths_v2()) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Counts data files with fewer rows than `max_rows_per_group` (candidates
/// for compaction).
///
/// # Safety
///
/// `ds` must be a valid handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_num_small_files(
    ds: *const LanceDataset,
    max_rows_per_group: u64,
    out: *mut u64,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    let count = block_on_cc!(dataset.num_small_files(max_rows_per_group as usize));
    // SAFETY: `out` is non-NULL and valid for writes.
    unsafe { *out = count as u64 };
    ok()
}

/// Returns session-cache statistics as JSON `{"cache_size_bytes": uint,
/// "index_cache_entry_count": uint, "index_cache_hit_rate": float}`. The
/// caller owns `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_cache_stats(
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
    let (entries, hit_rate) = block_on_cc!(async {
        (
            dataset.index_cache_entry_count().await,
            dataset.index_cache_hit_rate().await,
        )
    });
    let json = serde_json::json!({
        "cache_size_bytes": dataset.cache_size_bytes(),
        "index_cache_entry_count": entries as u64,
        "index_cache_hit_rate": hit_rate,
    });
    unsafe { emit_json(json.to_string(), out_json) }
}

/// Returns the dataset's storage locations as JSON `{"uri": string,
/// "base": string, "data_dir": string, "indices_dir": string,
/// "versions_dir": string, "branch": string|null}`. `base` and the `*_dir`
/// values are object-store paths (no scheme), and `uri` is fully qualified. The
/// caller owns `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_paths(
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
    let location = dataset.branch_location();
    let json = serde_json::json!({
        "uri": dataset.uri(),
        "base": location.path.to_string(),
        "data_dir": dataset.data_dir().to_string(),
        "indices_dir": dataset.indices_dir().to_string(),
        "versions_dir": dataset.versions_dir().to_string(),
        "branch": location.branch,
    });
    unsafe { emit_json(json.to_string(), out_json) }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CleanupPolicyJson {
    /// Remove versions committed before this RFC 3339 timestamp.
    before_timestamp: Option<String>,
    /// Remove versions numbered below this version.
    before_version: Option<u64>,
    delete_unverified: Option<bool>,
    error_if_tagged_old_versions: Option<bool>,
    clean_referenced_branches: Option<bool>,
    delete_rate_limit: Option<u64>,
}

/// Removes old dataset versions according to a cleanup policy.
///
/// - `policy_json`: optional JSON object
///   `{"before_timestamp"?: rfc3339, "before_version"?: uint,
///   "delete_unverified"?: bool, "error_if_tagged_old_versions"?: bool
///   (default true), "clean_referenced_branches"?: bool,
///   "delete_rate_limit"?: uint}`, or NULL for the default policy.
/// - `out_json`: receives the removal statistics as JSON (same shape as
///   `lance_dataset_cleanup_old_versions`). Free with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle, `policy_json` NULL or a valid C string, and
/// `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_cleanup_with_policy(
    ds: *const LanceDataset,
    policy_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let policy_json = match unsafe { storage::optional_str(policy_json, "policy_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let parsed: CleanupPolicyJson = match parse_json_options(policy_json, "cleanup policy") {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let mut policy = CleanupPolicy::default();
    if let Some(ts) = &parsed.before_timestamp {
        match chrono::DateTime::parse_from_rfc3339(ts) {
            Ok(dt) => policy.before_timestamp = Some(dt.with_timezone(&chrono::Utc)),
            Err(e) => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    format!("invalid before_timestamp (want RFC 3339): {e}"),
                );
            }
        }
    }
    policy.before_version = parsed.before_version;
    if let Some(v) = parsed.delete_unverified {
        policy.delete_unverified = v;
    }
    if let Some(v) = parsed.error_if_tagged_old_versions {
        policy.error_if_tagged_old_versions = v;
    }
    if let Some(v) = parsed.clean_referenced_branches {
        policy.clean_referenced_branches = v;
    }
    policy.delete_rate_limit = parsed.delete_rate_limit;

    let dataset = unsafe { &*ds }.dataset();
    let stats = match block_on_cc!(dataset.cleanup_with_policy(policy)) {
        Ok(stats) => stats,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
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

/// Computes per-field storage statistics as JSON
/// `{"fields": [{"id": uint, "bytes_on_disk": uint}, ...]}`. The caller owns
/// `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_data_stats(
    ds: *const LanceDataset,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let dataset = Arc::new(unsafe { &*ds }.dataset());
    let stats = match block_on_cc!(dataset.calculate_data_stats()) {
        Ok(stats) => stats,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    // DataStatistics does not derive Serialize, so build the JSON by hand.
    let json = serde_json::json!({
        "fields": stats
            .fields
            .iter()
            .map(|f| serde_json::json!({"id": f.id, "bytes_on_disk": f.bytes_on_disk}))
            .collect::<Vec<_>>(),
    });
    unsafe { emit_json(json.to_string(), out_json) }
}

/// One base path in `lance_dataset_add_bases`'s `bases_json`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BasePathJson {
    id: u32,
    name: Option<String>,
    #[serde(default)]
    is_dataset_root: bool,
    path: String,
}

/// Registers additional storage base paths in the dataset manifest
/// (committing a new version). The handle is updated to the new version.
///
/// - `bases_json`: JSON array `[{"id": uint (non-zero), "name"?: string,
///   "is_dataset_root"?: bool, "path": string}, ...]`. Must not be NULL.
/// - `properties_json`: optional JSON object of transaction properties, or
///   NULL.
///
/// # Safety
///
/// `ds` must be a valid handle and the strings valid C strings.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_add_bases(
    ds: *mut LanceDataset,
    bases_json: *const c_char,
    properties_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (bases, properties) = match unsafe {
        (|| -> Result<_, String> {
            let bases_json = storage::required_str(bases_json, "bases_json")?;
            let bases: Vec<BasePathJson> =
                serde_json::from_str(bases_json).map_err(|e| format!("invalid bases JSON: {e}"))?;
            let properties: Option<HashMap<String, String>> =
                match storage::optional_str(properties_json, "properties_json")? {
                    None => None,
                    Some(s) if s.trim().is_empty() => None,
                    Some(s) => Some(
                        serde_json::from_str(s)
                            .map_err(|e| format!("invalid properties JSON: {e}"))?,
                    ),
                };
            Ok((bases, properties))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let bases = bases
        .into_iter()
        .map(|b| lance::table::format::BasePath::new(b.id, b.path, b.name, b.is_dataset_root))
        .collect::<Vec<_>>();

    // add_bases returns a NEW dataset at the committed version. Hold the
    // mutex and swap the handle's dataset so subsequent calls see it.
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let arc = Arc::new(guard.clone());
    match block_on_cc!(arc.add_bases(bases, properties)) {
        Ok(updated) => {
            *guard = updated;
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
