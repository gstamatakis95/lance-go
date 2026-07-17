//! Branch management, dataset cloning, and extended tag operations.

use std::collections::HashMap;
use std::ffi::c_char;
use std::sync::Mutex;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::refs::Ref;
use serde::Deserialize;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::maintenance::emit_json;
use crate::runtime::block_on_cc;
use crate::storage;

/// JSON form of a version reference that, unlike the one accepted by
/// `lance_dataset_checkout`, can also point at a branch (optionally at a
/// specific version on that branch):
/// `{"version"?: uint, "tag"?: string, "branch"?: string}`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct BranchRefJson {
    version: Option<u64>,
    tag: Option<String>,
    branch: Option<String>,
}

impl BranchRefJson {
    fn into_ref(self) -> Result<Ref, String> {
        match (self.version, self.tag, self.branch) {
            (Some(version), None, None) => Ok(Ref::VersionNumber(version)),
            (None, Some(tag), None) => Ok(Ref::Tag(tag)),
            (version, None, Some(branch)) => Ok(Ref::Version(Some(branch), version)),
            _ => Err(
                "ref JSON must contain \"version\", \"tag\", or \"branch\" (optionally with \
                 \"version\")"
                    .to_string(),
            ),
        }
    }
}

/// Parses an optional branch-flavored ref JSON argument. NULL/empty falls
/// back to the version currently checked out on `dataset`.
///
/// # Safety
///
/// `ref_json` must be NULL or a valid C string.
unsafe fn parse_ref_or_current(
    ref_json: *const c_char,
    dataset: &lance::Dataset,
) -> Result<Ref, String> {
    match unsafe { storage::optional_str(ref_json, "ref_json") }? {
        None => Ok(Ref::VersionNumber(dataset.version().version)),
        Some(s) if s.trim().is_empty() => Ok(Ref::VersionNumber(dataset.version().version)),
        Some(json) => {
            let parsed: BranchRefJson =
                serde_json::from_str(json).map_err(|e| format!("invalid ref JSON: {e}"))?;
            parsed.into_ref()
        }
    }
}

/// Writes `dataset` into `out` as a new opaque handle.
///
/// # Safety
///
/// `out` must be non-NULL and valid for writes.
unsafe fn emit_new_handle(dataset: lance::Dataset, out: *mut *mut LanceDataset) {
    let handle = Box::into_raw(Box::new(LanceDataset(Mutex::new(dataset))));
    // SAFETY: `out` is non-NULL and valid for writes per the contract.
    unsafe { *out = handle };
}

/// Checks out the latest version of a branch as a NEW dataset handle. The
/// original handle remains valid and unchanged.
///
/// # Safety
///
/// `ds` must be a valid handle, `branch` a valid C string, and `out` valid
/// for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_checkout_branch(
    ds: *const LanceDataset,
    branch: *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let branch = match unsafe { storage::required_str(branch, "branch") } {
        Ok(branch) => branch,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.checkout_branch(branch)) {
        Ok(checked_out) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { emit_new_handle(checked_out, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Creates a branch pointing at the referenced version.
///
/// - `branch`: new branch name. Must not be NULL.
/// - `ref_json`: `{"version": uint}`, `{"tag": string}` or
///   `{"branch": string, "version"?: uint}`, or NULL for the version
///   currently checked out on `ds`.
/// - `out`: if non-NULL, receives a handle to the new branch's dataset
///   (release with `lance_dataset_close`). Pass NULL to discard it.
///
/// # Safety
///
/// `ds` must be a valid handle, the strings NULL or valid C strings, and
/// `out` NULL or valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_create_branch(
    ds: *mut LanceDataset,
    branch: *const c_char,
    ref_json: *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let branch = match unsafe { storage::required_str(branch, "branch") } {
        Ok(branch) => branch.to_owned(),
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    // create_branch takes `&mut Dataset`: hold the handle's mutex across the
    // operation.
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let reference = match unsafe { parse_ref_or_current(ref_json, &guard) } {
        Ok(reference) => reference,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    match block_on_cc!(guard.create_branch(&branch, reference, None)) {
        Ok(created) => {
            if !out.is_null() {
                // SAFETY: `out` is non-NULL and valid for writes.
                unsafe { emit_new_handle(created, out) };
            }
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Deletes a branch. With `force` set, the branch dataset is removed even if
/// its metadata (`BranchContents`) is missing (zombie cleanup).
///
/// # Safety
///
/// `ds` must be a valid handle and `branch` a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_delete_branch(
    ds: *mut LanceDataset,
    branch: *const c_char,
    force: bool,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let branch = match unsafe { storage::required_str(branch, "branch") } {
        Ok(branch) => branch,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let result = if force {
        block_on_cc!(guard.force_delete_branch(branch))
    } else {
        block_on_cc!(guard.delete_branch(branch))
    };
    match result {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Lists all branches of the dataset as a JSON object
/// `{"<branch>": {"parentBranch": string|null, "parentVersion": uint,
/// "createAt": uint, "manifestSize": uint, "metadata": {..}, ...}, ...}`.
/// The caller owns `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_list_branches(
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
    let branches = match block_on_cc!(dataset.list_branches()) {
        Ok(branches) => branches,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match serde_json::to_string(&branches) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Shared implementation of shallow/deep clone.
///
/// # Safety
///
/// Same contracts as `lance_dataset_shallow_clone`.
unsafe fn clone_impl(
    ds: *mut LanceDataset,
    target_uri: *const c_char,
    ref_json: *const c_char,
    storage_kv: *const *const c_char,
    out: *mut *mut LanceDataset,
    deep: bool,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let (target_uri, storage_options) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(target_uri, "target_uri")?,
                storage::parse_storage_kv(storage_kv)?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let store_params = storage::object_store_params(storage_options);

    // shallow_clone/deep_clone take `&mut Dataset`: hold the mutex across
    // the operation.
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let reference = match unsafe { parse_ref_or_current(ref_json, &guard) } {
        Ok(reference) => reference,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let result = if deep {
        block_on_cc!(guard.deep_clone(target_uri, reference, store_params))
    } else {
        block_on_cc!(guard.shallow_clone(target_uri, reference, store_params))
    };
    match result {
        Ok(cloned) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { emit_new_handle(cloned, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Shallow-clones the referenced version into a new dataset at `target_uri`
/// (manifests are copied, data files stay in and are referenced from the
/// source dataset).
///
/// - `ref_json`: same contract as in `lance_dataset_create_branch` (NULL for
///   the currently checked-out version).
/// - `storage_kv`: storage options for the TARGET, in the same format as
///   `lance_dataset_open`, or NULL.
/// - `out`: receives a handle to the cloned dataset. Release it with
///   `lance_dataset_close`.
///
/// # Safety
///
/// `ds` must be a valid handle, strings NULL or valid C strings,
/// `storage_kv` NULL or a valid NULL-terminated array, and `out` valid for
/// writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_shallow_clone(
    ds: *mut LanceDataset,
    target_uri: *const c_char,
    ref_json: *const c_char,
    storage_kv: *const *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    unsafe { clone_impl(ds, target_uri, ref_json, storage_kv, out, false) }
}

/// Deep-clones the referenced version into a new dataset at `target_uri`
/// (all data files are copied, and the clone is fully independent). Fails if a
/// dataset already exists at the target.
///
/// # Safety
///
/// Same contracts as `lance_dataset_shallow_clone`.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_deep_clone(
    ds: *mut LanceDataset,
    target_uri: *const c_char,
    ref_json: *const c_char,
    storage_kv: *const *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    unsafe { clone_impl(ds, target_uri, ref_json, storage_kv, out, true) }
}

/// Appends a stream of record batches to the dataset the handle is
/// currently checked out on, committing a new version. When the handle is
/// checked out on a branch (e.g. from `lance_dataset_checkout_branch`), the
/// append commits to that branch. The handle is advanced to the new
/// version.
///
/// - `stream`: Arrow C stream of record batches. Ownership is always taken,
///   even on error. Must not be NULL.
///
/// # Safety
///
/// `ds` must be a valid handle and `stream` a valid, unmoved Arrow C
/// stream.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_append(
    ds: *mut LanceDataset,
    stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    // Import the stream FIRST so the producer is released on every path.
    let reader = match unsafe { arrow_bridge::import_stream(stream) } {
        Ok(reader) => reader,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    // append takes `&mut self` and advances the dataset in place. Hold the
    // handle's mutex for the duration so subsequent calls see the new
    // version.
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.append(reader, None)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Lists all tags ordered by the version they point at as a JSON array
/// `[{"name": string, "branch": string|null, "version": uint,
/// "manifestSize": uint, "metadata": {..}, ...}, ...]`. Descending version
/// order by default. Set `ascending` for oldest-first. Ties are broken by
/// tag name. The caller owns `*out_json` and must free it with
/// `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_list_ordered(
    ds: *const LanceDataset,
    ascending: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let order = ascending.then_some(std::cmp::Ordering::Less);
    let dataset = unsafe { &*ds }.dataset();
    let tags = match block_on_cc!(async { dataset.tags().list_tags_ordered(order).await }) {
        Ok(tags) => tags,
        Err(e) => return set_error(map_lance_error(&e), e),
    };

    // Flatten (name, contents) pairs into objects carrying the name.
    let mut entries = Vec::with_capacity(tags.len());
    for (name, contents) in tags {
        let mut value = match serde_json::to_value(&contents) {
            Ok(value) => value,
            Err(e) => return set_error(ErrorCode::Internal, e),
        };
        if let Some(map) = value.as_object_mut() {
            map.insert("name".to_string(), serde_json::Value::String(name));
        }
        entries.push(value);
    }
    unsafe { emit_json(serde_json::Value::Array(entries).to_string(), out_json) }
}

/// Replaces the metadata key-value map attached to a tag. The tag must
/// exist.
///
/// - `metadata_json`: JSON object `{"key": "value", ...}` (the new complete
///   metadata map). Must not be NULL.
///
/// # Safety
///
/// `ds` must be a valid handle and the strings valid C strings.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_replace_metadata(
    ds: *const LanceDataset,
    tag: *const c_char,
    metadata_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (tag, metadata) = match unsafe {
        (|| -> Result<_, String> {
            let tag = storage::required_str(tag, "tag")?;
            let metadata_json = storage::required_str(metadata_json, "metadata_json")?;
            let metadata: HashMap<String, String> = serde_json::from_str(metadata_json)
                .map_err(|e| format!("invalid metadata JSON: {e}"))?;
            Ok((tag, metadata))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(async { dataset.tags().replace_metadata(tag, metadata).await }) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
