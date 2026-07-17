//! Version time travel and tag management.

use std::ffi::c_char;
use std::sync::Mutex;

use lance::dataset::refs::Ref;
use serde::Deserialize;

use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::maintenance::emit_json;
use crate::runtime::block_on_cc;
use crate::storage;

/// JSON form of a version reference: exactly one of
/// `{"version": uint}` or `{"tag": string}`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RefJson {
    version: Option<u64>,
    tag: Option<String>,
}

impl RefJson {
    fn into_ref(self) -> Result<Ref, String> {
        match (self.version, self.tag) {
            (Some(version), None) => Ok(Ref::from(version)),
            (None, Some(tag)) => Ok(Ref::from(tag.as_str())),
            _ => Err("ref JSON must contain exactly one of \"version\" or \"tag\"".to_string()),
        }
    }
}

/// Reads a required ref JSON argument into a [`Ref`].
///
/// # Safety
///
/// `ref_json` must be NULL or a valid C string (NULL is rejected).
unsafe fn parse_ref(ref_json: *const c_char) -> Result<Ref, String> {
    let json = unsafe { storage::required_str(ref_json, "ref_json") }?;
    let parsed: RefJson =
        serde_json::from_str(json).map_err(|e| format!("invalid ref JSON: {e}"))?;
    parsed.into_ref()
}

/// Checks out a historical version of the dataset as a NEW handle.
///
/// - `ref_json`: `{"version": uint}` or `{"tag": string}`. Must not be NULL.
/// - `out`: receives a new dataset handle fixed at the requested version.
///   Release it with `lance_dataset_close`. The original handle `ds` remains
///   valid and unchanged.
///
/// # Safety
///
/// `ds` must be a valid handle, `ref_json` a valid C string, and `out` valid
/// for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_checkout(
    ds: *const LanceDataset,
    ref_json: *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let reference = match unsafe { parse_ref(ref_json) } {
        Ok(reference) => reference,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    // checkout_version takes `&self` and returns a NEW Dataset. Work on a
    // clone so the handle mutex is not held across the IO.
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.checkout_version(reference)) {
        Ok(checked_out) => {
            let handle = Box::into_raw(Box::new(LanceDataset(Mutex::new(checked_out))));
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = handle };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Checks out the latest committed version on this handle (useful after
/// another writer committed, or after `lance_dataset_restore` on a different
/// handle).
///
/// # Safety
///
/// `ds` must be a valid handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_checkout_latest(ds: *mut LanceDataset) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.checkout_latest()) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Commits the currently checked-out version of this handle as the new
/// latest version of the dataset. Typically used on a handle produced by
/// `lance_dataset_checkout` to roll the dataset back.
///
/// # Safety
///
/// `ds` must be a valid handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_restore(ds: *mut LanceDataset) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.restore()) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Lists all committed versions of the dataset, oldest first, as JSON
/// `[{"version": uint, "timestamp": string, "metadata": {..}}, ...]`. The
/// caller owns `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_list_versions(
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
    let versions = match block_on_cc!(dataset.versions()) {
        Ok(versions) => versions,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match serde_json::to_string(&versions) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Creates a tag pointing at the referenced version. Fails if the tag
/// already exists.
///
/// - `tag`: tag name. Must not be NULL.
/// - `ref_json`: `{"version": uint}` or `{"tag": string}`. Must not be NULL.
///
/// # Safety
///
/// `ds` must be a valid handle and the strings valid C strings.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_create(
    ds: *const LanceDataset,
    tag: *const c_char,
    ref_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (tag, reference) = match unsafe {
        (|| -> Result<_, String> { Ok((storage::required_str(tag, "tag")?, parse_ref(ref_json)?)) })(
        )
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(async { dataset.tags().create(tag, reference).await }) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Moves an existing tag to the referenced version.
///
/// # Safety
///
/// Same contracts as `lance_dataset_tag_create`.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_update(
    ds: *const LanceDataset,
    tag: *const c_char,
    ref_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (tag, reference) = match unsafe {
        (|| -> Result<_, String> { Ok((storage::required_str(tag, "tag")?, parse_ref(ref_json)?)) })(
        )
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(async { dataset.tags().update(tag, reference).await }) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Deletes a tag.
///
/// # Safety
///
/// `ds` must be a valid handle and `tag` a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_delete(
    ds: *const LanceDataset,
    tag: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let tag = match unsafe { storage::required_str(tag, "tag") } {
        Ok(tag) => tag,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(async { dataset.tags().delete(tag).await }) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Lists all tags of the dataset as a JSON object
/// `{"<tag>": {"branch": string|null, "version": uint,
/// "manifestSize": uint, "metadata": {..}, ...}, ...}`. The caller owns
/// `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_list(
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
    let tags = match block_on_cc!(async { dataset.tags().list().await }) {
        Ok(tags) => tags,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match serde_json::to_string(&tags) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Returns the full contents of a single tag as JSON
/// `{"branch": string|null, "version": uint, "createdAt"?: string,
/// "updatedAt"?: string, "manifestSize": uint, "metadata": {..}}` (the same
/// per-tag shape as `lance_dataset_tag_list`). The caller owns `*out_json`
/// and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle, `tag` a valid C string, and `out_json` valid
/// for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_contents(
    ds: *const LanceDataset,
    tag: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let tag = match unsafe { storage::required_str(tag, "tag") } {
        Ok(tag) => tag,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    let contents = match block_on_cc!(async { dataset.tags().get(tag).await }) {
        Ok(contents) => contents,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match serde_json::to_string(&contents) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Resolves a tag to its version number.
///
/// # Safety
///
/// `ds` must be a valid handle, `tag` a valid C string, and `out` valid for
/// writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_tag_get_version(
    ds: *const LanceDataset,
    tag: *const c_char,
    out: *mut u64,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let tag = match unsafe { storage::required_str(tag, "tag") } {
        Ok(tag) => tag,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(async { dataset.tags().get_version(tag).await }) {
        Ok(version) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = version };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
