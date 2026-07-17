//! Blob columns: take [`BlobFile`] handles for selected rows and read their
//! bytes via random or cursor-based access.
//!
//! A blob column is a `LargeBinary` Arrow field tagged with the metadata key
//! `lance-encoding:blob` = `"true"`.
//!
//! Ownership model:
//! - `lance_dataset_take_blobs` returns an opaque [`LanceBlobList`] that owns
//!   its boxed [`LanceBlobFile`] entries. Each `BlobFile` is self-contained
//!   (holds its own storage references), so reads survive the originating
//!   dataset handle being closed, mirroring the scanner-stream contract.
//! - `lance_blob_list_get` returns a *borrowed* blob pointer whose lifetime is
//!   tied to the list: it is valid until `lance_blob_list_close` frees the
//!   list. `lance_blob_close` only closes the underlying file early. It does
//!   not free the handle.
//! - Bytes cross the boundary as an owned `(ptr, len)` pair that the caller
//!   must free with `lance_bytes_free`, passing back the returned length.

use std::ffi::{CString, c_char};
use std::ptr;
use std::sync::Arc;

use serde::Deserialize;

use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// Opaque handle to a single blob file returned by `take_blobs`. Borrowed from
/// its [`LanceBlobList`], valid until the list is closed.
pub struct LanceBlobFile(lance::dataset::BlobFile);

/// Opaque, owning list of blob files. Release with `lance_blob_list_close`.
///
/// The `Vec` is built once and never mutated, so element addresses stay stable
/// for the list's lifetime. `lance_blob_list_get` hands out borrowed pointers
/// into it.
pub struct LanceBlobList(Vec<LanceBlobFile>);

/// Selection spec for `lance_dataset_take_blobs`. Exactly one of `row_ids`,
/// `indices` or `addresses` must be set.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct BlobTakeSpec {
    /// Blob column name (must be a `LargeBinary` field tagged as a blob).
    column: String,
    /// Stable row ids.
    row_ids: Option<Vec<u64>>,
    /// Row offsets (0 = first live row).
    indices: Option<Vec<u64>>,
    /// Row addresses (`fragment_id << 32 | row_offset`).
    addresses: Option<Vec<u64>>,
}

/// Copies `data` into an owned `(ptr, len)` buffer handed to the caller, who
/// must free it with `lance_bytes_free`. An empty slice yields NULL/0.
///
/// # Safety
///
/// `out_buf` and `out_len` must be valid for writes.
unsafe fn emit_bytes(data: &[u8], out_buf: *mut *mut u8, out_len: *mut usize) -> i32 {
    if data.is_empty() {
        // SAFETY: out params valid per the contract.
        unsafe {
            *out_buf = ptr::null_mut();
            *out_len = 0;
        }
        return ok();
    }
    // Leak a boxed slice. The Go side frees it via lance_bytes_free, which
    // reconstitutes the box with the returned length.
    let mut boxed = data.to_vec().into_boxed_slice();
    let p = boxed.as_mut_ptr();
    let len = boxed.len();
    std::mem::forget(boxed);
    // SAFETY: out params valid per the contract.
    unsafe {
        *out_buf = p;
        *out_len = len;
    }
    ok()
}

/// Takes blob-file handles for the selected rows of a blob column.
///
/// - `spec_json`: JSON object `{"column": string, "row_ids"|"indices"|
///   "addresses": [uint]}` (exactly one selector). Must not be NULL.
/// - `out`: receives an owning blob-list handle. Release it with
///   `lance_blob_list_close`.
///
/// # Safety
///
/// `ds` must be a valid dataset handle, `spec_json` a valid C string, and
/// `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_take_blobs(
    ds: *const LanceDataset,
    spec_json: *const c_char,
    out: *mut *mut LanceBlobList,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let spec_json = match unsafe { storage::required_str(spec_json, "spec_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let spec: BlobTakeSpec = match serde_json::from_str(spec_json) {
        Ok(spec) => spec,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid take-blobs spec JSON: {e}"),
            );
        }
    };
    let selectors = [
        spec.row_ids.is_some(),
        spec.indices.is_some(),
        spec.addresses.is_some(),
    ];
    if selectors.iter().filter(|s| **s).count() != 1 {
        return set_error(
            ErrorCode::InvalidArgument,
            "take-blobs spec must contain exactly one of \"row_ids\", \"indices\" or \"addresses\"",
        );
    }

    let dataset = Arc::new(unsafe { &*ds }.dataset());
    let column = spec.column.as_str();
    let result = block_on_cc!(async {
        if let Some(ids) = &spec.row_ids {
            dataset.take_blobs(ids, column).await
        } else if let Some(indices) = &spec.indices {
            dataset.take_blobs_by_indices(indices, column).await
        } else {
            let addresses = spec.addresses.as_deref().unwrap_or(&[]);
            dataset.take_blobs_by_addresses(addresses, column).await
        }
    });
    let blobs = match result {
        Ok(blobs) => blobs,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let list = LanceBlobList(blobs.into_iter().map(LanceBlobFile).collect());
    let handle = Box::into_raw(Box::new(list));
    // SAFETY: `out` is non-NULL and valid for writes.
    unsafe { *out = handle };
    ok()
}

/// Returns the number of blob files in the list. NULL yields 0.
///
/// # Safety
///
/// `list` must be NULL or a valid blob-list handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_list_len(list: *const LanceBlobList) -> usize {
    if list.is_null() {
        return 0;
    }
    unsafe { &*list }.0.len()
}

/// Returns a *borrowed* pointer to the `i`-th blob file, valid until the list
/// is closed. Returns NULL if `list` is NULL or `i` is out of range. Do NOT
/// free the returned pointer. It is owned by the list.
///
/// # Safety
///
/// `list` must be NULL or a valid blob-list handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_list_get(
    list: *const LanceBlobList,
    i: usize,
) -> *mut LanceBlobFile {
    if list.is_null() {
        return ptr::null_mut();
    }
    let list = unsafe { &*list };
    match list.0.get(i) {
        // Borrowed pointer into the (never-reallocated) Vec. The list keeps
        // ownership.
        Some(blob) => blob as *const LanceBlobFile as *mut LanceBlobFile,
        None => ptr::null_mut(),
    }
}

/// Releases a blob list and every blob file it owns. NULL is a no-op. All blob
/// pointers obtained from the list become invalid.
///
/// # Safety
///
/// `list` must be NULL or a handle from `lance_dataset_take_blobs` that has not
/// already been closed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_list_close(list: *mut LanceBlobList) {
    if list.is_null() {
        return;
    }
    // SAFETY: per the contract, `list` came from Box::into_raw and is closed at
    // most once.
    drop(unsafe { Box::from_raw(list) });
}

/// Reads the blob from its current cursor to the end and returns the bytes.
/// Advances the cursor. On a fresh blob (cursor 0) this returns the whole
/// blob. `*out_buf`/`*out_len` receive an owned buffer (NULL/0 when empty).
/// Free it with `lance_bytes_free`.
///
/// # Safety
///
/// `bf` must be a valid blob handle, and `out_buf` and `out_len` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_read(
    bf: *const LanceBlobFile,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if bf.is_null() || out_buf.is_null() || out_len.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "bf, out_buf and out_len must not be NULL",
        );
    }
    let blob = &unsafe { &*bf }.0;
    match block_on_cc!(blob.read()) {
        Ok(bytes) => unsafe { emit_bytes(&bytes, out_buf, out_len) },
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Reads `len` bytes starting at blob-local offset `start` (random access,
/// does not move the cursor). `*out_buf`/`*out_len` receive an owned buffer
/// (NULL/0 when empty). Free it with `lance_bytes_free`.
///
/// # Safety
///
/// `bf` must be a valid blob handle, and `out_buf` and `out_len` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_read_range(
    bf: *const LanceBlobFile,
    start: u64,
    len: u64,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if bf.is_null() || out_buf.is_null() || out_len.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "bf, out_buf and out_len must not be NULL",
        );
    }
    let blob = &unsafe { &*bf }.0;
    let end = start.saturating_add(len);
    match block_on_cc!(blob.read_range(start..end)) {
        Ok(bytes) => unsafe { emit_bytes(&bytes, out_buf, out_len) },
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Moves the blob's read cursor to `pos`.
///
/// # Safety
///
/// `bf` must be a valid blob handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_seek(bf: *const LanceBlobFile, pos: u64) -> i32 {
    if bf.is_null() {
        return set_error(ErrorCode::InvalidArgument, "bf must not be NULL");
    }
    let blob = &unsafe { &*bf }.0;
    match block_on_cc!(blob.seek(pos)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Writes the blob's current cursor position into `out`.
///
/// # Safety
///
/// `bf` must be a valid blob handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_tell(bf: *const LanceBlobFile, out: *mut u64) -> i32 {
    if bf.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "bf and out must not be NULL");
    }
    let blob = &unsafe { &*bf }.0;
    match block_on_cc!(blob.tell()) {
        Ok(pos) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = pos };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns the blob's total size in bytes. NULL yields 0.
///
/// # Safety
///
/// `bf` must be NULL or a valid blob handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_size(bf: *const LanceBlobFile) -> u64 {
    if bf.is_null() {
        return 0;
    }
    unsafe { &*bf }.0.size()
}

/// Returns the blob's physical base offset within its data file. NULL yields 0.
///
/// # Safety
///
/// `bf` must be NULL or a valid blob handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_position(bf: *const LanceBlobFile) -> u64 {
    if bf.is_null() {
        return 0;
    }
    unsafe { &*bf }.0.position()
}

/// Returns the blob's storage kind as its `BlobKind` discriminant
/// (0 = inline, 1 = packed, 2 = dedicated, 3 = external). NULL yields 0.
///
/// # Safety
///
/// `bf` must be NULL or a valid blob handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_kind(bf: *const LanceBlobFile) -> u8 {
    if bf.is_null() {
        return 0;
    }
    unsafe { &*bf }.0.kind() as u8
}

/// Writes the blob's URI into `*out_uri` as an owned C string, or NULL when the
/// blob has no URI. The caller owns a non-NULL result and must free it with
/// `lance_string_free`.
///
/// # Safety
///
/// `bf` must be a valid blob handle and `out_uri` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_uri(
    bf: *const LanceBlobFile,
    out_uri: *mut *mut c_char,
) -> i32 {
    if bf.is_null() || out_uri.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "bf and out_uri must not be NULL",
        );
    }
    let blob = &unsafe { &*bf }.0;
    match blob.uri() {
        None => {
            // SAFETY: `out_uri` is non-NULL and valid for writes.
            unsafe { *out_uri = ptr::null_mut() };
            ok()
        }
        Some(uri) => match CString::new(uri) {
            Ok(cstr) => {
                // SAFETY: `out_uri` is non-NULL and valid for writes.
                unsafe { *out_uri = cstr.into_raw() };
                ok()
            }
            Err(e) => set_error(ErrorCode::Internal, e),
        },
    }
}

/// Closes the underlying blob file early (releasing its I/O resources). The
/// handle itself remains owned by the list and is freed by
/// `lance_blob_list_close`. Further reads on a closed blob fail.
///
/// # Safety
///
/// `bf` must be a valid blob handle.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_blob_close(bf: *const LanceBlobFile) -> i32 {
    if bf.is_null() {
        return set_error(ErrorCode::InvalidArgument, "bf must not be NULL");
    }
    let blob = &unsafe { &*bf }.0;
    match block_on_cc!(blob.close()) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
