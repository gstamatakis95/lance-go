//! Arrow C Data Interface import/export helpers.
//!
//! Data crosses the FFI boundary exclusively through the Arrow C Stream /
//! C Data interfaces: record batches come in as an `ArrowArrayStream`
//! produced by Go, and scan results go out as an `ArrowArrayStream`
//! produced by Rust.

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use lance::io::RecordBatchStream;

use crate::runtime::RT;

/// Takes ownership of a caller-provided Arrow C stream and wraps it in a
/// `RecordBatchReader`.
///
/// The stream contents are moved out immediately (the caller's struct is left
/// released/empty), so the producer side is always cleaned up exactly once,
/// either by the returned reader when it is dropped, or here on error.
///
/// # Safety
///
/// `stream` must be a valid, non-NULL pointer to an initialized
/// `ArrowArrayStream` that has not been released or moved.
pub(crate) unsafe fn import_stream(
    stream: *mut FFI_ArrowArrayStream,
) -> Result<ArrowArrayStreamReader, String> {
    if stream.is_null() {
        return Err("Arrow stream pointer must not be NULL".to_string());
    }
    // SAFETY: as documented above, from_raw moves the stream contents out,
    // leaving the caller's struct empty.
    unsafe { ArrowArrayStreamReader::from_raw(stream) }
        .map_err(|e| format!("failed to import Arrow stream: {e}"))
}

/// Exports a Lance record-batch stream into the caller-provided (typically
/// zero-initialized) `ArrowArrayStream` struct. The caller becomes the owner
/// and must eventually invoke the struct's `release` callback.
///
/// # Safety
///
/// `out` must be a valid, non-NULL pointer to writable memory for an
/// `ArrowArrayStream`, and any previous contents are overwritten without being
/// released.
pub(crate) unsafe fn export_stream(
    stream: impl RecordBatchStream + Unpin + 'static,
    out: *mut FFI_ArrowArrayStream,
) -> Result<(), String> {
    // Deliberately NOT wired into the cancellation runtime
    // (`crate::runtime::block_on_cancellable`): the exported stream's
    // `get_next` is driven later, from arbitrary consumer threads whose
    // cancel-token TLS slots are unrelated to the call that created the
    // stream. Cancellation of an in-progress read is handled on the Go side
    // instead (per-batch ctx checks and reader Close, which releases the
    // stream).
    let ffi_stream = lance_io::ffi::to_ffi_arrow_array_stream(stream, RT.handle().clone())
        .map_err(|e| format!("failed to export Arrow stream: {e}"))?;
    // SAFETY: `out` is valid for writes per the contract above. The write is
    // unaligned-safe because the caller may hand us arbitrarily aligned
    // memory (e.g. from a foreign allocator).
    unsafe { std::ptr::write_unaligned(out, ffi_stream) };
    Ok(())
}

/// Exports an Arrow schema into the caller-provided `ArrowSchema` struct.
/// The caller owns the result and must invoke its `release` callback.
///
/// # Safety
///
/// `out` must be a valid, non-NULL pointer to writable memory for an
/// `ArrowSchema`, and any previous contents are overwritten without being
/// released.
pub(crate) unsafe fn export_schema(
    schema: &arrow_schema::Schema,
    out: *mut FFI_ArrowSchema,
) -> Result<(), String> {
    let ffi_schema = FFI_ArrowSchema::try_from(schema)
        .map_err(|e| format!("failed to export Arrow schema: {e}"))?;
    // SAFETY: `out` is valid for writes per the contract above.
    unsafe { std::ptr::write_unaligned(out, ffi_schema) };
    Ok(())
}
