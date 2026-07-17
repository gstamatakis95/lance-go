//! Generic Go-callback ("plugin") bridge.
//!
//! Later waves bind Lance extension hooks (cache backends, byte caches,
//! write-progress and execution-stats callbacks, UDFs, ...) to Go
//! implementations. This module provides only the generic mechanism:
//!
//! - Go registers a process-wide [`LanceGoVTable`] of C function pointers
//!   (cgo `//export` trampolines) exactly once at package init via
//!   [`lance_register_go_vtable`].
//! - Rust-side code wraps an opaque Go plugin handle in [`GoPlugin`] and
//!   dispatches byte-payload calls through the vtable's universal `invoke`
//!   slot. Per-plugin method enums and payload encodings are defined by the
//!   individual plugin bindings, not here.
//!
//! Ownership and conventions:
//! - Input payloads cross as borrowed `(ptr, len)`. Go copies what it needs
//!   before the call returns.
//! - Output buffers are allocated by Go with `malloc` (`C.CBytes`) and freed
//!   here with `libc::free`. Both sides use the process C allocator.
//! - `invoke` return codes: [`LANCE_GO_CALL_OK`] (out buffer holds the
//!   response, possibly empty), [`LANCE_GO_CALL_ERROR`] (out buffer holds a
//!   UTF-8 error message), [`LANCE_GO_CALL_MISS`] (not found / miss, no out
//!   buffer).
//! - [`GoPlugin`] is a borrowed, `Copy` registry key for callbacks whose Go
//!   caller retains ownership. [`OwnedGoPlugin`] is an `Arc`-backed lease for
//!   registrations handed to native objects; its final drop unregisters the
//!   Go value exactly when the last native clone is gone.

use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::{Arc, OnceLock};

use crate::error::{self, ErrorCode, set_error};

/// ABI version of [`LanceGoVTable`]. Bump on any breaking change to the
/// vtable layout or calling conventions. [`lance_register_go_vtable`]
/// rejects mismatches.
pub const LANCE_GO_VTABLE_ABI_VERSION: u32 = 1;

/// `invoke` succeeded. The out buffer holds the response payload (a NULL out
/// buffer means an empty response).
pub const LANCE_GO_CALL_OK: i32 = 0;
/// `invoke` failed. The out buffer holds a UTF-8 error message.
pub const LANCE_GO_CALL_ERROR: i32 = 1;
/// `invoke` reports "not found" / cache miss, with no out buffer.
pub const LANCE_GO_CALL_MISS: i32 = 2;

/// Table of C function pointers implemented by cgo-exported Go functions,
/// registered once per process at Go package init via
/// [`lance_register_go_vtable`].
///
/// `in_ptr` is logically `const`: the callee must never write through it. It
/// is typed mutable only because cgo cannot express `const` parameters in
/// exported Go function signatures.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LanceGoVTable {
    /// Must equal [`LANCE_GO_VTABLE_ABI_VERSION`].
    pub abi_version: u32,
    /// Universal dispatch slot: invokes `method` on the Go plugin behind
    /// `handle` with the `in_ptr`/`in_len` payload. On return, a non-NULL
    /// `*out_ptr` is a `malloc`-allocated buffer of `*out_len` bytes that
    /// the caller must release with `free`. Returns a `LANCE_GO_CALL_*`
    /// code.
    pub invoke: Option<
        unsafe extern "C" fn(
            handle: usize,
            method: i32,
            in_ptr: *mut u8,
            in_len: usize,
            out_ptr: *mut *mut u8,
            out_len: *mut usize,
        ) -> i32,
    >,
    /// Drops the Go-side registration for `handle`. Idempotent: releasing a
    /// stale or unknown handle is a no-op.
    pub release: Option<unsafe extern "C" fn(handle: usize)>,
}

/// The registered vtable. A copy is stored (the struct is all plain data),
/// so no lifetime concerns. The function pointers are cgo export
/// trampolines, valid for the process lifetime.
static VTABLE: OnceLock<LanceGoVTable> = OnceLock::new();

/// Error produced by a Go plugin call: either a transport failure (vtable
/// missing) or an error returned by the Go plugin itself, whose message
/// crossed the boundary as UTF-8 bytes.
#[derive(Debug, Clone)]
pub(crate) struct GoCallbackError(pub(crate) String);

impl std::fmt::Display for GoCallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for GoCallbackError {}

/// A handle to a Go-side plugin object living in the Go handle registry.
///
/// `Copy` on purpose: it is just the registry key. The function pointers
/// live in the global [`VTABLE`], so the type is trivially `Send + Sync`.
/// There is deliberately no `Drop` impl. See the module docs for the
/// release-ownership contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GoPlugin {
    handle: usize,
}

impl GoPlugin {
    /// Wraps a raw handle previously issued by the Go handle registry.
    pub(crate) fn new(handle: usize) -> Self {
        Self { handle }
    }

    /// Synchronously invokes `method` on the Go plugin.
    ///
    /// Returns `Ok(Some(bytes))` on success (an empty response is an empty
    /// vec), `Ok(None)` on a miss, and `Err` with the Go-side message on
    /// failure (including calls on released/unknown handles, which the Go
    /// registry reports as a clean error rather than crashing).
    ///
    /// This blocks the calling thread while Go runs. From async code use
    /// [`GoPlugin::call_blocking`] instead so runtime workers are not
    /// stalled (and re-entrant Go callbacks cannot deadlock the runtime).
    pub(crate) fn call(
        &self,
        method: i32,
        payload: &[u8],
    ) -> Result<Option<Vec<u8>>, GoCallbackError> {
        let vt = VTABLE
            .get()
            .ok_or_else(|| GoCallbackError("go callback vtable not registered".to_string()))?;
        let invoke = vt
            .invoke
            .ok_or_else(|| GoCallbackError("go callback vtable has no invoke slot".to_string()))?;
        let mut out_ptr: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        // SAFETY: the vtable was validated at registration. `payload` stays
        // borrowed for the duration of the call, and the out params point at
        // live locals. Go never writes through `in_ptr` (see LanceGoVTable
        // docs for why it is typed mutable).
        let rc = unsafe {
            invoke(
                self.handle,
                method,
                payload.as_ptr().cast_mut(),
                payload.len(),
                &mut out_ptr,
                &mut out_len,
            )
        };
        let out = if out_ptr.is_null() {
            None
        } else {
            // SAFETY: a non-NULL out buffer is a live malloc allocation of
            // `out_len` bytes handed to us by Go. Copy it out and free it
            // with the matching allocator.
            let bytes = unsafe { slice::from_raw_parts(out_ptr, out_len) }.to_vec();
            unsafe { libc::free(out_ptr as *mut c_void) };
            Some(bytes)
        };
        match rc {
            LANCE_GO_CALL_OK => Ok(Some(out.unwrap_or_default())),
            LANCE_GO_CALL_MISS => Ok(None),
            _ => Err(GoCallbackError(match out {
                Some(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                None => format!("go plugin call failed with code {rc} and no message"),
            })),
        }
    }

    /// Like [`GoPlugin::call`], but runs on tokio's blocking pool, for use
    /// inside async trait implementations: a Go callback may block for
    /// arbitrarily long (or re-enter this library), so it must never run on
    /// a runtime worker thread.
    pub(crate) async fn call_blocking(
        self,
        method: i32,
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, GoCallbackError> {
        tokio::task::spawn_blocking(move || self.call(method, &payload))
            .await
            .map_err(|e| GoCallbackError(format!("go plugin call task failed: {e}")))?
    }

    /// Releases the Go-side registration for this handle via the vtable's
    /// `release` slot. Only for native code that was explicitly handed
    /// ownership of the registration. Call at most once per registration
    /// (the Go side tolerates stale releases as no-ops). No-op if no vtable
    /// is registered.
    pub(crate) fn release(self) {
        let Some(vt) = VTABLE.get() else { return };
        let Some(release) = vt.release else { return };
        // SAFETY: the vtable was validated at registration. Release is
        // idempotent on the Go side.
        unsafe { release(self.handle) };
    }
}

/// An owning lease for a Go plugin registration shared by native objects.
/// Cloning the lease is cheap; the Go registration is released exactly once
/// when the final clone is dropped.
#[derive(Debug, Clone)]
pub(crate) struct OwnedGoPlugin {
    inner: Arc<OwnedGoPluginInner>,
}

#[derive(Debug)]
struct OwnedGoPluginInner {
    plugin: GoPlugin,
}

impl Drop for OwnedGoPluginInner {
    fn drop(&mut self) {
        self.plugin.release();
    }
}

impl OwnedGoPlugin {
    pub(crate) fn new(handle: usize) -> Self {
        Self {
            inner: Arc::new(OwnedGoPluginInner {
                plugin: GoPlugin::new(handle),
            }),
        }
    }

    pub(crate) async fn call_blocking(
        &self,
        method: i32,
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, GoCallbackError> {
        self.inner.plugin.call_blocking(method, payload).await
    }

    /// Synchronous variant of [`OwnedGoPlugin::call_blocking`] for
    /// non-async callback contexts (e.g. Lance's synchronous stats hooks).
    /// Same caveats as [`GoPlugin::call`]: blocks the calling thread while Go
    /// runs, so keep the Go side short and never re-enter this library.
    pub(crate) fn call(
        &self,
        method: i32,
        payload: &[u8],
    ) -> Result<Option<Vec<u8>>, GoCallbackError> {
        self.inner.plugin.call(method, payload)
    }
}

/// Registers the process-wide Go callback vtable. Called exactly once from
/// the Go package init. The struct is copied, so the pointer only needs to
/// stay valid for the duration of the call.
///
/// Errors: `InvalidArgument` for a NULL vtable, an `abi_version` mismatch,
/// or NULL function-pointer slots. `AlreadyExists` if a vtable has already
/// been registered in this process.
///
/// # Safety
///
/// `vtable` must be NULL or point to a valid, initialized `LanceGoVTable`
/// whose function pointers remain valid for the process lifetime (cgo
/// export trampolines are).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_register_go_vtable(vtable: *const LanceGoVTable) -> i32 {
    if vtable.is_null() {
        return set_error(ErrorCode::InvalidArgument, "vtable must not be NULL");
    }
    // SAFETY: non-NULL per the check above. Validity is the caller's
    // contract. The struct is plain data, so copying it out is sound.
    let vt = unsafe { *vtable };
    if vt.abi_version != LANCE_GO_VTABLE_ABI_VERSION {
        return set_error(
            ErrorCode::InvalidArgument,
            format!(
                "go vtable ABI version mismatch: got {}, expected {} \
                 (rebuild the native library and the Go package together)",
                vt.abi_version, LANCE_GO_VTABLE_ABI_VERSION
            ),
        );
    }
    if vt.invoke.is_none() || vt.release.is_none() {
        return set_error(
            ErrorCode::InvalidArgument,
            "go vtable has NULL function-pointer slots",
        );
    }
    if VTABLE.set(vt).is_err() {
        return set_error(
            ErrorCode::AlreadyExists,
            "go callback vtable already registered",
        );
    }
    error::ok()
}

/// Reconstructs a payload slice from FFI `(ptr, len)`, treating NULL/empty
/// as an empty slice.
///
/// SAFETY contract (private helper): a non-NULL `ptr` must point at `len`
/// readable bytes that outlive the returned slice's use.
unsafe fn payload_slice<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: per the contract above.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// Maps a [`GoPlugin::call`] result onto the FFI out-params and error slot:
/// success exports a `malloc`ed copy of the response (NULL/0 for empty) that
/// the caller frees with `free`. A miss becomes `ErrorCode::NotFound`, and an
/// error becomes `ErrorCode::Internal` carrying the plugin's message.
///
/// SAFETY contract (private helper): `out_ptr` and `out_len` must be valid,
/// writable, and already zeroed by the caller.
unsafe fn export_roundtrip_result(
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    result: Result<Option<Vec<u8>>, GoCallbackError>,
) -> i32 {
    match result {
        Ok(Some(bytes)) => {
            if !bytes.is_empty() {
                // SAFETY: malloc'd buffer of the right size. Out params are
                // valid per the contract above.
                unsafe {
                    let p = libc::malloc(bytes.len()) as *mut u8;
                    if p.is_null() {
                        return set_error(ErrorCode::Internal, "malloc failed");
                    }
                    ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
                    *out_ptr = p;
                    *out_len = bytes.len();
                }
            }
            error::ok()
        }
        Ok(None) => set_error(ErrorCode::NotFound, "go plugin returned a miss"),
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Test-only: drives [`GoPlugin::call`] synchronously so the Go test suite
/// can exercise the full Go → Rust → Go loop. On success `*out_ptr` /
/// `*out_len` receive a `malloc`-allocated copy of the plugin response
/// (NULL/0 for an empty response) that the caller must release with `free`.
/// A plugin miss maps to `ErrorCode::NotFound`, and a plugin error maps to
/// `ErrorCode::Internal` with the plugin's message.
///
/// # Safety
///
/// `payload` must be NULL or point at `payload_len` readable bytes, and
/// `out_ptr` and `out_len` must be valid writable pointers.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_test_callback_roundtrip(
    handle: usize,
    method: i32,
    payload: *const u8,
    payload_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if out_ptr.is_null() || out_len.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "out_ptr and out_len must not be NULL",
        );
    }
    // SAFETY: out params are non-NULL and writable per the contract.
    unsafe {
        *out_ptr = ptr::null_mut();
        *out_len = 0;
    }
    // SAFETY: payload ptr/len validity is the caller's contract.
    let payload = unsafe { payload_slice(payload, payload_len) };
    let result = GoPlugin::new(handle).call(method, payload);
    // SAFETY: out params validated and zeroed above.
    unsafe { export_roundtrip_result(out_ptr, out_len, result) }
}

/// Test-only: like [`lance_test_callback_roundtrip`] but through
/// [`GoPlugin::call_blocking`] on the shared tokio runtime
/// (`block_on(spawn_blocking(...))`), the path async plugin bindings use.
///
/// # Safety
///
/// Same contract as [`lance_test_callback_roundtrip`].
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_test_callback_roundtrip_async(
    handle: usize,
    method: i32,
    payload: *const u8,
    payload_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if out_ptr.is_null() || out_len.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "out_ptr and out_len must not be NULL",
        );
    }
    // SAFETY: out params are non-NULL and writable per the contract.
    unsafe {
        *out_ptr = ptr::null_mut();
        *out_len = 0;
    }
    // SAFETY: payload ptr/len validity is the caller's contract. The copy
    // to a Vec is required: spawn_blocking needs 'static ownership.
    let payload = unsafe { payload_slice(payload, payload_len) }.to_vec();
    // not cancellable: test transport for the callback bridge; its semantics
    // must stay deterministic regardless of any installed cancel token.
    let result = crate::runtime::block_on(GoPlugin::new(handle).call_blocking(method, payload));
    // SAFETY: out params validated and zeroed above.
    unsafe { export_roundtrip_result(out_ptr, out_len, result) }
}

/// Test-only: releases a Go plugin handle through the vtable's `release`
/// slot, exercising [`GoPlugin::release`] from native code.
///
/// # Safety
///
/// Callable from any thread. `handle` may be stale (release is idempotent
/// on the Go side).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_test_callback_release(handle: usize) {
    GoPlugin::new(handle).release();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static RELEASED: AtomicUsize = AtomicUsize::new(0);

    /// Fake Go side: method 0 echoes `h<handle>:<payload>`, method 1 is a
    /// miss, anything else fails with a message.
    unsafe extern "C" fn fake_invoke(
        handle: usize,
        method: i32,
        in_ptr: *mut u8,
        in_len: usize,
        out_ptr: *mut *mut u8,
        out_len: *mut usize,
    ) -> i32 {
        let payload = unsafe { payload_slice(in_ptr.cast_const(), in_len) };
        let (bytes, rc) = match method {
            0 => {
                let mut b = format!("h{handle}:").into_bytes();
                b.extend_from_slice(payload);
                (b, LANCE_GO_CALL_OK)
            }
            1 => return LANCE_GO_CALL_MISS,
            _ => (b"fake plugin failure".to_vec(), LANCE_GO_CALL_ERROR),
        };
        unsafe {
            let p = libc::malloc(bytes.len()) as *mut u8;
            ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
            *out_ptr = p;
            *out_len = bytes.len();
        }
        rc
    }

    unsafe extern "C" fn fake_release(handle: usize) {
        RELEASED.store(handle, Ordering::SeqCst);
    }

    /// One sequential test: VTABLE is process-global, so registration
    /// validation and call semantics must run in a fixed order.
    #[test]
    fn vtable_registration_and_calls() {
        // Calls before registration fail cleanly.
        let err = GoPlugin::new(1).call(0, b"x").unwrap_err();
        assert!(err.0.contains("not registered"), "{err}");

        // NULL vtable.
        assert_eq!(
            unsafe { lance_register_go_vtable(ptr::null()) },
            ErrorCode::InvalidArgument as i32
        );
        // ABI mismatch.
        let bad = LanceGoVTable {
            abi_version: LANCE_GO_VTABLE_ABI_VERSION + 1,
            invoke: Some(fake_invoke),
            release: Some(fake_release),
        };
        assert_eq!(
            unsafe { lance_register_go_vtable(&bad) },
            ErrorCode::InvalidArgument as i32
        );
        // NULL slots.
        let holey = LanceGoVTable {
            abi_version: LANCE_GO_VTABLE_ABI_VERSION,
            invoke: None,
            release: Some(fake_release),
        };
        assert_eq!(
            unsafe { lance_register_go_vtable(&holey) },
            ErrorCode::InvalidArgument as i32
        );

        // Valid registration succeeds exactly once.
        let good = LanceGoVTable {
            abi_version: LANCE_GO_VTABLE_ABI_VERSION,
            invoke: Some(fake_invoke),
            release: Some(fake_release),
        };
        assert_eq!(unsafe { lance_register_go_vtable(&good) }, 0);
        assert_eq!(
            unsafe { lance_register_go_vtable(&good) },
            ErrorCode::AlreadyExists as i32
        );

        // Sync call: ok / miss / error.
        let plugin = GoPlugin::new(42);
        assert_eq!(
            plugin.call(0, b"ping").unwrap().as_deref(),
            Some(&b"h42:ping"[..])
        );
        assert_eq!(plugin.call(1, b"").unwrap(), None);
        let err = plugin.call(2, b"").unwrap_err();
        assert_eq!(err.0, "fake plugin failure");

        // Async path through spawn_blocking.
        let out = crate::runtime::block_on(plugin.call_blocking(0, b"pong".to_vec())).unwrap();
        assert_eq!(out.as_deref(), Some(&b"h42:pong"[..]));

        // Release reaches the fake Go side.
        plugin.release();
        assert_eq!(RELEASED.load(Ordering::SeqCst), 42);
    }
}
