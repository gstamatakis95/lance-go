//! C FFI shim over the Lance columnar format for the Go bindings
//! (`github.com/gstamatakis95/lance-go`).
//!
//! Conventions:
//! - Every exported symbol is prefixed `lance_`.
//! - Fallible functions return an `i32` error code (see [`error::ErrorCode`]).
//!   The message for the most recent error on the calling thread is available
//!   via `lance_last_error_message`.
//! - Strings returned as `*mut c_char` are owned by the caller and must be
//!   released with `lance_string_free`.

pub mod arrow_bridge;
pub mod blob;
pub mod branch;
pub mod cache;
pub mod callbacks;
pub mod config;
pub mod dataset;
pub mod delta;
pub mod distributed;
pub mod error;
pub mod evolution;
pub mod fragment;
pub mod hooks;
pub mod index;
pub mod maintenance;
pub mod mutation;
pub mod runtime;
pub mod scanner;
pub mod session;
pub mod sql;
pub mod storage;
pub mod take;
pub mod transaction;
pub mod version;

use std::ffi::{CString, c_char};

/// ABI version of the complete lance-go native interface. This is separate
/// from `LANCE_GO_VTABLE_ABI_VERSION`, which versions only the callback table.
/// Bump this whenever a C signature, error code, ownership convention, or
/// internal JSON wire contract changes incompatibly.
pub const LANCE_GO_ABI_VERSION: u32 = 1;

/// Returns the ABI version implemented by the linked native library.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_abi_version() -> u32 {
    LANCE_GO_ABI_VERSION
}

/// Returns a heap-allocated, NUL-terminated version string, e.g.
/// `"lance-go-ffi 0.2.0 (lance 8.0.0)"`.
///
/// The caller owns the returned pointer and must free it with
/// `lance_string_free`.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_version() -> *mut c_char {
    let version = format!(
        "lance-go-ffi {} (lance {})",
        env!("CARGO_PKG_VERSION"),
        LANCE_VERSION,
    );
    // The string above never contains an interior NUL byte.
    CString::new(version)
        .expect("version string contains no NUL bytes")
        .into_raw()
}

/// The version of the `lance` crate this shim was built against.
const LANCE_VERSION: &str = "8.0.0";

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn version_round_trip() {
        let ptr = lance_version();
        assert!(!ptr.is_null());
        let s = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_owned();
        unsafe { crate::error::lance_string_free(ptr) };
        assert!(s.contains(&format!("lance-go-ffi {}", env!("CARGO_PKG_VERSION"))));
        assert!(s.contains("lance 8.0.0"));
    }

    #[test]
    fn abi_version_round_trip() {
        assert_eq!(lance_abi_version(), LANCE_GO_ABI_VERSION);
    }
}
