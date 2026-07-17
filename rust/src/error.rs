//! Thread-local error reporting for the C ABI.
//!
//! Fallible FFI functions return an [`ErrorCode`] as `i32`. On failure they
//! record the code and message in a thread-local slot. Go reads it back via
//! `lance_last_error_code` / `lance_last_error_message` on the same thread.
//! cgo only keeps a goroutine on its OS thread for the duration of a single
//! C call, so the Go side's `ffiCall` helper pins the goroutine to its OS
//! thread (`runtime.LockOSThread`) across the failing call and the pair of
//! error reads. All fallible calls must go through it.

use std::any::Any;
use std::cell::RefCell;
use std::ffi::{CString, c_char};
use std::fmt::Display;

/// Error codes returned by fallible `lance_*` functions.
///
/// Mirrored by the sentinel errors in the Go package
/// (`lance.ErrInvalidArgument`, `lance.ErrIO`, ...).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Ok = 0,
    InvalidArgument = 1,
    Io = 2,
    NotFound = 3,
    AlreadyExists = 4,
    Index = 5,
    Internal = 6,
    NotImplemented = 7,
    Conflict = 8,
    Timeout = 9,
    /// The calling thread's cancellation token fired before the operation
    /// completed (see `crate::runtime`). Never produced by `map_lance_error`.
    Cancelled = 10,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<(i32, CString)>> = const { RefCell::new(None) };
}

/// Records `err` as the calling thread's last error and returns `code` as
/// `i32`, ready to be returned straight from an FFI function.
pub fn set_error(code: ErrorCode, err: impl Display) -> i32 {
    let message = err.to_string();
    // Interior NUL bytes would make CString::new fail, so replace rather than
    // lose the error entirely.
    let c_message = CString::new(message).unwrap_or_else(|e| {
        let sanitized: Vec<u8> = e
            .into_vec()
            .into_iter()
            .map(|b| if b == 0 { b' ' } else { b })
            .collect();
        CString::new(sanitized).expect("NUL bytes were replaced")
    });
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some((code as i32, c_message));
    });
    code as i32
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string Rust panic")
}

/// Records a caught Rust panic in the thread-local FFI error channel.
pub(crate) fn record_panic(payload: &(dyn Any + Send)) {
    let _ = set_error(
        ErrorCode::Internal,
        format!(
            "panic contained at lance-go FFI boundary: {}",
            panic_message(payload)
        ),
    );
}

/// Records a caught panic and returns the numeric Internal error code.
pub(crate) fn set_panic_error(payload: &(dyn Any + Send)) -> i32 {
    record_panic(payload);
    ErrorCode::Internal as i32
}

/// Clears the calling thread's last error and returns `ErrorCode::Ok` as
/// `i32`.
pub fn ok() -> i32 {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
    ErrorCode::Ok as i32
}

/// Maps a [`lance::Error`] to the FFI [`ErrorCode`].
///
/// Keep this match exhaustive. When the pinned Lance dependency adds an error
/// variant, compilation must fail until the public Go error contract has made
/// an explicit classification decision for it.
pub fn map_lance_error(err: &lance::Error) -> ErrorCode {
    use lance::Error;
    match err {
        Error::InvalidInput { .. }
        | Error::SchemaMismatch { .. }
        | Error::IncompatibleTransaction { .. }
        | Error::InvalidTableLocation { .. }
        | Error::InvalidRef { .. }
        | Error::FieldNotFound { .. } => ErrorCode::InvalidArgument,
        Error::IO { .. } | Error::CorruptFile { .. } | Error::Cleanup { .. } => ErrorCode::Io,
        Error::NotFound { .. }
        | Error::DatasetNotFound { .. }
        | Error::IndexNotFound { .. }
        | Error::RefNotFound { .. }
        | Error::VersionNotFound { .. } => ErrorCode::NotFound,
        Error::DatasetAlreadyExists { .. } => ErrorCode::AlreadyExists,
        Error::Index { .. } => ErrorCode::Index,
        Error::NotSupported { .. } => ErrorCode::NotImplemented,
        Error::CommitConflict { .. }
        | Error::RetryableCommitConflict { .. }
        | Error::TooMuchWriteContention { .. }
        | Error::RefConflict { .. }
        | Error::VersionConflict { .. } => ErrorCode::Conflict,
        Error::Timeout { .. } => ErrorCode::Timeout,
        Error::Internal { .. }
        | Error::PrerequisiteFailed { .. }
        | Error::Unprocessable { .. }
        | Error::Arrow { .. }
        | Error::Schema { .. }
        | Error::Stop
        | Error::Wrapped { .. }
        | Error::Cloned { .. }
        | Error::Execution { .. }
        | Error::Namespace { .. }
        | Error::External { .. } => ErrorCode::Internal,
    }
}

/// Returns the error code recorded by the most recent failing `lance_*` call
/// on the calling thread, or `0` (Ok) if none.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_last_error_code() -> i32 {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|(code, _)| *code)
            .unwrap_or(ErrorCode::Ok as i32)
    })
}

/// Returns the message for the calling thread's last error, or NULL if none.
///
/// The pointer is borrowed: it stays valid until the next error recorded on
/// the same thread. Do NOT pass it to `lance_string_free`.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_last_error_message() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|(_, message)| message.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Frees a string previously returned as an owned `*mut c_char` by this
/// library (e.g. `lance_version`). NULL is a no-op.
///
/// # Safety
///
/// `s` must be NULL or a pointer obtained from an owned-string-returning
/// function of this library, and must not be used (or freed) again afterwards.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    // SAFETY: the contract requires `s` to originate from CString::into_raw
    // in this library and to be freed at most once.
    unsafe {
        drop(CString::from_raw(s));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[lance_go_ffi_macros::ffi_guard]
    fn guarded_panic() -> i32 {
        panic!("ffi guard test panic")
    }

    #[test]
    fn set_and_read_error() {
        assert_eq!(ok(), 0);
        assert_eq!(lance_last_error_code(), 0);
        assert!(lance_last_error_message().is_null());

        let code = set_error(ErrorCode::NotFound, "dataset missing");
        assert_eq!(code, 3);
        assert_eq!(lance_last_error_code(), 3);
        let msg = unsafe { CStr::from_ptr(lance_last_error_message()) };
        assert_eq!(msg.to_str().unwrap(), "dataset missing");

        assert_eq!(ok(), 0);
        assert!(lance_last_error_message().is_null());
    }

    #[test]
    fn interior_nul_is_sanitized() {
        set_error(ErrorCode::Internal, "bad\0byte");
        let msg = unsafe { CStr::from_ptr(lance_last_error_message()) };
        assert_eq!(msg.to_str().unwrap(), "bad byte");
    }

    #[test]
    fn error_code_values_are_stable() {
        let cases = [
            (ErrorCode::Ok, 0),
            (ErrorCode::InvalidArgument, 1),
            (ErrorCode::Io, 2),
            (ErrorCode::NotFound, 3),
            (ErrorCode::AlreadyExists, 4),
            (ErrorCode::Index, 5),
            (ErrorCode::Internal, 6),
            (ErrorCode::NotImplemented, 7),
            (ErrorCode::Conflict, 8),
            (ErrorCode::Timeout, 9),
            (ErrorCode::Cancelled, 10),
        ];
        for (code, value) in cases {
            assert_eq!(code as i32, value);
        }
    }

    #[test]
    fn representative_lance_errors_are_classified() {
        use lance::Error;

        let cases = [
            (
                Error::invalid_input("bad input"),
                ErrorCode::InvalidArgument,
            ),
            (
                Error::schema_mismatch("different field"),
                ErrorCode::InvalidArgument,
            ),
            (
                Error::field_not_found("missing", vec![]),
                ErrorCode::InvalidArgument,
            ),
            (Error::io("disk failed"), ErrorCode::Io),
            (
                Error::corrupt_file(object_store::path::Path::from("data.lance"), "bad"),
                ErrorCode::Io,
            ),
            (Error::not_found("missing"), ErrorCode::NotFound),
            (Error::index_not_found("idx"), ErrorCode::NotFound),
            (
                Error::dataset_already_exists("dataset"),
                ErrorCode::AlreadyExists,
            ),
            (Error::index("index failed"), ErrorCode::Index),
            (Error::internal("bug"), ErrorCode::Internal),
            (
                Error::not_supported("unsupported"),
                ErrorCode::NotImplemented,
            ),
            (Error::version_conflict("stale", 1, 0), ErrorCode::Conflict),
            (
                Error::too_much_write_contention("busy"),
                ErrorCode::Conflict,
            ),
            (Error::timeout("slow"), ErrorCode::Timeout),
        ];
        for (err, want) in cases {
            assert_eq!(map_lance_error(&err), want, "{err}");
        }
    }

    #[test]
    fn ffi_guard_contains_rust_panic() {
        assert_eq!(guarded_panic(), ErrorCode::Internal as i32);
        assert_eq!(lance_last_error_code(), ErrorCode::Internal as i32);
        let msg = unsafe { CStr::from_ptr(lance_last_error_message()) };
        assert!(msg.to_string_lossy().contains("ffi guard test panic"));
    }
}
