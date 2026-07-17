//! Shared tokio runtime for all blocking FFI entry points, plus the
//! cooperative cancellation plumbing that lets Go `context.Context`s cancel
//! in-flight FFI calls.
//!
//! Lance is async end-to-end, but the C ABI is synchronous. Every exported
//! function that needs async Lance APIs funnels through [`block_on`] (or its
//! cancellable sibling [`block_on_cancellable`]), which drives the future on
//! a single process-wide multi-threaded runtime (the same pattern lance-jni
//! uses).
//!
//! # Cancellation model
//!
//! Go owns a [`LanceCancelToken`] handle (created with
//! [`lance_cancel_token_new`]) and, per FFI call, pins the goroutine to its
//! OS thread and installs the token in that thread's [`CURRENT_CANCEL`] slot
//! via [`lance_thread_set_cancel_token`]. Fallible exports that go through
//! the `block_on_cc!` macro then race the Lance future against the token and
//! return [`ErrorCode::Cancelled`] when the token wins. Cancelling drops the
//! in-flight future, so the operation is abandoned cooperatively (already
//! spawned background work may still run to completion).
//!
//! [`ErrorCode::Cancelled`]: crate::error::ErrorCode::Cancelled

use std::cell::RefCell;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;

/// Process-wide tokio runtime shared by all FFI calls.
pub static RT: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .thread_name("lance-go-ffi")
        .build()
        .expect("failed to build lance-go tokio runtime")
});

thread_local! {
    /// The cancellation token installed on the calling (pinned Go) thread for
    /// the duration of one FFI call, or `None` when the call is not
    /// cancellable. Set/cleared via [`lance_thread_set_cancel_token`].
    static CURRENT_CANCEL: RefCell<Option<CancellationToken>> = const { RefCell::new(None) };
}

/// Opaque handle to a cancellation token shared between Go and this library.
/// Created by [`lance_cancel_token_new`], cancelled (from any thread) with
/// [`lance_cancel_token_cancel`], and released with
/// [`lance_cancel_token_free`].
pub struct LanceCancelToken(CancellationToken);

/// Marker error returned by [`block_on_cancellable`] when the installed token
/// was cancelled before the future completed.
pub struct Cancelled;

/// Runs a future to completion on the shared runtime, blocking the calling
/// (Go) thread.
///
/// # Panics
///
/// Panics ("Cannot start a runtime from within a runtime") if called from a
/// thread already inside the tokio runtime, e.g. a synchronous Go callback
/// (BatchUDF mapper, checkpoint store, write-progress) that re-enters a
/// `lance_*` FFI function. The Go side prevents this by rejecting re-entrant
/// FFI calls at the `ffiCall` boundary using [`lance_in_tokio_runtime`]; the
/// outer FFI guard remains a final containment layer.
pub fn block_on<F: Future>(future: F) -> F::Output {
    RT.block_on(future)
}

/// Like [`block_on`], but races the future against the cancellation token
/// installed on the calling thread (if any). Returns `Err(Cancelled)` when
/// the token is cancelled first; the future is dropped at that point.
///
/// The TLS slot is read synchronously on the calling (pinned Go) thread
/// BEFORE entering the runtime: runtime worker threads have their own
/// (empty) slots, so the token must be captured here.
///
/// # Panics
///
/// Same re-entrancy contract as [`block_on`].
pub fn block_on_cancellable<F: Future>(future: F) -> Result<F::Output, Cancelled> {
    let token = CURRENT_CANCEL.with(|t| t.borrow().clone());
    match token {
        None => Ok(RT.block_on(future)),
        Some(tok) => RT.block_on(async {
            tokio::select! {
                biased;
                out = future => Ok(out),
                _ = tok.cancelled() => Err(Cancelled),
            }
        }),
    }
}

/// Drives `$fut` through [`block_on_cancellable`], early-returning
/// `ErrorCode::Cancelled` (as `i32`) from the enclosing function when the
/// calling thread's cancellation token fires first.
///
/// Only valid inside `i32`-error-code-returning FFI exports (or helpers with
/// the same convention), where an early error-code return is well-formed.
macro_rules! block_on_cc {
    ($fut:expr $(,)?) => {
        match $crate::runtime::block_on_cancellable($fut) {
            Ok(v) => v,
            Err($crate::runtime::Cancelled) => {
                return $crate::error::set_error(
                    $crate::error::ErrorCode::Cancelled,
                    "operation canceled by caller",
                );
            }
        }
    };
}
pub(crate) use block_on_cc;

/// Creates a new cancellation token handle. Never fails. Release it with
/// [`lance_cancel_token_free`].
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_cancel_token_new() -> *mut LanceCancelToken {
    Box::into_raw(Box::new(LanceCancelToken(CancellationToken::new())))
}

/// Cancels the token, waking every FFI call currently racing against it.
/// Callable from ANY thread (this is the whole point: the Go side calls it
/// from a watcher goroutine while another OS thread is blocked inside an FFI
/// call). NULL is a no-op. Cancelling more than once is a no-op.
///
/// # Safety
///
/// `t` must be NULL or a live handle from [`lance_cancel_token_new`] that has
/// not been freed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_cancel_token_cancel(t: *const LanceCancelToken) {
    if t.is_null() {
        return;
    }
    // SAFETY: `t` is a live handle per the contract above.
    unsafe { &*t }.0.cancel();
}

/// Releases a cancellation token handle. NULL is a no-op. Safe to call after
/// [`lance_cancel_token_cancel`]; threads that cloned the inner token via
/// [`lance_thread_set_cancel_token`] are unaffected (the token is
/// reference-counted internally).
///
/// # Safety
///
/// `t` must be NULL or a handle from [`lance_cancel_token_new`] not already
/// freed, and must not be used again afterwards.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_cancel_token_free(t: *mut LanceCancelToken) {
    if t.is_null() {
        return;
    }
    // SAFETY: per the contract, `t` came from Box::into_raw and is freed at
    // most once.
    drop(unsafe { Box::from_raw(t) });
}

/// Installs (a clone of) the token in the calling thread's cancellation slot,
/// making subsequent `block_on_cc!`-based FFI calls on THIS thread
/// cancellable. NULL clears the slot. The Go side pairs a set/clear around
/// each cancellable FFI call while the goroutine is pinned to its OS thread.
///
/// # Safety
///
/// `t` must be NULL or a live handle from [`lance_cancel_token_new`] that has
/// not been freed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_thread_set_cancel_token(t: *const LanceCancelToken) {
    let token = if t.is_null() {
        None
    } else {
        // SAFETY: `t` is a live handle per the contract above. Cloning the
        // inner token detaches this thread's slot from the handle's lifetime.
        Some(unsafe { &*t }.0.clone())
    };
    CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = token);
}

/// Reports whether the calling thread is currently executing inside the shared
/// tokio runtime (returns `1` if so, `0` otherwise).
///
/// This is `true` exactly on the threads where a nested [`block_on`] would
/// panic: the `block_on` driver thread, runtime worker threads, and blocking
/// threads. The Go side calls this at the start of every fallible FFI call
/// (`ffiCall`) to reject re-entrant calls made from inside a synchronous Go
/// callback, converting a would-be Tokio panic into a specific, catchable Go
/// error (`ErrReentrantCall`) before the outer FFI guard is needed.
///
/// Non-fallible (top-level) Go calls never run on a runtime thread, so this
/// returns `0` for them and they proceed normally.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_in_tokio_runtime() -> i32 {
    i32::from(tokio::runtime::Handle::try_current().is_ok())
}

/// Test-only support for the Go cancellation tests: a fallible export whose
/// entire body is a cancellable sleep. Lets the Go side verify the
/// set-token / cancel / `ERROR_CODE_CANCELLED` round trip without needing a
/// slow real dataset operation.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn lance_test_cancellable_sleep(millis: u64) -> i32 {
    // The sleep future must be constructed inside the runtime (it captures
    // the timer handle at creation), hence the async block.
    block_on_cc!(async move { tokio::time::sleep(Duration::from_millis(millis)).await });
    crate::error::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use std::time::Instant;

    #[test]
    fn block_on_works() {
        let out = block_on(async { 21 * 2 });
        assert_eq!(out, 42);
    }

    #[test]
    fn in_tokio_runtime_reports_reentrancy() {
        // Outside any runtime: not re-entrant.
        assert_eq!(lance_in_tokio_runtime(), 0);
        // Inside block_on (the state a nested block_on would panic in): the
        // driver thread reports as inside the runtime.
        let inside = block_on(async { lance_in_tokio_runtime() });
        assert_eq!(inside, 1);
        // Back outside again.
        assert_eq!(lance_in_tokio_runtime(), 0);
    }

    #[test]
    fn block_on_cancellable_without_token_completes() {
        // No token installed on this thread: plain completion.
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = None);
        let out = block_on_cancellable(async { 7 * 6 });
        assert!(matches!(out, Ok(42)));
    }

    #[test]
    fn block_on_cancellable_with_pre_cancelled_token_errs() {
        let token = CancellationToken::new();
        token.cancel();
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = Some(token));
        // A pending-forever future: only cancellation can win.
        let out = block_on_cancellable(std::future::pending::<()>());
        assert!(out.is_err());
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = None);
    }

    #[test]
    fn cancel_from_another_thread_interrupts_promptly() {
        let token = CancellationToken::new();
        let remote = token.clone();
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = Some(token));
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            remote.cancel();
        });
        let start = Instant::now();
        let out = block_on_cancellable(async { tokio::time::sleep(Duration::from_secs(30)).await });
        assert!(out.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "cancellation was not prompt: {:?}",
            start.elapsed()
        );
        canceller.join().unwrap();
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = None);
    }

    #[test]
    fn completed_future_wins_over_cancelled_token() {
        // `biased` polls the future first, so a ready future beats an
        // already-cancelled token.
        let token = CancellationToken::new();
        token.cancel();
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = Some(token));
        let out = block_on_cancellable(std::future::ready(42));
        assert!(matches!(out, Ok(42)));
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = None);
    }

    #[test]
    fn tls_set_and_clear_via_exports() {
        let handle = lance_cancel_token_new();
        assert!(!handle.is_null());

        // Install: the thread slot now holds a clone of the handle's token.
        unsafe { lance_thread_set_cancel_token(handle) };
        assert!(CURRENT_CANCEL.with(|slot| slot.borrow().is_some()));

        // Cancelling the handle is observed through a cancellable export.
        unsafe { lance_cancel_token_cancel(handle) };
        assert_eq!(
            lance_test_cancellable_sleep(30_000),
            ErrorCode::Cancelled as i32
        );
        assert_eq!(
            crate::error::lance_last_error_code(),
            ErrorCode::Cancelled as i32
        );

        // Freeing the handle is safe after cancel and does not disturb the
        // installed clone.
        unsafe { lance_cancel_token_free(handle) };
        assert!(CURRENT_CANCEL.with(|slot| slot.borrow().is_some()));

        // NULL clears the slot; the sleep completes normally again.
        unsafe { lance_thread_set_cancel_token(std::ptr::null()) };
        assert!(CURRENT_CANCEL.with(|slot| slot.borrow().is_none()));
        assert_eq!(lance_test_cancellable_sleep(1), 0);
    }

    #[test]
    fn null_token_exports_are_no_ops() {
        unsafe {
            lance_cancel_token_cancel(std::ptr::null());
            lance_cancel_token_free(std::ptr::null_mut());
        }
    }
}
