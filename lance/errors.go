package lance

/*
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

// Sentinel errors mirroring the ErrorCode enum in rust/src/error.rs.
// Errors returned by this package wrap exactly one of these, so callers can
// classify failures with errors.Is.
//
// Sentinels carry NO "lance: " prefix. The single package prefix is added
// exactly once at the wrap layer (the datasetOp/datasetDo/fragmentOp helpers
// in ops.go, or the hand-rolled "lance: <verb>: %w" wraps), so error strings
// read "lance: <verb>: <sentinel>[: <native message>]".
var (
	// ErrInvalidArgument is returned when a call receives a malformed or
	// out-of-range argument (e.g. an unresolvable Ref, a negative offset).
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrIO is returned when the underlying object store or filesystem
	// reports a read/write failure.
	ErrIO = errors.New("io error")
	// ErrNotFound is returned when the referenced dataset, version, tag,
	// branch, fragment, or file does not exist.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists is returned when a create-only operation targets a
	// name or path that is already taken (e.g. an existing tag or branch).
	ErrAlreadyExists = errors.New("already exists")
	// ErrIndex is returned when an index build, load, or query fails.
	ErrIndex = errors.New("index error")
	// ErrInternal is returned for native-side failures that do not map to a
	// more specific sentinel, including panics caught at the FFI boundary.
	ErrInternal = errors.New("internal error")
	// ErrNotImplemented is returned when the requested operation or option
	// combination is not supported by the underlying Lance crate.
	ErrNotImplemented = errors.New("not implemented")
	// ErrConflict is returned when a commit loses a concurrent-write race
	// against another writer (retry against the latest version).
	ErrConflict = errors.New("conflict")
	// ErrTimeout is returned when the native side reports its own timeout,
	// distinct from ctx exceeding its deadline (which surfaces as
	// context.DeadlineExceeded).
	ErrTimeout = errors.New("timeout")
	// ErrReentrantCall is returned when a synchronous Go callback (an
	// AddColumnsUDF mapper, a UDFCheckpointStore method, or a WriteWithProgress
	// callback) re-enters this package while the native operation that invoked
	// it is still running. Such a nested call would drive the shared tokio
	// runtime from within itself, which would panic in Tokio, so it is rejected
	// up front. Callbacks must not call
	// back into lance-go. See the docs on those APIs.
	ErrReentrantCall = errors.New("reentrant call from within a callback")
	// ErrCanceled is the defensive fallback sentinel for ERROR_CODE_CANCELLED.
	// In practice a cancelled FFI call surfaces ctx.Err() directly
	// (context.Canceled or context.DeadlineExceeded); ErrCanceled is only
	// returned in the theoretical race where the native side reports
	// cancellation before the Go context observes it.
	ErrCanceled = errors.New("operation canceled")
)

// codeToSentinel maps FFI error codes (ErrorCode in rust/src/error.rs) to
// the package sentinel errors.
func codeToSentinel(code int32) error {
	switch code {
	case C.ERROR_CODE_INVALID_ARGUMENT:
		return ErrInvalidArgument
	case C.ERROR_CODE_IO:
		return ErrIO
	case C.ERROR_CODE_NOT_FOUND:
		return ErrNotFound
	case C.ERROR_CODE_ALREADY_EXISTS:
		return ErrAlreadyExists
	case C.ERROR_CODE_INDEX:
		return ErrIndex
	case C.ERROR_CODE_NOT_IMPLEMENTED:
		return ErrNotImplemented
	case C.ERROR_CODE_CONFLICT:
		return ErrConflict
	case C.ERROR_CODE_TIMEOUT:
		return ErrTimeout
	case C.ERROR_CODE_CANCELLED:
		return ErrCanceled
	default:
		return ErrInternal
	}
}

// ffiCall runs one fallible FFI call with OS-thread pinning, re-entrancy
// protection, and context cancellation.
//
// Thread pinning: the goroutine is pinned to its OS thread across the FFI
// call and the thread-local error retrieval, so lastError() always reads the
// error set by THIS call. Without the pin, the goroutine could migrate to a
// different OS thread between the failing lance_* call and the
// lance_last_error_* reads (Go only keeps a goroutine on its thread for the
// duration of a single cgo call), observing an empty or unrelated error slot.
//
// Cancellation: when ctx is cancellable (ctx.Done() != nil), a cancel token
// is installed in the pinned thread's cancellation slot and a watcher
// goroutine fires it when ctx is done, aborting the native operation, which
// then returns ERROR_CODE_CANCELLED. That result surfaces as ctx.Err()
// (context.Canceled or context.DeadlineExceeded), with ErrCanceled as a
// defensive fallback. context.Background() and other non-cancellable
// contexts skip the token machinery entirely.
//
// All fallible C.lance_* calls must go through this helper. Non-fallible calls
// such as lance_dataset_close, lance_abi_version, lance_version, and native
// handle property accessors may be made directly.
func ffiCall(ctx context.Context, f func() C.int32_t) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	// Reject re-entrant calls: if this thread is already inside the shared
	// tokio runtime, the call originates from a synchronous Go callback still
	// running inside a native operation. Letting it proceed would drive the
	// runtime from within itself and panic in Tokio. Fail cleanly instead.
	// Non-reentrant top-level calls never
	// run on a runtime thread, so this is a no-op for them.
	if C.lance_in_tokio_runtime() != 0 {
		return fmt.Errorf("%w", ErrReentrantCall)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() != nil {
		tok := C.lance_cancel_token_new()
		C.lance_thread_set_cancel_token(tok)
		stop := make(chan struct{})
		watcherDone := make(chan struct{})
		go func() {
			defer close(watcherDone)
			select {
			case <-ctx.Done():
				C.lance_cancel_token_cancel(tok)
			case <-stop:
			}
		}()
		// Teardown ordering is load-bearing:
		//   close(stop) → join watcher → clear thread slot → free token.
		defer func() {
			close(stop)
			<-watcherDone                        // watcher can no longer touch tok
			C.lance_thread_set_cancel_token(nil) // MUST clear: OS threads are reused
			C.lance_cancel_token_free(tok)
		}()
	}
	rc := f()
	if rc == C.ERROR_CODE_OK {
		return nil
	}
	if rc == C.ERROR_CODE_CANCELLED {
		// The native side also records the cancellation in the thread-local
		// error slot; we deliberately bypass lastError() here and surface the
		// context's own error so callers see context.Canceled /
		// context.DeadlineExceeded.
		if err := ctx.Err(); err != nil {
			return err
		}
		// Defensive race fallback: native reported cancellation before the Go
		// context observed it.
		return fmt.Errorf("%w", ErrCanceled)
	}
	if err := lastError(); err != nil {
		return err
	}
	// Defensive: the native side returned failure without recording an
	// error. Never return nil for a failed call.
	return ErrInternal
}

// lastError reads the calling thread's last FFI error and wraps it in the
// matching sentinel. It must only be called from ffiCall, which pins the
// OS thread across the failing lance_* call and this retrieval. Returns nil
// if the native side reports no error.
func lastError() error {
	code := int32(C.lance_last_error_code())
	if code == C.ERROR_CODE_OK {
		return nil
	}
	msg := "unknown error"
	if p := C.lance_last_error_message(); p != nil {
		msg = C.GoString(p)
	}
	return fmt.Errorf("%w: %s", codeToSentinel(code), msg)
}

// testCancellableSleep exists for cancel_test.go only (_test.go files cannot
// use cgo). It drives the lance_test_cancellable_sleep export through
// ffiCall, exercising the set-token / cancel / ERROR_CODE_CANCELLED round
// trip without a slow real dataset operation.
func testCancellableSleep(ctx context.Context, ms uint64) error {
	return ffiCall(ctx, func() C.int32_t {
		return C.lance_test_cancellable_sleep(C.uint64_t(ms))
	})
}
