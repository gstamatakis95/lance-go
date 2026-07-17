package lance

/*
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// This file holds the standard plumbing helpers for FFI-backed operations.
// datasetOp / datasetDo / fragmentOp are the STANDARD ENTRY POINTS for
// terminal operations: they bundle the obs span (see observability.go, the
// frozen contract), the handle lock, the open/ctx check, and the single
// "lance: <verb>: %w" error wrap. New and migrated methods should be thin
// bodies passed to one of them.
//
// Contract for fn:
//   - fn receives the live native pointer; the helper holds the handle lock
//     (Dataset: d.mu.RLock, Fragment: f.mu.Lock) for the WHOLE duration of
//     fn, exactly like the hand-rolled methods did, so fn may use ptr freely
//     but must not call methods that re-acquire the same lock.
//   - fn must route every fallible native call through ffiCall(ctx, ...).
//   - fn returns RAW, UNPREFIXED errors ("decode versions: %w", the bare
//     ffiCall error, ...). The helper applies the package's single
//     "lance: <verb>: %w" wrap. Do not add another "lance: " prefix inside
//     fn, and never wrap more than one sentinel.
//   - checkOpen/ctx errors detected before fn runs are returned unwrapped
//     (they already carry their final text), matching the pre-helper
//     behavior.
//
// Patterns that MUST stay hand-rolled (do not force them through these
// helpers):
//   - Stream-returning ops (Scanner.Reader, FragmentScanner.Reader, SQL,
//     Take* streams, ...): they use observedStream, whose span ends at
//     reader release, not at method return.
//   - Ops that record result metrics into the deferred end() closer (e.g.
//     Dataset.Delete's lance.rows_deleted attribute + recordRows): the
//     metric needs the named result at defer time, outside fn.
//   - Top-level constructors with no receiver (Open, Write, NewSession):
//     they resolve obs themselves and have no handle lock to take. They call
//     ffiCall(ctx, ...) directly with a hand-rolled single-prefix wrap.

// datasetOp runs a Dataset FFI operation with the standard plumbing:
// obs span (name, attrs) + read lock + open/ctx check + single
// "lance: <verb>: %w" wrap around fn's error.
func datasetOp[T any](ctx context.Context, d *Dataset, name, verb string,
	fn func(ctx context.Context, ptr *C.LanceDataset) (T, error),
	attrs ...attribute.KeyValue) (res T, err error) {
	ctx, end := d.obs().start(ctx, name, attrs...)
	defer func() { end(&err) }()
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return res, err
	}
	out, err := fn(ctx, ptr)
	if err != nil {
		return res, fmt.Errorf("lance: %s: %w", verb, err)
	}
	return out, nil
}

// datasetDo is datasetOp for operations with no result.
func datasetDo(ctx context.Context, d *Dataset, name, verb string,
	fn func(ctx context.Context, ptr *C.LanceDataset) error,
	attrs ...attribute.KeyValue) error {
	_, err := datasetOp(ctx, d, name, verb, func(ctx context.Context, ptr *C.LanceDataset) (struct{}, error) {
		return struct{}{}, fn(ctx, ptr)
	}, attrs...)
	return err
}

// fragmentOp mirrors datasetOp over *Fragment. Fragment uses a plain
// sync.Mutex (methods serialize), so the helper holds f.mu.Lock across fn,
// exactly like the hand-rolled fragment methods.
func fragmentOp[T any](ctx context.Context, f *Fragment, name, verb string,
	fn func(ctx context.Context, ptr *C.LanceFragment) (T, error),
	attrs ...attribute.KeyValue) (res T, err error) {
	ctx, end := f.obs().start(ctx, name, attrs...)
	defer func() { end(&err) }()
	f.mu.Lock()
	defer f.mu.Unlock()
	ptr, err := f.checkOpen(ctx)
	if err != nil {
		return res, err
	}
	out, err := fn(ctx, ptr)
	if err != nil {
		return res, fmt.Errorf("lance: %s: %w", verb, err)
	}
	return out, nil
}
