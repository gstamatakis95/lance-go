// Package lance provides Go bindings for the Lance columnar format,
// implemented as cgo bindings over the lance-go-ffi Rust static library
// (see rust/ and include/lance_go.h in this repository).
//
// Build the native library first with `make rust` from the repository root.
//
// Handles (Dataset, Session, ...) wrap native resources and must be closed
// with Close; each constructor also registers a runtime.AddCleanup leak
// safety net, but that is only a backstop, not a substitute for calling
// Close. Every method that takes a context.Context cancels its in-flight
// native call when the context is done, surfacing context.Canceled or
// context.DeadlineExceeded. Arrow data passed into this package (Write,
// MergeInsert, query vectors, ...) crosses the Arrow C Data Interface, so
// its buffers must live outside the Go heap: build it with Allocator, never
// memory.DefaultAllocator, or it crashes under GOEXPERIMENT=cgocheck2.
// Errors wrap exactly one sentinel from errors.go (ErrNotFound,
// ErrInvalidArgument, ...) so callers can classify failures with errors.Is.
// See docs/ in this repository for narrative guides.
package lance

/*
#cgo CFLAGS: -I${SRCDIR}/../include
#cgo LDFLAGS: -L${SRCDIR}/../rust/target/release -llance_go
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -framework SystemConfiguration -framework IOKit
#cgo linux LDFLAGS: -lm -ldl -lpthread
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

// Version reports the version of the underlying lance-go-ffi native library
// and the Lance crate it was built against, e.g.
// "lance-go-ffi <version> (lance 8.0.0)".
func Version() string {
	cs := C.lance_version()
	defer C.lance_string_free(cs)
	return C.GoString(cs)
}
