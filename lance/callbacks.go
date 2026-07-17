// Go side of the generic callback/plugin bridge (see rust/src/callbacks.rs).
//
// The native library invokes Go plugins through a process-wide vtable of
// cgo-exported functions (registered once in callbacks_init.go). Plugins are
// identified by opaque uintptr handles issued by a locked map registry.
//
// Design notes:
//
//   - Registry: a mutex-guarded map[uintptr]plugin with a monotonically
//     increasing key, NOT runtime/cgo.Handle. cgo.Handle.Value panics on
//     invalid handles, but the native side may race a release and call with
//     a stale handle. A map lookup lets that fail with a clean error code
//     instead of a crash. Handles are never reused within a process.
//   - Output payload ownership: Go allocates result buffers with C.malloc
//     (via C.CBytes). Rust copies and frees them with libc::free. Inputs are
//     borrowed for the duration of the call and copied by Go.
//   - Panic containment: a Go panic must NEVER unwind through an //export
//     shim into Rust. Every shim
//     recovers and converts the panic into error return code
//     LANCE_GO_CALL_ERROR with the panic message as the payload.

package lance

/*
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// plugin is the package-internal contract implemented by Go objects that the
// native side invokes through the callback vtable. Later waves layer typed
// plugin interfaces (cache backends, write-progress callbacks, UDFs, ...)
// over this generic byte-payload dispatch. The per-plugin method enums and
// payload encodings are defined there, not here.
//
// Invoke receives a plugin-specific method discriminator and an opaque
// payload (nil for empty), and returns the response payload (nil for empty).
// Returning an error that is (or wraps) errPluginMiss reports a "miss" to
// the native side (Ok(None) in Rust). Any other error crosses the boundary
// as its message. Invoke must be safe for concurrent use: the native side
// may call it from many threads at once.
type plugin interface {
	Invoke(method int32, payload []byte) ([]byte, error)
}

// errPluginMiss is the sentinel a plugin returns from Invoke to signal
// "not found" / cache miss (native return code LANCE_GO_CALL_MISS) rather
// than a failure.
var errPluginMiss = errors.New("lance: plugin miss")

// pluginRegistry maps handles to live plugins. Handles start at 1 (0 is
// never issued, so a zero value is always invalid).
var pluginRegistry = struct {
	sync.RWMutex
	m    map[uintptr]plugin
	next uintptr
}{m: make(map[uintptr]plugin)}

// registerPlugin adds p to the registry and returns its handle for the
// native side. The caller must either release the handle after its operation
// or explicitly transfer its lifetime to a native OwnedGoPlugin lease.
func registerPlugin(p plugin) uintptr {
	if p == nil {
		panic("lance: registerPlugin called with a nil plugin")
	}
	pluginRegistry.Lock()
	defer pluginRegistry.Unlock()
	pluginRegistry.next++
	h := pluginRegistry.next
	pluginRegistry.m[h] = p
	return h
}

// releasePlugin removes the handle from the registry. Idempotent: releasing
// a stale or never-issued handle is a no-op. Native calls made with the
// handle afterwards fail with a clean "no plugin registered" error.
func releasePlugin(h uintptr) {
	pluginRegistry.Lock()
	defer pluginRegistry.Unlock()
	delete(pluginRegistry.m, h)
}

// lookupPlugin resolves a handle, tolerating stale ones.
func lookupPlugin(h uintptr) (plugin, bool) {
	pluginRegistry.RLock()
	defer pluginRegistry.RUnlock()
	p, ok := pluginRegistry.m[h]
	return p, ok
}

// setCallbackOut stores a C.malloc-allocated copy of msg in the out params
// (the native side frees it with libc::free). Nil out params or an empty
// message are tolerated (the native side then reports a generic error).
func setCallbackOut(outPtr **C.uint8_t, outLen *C.size_t, msg []byte) {
	if outPtr == nil || outLen == nil || len(msg) == 0 {
		return
	}
	*outPtr = (*C.uint8_t)(C.CBytes(msg)) // malloc + copy
	*outLen = C.size_t(len(msg))
}

// go_callback_invoke is the universal vtable dispatch slot: it routes a
// native call to the registered plugin's Invoke. See the file header for
// the payload-ownership and return-code conventions.
//
//export go_callback_invoke
func go_callback_invoke(handle C.size_t, method C.int32_t, inPtr *C.uint8_t, inLen C.size_t, outPtr **C.uint8_t, outLen *C.size_t) (rc C.int32_t) {
	// A Go panic must never unwind into Rust. Convert it into a plain error
	// return instead.
	defer func() {
		if r := recover(); r != nil {
			setCallbackOut(outPtr, outLen, fmt.Appendf(nil, "lance-go: plugin panic: %v", r))
			rc = C.LANCE_GO_CALL_ERROR
		}
	}()
	if outPtr == nil || outLen == nil {
		return C.LANCE_GO_CALL_ERROR
	}
	*outPtr = nil
	*outLen = 0
	p, ok := lookupPlugin(uintptr(handle))
	if !ok {
		setCallbackOut(outPtr, outLen, fmt.Appendf(nil, "lance-go: no plugin registered for handle %d", uintptr(handle)))
		return C.LANCE_GO_CALL_ERROR
	}
	var payload []byte
	if inLen > 0 {
		var err error
		payload, err = copyCBytes(unsafe.Pointer(inPtr), uint64(inLen))
		if err != nil {
			setCallbackOut(outPtr, outLen, []byte(err.Error()))
			return C.LANCE_GO_CALL_ERROR
		}
	}
	out, err := p.Invoke(int32(method), payload)
	if err != nil {
		if errors.Is(err, errPluginMiss) {
			return C.LANCE_GO_CALL_MISS
		}
		setCallbackOut(outPtr, outLen, []byte(err.Error()))
		return C.LANCE_GO_CALL_ERROR
	}
	setCallbackOut(outPtr, outLen, out)
	return C.LANCE_GO_CALL_OK
}

// go_handle_release is the vtable release slot: it drops the Go-side
// registration for a handle. Idempotent, panic-safe.
//
//export go_handle_release
func go_handle_release(handle C.size_t) {
	defer func() { _ = recover() }()
	releasePlugin(uintptr(handle))
}

// ScanStats reports summary I/O statistics for one scan execution, delivered
// to the callback registered with Scanner.WithScanStats. It mirrors the
// public fields of lance's ExecutionSummaryCounts (rust/src/scanner.rs).
// AllCounts / AllTimes are additional debugging metrics whose keys are
// subject to change upstream; treat them as opaque.
type ScanStats struct {
	// IOPS is the number of I/O operations performed.
	IOPS uint64 `json:"iops"`
	// Requests is the number of requests made to the storage layer (may
	// differ from IOPS depending on coalescing configuration).
	Requests uint64 `json:"requests"`
	// BytesRead is the number of bytes read during plan execution.
	BytesRead uint64 `json:"bytes_read"`
	// IndicesLoaded is the number of top-level indices loaded.
	IndicesLoaded uint64 `json:"indices_loaded"`
	// PartsLoaded is the number of index partitions loaded.
	PartsLoaded uint64 `json:"parts_loaded"`
	// IndexComparisons is the number of index comparisons performed (the
	// exact meaning depends on the index type).
	IndexComparisons uint64 `json:"index_comparisons"`
	// AllCounts holds additional, unstable count metrics for debugging.
	AllCounts map[string]uint64 `json:"all_counts"`
	// AllTimes holds additional, unstable time metrics (nanoseconds) for
	// debugging.
	AllTimes map[string]uint64 `json:"all_times"`
}

// scanStatsReport is the method discriminator for the scan-stats plugin.
// Must match SCAN_STATS_REPORT in rust/src/scanner.rs.
const scanStatsReport int32 = 0

// scanStatsAdapter bridges the native scan-stats-report callback to a Go
// func. The payload is the JSON encoding of ScanStats. Registered by
// Scanner.newNative (scanner.go); the native scanner takes ownership of the
// registration on success (an Arc-backed OwnedGoPlugin releases it when the
// scanner, and any streams cloned from it, drop), mirroring the
// write-progress and cache-backend plugins (see hooks.go, cache.go).
type scanStatsAdapter struct {
	fn func(ScanStats)
}

// Invoke implements plugin by decoding the JSON-encoded ScanStats payload and
// forwarding it to the registered callback.
func (a *scanStatsAdapter) Invoke(method int32, payload []byte) ([]byte, error) {
	if method != scanStatsReport {
		return nil, fmt.Errorf("lance: unknown scan stats method %d", method)
	}
	var stats ScanStats
	if err := json.Unmarshal(payload, &stats); err != nil {
		return nil, fmt.Errorf("lance: decode scan stats: %w", err)
	}
	a.fn(stats)
	return nil, nil
}

// testCallbackRoundtrip drives the full Go → Rust → Go loop through the
// test-only lance_test_callback_roundtrip export (or its _async variant,
// which goes through block_on(spawn_blocking) on the shared tokio runtime).
// A plugin miss surfaces as ErrNotFound, and a plugin error as ErrInternal with
// the plugin's message. Test support for callbacks_test.go, which cannot use
// cgo directly (cgo is not permitted in _test.go files).
func testCallbackRoundtrip(handle uintptr, method int32, payload []byte, async bool) ([]byte, error) {
	var inPtr *C.uint8_t
	if len(payload) > 0 {
		inPtr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}
	var outPtr *C.uint8_t
	var outLen C.size_t
	err := ffiCall(context.Background(), func() C.int32_t {
		if async {
			return C.lance_test_callback_roundtrip_async(C.size_t(handle), C.int32_t(method), inPtr, C.size_t(len(payload)), &outPtr, &outLen)
		}
		return C.lance_test_callback_roundtrip(C.size_t(handle), C.int32_t(method), inPtr, C.size_t(len(payload)), &outPtr, &outLen)
	})
	if outPtr != nil {
		defer C.free(unsafe.Pointer(outPtr))
	}
	if err != nil {
		return nil, err
	}
	out, err := copyCBytes(unsafe.Pointer(outPtr), uint64(outLen))
	if err != nil {
		return nil, fmt.Errorf("lance: copy callback output: %w", err)
	}
	return out, nil
}

// testCallbackReleaseFromNative releases a plugin handle from the native
// side (Rust GoPlugin::release → vtable → go_handle_release). Test support
// for callbacks_test.go.
func testCallbackReleaseFromNative(handle uintptr) {
	C.lance_test_callback_release(C.size_t(handle))
}
