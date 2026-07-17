// Vtable registration for the callback bridge. This lives in its own file
// because cgo forbids C definitions (the static helper below) in the
// preamble of any file containing //export directives (callbacks.go).

package lance

/*
#include <stdint.h>
#include <stddef.h>
#include "lance_go.h"

// Prototypes for the cgo-exported Go callbacks defined in callbacks.go. cgo
// generates the definitions. These signatures must stay in sync with the
// //export declarations there and with the LanceGoVTable slots in
// include/lance_go.h.
extern int32_t go_callback_invoke(size_t handle, int32_t method, uint8_t *in_ptr, size_t in_len, uint8_t **out_ptr, size_t *out_len);
extern void go_handle_release(size_t handle);

// Builds the vtable pointing at the Go exports and registers it with the
// native library. Written in C because Go code cannot take the address of
// an exported Go function. The C-linkage trampolines cgo generates for
// //export functions are valid C function pointers for the process
// lifetime.
static int32_t lance_go_register_callback_vtable(void) {
	LanceGoVTable vt;
	vt.abi_version = LANCE_GO_VTABLE_ABI_VERSION;
	vt.invoke = go_callback_invoke;
	vt.release = go_handle_release;
	return lance_register_go_vtable(&vt);
}
*/
import "C"

import (
	"context"
	"fmt"
)

// init verifies the complete native ABI and registers the Go callback vtable
// exactly once per process. A mismatch is an unrecoverable build/packaging
// error, so initialization panics with both versions.
func init() {
	if got, want := uint32(C.lance_abi_version()), uint32(C.LANCE_GO_ABI_VERSION); got != want {
		panic(fmt.Sprintf("lance: native ABI version mismatch: linked library has %d, Go bindings require %d", got, want))
	}
	err := ffiCall(context.Background(), func() C.int32_t { return C.lance_go_register_callback_vtable() })
	if err != nil {
		panic(fmt.Sprintf("lance: registering the Go callback vtable failed: %v", err))
	}
}
