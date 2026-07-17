package lance

import (
	"fmt"
	"unsafe"
)

// copyCBytes copies n bytes from native memory without narrowing the length to
// C.int (the limit imposed by C.GoBytes). The caller remains responsible for
// freeing ptr according to the native API's ownership contract.
func copyCBytes(ptr unsafe.Pointer, n uint64) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	maxInt := uint64(^uint(0) >> 1)
	if n > maxInt {
		return nil, fmt.Errorf("lance: native byte buffer length %d exceeds the Go address space: %w", n, ErrInternal)
	}
	if ptr == nil {
		return nil, fmt.Errorf("lance: native byte buffer is NULL with length %d: %w", n, ErrInternal)
	}
	src := unsafe.Slice((*byte)(ptr), int(n))
	return append([]byte(nil), src...), nil
}
