package lance

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// Allocator returns a C-backed Arrow allocator suitable for every record or
// array that may cross into Lance through cgo. Prefer this helper to Arrow's
// memory.DefaultAllocator for Write, Merge, Append, UDF output, and query
// vectors; Go-heap-backed Arrow buffers cannot safely cross the C Data
// Interface under cgocheck2.
func Allocator() memory.Allocator {
	return mallocator.NewMallocator()
}
