package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync"
	"unsafe"
)

// BlobList is a handle to a set of blob files returned by Dataset.TakeBlobs
// (and its ByIndices/ByAddresses variants). It owns the blob files, and every
// BlobFile obtained from Get is only valid until the BlobList is closed.
//
// A BlobList is self-contained: its blob reads stay valid after the
// originating Dataset is closed. Close must be called to release native
// resources. It is idempotent and safe for concurrent use.
type BlobList struct {
	mu  sync.RWMutex
	ptr *C.LanceBlobList
	// withObs is inherited from the originating Dataset (nil-safe). BlobFiles
	// obtained via Get reach it through their parent list.
	withObs *obs
	// cleanup is a leak safety net: if the caller drops every reference to
	// this BlobList without calling Close, the runtime eventually releases
	// the native handle (and every blob file it owns) anyway. Close stops it
	// so the native pointer is never closed twice.
	cleanup runtime.Cleanup
}

// obs returns the instrumentation handle for this blob list (nil-safe).
func (l *BlobList) obs() *obs { return l.withObs }

// newBlobList wraps a native blob-list pointer, attaching the observability
// handle o. All &BlobList construction in this package goes through here so
// every handle gets the GC leak safety net. BlobFile, which is borrowed from
// its BlobList, gets no cleanup of its own.
func newBlobList(ptr *C.LanceBlobList, o *obs) *BlobList {
	l := &BlobList{ptr: ptr, withObs: o}
	// The cleanup func must capture nothing but its argument: closing over l
	// or ptr would keep the BlobList reachable and defeat collection.
	l.cleanup = runtime.AddCleanup(l, func(p *C.LanceBlobList) { C.lance_blob_list_close(p) }, ptr)
	return l
}

// BlobFile is a handle to one blob's bytes, obtained from BlobList.Get. It is
// borrowed from its BlobList and becomes invalid once the list is closed.
type BlobFile struct {
	list *BlobList
	ptr  *C.LanceBlobFile
}

// blobTakeSpecJSON mirrors the spec_json contract of lance_dataset_take_blobs.
// Exactly one selector is set to a non-nil (possibly empty) slice, and the other
// two stay nil and serialize as JSON null (which the native side reads as
// "absent"). An empty active selector yields an empty blob list.
type blobTakeSpecJSON struct {
	Column    string   `json:"column"`
	RowIDs    []uint64 `json:"row_ids"`
	Indices   []uint64 `json:"indices"`
	Addresses []uint64 `json:"addresses"`
}

// takeBlobs factors TakeBlobs / TakeBlobsByIndices / TakeBlobsByAddresses over
// datasetOp. name is the span name of the calling exported method.
func (d *Dataset) takeBlobs(ctx context.Context, name string, spec *blobTakeSpecJSON) (*BlobList, error) {
	return datasetOp(ctx, d, name, "take blobs",
		func(ctx context.Context, ptr *C.LanceDataset) (*BlobList, error) {
			// marshalOptions (dataset.go) returns an already-prefixed
			// "lance: marshal options: %w" error, which would double-prefix
			// if returned from inside this fn, so marshal inline instead.
			data, err := json.Marshal(spec)
			if err != nil {
				return nil, fmt.Errorf("marshal options: %w", err)
			}
			cSpec, freeSpec := cString(string(data))
			defer freeSpec()

			var listPtr *C.LanceBlobList
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_take_blobs(ptr, cSpec, &listPtr)
			}); err != nil {
				return nil, err
			}
			return newBlobList(listPtr, d.obs()), nil
		})
}

// nonNil returns s, or an empty (non-nil) slice when s is nil, so the chosen
// selector is always present in the marshaled spec.
func nonNil(s []uint64) []uint64 {
	if s == nil {
		return []uint64{}
	}
	return s
}

// TakeBlobs returns blob-file handles for the given stable row ids of a blob
// column. The caller must Close the returned BlobList.
func (d *Dataset) TakeBlobs(ctx context.Context, column string, rowIDs []uint64) (*BlobList, error) {
	return d.takeBlobs(ctx, "Dataset.TakeBlobs", &blobTakeSpecJSON{Column: column, RowIDs: nonNil(rowIDs)})
}

// TakeBlobsByIndices returns blob-file handles for the given row offsets
// (0 = first live row) of a blob column. The caller must Close the returned
// BlobList.
func (d *Dataset) TakeBlobsByIndices(ctx context.Context, column string, indices []uint64) (*BlobList, error) {
	return d.takeBlobs(ctx, "Dataset.TakeBlobsByIndices", &blobTakeSpecJSON{Column: column, Indices: nonNil(indices)})
}

// TakeBlobsByAddresses returns blob-file handles for the given row addresses
// (fragment_id << 32 | row_offset) of a blob column. The caller must Close the
// returned BlobList.
func (d *Dataset) TakeBlobsByAddresses(ctx context.Context, column string, addresses []uint64) (*BlobList, error) {
	return d.takeBlobs(ctx, "Dataset.TakeBlobsByAddresses", &blobTakeSpecJSON{Column: column, Addresses: nonNil(addresses)})
}

// Len returns the number of blob files in the list.
func (l *BlobList) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.ptr == nil {
		return 0
	}
	return int(C.lance_blob_list_len(l.ptr))
}

// obs returns the instrumentation handle for this blob file, inherited from
// its list (nil-safe).
func (bf *BlobFile) obs() *obs { return bf.list.obs() }

// Get returns a handle to the i-th blob file. It returns nil if the list is
// closed or i is out of range. The returned BlobFile is valid until the
// BlobList is closed.
func (l *BlobList) Get(i int) *BlobFile {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.ptr == nil || i < 0 {
		return nil
	}
	bf := C.lance_blob_list_get(l.ptr, C.size_t(i))
	if bf == nil {
		return nil
	}
	return &BlobFile{list: l, ptr: bf}
}

// Close releases the blob list and every blob file it owns. It is idempotent.
// BlobFiles obtained from Get must not be used after Close. Close waits for any
// in-flight blob reads to finish (it takes the write lock).
func (l *BlobList) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ptr != nil {
		C.lance_blob_list_close(l.ptr)
		l.cleanup.Stop()
		l.ptr = nil
	}
	return nil
}

// withLock runs fn while holding the list read lock, so the borrowed blob
// handle cannot be freed by a concurrent BlobList.Close for the span of the
// FFI call. It errors if the list is closed or ctx is done.
func (b *BlobFile) withLock(ctx context.Context, fn func(ptr *C.LanceBlobFile) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.list.mu.RLock()
	defer b.list.mu.RUnlock()
	if b.list.ptr == nil {
		return fmt.Errorf("lance: blob list is closed: %w", ErrInvalidArgument)
	}
	return fn(b.ptr)
}

// readBytes runs a blob-read FFI call and copies the returned buffer into a Go
// slice, freeing the native buffer.
func readBytes(ctx context.Context, what string, call func(outBuf **C.uint8_t, outLen *C.size_t) C.int32_t) ([]byte, error) {
	var buf *C.uint8_t
	var length C.size_t
	callErr := ffiCall(ctx, func() C.int32_t {
		return call(&buf, &length)
	})
	if buf != nil {
		defer C.lance_bytes_free(buf, length)
	}
	if callErr != nil {
		return nil, fmt.Errorf("lance: %s: %w", what, callErr)
	}
	if length == 0 {
		return []byte{}, nil
	}
	data, err := copyCBytes(unsafe.Pointer(buf), uint64(length))
	if err != nil {
		return nil, fmt.Errorf("lance: %s: %w", what, err)
	}
	return data, nil
}

// Read reads the blob from its current cursor to the end and returns the
// bytes, advancing the cursor. On a freshly taken blob (cursor 0) this returns
// the whole blob. After Seek(off) it returns the bytes from off onward.
func (b *BlobFile) Read(ctx context.Context) (res []byte, err error) {
	ctx, end := b.obs().start(ctx, "BlobFile.Read")
	defer func() { end(&err) }()
	var out []byte
	err = b.withLock(ctx, func(ptr *C.LanceBlobFile) error {
		data, err := readBytes(ctx, "blob read", func(outBuf **C.uint8_t, outLen *C.size_t) C.int32_t {
			return C.lance_blob_read(ptr, outBuf, outLen)
		})
		out = data
		return err
	})
	return out, err
}

// ReadAll reads from the blob's current cursor to the end. It is the
// io.ReadAll-style name for Read; Read remains available for compatibility.
func (b *BlobFile) ReadAll(ctx context.Context) ([]byte, error) {
	return b.Read(ctx)
}

// Reader returns a context-bound, independently positioned reader for this
// blob. The reader implements io.Reader, io.ReaderAt, and io.Seeker and fetches
// only the requested ranges, avoiding whole-blob materialization. It remains
// borrowed from the BlobList and becomes invalid when the list is closed.
func (b *BlobFile) Reader(ctx context.Context) *BlobReader {
	if ctx == nil {
		ctx = context.Background()
	}
	return &BlobReader{blob: b, ctx: ctx}
}

// BlobReader is a chunked reader over a BlobFile. Its sequential cursor is
// independent from BlobFile.Seek/Tell and from other BlobReaders.
type BlobReader struct {
	blob *BlobFile
	ctx  context.Context
	mu   sync.Mutex
	off  int64
}

// Read implements io.Reader.
func (r *BlobReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := r.readAt(p, r.off)
	r.off += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt without changing the sequential cursor.
func (r *BlobReader) ReadAt(p []byte, off int64) (int, error) {
	return r.readAt(p, off)
}

func (r *BlobReader) readAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("lance: blob read offset %d is negative: %w", off, ErrInvalidArgument)
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	size, err := r.blob.size(r.ctx)
	if err != nil {
		return 0, err
	}
	if uint64(off) >= size {
		return 0, io.EOF
	}
	want := uint64(len(p))
	if remaining := size - uint64(off); want > remaining {
		want = remaining
	}
	data, err := r.blob.ReadRange(r.ctx, uint64(off), want)
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Seek implements io.Seeker for the sequential cursor.
func (r *BlobReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.off
	case io.SeekEnd:
		size, err := r.blob.size(r.ctx)
		if err != nil {
			return 0, err
		}
		if size > math.MaxInt64 {
			return 0, fmt.Errorf("lance: blob is too large for io.Seeker: %w", ErrInvalidArgument)
		}
		base = int64(size)
	default:
		return 0, fmt.Errorf("lance: invalid blob seek whence %d: %w", whence, ErrInvalidArgument)
	}
	if (offset > 0 && base > math.MaxInt64-offset) || (offset < 0 && base < math.MinInt64-offset) {
		return 0, fmt.Errorf("lance: blob seek offset overflows int64: %w", ErrInvalidArgument)
	}
	next := base + offset
	if next < 0 {
		return 0, fmt.Errorf("lance: negative blob seek position %d: %w", next, ErrInvalidArgument)
	}
	r.off = next
	return next, nil
}

// ReadRange reads length bytes starting at blob-local offset start (random
// access, it does not move the cursor).
func (b *BlobFile) ReadRange(ctx context.Context, start, length uint64) (res []byte, err error) {
	ctx, end := b.obs().start(ctx, "BlobFile.ReadRange")
	defer func() { end(&err) }()
	var out []byte
	err = b.withLock(ctx, func(ptr *C.LanceBlobFile) error {
		data, err := readBytes(ctx, "blob read range", func(outBuf **C.uint8_t, outLen *C.size_t) C.int32_t {
			return C.lance_blob_read_range(ptr, C.uint64_t(start), C.uint64_t(length), outBuf, outLen)
		})
		out = data
		return err
	})
	return out, err
}

// Size returns the blob's total size in bytes. It returns 0 if the list is
// closed.
func (b *BlobFile) Size() uint64 {
	b.list.mu.RLock()
	defer b.list.mu.RUnlock()
	if b.list.ptr == nil {
		return 0
	}
	return uint64(C.lance_blob_size(b.ptr))
}

func (b *BlobFile) size(ctx context.Context) (uint64, error) {
	var size uint64
	err := b.withLock(ctx, func(ptr *C.LanceBlobFile) error {
		size = uint64(C.lance_blob_size(ptr))
		return nil
	})
	return size, err
}

// Seek moves the blob's read cursor to off.
func (b *BlobFile) Seek(ctx context.Context, off uint64) (err error) {
	ctx, end := b.obs().start(ctx, "BlobFile.Seek")
	defer func() { end(&err) }()
	return b.withLock(ctx, func(ptr *C.LanceBlobFile) error {
		if err := ffiCall(ctx, func() C.int32_t {
			return C.lance_blob_seek(ptr, C.uint64_t(off))
		}); err != nil {
			return fmt.Errorf("lance: blob seek: %w", err)
		}
		return nil
	})
}

// Tell returns the blob's current cursor position.
func (b *BlobFile) Tell(ctx context.Context) (n uint64, err error) {
	ctx, end := b.obs().start(ctx, "BlobFile.Tell")
	defer func() { end(&err) }()
	var pos uint64
	err = b.withLock(ctx, func(ptr *C.LanceBlobFile) error {
		var cPos C.uint64_t
		if err := ffiCall(ctx, func() C.int32_t {
			return C.lance_blob_tell(ptr, &cPos)
		}); err != nil {
			return fmt.Errorf("lance: blob tell: %w", err)
		}
		pos = uint64(cPos)
		return nil
	})
	return pos, err
}

// URI returns the blob's URI, or an empty string when it has none.
func (b *BlobFile) URI(ctx context.Context) (res string, err error) {
	ctx, end := b.obs().start(ctx, "BlobFile.URI")
	defer func() { end(&err) }()
	var uri string
	err = b.withLock(ctx, func(ptr *C.LanceBlobFile) error {
		var cURI *C.char
		if err := ffiCall(ctx, func() C.int32_t {
			return C.lance_blob_uri(ptr, &cURI)
		}); err != nil {
			return fmt.Errorf("lance: blob uri: %w", err)
		}
		if cURI == nil {
			return nil
		}
		defer C.lance_string_free(cURI)
		uri = C.GoString(cURI)
		return nil
	})
	return uri, err
}

// Close closes the underlying blob file early, releasing its I/O resources.
// The handle memory is still owned by the BlobList, and further reads fail
// after Close.
func (b *BlobFile) Close() error {
	return b.withLock(context.Background(), func(ptr *C.LanceBlobFile) error {
		if err := ffiCall(context.Background(), func() C.int32_t {
			return C.lance_blob_close(ptr)
		}); err != nil {
			return fmt.Errorf("lance: blob close: %w", err)
		}
		return nil
	})
}
