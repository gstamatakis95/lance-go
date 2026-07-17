package lance

/*
#include <stdlib.h>
#include <string.h>
#include "lance_go.h"

static struct ArrowArrayStream* lance_go_stream_move(struct ArrowArrayStream* src) {
	if (src == NULL) {
		return NULL;
	}
	struct ArrowArrayStream* dst = (struct ArrowArrayStream*)calloc(1, sizeof(struct ArrowArrayStream));
	if (dst == NULL) {
		if (src->release != NULL) {
			src->release(src);
		}
		memset(src, 0, sizeof(struct ArrowArrayStream));
		return NULL;
	}
	memcpy(dst, src, sizeof(struct ArrowArrayStream));
	memset(src, 0, sizeof(struct ArrowArrayStream));
	return dst;
}

static int lance_go_stream_get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
	return stream->get_schema(stream, out);
}

static int lance_go_stream_get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
	return stream->get_next(stream, out);
}

static const char* lance_go_stream_get_last_error(struct ArrowArrayStream* stream) {
	return stream->get_last_error == NULL ? NULL : stream->get_last_error(stream);
}

static void lance_go_stream_release(struct ArrowArrayStream* stream) {
	if (stream == NULL) {
		return;
	}
	if (stream->release != NULL) {
		stream->release(stream);
		stream->release = NULL;
	}
	free(stream);
}

static int lance_go_array_is_released(struct ArrowArray* array) {
	return array->release == NULL;
}

static void lance_go_array_release(struct ArrowArray* array) {
	if (array->release != NULL) {
		array->release(array);
		array->release = NULL;
	}
}

static void lance_go_schema_release(struct ArrowSchema* schema) {
	if (schema->release != NULL) {
		schema->release(schema);
		schema->release = NULL;
	}
}
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"go.opentelemetry.io/otel/attribute"
)

// streamReaderConfig controls behavior that spans lazy record iteration. The
// zero value still provides deterministic ArrowArrayStream ownership.
type streamReaderConfig struct {
	ctx    context.Context
	finish func(error, int64)
}

func observedStream(ctx context.Context, o *obs, op string) streamReaderConfig {
	streamCtx, end := o.start(ctx, op)
	return streamReaderConfig{
		ctx: streamCtx,
		finish: func(err error, rows int64) {
			end(&err, attribute.Int64("lance.rows_read", rows))
		},
	}
}

func (cfg streamReaderConfig) endOnError(errp *error) {
	if errp != nil && *errp != nil && cfg.finish != nil {
		cfg.finish(*errp, 0)
	}
}

// ownedRecordReader consumes an Arrow C Stream directly instead of delegating
// to cdata.ImportCRecordReader. Arrow-Go's imported reader currently has a
// no-op Release method and relies on a GC finalizer, which cannot account for
// native memory pressure. Keeping the stream here makes Release deterministic.
type ownedRecordReader struct {
	refs atomic.Int64

	mu       sync.Mutex
	stream   *C.struct_ArrowArrayStream
	schema   *arrow.Schema
	current  arrow.RecordBatch
	err      error
	released bool

	ctx      context.Context
	finish   func(error, int64)
	finished bool
	rows     int64
	what     string
}

func newOwnedRecordReader(src unsafe.Pointer, what string, cfg streamReaderConfig) (*ownedRecordReader, error) {
	stream := C.lance_go_stream_move((*C.struct_ArrowArrayStream)(src))
	if stream == nil {
		return nil, fmt.Errorf("import %s stream: allocate stream: %w", what, ErrInternal)
	}
	cleanup := true
	defer func() {
		if cleanup {
			C.lance_go_stream_release(stream)
		}
	}()

	var cSchema C.struct_ArrowSchema
	if errno := C.lance_go_stream_get_schema(stream, &cSchema); errno != 0 {
		C.lance_go_schema_release(&cSchema)
		return nil, streamCallErrorRaw(stream, what, "schema", errno)
	}
	schema, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&cSchema)))
	if err != nil {
		return nil, fmt.Errorf("import %s stream schema: %v: %w", what, err, ErrInternal)
	}

	ctx := cfg.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r := &ownedRecordReader{stream: stream, schema: schema, ctx: ctx, what: what}
	r.refs.Store(1)
	r.finish = cfg.finish
	runtime.SetFinalizer(r, func(orphan *ownedRecordReader) { orphan.forceRelease() })
	cleanup = false
	return r, nil
}

func streamCallError(stream *C.struct_ArrowArrayStream, what, action string, errno C.int) error {
	return fmt.Errorf("lance: %w", streamCallErrorRaw(stream, what, action, errno))
}

// streamCallErrorRaw is streamCallError without the package prefix, for
// construction-time errors that flow through a wrap layer (importRecordReader
// or a datasetOp fn closure) which applies the single "lance:" prefix.
func streamCallErrorRaw(stream *C.struct_ArrowArrayStream, what, action string, errno C.int) error {
	message := "unknown Arrow C stream error"
	if ptr := C.lance_go_stream_get_last_error(stream); ptr != nil {
		message = C.GoString(ptr)
	}
	return fmt.Errorf("%s stream %s failed (errno %d): %s: %w", what, action, int(errno), message, ErrInternal)
}

// Retain increases the reader reference count. It may be called concurrently.
func (r *ownedRecordReader) Retain() {
	for {
		refs := r.refs.Load()
		if refs <= 0 {
			panic("lance: retain released record reader")
		}
		if r.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

// Release deterministically releases the current record and native stream
// when the final reader reference is dropped. It may be called concurrently.
func (r *ownedRecordReader) Release() {
	refs := r.refs.Add(-1)
	if refs < 0 {
		panic("lance: too many record reader releases")
	}
	if refs == 0 {
		r.forceRelease()
	}
}

func (r *ownedRecordReader) forceRelease() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	runtime.SetFinalizer(r, nil)
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.closeStreamLocked()
	r.finishLocked()
}

func (r *ownedRecordReader) closeStreamLocked() {
	if r.stream != nil {
		C.lance_go_stream_release(r.stream)
		r.stream = nil
	}
}

func (r *ownedRecordReader) finishLocked() {
	if r.finished {
		return
	}
	r.finished = true
	if r.finish != nil {
		r.finish(r.err, r.rows)
	}
}

// Schema implements array.RecordReader.
func (r *ownedRecordReader) Schema() *arrow.Schema { return r.schema }

// RecordBatch implements array.RecordReader, returning the batch most
// recently produced by Next.
func (r *ownedRecordReader) RecordBatch() arrow.RecordBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
}

// Record is retained for compatibility with Arrow's deprecated alias.
func (r *ownedRecordReader) Record() arrow.RecordBatch { return r.RecordBatch() }

// Err implements array.RecordReader, returning the error that stopped
// iteration, if any.
func (r *ownedRecordReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// Next implements array.RecordReader, advancing to the next record batch.
func (r *ownedRecordReader) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released || r.stream == nil || r.err != nil {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		r.closeStreamLocked()
		r.finishLocked()
		return false
	}

	var cArray C.struct_ArrowArray
	if errno := C.lance_go_stream_get_next(r.stream, &cArray); errno != 0 {
		r.err = streamCallError(r.stream, r.what, "next", errno)
		C.lance_go_array_release(&cArray)
		r.closeStreamLocked()
		r.finishLocked()
		return false
	}
	if C.lance_go_array_is_released(&cArray) != 0 {
		r.closeStreamLocked()
		r.finishLocked()
		return false
	}

	record, err := cdata.ImportCRecordBatchWithSchema(
		(*cdata.CArrowArray)(unsafe.Pointer(&cArray)), r.schema,
	)
	if err != nil {
		C.lance_go_array_release(&cArray)
		r.err = fmt.Errorf("lance: import %s record batch: %v: %w", r.what, err, ErrInternal)
		r.closeStreamLocked()
		r.finishLocked()
		return false
	}
	r.current = record
	r.rows += record.NumRows()
	return true
}

var _ array.RecordReader = (*ownedRecordReader)(nil)
