package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Dataset is a handle to an opened Lance dataset. It is safe for concurrent
// use. Close must be called to release the native resources. It is
// idempotent.
type Dataset struct {
	mu  sync.RWMutex
	ptr *C.LanceDataset
	// withObs carries the resolved OpenTelemetry instrumentation for this
	// handle. It is shared by pointer with child handles (Scanner, Fragment,
	// …) and inherited by Datasets returned from this one (Checkout, branch
	// ops, …). May be nil on a zero-value handle. All obs methods are
	// nil-safe.
	withObs *obs
	// cleanup is a leak safety net: if the caller drops every reference to
	// this Dataset without calling Close, the runtime eventually releases the
	// native handle anyway. It is a zero-value no-op until newDataset sets
	// it, and Close stops it once the handle is explicitly released so the
	// native pointer is never closed twice.
	cleanup runtime.Cleanup
}

// newDataset wraps a native dataset pointer, attaching the observability
// config o (which child/returned handles inherit). All &Dataset construction
// in this package goes through here so obs propagates uniformly and every
// handle gets the GC leak safety net.
func newDataset(ptr *C.LanceDataset, o *obs) *Dataset {
	d := &Dataset{ptr: ptr, withObs: o}
	// The cleanup func must capture nothing but its argument: closing over d
	// or ptr would keep the Dataset reachable and defeat collection.
	d.cleanup = runtime.AddCleanup(d, func(p *C.LanceDataset) { C.lance_dataset_close(p) }, ptr)
	return d
}

// obs returns the instrumentation handle for this dataset (nil-safe).
func (d *Dataset) obs() *obs { return d.withObs }

// VersionInfo describes a committed version of a dataset.
type VersionInfo struct {
	// Version is the version number.
	Version uint64 `json:"version"`
	// Timestamp is the UTC creation time of the version.
	Timestamp time.Time `json:"timestamp"`
	// Metadata holds key-value pairs attached to the version.
	Metadata map[string]string `json:"metadata"`
}

// WriteMode controls how Write behaves when the destination dataset exists.
type WriteMode int

const (
	// WriteModeCreate creates a new dataset and fails if one already exists.
	WriteModeCreate WriteMode = iota
	// WriteModeAppend appends to an existing dataset.
	WriteModeAppend
	// WriteModeOverwrite replaces the contents of an existing dataset as a
	// new version (older versions remain accessible via time travel).
	WriteModeOverwrite
)

// String returns the mode's wire representation ("create", "append", or
// "overwrite").
func (m WriteMode) String() string {
	switch m {
	case WriteModeCreate:
		return "create"
	case WriteModeAppend:
		return "append"
	case WriteModeOverwrite:
		return "overwrite"
	default:
		return fmt.Sprintf("WriteMode(%d)", int(m))
	}
}

// openConfig mirrors the options_json contract of lance_dataset_open.
type openConfig struct {
	Version   *uint64 `json:"version,omitempty"`
	Tag       string  `json:"tag,omitempty"`
	Branch    string  `json:"branch,omitempty"`
	tagSet    bool
	branchSet bool

	storage map[string]string
	// obsCfg holds OpenTelemetry providers, unexported so it is never
	// marshaled into the options JSON sent across the FFI boundary.
	obsCfg obsConfig
	// session, when set, opens the dataset sharing its caches (see
	// WithSession). Unexported so it is never marshaled into the options JSON.
	session *Session
	// objectStoreCache, when set, wraps the opened dataset's object store with
	// a Go byte-range cache (see WithObjectStoreCache). Unexported so it is
	// never marshaled into the options JSON.
	objectStoreCache ObjectStoreCache
}

func (cfg *openConfig) validate() error {
	if cfg.tagSet && cfg.Tag == "" {
		return fmt.Errorf("lance: tag must not be empty: %w", ErrInvalidArgument)
	}
	if cfg.branchSet && cfg.Branch == "" {
		return fmt.Errorf("lance: branch must not be empty: %w", ErrInvalidArgument)
	}
	if cfg.tagSet && (cfg.Version != nil || cfg.branchSet) {
		return fmt.Errorf("lance: WithTag cannot be combined with WithVersion or WithBranch: %w", ErrInvalidArgument)
	}
	return nil
}

// OpenOption configures Open. The concrete options are produced by the With*
// constructors (WithVersion, WithTag, WithSession, WithObjectStoreCache, ...).
type OpenOption func(*openConfig)

// WithStorageOptions passes object-store configuration key/value pairs
// (credentials, endpoints, ...) to the underlying storage layer.
func WithStorageOptions(options map[string]string) OpenOption {
	return func(cfg *openConfig) { cfg.storage = options }
}

// WithVersion opens the dataset at a specific historical version.
func WithVersion(version uint64) OpenOption {
	return func(cfg *openConfig) { cfg.Version = &version }
}

// WithTag opens the dataset at the version referenced by a tag.
func WithTag(tag string) OpenOption {
	return func(cfg *openConfig) {
		cfg.Tag = tag
		cfg.tagSet = true
	}
}

// writeConfig mirrors the options_json contract of lance_dataset_write.
type writeConfig struct {
	Mode               string `json:"mode"`
	MaxRowsPerFile     uint64 `json:"max_rows_per_file,omitempty"`
	MaxRowsPerGroup    uint64 `json:"max_rows_per_group,omitempty"`
	MaxBytesPerFile    uint64 `json:"max_bytes_per_file,omitempty"`
	DataStorageVersion string `json:"data_storage_version,omitempty"`
	EnableStableRowIDs bool   `json:"enable_stable_row_ids,omitempty"`

	extendedWriteConfig

	storage map[string]string
	// obsCfg holds OpenTelemetry providers, unexported so it is never
	// marshaled into the options JSON sent across the FFI boundary.
	obsCfg obsConfig
	// session, when set, writes sharing its caches (see WithSession).
	// Unexported so it is never marshaled into the options JSON.
	session *Session
	// progress, when set, receives cumulative WriteStats after each batch (see
	// WithWriteProgress). Unexported so it is never marshaled into the options
	// JSON.
	progress func(WriteStats)
}

// WriteOption configures Write. The concrete options are produced by the With*
// constructors (WithMode, WithWriteSession, WithWriteProgress, ...).
type WriteOption func(*writeConfig)

// WithMode sets the write mode (default WriteModeCreate).
func WithMode(mode WriteMode) WriteOption {
	return func(cfg *writeConfig) { cfg.Mode = mode.String() }
}

// WithMaxRowsPerFile caps the number of rows written to a single data file.
func WithMaxRowsPerFile(n uint64) WriteOption {
	return func(cfg *writeConfig) { cfg.MaxRowsPerFile = n }
}

// WithMaxRowsPerGroup caps the number of rows in a single row group.
func WithMaxRowsPerGroup(n uint64) WriteOption {
	return func(cfg *writeConfig) { cfg.MaxRowsPerGroup = n }
}

// WithMaxBytesPerFile caps the (approximate) size of a single data file.
func WithMaxBytesPerFile(n uint64) WriteOption {
	return func(cfg *writeConfig) { cfg.MaxBytesPerFile = n }
}

// WithDataStorageVersion selects the Lance file format version to write,
// e.g. "2.0", "2.1", "2.2" or "stable".
func WithDataStorageVersion(version string) WriteOption {
	return func(cfg *writeConfig) { cfg.DataStorageVersion = version }
}

// WithStableRowIDs enables stable row IDs that survive compaction and
// updates.
func WithStableRowIDs(enable bool) WriteOption {
	return func(cfg *writeConfig) { cfg.EnableStableRowIDs = enable }
}

// WithWriteStorageOptions passes object-store configuration key/value pairs
// to the underlying storage layer for the write.
func WithWriteStorageOptions(options map[string]string) WriteOption {
	return func(cfg *writeConfig) { cfg.storage = options }
}

// ObsOption configures the OpenTelemetry providers a handle uses for tracing,
// metrics and logging. Pass ObsOptions to constructors via WithObservability
// (Open), WithWriteObservability (Write), or directly to NewSession. When no
// provider is supplied the corresponding OTel global is used, which is a no-op
// until the application installs an SDK.
type ObsOption func(*obsConfig)

// WithTracerProvider sets the OpenTelemetry TracerProvider (default: the OTel
// global tracer provider).
func WithTracerProvider(tp trace.TracerProvider) ObsOption {
	return func(c *obsConfig) { c.tracerProvider = tp }
}

// WithMeterProvider sets the OpenTelemetry MeterProvider (default: the OTel
// global meter provider).
func WithMeterProvider(mp metric.MeterProvider) ObsOption {
	return func(c *obsConfig) { c.meterProvider = mp }
}

// WithLoggerProvider sets the OpenTelemetry LoggerProvider (default: the OTel
// global logger provider).
func WithLoggerProvider(lp otellog.LoggerProvider) ObsOption {
	return func(c *obsConfig) { c.loggerProvider = lp }
}

// WithObservability attaches OpenTelemetry providers to Open. The opened
// dataset, and every handle derived from it, is instrumented with them.
func WithObservability(opts ...ObsOption) OpenOption {
	return func(cfg *openConfig) {
		for _, o := range opts {
			o(&cfg.obsCfg)
		}
	}
}

// WithWriteObservability attaches OpenTelemetry providers to Write. The
// returned dataset, and every handle derived from it, is instrumented with
// them.
func WithWriteObservability(opts ...ObsOption) WriteOption {
	return func(cfg *writeConfig) {
		for _, o := range opts {
			o(&cfg.obsCfg)
		}
	}
}

// cString converts s to a C string. The caller must free the result.
// An empty string maps to NULL (with a no-op free).
func cString(s string) (*C.char, func()) {
	if s == "" {
		return nil, func() {}
	}
	cs := C.CString(s)
	return cs, func() { C.free(unsafe.Pointer(cs)) }
}

// cStorageKV marshals a map into the NULL-terminated [k1, v1, ..., NULL]
// C-string array expected by the FFI layer. The array and its strings live
// in C memory. The caller must call the returned cleanup function. An empty
// map yields NULL.
func cStorageKV(m map[string]string) (**C.char, func()) {
	if len(m) == 0 {
		return nil, func() {}
	}
	n := 2*len(m) + 1
	arr := (**C.char)(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	slots := unsafe.Slice(arr, n)
	i := 0
	for k, v := range m {
		slots[i] = C.CString(k)
		slots[i+1] = C.CString(v)
		i += 2
	}
	// slots[n-1] is already NULL from calloc.
	return arr, func() {
		for _, p := range slots[:n-1] {
			C.free(unsafe.Pointer(p))
		}
		C.free(unsafe.Pointer(arr))
	}
}

// marshalOptions renders v as a JSON C string. The caller must free it.
func marshalOptions(v any) (*C.char, func(), error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, nil, fmt.Errorf("lance: marshal options: %w", err)
	}
	cs, free := cString(string(data))
	return cs, free, nil
}

// Open opens an existing Lance dataset at uri.
//
// Options compose freely. WithSession(s) shares s's index/metadata caches with
// the open, and WithObjectStoreCache(c) wraps the dataset's object store with a
// Go byte-range cache. Passing both opens the dataset with a shared session AND
// a byte cache on the same handle.
//
// Fails with ErrNotFound if uri does not exist, or if the requested
// version/tag/branch does not.
func Open(ctx context.Context, uri string, opts ...OpenOption) (ds *Dataset, err error) {
	var cfg openConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// obs inherits from the session when one is attached, else resolves from
	// the open options (WithObservability) or the OTel globals.
	o := newObs(cfg.obsCfg)
	if cfg.session != nil {
		o = cfg.session.obs()
	}
	ctx, end := o.start(ctx, "Open", datasetURIAttribute(uri))
	defer func() { end(&err) }()

	if err = ctx.Err(); err != nil {
		return nil, err
	}

	cURI, freeURI := cString(uri)
	defer freeURI()
	kv, freeKV := cStorageKV(cfg.storage)
	defer freeKV()
	cOpts, freeOpts, err := marshalOptions(&cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	var ptr *C.LanceDataset
	if cfg.session != nil {
		ptr, err = openWithSession(ctx, cfg.session, uri, cURI, kv, cOpts)
		if err != nil {
			return nil, err
		}
	} else {
		if err := ffiCall(ctx, func() C.int32_t {
			return C.lance_dataset_open(cURI, kv, cOpts, &ptr)
		}); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("lance: open %q: %w (check the URI, version/tag, and storage options)", uri, err)
			}
			return nil, fmt.Errorf("lance: open %q: %w", uri, err)
		}
	}
	ds = newDataset(ptr, o)
	if cfg.objectStoreCache != nil {
		return wrapObjectStoreCache(ds, cfg.objectStoreCache, o, uri)
	}
	return ds, nil
}

// openWithSession opens a dataset sharing sess's caches. It holds sess's lock
// only for the duration of the native call, so callers may apply further
// wrappers to the returned handle without holding it.
func openWithSession(ctx context.Context, sess *Session, uri string, cURI *C.char, kv **C.char, cOpts *C.char) (*C.LanceDataset, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.ptr == nil {
		return nil, fmt.Errorf("lance: session is closed: %w", ErrInvalidArgument)
	}
	var ptr *C.LanceDataset
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_open_with_session(cURI, kv, cOpts, sess.ptr, &ptr)
	}); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("lance: open %q with session: %w (check the URI, version/tag, and storage options)", uri, err)
		}
		return nil, fmt.Errorf("lance: open %q with session: %w", uri, err)
	}
	return ptr, nil
}

// Write writes all record batches from rdr to a Lance dataset at uri and
// returns a handle to the resulting dataset. The reader is retained for the
// duration of the write and fully consumed. The caller keeps its own
// reference and should still Release it as usual.
//
// The reader's batches are exported across the Arrow C Data Interface, so
// their buffers must live outside the Go heap to satisfy the cgo
// pointer-passing rules: allocate them with Allocator (this is enforced when
// building with GOEXPERIMENT=cgocheck2).
//
// Fails with ErrAlreadyExists if a dataset already exists at uri under the
// default WriteModeCreate; pass WithMode(WriteModeAppend) or
// WithMode(WriteModeOverwrite) instead.
func Write(ctx context.Context, uri string, rdr array.RecordReader, opts ...WriteOption) (ds *Dataset, err error) {
	cfg := writeConfig{Mode: WriteModeCreate.String()}
	for _, opt := range opts {
		opt(&cfg)
	}
	// obs inherits from the session when one is attached, else resolves from
	// the write options (WithWriteObservability) or the OTel globals.
	o := newObs(cfg.obsCfg)
	if cfg.session != nil {
		o = cfg.session.obs()
	}
	ctx, end := o.start(ctx, "Write",
		datasetURIAttribute(uri),
		attribute.String("lance.write_mode", cfg.Mode))
	defer func() { end(&err) }()

	if err = ctx.Err(); err != nil {
		return nil, err
	}

	cURI, freeURI := cString(uri)
	defer freeURI()
	kv, freeKV := cStorageKV(cfg.storage)
	defer freeKV()
	cOpts, freeOpts, err := marshalOptions(&cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	// A session and/or a progress callback route through the session-aware
	// entry point. Both native entry points decode the same complete write
	// options and build WriteParams through one shared implementation.
	usePlugin := cfg.session != nil || cfg.progress != nil

	var progressHandle uintptr
	if cfg.progress != nil {
		progressHandle = registerPlugin(writeProgressAdapter{fn: cfg.progress})
		defer releasePlugin(progressHandle)
	}
	// Resolve and hold the session pointer across the native call. Checked
	// before the stream export so a closed session leaks nothing.
	var sessPtr *C.LanceSession
	if cfg.session != nil {
		cfg.session.mu.Lock()
		defer cfg.session.mu.Unlock()
		if cfg.session.ptr == nil {
			return nil, fmt.Errorf("lance: session is closed: %w", ErrInvalidArgument)
		}
		sessPtr = cfg.session.ptr
	}

	// The stream struct must be zero-initialized (Go zeroes it). The native
	// side always takes ownership of the exported stream, even on error, so
	// the exported reader is released exactly once.
	var stream C.struct_ArrowArrayStream
	cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&stream)))

	var ptr *C.LanceDataset
	if usePlugin {
		if err := ffiCall(ctx, func() C.int32_t {
			return C.lance_dataset_write_with_session(&stream, cURI, cOpts, kv, sessPtr, C.size_t(progressHandle), &ptr)
		}); err != nil {
			if cfg.Mode == WriteModeCreate.String() && errors.Is(err, ErrAlreadyExists) {
				return nil, fmt.Errorf("lance: write %q with session: %w (dataset exists; use WithMode(WriteModeAppend) or WithMode(WriteModeOverwrite))", uri, err)
			}
			return nil, fmt.Errorf("lance: write %q with session: %w", uri, err)
		}
	} else {
		if err := ffiCall(ctx, func() C.int32_t {
			return C.lance_dataset_write(&stream, cURI, cOpts, kv, &ptr)
		}); err != nil {
			if cfg.Mode == WriteModeCreate.String() && errors.Is(err, ErrAlreadyExists) {
				return nil, fmt.Errorf("lance: write %q: %w (dataset exists; use WithMode(WriteModeAppend) or WithMode(WriteModeOverwrite))", uri, err)
			}
			return nil, fmt.Errorf("lance: write %q: %w", uri, err)
		}
	}
	return newDataset(ptr, o), nil
}

// Close releases the native dataset handle. It is idempotent and safe to
// call concurrently with other methods (which will fail cleanly once the
// handle is closed). Readers previously obtained from Scan remain valid.
func (d *Dataset) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ptr != nil {
		C.lance_dataset_close(d.ptr)
		d.cleanup.Stop()
		d.ptr = nil
	}
	return nil
}

// checkOpen returns the native handle, or an error if the dataset is closed
// or ctx is done. Callers must hold d.mu (read lock is sufficient) for the
// duration of the native call.
func (d *Dataset) checkOpen(ctx context.Context) (*C.LanceDataset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.ptr == nil {
		return nil, fmt.Errorf("lance: dataset is closed: %w", ErrInvalidArgument)
	}
	return d.ptr, nil
}

// CountRows returns the number of rows in the dataset. A non-empty filter
// restricts the count to rows matching the SQL predicate. An empty filter
// counts all rows.
func (d *Dataset) CountRows(ctx context.Context, filter string) (uint64, error) {
	return datasetOp(ctx, d, "Dataset.CountRows", "count rows",
		func(ctx context.Context, ptr *C.LanceDataset) (uint64, error) {
			cFilter, freeFilter := cString(filter)
			defer freeFilter()
			var count C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_count_rows(ptr, cFilter, &count)
			}); err != nil {
				return 0, err
			}
			return uint64(count), nil
		}, expressionAttribute("filter", filter))
}

// Schema returns the Arrow schema of the dataset.
func (d *Dataset) Schema(ctx context.Context) (*arrow.Schema, error) {
	return datasetOp(ctx, d, "Dataset.Schema", "schema",
		func(ctx context.Context, ptr *C.LanceDataset) (*arrow.Schema, error) {
			// Must be zero-initialized (Go zeroes it).
			var cSchema C.struct_ArrowSchema
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_schema(ptr, &cSchema)
			}); err != nil {
				return nil, err
			}
			imported := (*cdata.CArrowSchema)(unsafe.Pointer(&cSchema))
			defer cdata.ReleaseCArrowSchema(imported)
			schema, err := cdata.ImportCArrowSchema(imported)
			if err != nil {
				return nil, fmt.Errorf("import schema: %w", err)
			}
			return schema, nil
		})
}

// Version returns information about the currently checked-out version of
// the dataset.
func (d *Dataset) Version(ctx context.Context) (VersionInfo, error) {
	return datasetOp(ctx, d, "Dataset.Version", "version",
		func(ctx context.Context, ptr *C.LanceDataset) (VersionInfo, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_version(ptr, &cJSON)
			}); err != nil {
				return VersionInfo{}, err
			}
			defer C.lance_string_free(cJSON)
			var info VersionInfo
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &info); err != nil {
				return VersionInfo{}, fmt.Errorf("decode version info: %w", err)
			}
			return info, nil
		})
}

// LatestVersion returns the latest committed version number of the dataset,
// which may be newer than the checked-out version when time traveling.
func (d *Dataset) LatestVersion(ctx context.Context) (uint64, error) {
	return datasetOp(ctx, d, "Dataset.LatestVersion", "latest version",
		func(ctx context.Context, ptr *C.LanceDataset) (uint64, error) {
			var version C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_latest_version(ptr, &version)
			}); err != nil {
				return 0, err
			}
			return uint64(version), nil
		})
}
