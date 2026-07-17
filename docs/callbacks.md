# Callbacks and plugins

The callback bridge is a generic Go-callback mechanism so native Lance code
can call back into Go: cache backends (see [caching.md](caching.md)),
write-progress reporting, and user-defined column transforms. This page
covers the shipped sync hooks and the rules every callback must obey. The
mechanism lives in `lance/callbacks.go` (Go side) and
`rust/src/callbacks.rs` (native side).

## How a Go callback crosses FFI

The native library invokes Go plugins through a process-wide vtable of
cgo-exported functions. Each plugin is a Go object registered under an opaque
handle. The native side calls it with a method discriminator and an opaque
byte payload, and the plugin returns a byte payload (or a miss/error). A few
properties matter to you as a user of the hooks:

- **Panic-contained.** A Go panic must never unwind into Rust. Every exported
  Go shim recovers panics and
  converts them into a clean error return (the panic message becomes the
  error payload). Your callback panicking will surface as an operation error,
  not a process abort, but treat that as a bug to fix, not a control-flow
  mechanism.
- **Thread-safe / thread-pinned.** The native side may invoke your callback
  from many worker threads at once, so **every callback must be safe for
  concurrent use.** Fallible calls are pinned to their OS thread across the
  call and the error read (the `ffiCall` discipline).
- **Payload ownership.** Inputs are *borrowed* for the duration of the call
  only. Copy anything you need to keep. Outputs you return are copied into
  C memory by the bridge and freed on the native side. You don't manage that
  memory.
- **No re-entrancy.** A synchronous callback runs inside the native operation
  on a shared-runtime thread, so it **must not call back into lance-go** (open
  or scan a dataset, `CountRows`, etc.). A nested call would drive the Tokio
  runtime from within itself. The bindings reject it with `ErrReentrantCall`
  rather than crashing. For UDF mappers and checkpoint-store methods that error
  fails the enclosing operation. For the best-effort write-progress callback it
  is ignored and the write completes. If you need dataset state inside a
  callback, capture it *before* the operation starts.

Transient callback handles are released when their operation returns. Cache
handles handed to native sessions and object stores are leased instead: native
datasets, scanners, and readers retain the lease, and its last native clone
unregisters the Go object. Closing a parent handle therefore cannot invalidate
a still-live child. Releasing is idempotent, and a stale handle fails cleanly.

## Shipped hooks

### Write progress

`WriteWithProgress` (standalone or on a `Session`) reports cumulative
`WriteStats` to your callback after each batch is written:

```go
ds, err := lance.WriteWithProgress(ctx, uri, rdr, func(s lance.WriteStats) {
	log.Printf("written: %d rows, %d bytes, %d files", s.RowsWritten, s.BytesWritten, s.FilesWritten)
})

// Session variant (shares caches, see caching.md):
ds, err = sess.WriteWithProgress(ctx, uri, rdr, progressFn, opts...)
```

`WriteStats` is `{BytesWritten, RowsWritten, FilesWritten}`, all cumulative.
The callback runs synchronously on the write path. Keep it cheap.

### Column UDF (`AddColumnsUDF`)

`AddColumnsUDF` adds new columns whose values are computed by a Go function,
one input `RecordBatch` at a time, committing a new version:

```go
outSchema := arrow.NewSchema([]arrow.Field{
	{Name: "score2", Type: arrow.PrimitiveTypes.Float64},
}, nil)

err := ds.AddColumnsUDF(ctx, outSchema, func(in arrow.RecordBatch) (arrow.RecordBatch, error) {
	// compute the new columns for `in`. The result must match outSchema and
	// have the same row count as `in`, aligned row-for-row.
	mem := lance.Allocator() // C-backed, REQUIRED (see below)
	b := array.NewRecordBuilder(mem, outSchema)
	defer b.Release()
	// ... fill b from `in` ...
	return b.NewRecordBatch(), nil
}, lance.WithUDFReadColumns("score"), lance.WithUDFBatchSize(4096))
```

Two ownership rules are load-bearing:

- **The result batch is C-allocated.** The batch your function returns is
  exported across the Arrow C Data Interface, so its buffers must live
  outside the Go heap: build it with `lance.Allocator()`, exactly as
  for `Write`. A Go-heap-backed result crashes under `GOEXPERIMENT=cgocheck2`
  (which CI runs).
- **The input batch is valid only during the call.** `in` is borrowed. Do not
  retain it past the function return unless you `Retain()` it.

Options: `WithUDFReadColumns(cols...)` (restrict input columns),
`WithUDFBatchSize(n)`, `WithUDFCheckpoint(store)`.

### UDF checkpointing

For long UDF runs, attach a `UDFCheckpointStore` so an interrupted run
resumes without recomputing:

```go
type UDFCheckpointStore interface {
	GetBatch(fragmentID uint32, batchIndex int) (batch arrow.RecordBatch, found bool, err error)
	InsertBatch(fragmentID uint32, batchIndex int, batch arrow.RecordBatch) error
	GetFragment(fragmentID uint32) (fragmentJSON []byte, found bool, err error)
	InsertFragment(fragmentJSON []byte) error
}

err := ds.AddColumnsUDF(ctx, outSchema, fn, lance.WithUDFCheckpoint(store))
```

The store is consulted before recomputing each batch/fragment. Two rules:

- **Batches passed to `InsertBatch` are valid only during the call**: copy or
  `Retain()` them to persist. Batches you return from `GetBatch` must be
  C-allocated (they are exported to the native side). The bridge **zero-copy
  exports them and then releases the reference you returned**, so do **not**
  `Release` the returned batch yourself, and if you keep it in your store,
  `Retain()` it before returning (otherwise the bridge's release frees your
  stored copy).
- **Fragments are opaque JSON tokens.** Treat `fragmentJSON` as an opaque blob
  keyed by fragment id (the id is embedded in the JSON). Store and return it
  verbatim. Return `found=false` to signal "not checkpointed" and force
  recomputation.

The store is called concurrently. Make it safe for concurrent use.

## See also

- [observability.md](observability.md#per-scan-execution-stats-withscanstats):
  `Scanner.WithScanStats`, a separate synchronous callback for per-scan
  execution counters (same re-entrancy rule as the hooks above).

- [caching.md](caching.md): `CacheBackend` and `ObjectStoreCache`, the
  other consumers of this bridge.
- [memory.md](memory.md): the C-allocator rule, Arrow ownership, and panic
  containment contract in one place.
