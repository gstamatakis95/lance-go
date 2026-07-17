# Memory, ownership, and threading

lance-go moves Arrow data across the cgo boundary zero-copy. That buys
performance at the cost of explicit ownership rules. This page is the
contract. Violating it leaks native memory at best and corrupts the Go heap
at worst.

## The two rules that matter most

1. **Everything you import must be `Release()`d.**
2. **Everything you export must be C-allocated.**

## Release() obligations (data coming *out* of lance-go)

| You obtained | You must |
| --- | --- |
| `array.RecordReader` from `Scanner.Reader` | `Release()` it when done |
| each record from `rdr.Next()` / `rdr.RecordBatch()` | nothing: it is only valid until the next `Next()`. Call `Retain()` (then later `Release()`) to keep it |
| `*arrow.Schema` from `Dataset.Schema` | nothing: it is fully imported into Go memory |
| `*Dataset` from `Open` / `Write` / `Checkout` / `Session.Open` | `Close()` it (idempotent) |
| `arrow.RecordBatch` from `Take` / `Sample` | `Release()` it |
| `*BlobList` from `TakeBlobs*` | `Close()` it (frees every `BlobFile` it owns) |
| `[]byte` from `BlobFile.Read` / `ReadAll` / `ReadRange` | nothing: it is an owned Go slice (a copy) |
| `*Session` from `NewSession*` | `Close()` its handle when done; datasets retain the native session state they need |

Readers returned by `Scanner.Reader` are self-contained: they stay valid
after the originating `*Dataset` is closed. Each `Checkout` returns a *new*
handle. Close it independently of the receiver.

Reader `Release()` is deterministic: it closes the Arrow C stream immediately
rather than waiting for Go garbage collection. Reaching EOF or a context
cancellation also closes the native stream; callers must still call `Release`
to drop the reader's current record and final reference.

### Leak safety nets (`runtime.AddCleanup`)

`Dataset`, `Session`, `Fragment`, and `BlobList` handles register a
`runtime.AddCleanup` cleanup when constructed: if you drop every reference to
the handle without calling `Close`, the Go runtime eventually releases the
native handle anyway, after the object becomes unreachable.

These are **safety nets, not lifecycle management**:

- **Still call `Close` explicitly.** The GC gives no timeliness guarantee:
  native memory is invisible to the Go heap profiler, so a "leaked" handle
  can hold large native allocations (caches, manifests, index data) for an
  arbitrarily long time before the cleanup fires. Deterministic `Close` (or
  `defer Close()`) remains the required and preferred pattern.
- **Cleanups run on the runtime's finalizer goroutine**, at an unpredictable
  time and concurrently with your code. Do not rely on ordering between
  cleanups or between a cleanup and anything else.
- **No double-free:** `Close` releases the native pointer, then stops the
  registered cleanup (`runtime.Cleanup.Stop`). Stopping after the close is
  safe: the receiver (`d`/`s`/...) is still reachable for the duration of
  the `Close` call, so the cleanup cannot have already fired and raced the
  close. What actually prevents a double-free is that ordering guarantee,
  not the specific before/after sequence of close-then-stop. `Close` stays
  idempotent.

## C-allocator requirement (data going *into* lance-go)

cgo forbids passing pointers to Go-heap memory that itself contains
pointers, which is exactly what an exported Arrow buffer tree is. So any
Arrow data you hand to the native side must be backed by C memory:

```go
import "github.com/gstamatakis95/lance-go/lance"

mem := lance.Allocator()
b := array.NewRecordBuilder(mem, schema) // NOT memory.DefaultAllocator
```

This applies to:

- `lance.Write`, `lance.WriteFragments`, `Session.Write`,
  `WriteWithProgress`, `Dataset.Append` (the record reader's batches)
- `MergeInsertBuilder.Execute` (source rows)
- `Dataset.AddColumnsFromReader`, `Dataset.Merge` (new-column rows)
- `Dataset.AddColumnsUDF`: the result batch your mapper **returns** (the input
  batch is imported for you, it is borrowed, valid only during the call)
- `Scanner.NearestArrow` (the query array, plain `Nearest([]float32)` is
  safe because the bindings copy it into C memory internally)
- `VectorOptions.Centroids` / `Codebook` and `CreateIndexUncommitted`'s
  centroids array (the distributed index build)

Records built with Go-heap allocators (`memory.DefaultAllocator`) **crash
under `GOEXPERIMENT=cgocheck2`** ("cgo argument has Go pointer to
unpinned Go pointer") and are unsound without it. They may appear to work
until the GC moves or collects a buffer mid-write.

CI runs the test suite under `GOEXPERIMENT=cgocheck2` (`make
go-test-cgocheck`) precisely to catch violations. Note: in Go 1.25 this
replaces the older `GODEBUG=cgocheck=2`, which is no longer accepted.

### Export ownership on error

When the bindings export a stream/array to the native side, **the native
side takes ownership even if the call fails**, releasing the export exactly
once. You keep your own reference to the reader you passed in and should
still `Release()` it as usual. Never assume a failed `Write` left your
reader unconsumed.

## Blobs

`TakeBlobs` (and its `ByIndices`/`ByAddresses` variants) return a `*BlobList`
you must `Close`. It owns every `BlobFile` obtained from `Get`, and those
handles become invalid after `Close`. Blob reads are self-contained. They
stay valid after the originating `*Dataset` is closed (like scanner streams).
`BlobFile.Read`/`ReadAll`/`ReadRange` return **owned Go byte slices** (copies
of the native buffer, already freed). `BlobFile.Reader(ctx)` instead provides
a range-backed `io.Reader`, `io.ReaderAt`, and `io.Seeker` for chunked access;
it is borrowed from the same list and needs no separate close. Concurrent
reads across distinct blobs of one list are safe. `Close` waits for in-flight
reads to finish.

## Callbacks and plugins

The callback hooks (`CacheBackend`, `ObjectStoreCache`, `WriteWithProgress`,
`AddColumnsUDF`, `UDFCheckpointStore`) run Go code invoked by the native side.
Their ownership rules (full detail in [callbacks.md](callbacks.md)):

- **Inputs are borrowed** for the duration of the call: copy anything you
  keep. Byte payloads and imported record batches passed *into* your callback
  are valid only during the call. `Retain()`/copy to persist.
- **Byte outputs you return** are copied into C memory by the bridge (freed
  on the native side). You keep ownership of your Go slice.
- **Record-batch outputs you return** (a UDF result, or a checkpoint
  `GetBatch` result) are **zero-copy exported** across the Arrow C Data
  Interface, so they must be **C-allocated** with `lance.Allocator()`, same as `Write`.
  The bridge releases the reference you return. Do not `Release` it yourself.
  Call `Retain()` first if you plan to keep the batch (e.g. in a checkpoint store).
- **Native cache registrations follow native references.** Closing the Go
  `Session` or `Dataset` handle does not unregister a cache while a derived
  native dataset, scanner, or reader still needs it. The last native clone
  releases the registration. Transient callbacks remain scoped to their call.
  Releasing is idempotent, and a stale handle fails cleanly.
- **Panics are contained.** A Go panic inside a callback is recovered at the
  boundary and converted to an operation error. Rust export panics are also
  contained separately as `ErrInternal`. A panic is still a bug to fix, not a
  control-flow tool. Callbacks may run on many threads at once: make them safe
  for concurrent use.
- **Callbacks must not re-enter lance-go.** A synchronous callback (an
  `AddColumnsUDF` mapper, a `UDFCheckpointStore` method, or a
  `WriteWithProgress` callback) runs inside the native operation on a runtime
  thread. Calling back into any lance-go API from there would drive the shared
  Tokio runtime from within itself. Such a re-entrant call is rejected with
  `ErrReentrantCall` rather than crashing the process. For the UDF/checkpoint
  paths that error surfaces as a failure of the enclosing operation. For the
  best-effort write-progress callback it is ignored and the write completes.

## Error contract

- Every failing call returns an error wrapping exactly one sentinel
  (`ErrInvalidArgument`, `ErrIO`, `ErrNotFound`, `ErrAlreadyExists`,
  `ErrIndex`, `ErrConflict`, `ErrTimeout`, `ErrInternal`,
  `ErrNotImplemented`, `ErrReentrantCall`, or — rarely — `ErrCanceled`;
  cancelled calls normally return `ctx.Err()` directly). Classify with
  `errors.Is`, read details from `err.Error()`. The `lance: <verb>:` prefix
  appears exactly once per error string; the sentinels themselves are
  unprefixed.
- The native side records errors in a thread-local slot. The bindings'
  `ffiCall` helper pins the goroutine to its OS thread
  (`runtime.LockOSThread`) across the native call and the error retrieval,
  so the error read is always the one set by that call. This is invisible
  to you. There is no cross-call error state to reset.
- Every exported Rust C function contains Rust panics at the boundary and
  converts them to `ErrInternal`. The crate uses `panic = "unwind"` only for
  this containment; unwinding never crosses into Go. Treat such an error as a
  native bug and preserve its message for diagnosis.

## Goroutines and OS threads

- `*Dataset` is safe for concurrent use from any goroutine (internally a
  RW lock in Go plus a mutex around the Rust dataset). `Close` is
  idempotent and safe to race with other methods, which fail cleanly with
  `ErrInvalidArgument` once closed.
- A `Scanner` or `MergeInsertBuilder` must not be configured concurrently.
  Terminal methods are safe to call repeatedly.
- Each native call **blocks its OS thread** for the full duration (the
  async work runs on a shared Tokio runtime inside the shim). The Go
  scheduler compensates as it does for blocking syscalls, but hundreds of
  simultaneous in-flight calls mean hundreds of parked OS threads. Bound
  your fan-out with a semaphore if that matters.
- Long CPU-heavy operations (index builds, compaction) also occupy the
  shared Tokio runtime's worker threads. Expect them to compete with
  concurrent scans.

## Context cancellation

`ctx` is checked before each native call, and cancelling it (or hitting its
deadline) **aborts the in-flight native call**: the bindings install a
cancellation token on the pinned OS thread for the duration of the call and a
watcher fires it when `ctx` is done, racing the native future against it.
The cancelled call returns `ctx.Err()` (`context.Canceled` /
`context.DeadlineExceeded`), with `ErrCanceled` as a defensive fallback.
Cancellation is cooperative: the in-flight future is dropped, but background
work it already spawned may run to completion, and a cancelled write can
leave orphaned data files for `CleanupOldVersions` to reclaim (commits are
atomic manifest swaps, so committed versions are never corrupted).

Streamed readers additionally check `ctx` before each record batch, so
cancellation closes a lazy reader at the next batch boundary. See the
"Context cancellation" section of [usage.md](usage.md).

## Debugging checklist

- Crash mentioning "Go pointer" under `GOEXPERIMENT=cgocheck2` → some
  exported record was built with a Go-heap allocator. Switch to
  `lance.Allocator()`.
- Steadily growing RSS with stable Go heap → missing `Release()` on a
  reader/record or `Close()` on a dataset (native memory is invisible to
  the Go GC and `pprof`).
- Use-after-release panics inside arrow-go → a record obtained from
  `Next()` was used after the following `Next()` without `Retain()`.
