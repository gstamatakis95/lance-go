# Using lance-go

A walkthrough of the dataset lifecycle: writing, opening, scanning, point
reads, SQL, mutating, change-data-capture, config, evolving the schema,
branches, and time travel. Vector/FTS search and index management live in
[indexes.md](indexes.md), object-store configuration in
[storage.md](storage.md), the distributed write/index workflow in
[distributed.md](distributed.md), caching in [caching.md](caching.md),
callbacks/UDFs in [callbacks.md](callbacks.md), OpenTelemetry tracing, metrics,
and logs in [observability.md](observability.md), and ownership and threading
rules in [memory.md](memory.md). Read that one before shipping anything.

All examples assume:

```go
import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gstamatakis95/lance-go/lance"
)

mem := lance.Allocator() // C-backed allocator, required for writes
```

## Writing datasets

`lance.Write` consumes an `array.RecordReader` and returns an open
`*Dataset` handle for the result:

```go
ds, err := lance.Write(ctx, "/data/events.lance", rdr) // default: create
```

Write modes and knobs:

```go
ds, err := lance.Write(ctx, uri, rdr,
	lance.WithMode(lance.WriteModeAppend),      // or WriteModeCreate (default), WriteModeOverwrite
	lance.WithMaxRowsPerFile(1_000_000),
	lance.WithMaxRowsPerGroup(8192),
	lance.WithMaxBytesPerFile(512<<20),
	lance.WithDataStorageVersion("2.1"),        // "2.0", "2.1", "2.2" or "stable"
	lance.WithStableRowIDs(true),               // row IDs survive compaction/updates
	lance.WithWriteStorageOptions(storageOpts), // see storage.md
)
```

- `WriteModeCreate` fails with `lance.ErrAlreadyExists` if the dataset exists.
- `WriteModeOverwrite` replaces the contents *as a new version*. Older
  versions remain reachable via time travel.
- The reader's buffers must be allocated with a C-backed allocator
  (`lance.Allocator()`). See [memory.md](memory.md).

## Opening datasets

```go
ds, err := lance.Open(ctx, "/data/events.lance")
if errors.Is(err, lance.ErrNotFound) { /* ... */ }
defer ds.Close()

count, err := ds.CountRows(ctx, "")            // all rows
n, err := ds.CountRows(ctx, "status = 'ok'")   // filtered count
schema, err := ds.Schema(ctx)                  // *arrow.Schema
info, err := ds.Version(ctx)                   // checked-out version (number, timestamp, metadata)
latest, err := ds.LatestVersion(ctx)           // latest committed version
```

Open at a point in time:

```go
old, err := lance.Open(ctx, uri, lance.WithVersion(3))
rel, err := lance.Open(ctx, uri, lance.WithTag("v1.0"))
```

`*Dataset` is safe for concurrent use. `Close` is idempotent. It releases
the native handle. Readers previously obtained from scans stay valid after
`Close`.

### Handle semantics

Operations that commit a new version (mutations, index operations, schema
evolution, compaction) **update the handle in place**: subsequent reads
through the same `*Dataset` see the new version without reopening. The one
exception is `Checkout`, which returns a **new** handle and leaves the
receiver untouched (see [Versioning](#versioning-and-time-travel)).

## Scanning

`ds.Scan()` returns a builder. Configure it, then call a terminal method:
`Reader`, `CountRows`, or `Explain`. Each terminal call snapshots the
dataset, so a `Scanner` is cheap and reusable.

```go
rdr, err := ds.Scan().
	Columns("id", "name", "score").  // projection (default: all columns)
	Filter("score > 0.5 AND name LIKE 'a%'"). // SQL predicate
	Limit(1000).
	Offset(100).
	WithRowID().                     // include the _rowid column
	BatchSize(4096).                 // rows per emitted batch
	ScanInOrder(false).              // allow out-of-order batches for throughput
	Reader(ctx)
if err != nil { ... }
defer rdr.Release()

for rdr.Next() {
	rec := rdr.RecordBatch() // valid until the next Next(); Retain() to keep
	// ...
}
if err := rdr.Err(); err != nil { ... }
```

Other terminals:

```go
n, err := ds.Scan().Filter("score > 0.5").CountRows(ctx)
plan, err := ds.Scan().Filter("score > 0.5").Explain(ctx, true /* verbose */)
```

Vector search (`Nearest`, `NearestArrow`, `Metric`, `Nprobes`, ...) and
full-text search (`FullTextSearch`) are covered in
[indexes.md](indexes.md). The scanner also has an `OrderBy`/`OrderByDesc`,
`Aggregate`, `ProjectExpr`, `FilterSubstrait`, `DistanceRange`, and a
`ScanFragments` fragment-scoped variant, plus a `Batch(ctx)` terminal that
returns a single materialized batch and `AnalyzePlan(ctx)` for a profiled
plan. See the package docs for the full builder.

## Point reads

Read specific rows without a full scan. `Take` selects rows by offset, stable
row id, or address. Convenience wrappers cover the common cases. Each returns
a single `arrow.RecordBatch` in request order that the caller must `Release`.

```go
// By row offset (0 = first live row):
rec, err := ds.TakeIndices(ctx, []uint64{0, 5, 9}, "id", "name")
defer rec.Release()

// By stable row id (write with WithStableRowIDs(true) for ids that survive
// compaction; otherwise row ids equal row addresses):
rec, err = ds.TakeRows(ctx, []uint64{1000, 1001}, "id")

// Full control: choose the selector and an optional SQL projection:
rec, err = ds.Take(ctx, lance.TakeSpec{
	Indices:       []uint64{0, 1, 2},
	SQLProjection: []lance.NamedExpr{{Name: "double", SQL: "score * 2"}},
})

// Stream ranges of row offsets (returns a RecordReader):
rdr, err := ds.TakeScan(ctx, [][2]uint64{{0, 100}, {500, 600}}, "id")
defer rdr.Release()

// Random sample of n rows (returns them in row-id order):
sample, err := ds.Sample(ctx, 10, "id", "score")
defer sample.Release()
```

Blob columns are read separately with `TakeBlobs` (see the
[take_blobs example](../examples/take_blobs) and [memory.md](memory.md)).

## SQL queries

`Dataset.SQL` runs a DataFusion SQL query over the dataset, registered as the
table `dataset` (rename with `TableName`).

```go
rdr, err := ds.SQL("SELECT category, COUNT(*) AS n FROM dataset GROUP BY category").
	Reader(ctx)
if err != nil { ... }
defer rdr.Release()
for rdr.Next() {
	rec := rdr.RecordBatch()
	// ...
}
```

`WithRowID()` / `WithRowAddr()` expose the internal `_rowid` / `_rowaddr`
columns to the query. See the [sql_query example](../examples/sql_query).

## Mutations

All mutations commit a new dataset version and update the handle in place.

### Delete

```go
res, err := ds.Delete(ctx, "status = 'expired'")
// res.NumDeletedRows
```

The predicate is required. Deleting everything needs an explicit
always-true predicate.

### Update

```go
res, err := ds.Update(ctx, lance.UpdateSpec{
	Set:   map[string]string{"score": "score * 2", "status": "'boosted'"},
	Where: "score < 0.1", // empty = every row
})
// res.RowsUpdated
```

`Set` values are SQL expressions over the existing columns.
`ConflictRetries` (default 10) and `RetryTimeout` (default 30s) tune
optimistic-commit retries under contention.

### MergeInsert (upsert / find-or-create)

`MergeInsert` joins source rows against the dataset on key columns. The
defaults implement find-or-create: matched rows are kept
(`WhenMatchedDoNothing`), unmatched source rows are inserted
(`WhenNotMatchedInsertAll`), target rows missing from the source are kept
(`WhenNotMatchedBySourceKeep`), with 10 conflict retries over at most 30s.

```go
// Upsert:
stats, err := ds.MergeInsert("id").
	WhenMatchedUpdateAll().
	Execute(ctx, srcReader)
// stats.NumInsertedRows, stats.NumUpdatedRows, stats.NumDeletedRows, ...

// Conditional update + delete rows that vanished from the source:
stats, err = ds.MergeInsert("id").
	WhenMatchedUpdateIf("source.updated_at > target.updated_at").
	WhenNotMatchedBySourceDelete().
	ConflictRetries(20).
	RetryTimeout(time.Minute).
	Execute(ctx, srcReader)
```

The source reader has the same C-allocator requirement as `Write`.

Four builder methods expose the rest of Lance's merge-insert knobs:

- `SourceDedupeBehavior(behavior)` controls how duplicate source rows that
  match the same target row are handled. The default is `SourceDedupeFail`,
  which errors when the source has duplicate keys. `SourceDedupeFirstSeen`
  keeps the first-encountered source row and skips the rest. This is a
  correctness knob, so leave it at `SourceDedupeFail` unless your source can
  legitimately carry duplicate keys.
- `UseIndex(use)` (default true) uses a scalar index on the join key when one
  exists.
- `CommitRetries(n)` (default 20) is the inner manifest-conflict retry count,
  distinct from `ConflictRetries`.
- `SkipAutoCleanup(skip)` (default false) skips automatic cleanup of old
  versions during the commit.

```go
stats, err := ds.MergeInsert("id").
	WhenMatchedUpdateAll().
	SourceDedupeBehavior(lance.SourceDedupeFirstSeen).
	UseIndex(true).
	CommitRetries(20).
	SkipAutoCleanup(false).
	Execute(ctx, srcReader)
```

## Schema evolution

All evolution methods commit a new version and update the handle in place.

```go
// New columns from SQL expressions over existing rows:
err = ds.AddColumnsSQL(ctx, []lance.NamedExpr{
	{Name: "double_score", SQL: "score * 2"},
}, nil /* readColumns: auto */, 0 /* batchSize: default */)

// New columns supplied by a reader (one value per existing row, in order):
err = ds.AddColumnsFromReader(ctx, colReader, 0)

// Metadata-only all-null columns (every field must be nullable):
err = ds.AddColumnsAllNulls(ctx, extraSchema)

// Drop columns (metadata-only; data reclaimed by CompactFiles + CleanupOldVersions):
err = ds.DropColumns(ctx, "obsolete_a", "obsolete_b")

// Rename / change nullability / cast:
nullable := true
err = ds.AlterColumns(ctx,
	lance.ColumnAlteration{Path: "score", Rename: "relevance"},
	lance.ColumnAlteration{Path: "note", Nullable: &nullable},
	lance.ColumnAlteration{Path: "id", DataType: "int64"},
)

// Join new columns from another table (left join on key):
err = ds.Merge(ctx, rightReader, "id" /* leftOn */, "id" /* rightOn */)
```

Supported `DataType` cast targets: `int32`, `int64`, `float32`, `float64`,
`utf8`, `large_utf8`, `binary`, `bool`, `date32`, `date64`, `timestamp_us`,
`timestamp_ns`. Renames and nullability changes are zero-copy. Casts
rewrite the column and drop indices covering it.

## Maintenance

```go
// Compact small fragments (defaults: 1M rows/fragment, materialize
// deletions above 10%):
metrics, err := ds.CompactFiles(ctx, lance.CompactionOptions{
	TargetRowsPerFragment: 2_000_000,
})

// Reclaim storage from versions older than 7 days:
stats, err := ds.CleanupOldVersions(ctx, 7*24*time.Hour,
	lance.WithErrorIfTaggedOldVersions(false), // silently keep tagged versions
)
```

`WithDeleteUnverified(true)` also removes files not referenced by any
manifest. This is only safe when no other writer is running. Cleaned-up
versions can no longer be checked out or restored.

## Versioning and time travel

Every commit (write, mutation, index op, evolution) creates a new numbered
version.

```go
versions, err := ds.Versions(ctx) // []VersionInfo, oldest first

// Checkout returns a NEW handle fixed at the referenced version.
// The receiver keeps tracking its own version; close both.
old, err := ds.Checkout(ctx, lance.VersionRef(3))
defer old.Close()
rel, err := ds.Checkout(ctx, lance.TagRef("v1.0"))
defer rel.Close()

// Roll the dataset back: commit the checked-out version as the new latest.
err = old.Restore(ctx)

// Move a (possibly stale) handle to the latest committed version:
err = ds.CheckoutLatest(ctx)
```

### Tags

Tags are named references to versions. Tagged versions are protected from
`CleanupOldVersions` (unless disabled via
`WithErrorIfTaggedOldVersions(false)`).

```go
tags := ds.Tags()
err = tags.Create(ctx, "v1.0", lance.VersionRef(12))
err = tags.Update(ctx, "v1.0", lance.VersionRef(15))
v, err := tags.GetVersion(ctx, "v1.0")
all, err := tags.List(ctx) // map[string]TagInfo
err = tags.Delete(ctx, "v1.0")
```

## Change data capture (delta)

`Dataset.Delta()` builds a change-data-capture query between two versions. The
begin bound is exclusive and the end bound inclusive (a delta from version 1
to 3 covers the changes of versions 2 and 3). Tracking inserted/updated rows
requires the dataset to have been written with `WithStableRowIDs(true)`.

```go
d := ds.Delta().FromVersion(1).ToVersion(3) // or ComparedAgainstVersion / FromDate+ToDate

ins, err := d.InsertedRows(ctx)   // rows created in the range
upd, err := d.UpdatedRows(ctx)    // rows that pre-existed and were updated
ups, err := d.UpsertedRows(ctx)   // both of the above
// each returns a RecordReader with _rowid, _row_created_at_version,
// _row_last_updated_at_version columns; Release it.

txns, err := d.Transactions(ctx)  // []TransactionInfo committed in the range
```

## Config and metadata

Datasets carry three kinds of key/value maps, each with read/update/delete
(and, for schema/field metadata, replace) methods that commit a new version:

```go
cfg, err := ds.Config(ctx)                                    // dataset config
_, err = ds.UpdateConfig(ctx, map[string]string{"team": "lance"})
err = ds.DeleteConfigKeys(ctx, "team")

md, err := ds.Metadata(ctx)                                   // table metadata
_, err = ds.UpdateMetadata(ctx, map[string]string{"owner": "search"})

_, err = ds.UpdateSchemaMetadata(ctx, map[string]string{"schema_note": "v2"})
err = ds.UpdateFieldMetadata(ctx, "score", map[string]string{"unit": "ratio"})
```

Related introspection helpers: `Manifest`, `Fragments`/`Fragment`,
`CountDeletedRows`, `NumSmallFiles`, `DataStats`, `CacheStats`, `Paths`,
`IsStale`, `HasSuccessorVersion`, and `Validate`.

## Branches and cloning

Branches are independent, named lines of versions. Clones copy a dataset to a
new URI (shallow = shares data files, deep = fully independent).

```go
// CreateBranch / CheckoutBranch return a NEW handle on the branch; the
// receiver is unchanged. Close both.
br, err := ds.CreateBranch(ctx, "experiment", lance.VersionRef(5))
defer br.Close()
err = br.Append(ctx, rdr) // commits to the branch (C-allocated reader)

branches, err := ds.ListBranches(ctx) // map[string]BranchInfo
err = ds.DeleteBranch(ctx, "experiment", false /* force */)

// Open directly on a branch:
b, err := lance.Open(ctx, uri, lance.WithBranch("experiment"))

// Clones:
sc, err := ds.ShallowClone(ctx, targetURI, lance.VersionRef(5), nil)
dc, err := ds.DeepClone(ctx, targetURI, lance.Ref{}, nil) // zero Ref = current version
```

## Error handling

Every error wraps exactly one sentinel, so classify with `errors.Is`:

```go
_, err := lance.Open(ctx, uri)
switch {
case errors.Is(err, lance.ErrNotFound):        // dataset/version/tag/index missing
case errors.Is(err, lance.ErrAlreadyExists):   // create-mode write, duplicate tag/index
case errors.Is(err, lance.ErrInvalidArgument): // bad params, closed handle
case errors.Is(err, lance.ErrIO):              // storage-layer failure
case errors.Is(err, lance.ErrIndex):           // index build/search failure
case errors.Is(err, lance.ErrConflict):        // concurrent commit/ref/version conflict
case errors.Is(err, lance.ErrTimeout):         // native operation timed out
case errors.Is(err, lance.ErrCanceled):        // rare fallback; cancelled calls normally return ctx.Err()
case errors.Is(err, lance.ErrNotImplemented):
case errors.Is(err, lance.ErrInternal):
}
```

Error strings carry exactly one `lance: <verb>:` prefix (e.g.
`lance: open "s3://...": not found: ...`); the sentinels themselves are
unprefixed. Classify with `errors.Is`, never by string matching.

## Context cancellation

Cancelling the `ctx` (or letting its deadline expire) **aborts the in-flight
native operation**, not just the Go-side bookkeeping. Every operation that
goes through a fallible native call is cancellable: opens, writes, index
builds, compaction, mutations, commits, and the blocking calls of scans
(`CountRows`, `Batch`, `Explain`, `AnalyzePlan`, and the construction of a
`Reader`). A cancelled call returns `ctx.Err()` — `context.Canceled` or
`context.DeadlineExceeded` — so classify with `errors.Is` against those.
`lance.ErrCanceled` exists only as a defensive fallback for the rare race
where the native side observes the cancellation before the Go context does.

```go
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
err := ds.CreateIndex(ctx, "embedding", lance.IvfPq{SubVectors: 16})
if errors.Is(err, context.DeadlineExceeded) { /* index build aborted */ }
```

Streaming readers (`Scanner.Reader`, `SQL`, `TakeScan`, delta readers, and
the `All` iterators) check `ctx` per batch: cancellation stops the stream at
the next batch boundary and surfaces via `rdr.Err()` (or as the iterator's
final `(nil, err)` yield). `Release` the reader as usual.

One caveat: a **cancelled write may leave uncommitted, orphaned data files**
on storage. Commits are atomic manifest swaps, so a cancelled write never
corrupts a committed version — but the partially written files linger until
`CleanupOldVersions` reclaims them (see [Maintenance](#maintenance)).
Cancellation is cooperative on the native side; background work already
spawned may still run to completion. See [memory.md](memory.md) for the
threading details.

**Cancellation is ambiguous for commits.** Any commit-shaped operation —
`Write` (append/overwrite), `Delete`/`Update`/`MergeInsert`,
`CommitBuilder.Execute`/`ExecuteBatch`, index commits — performs cancellable
work *after* the atomic manifest write (metadata-cache updates, the
auto-cleanup hook), so a `ctx.Err()` return does not prove the commit failed:
the new version may already be durable. Treat a cancelled mutating call like
a network timeout — check the dataset's version/state before retrying, or a
naive retry can apply the change twice.

One operation is deliberately **non-cancellable**:
`Dataset.MigrateManifestPathsV2` ignores ctx cancellation once started,
because aborting mid-migration would leave a mix of V1 and V2 manifest names
that Lance refuses to open.
