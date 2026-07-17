# Distributed writes and index builds

lance-go exposes Lance's distributed primitives so N workers can write data or
build an index in parallel and a driver commits the result atomically. Two
independent workflows share the same shape (workers produce opaque, shippable
artifacts, a driver commits them):

- **Distributed write:** `WriteFragments` → `Transaction` → `CommitBuilder`.
- **Distributed index build:** `CreateIndexUncommitted` → `IndexMetadata` →
  `MergeIndexSegments` → `CommitIndexSegments`.

The runnable end-to-end example is
[`examples/distributed_index`](../examples/distributed_index).

## Why protobuf, not JSON

`Transaction` and `IndexMetadata` each carry **two** representations:

- an opaque, **lossless protobuf** encoding (the exact form Lance persists):
  this is what you ship between machines and what a commit consumes, and
- a JSON *view* for inspection (typed accessors + a `Raw json.RawMessage`).

Lance's `Transaction`/`Operation` and index-metadata types are
protobuf-backed, not serde/JSON. The JSON view is lossy (it drops details a
commit needs), so **always round-trip via `Bytes()`**, never by
re-serializing the JSON view. Ship `txn.Bytes()` / `seg.Bytes()` to the
driver and reconstruct nothing. The commit APIs take the metadata objects (or
their bytes) directly.

## Distributed write

### 1. Workers write fragments

Each worker writes its disjoint slice as new, **uncommitted** fragments and
gets back a `Transaction`. Nothing is committed yet.

```go
txn, err := lance.WriteFragments(ctx, uri, rdr,
	lance.WithFragmentsMode(lance.WriteModeAppend))
// txn.Operation().Type, txn.ReadVersion(), txn.Bytes() (ship this)
```

Mode semantics: **read these carefully**, they decide whether commits
compose:

- `WriteModeCreate` (the default) writes an **Overwrite** transaction that
  *creates* the dataset. Two Overwrites to one fresh URI cannot both apply.
- `WriteModeAppend` writes an **Append** transaction against an existing
  dataset (the distributed-append case). The dataset (schema + v1) must
  already exist. Seed it with a normal `lance.Write` (or one worker's
  committed Overwrite) first.

As with `Write`, `rdr`'s batches are exported across the Arrow C Data
Interface and **must be C-allocated** (`lance.Allocator()`). See
[memory.md](memory.md).

### 2. Driver commits

Build a commit against the URI and execute it:

```go
// Single transaction -> one new version, returns an open handle:
ds, err := lance.NewCommit(uri).Execute(ctx, txn)

// Batch of Append transactions -> Lance merges them into ONE version:
versions, err := lance.NewCommit(uri).ExecuteBatch(ctx, []*lance.Transaction{txn1, txn2})
// len(versions) == 1: the single merged version
```

`ExecuteBatch` merges compatible **Append** transactions into a single
manifest (one new version), which is the efficient path for many-worker
appends. `Execute` commits a single transaction and returns a `*Dataset` at
the resulting version. `CommitBuilder` also exposes `UseStableRowIDs`,
`EnableV2ManifestPaths`, `Detached`, `MaxRetries`, `SkipAutoCleanup`,
`WithTransactionProperties`, `WithStorageFormat`, and `WithStorageOptions`.

For a version outside the dataset's lineage, set `Detached()` on the builder:
`Execute` then commits the transaction as a **detached** version and returns a
`*Dataset` handle checked out at it — `Version()` carries the detached bit and
`ManifestLocation`/`ReadTransaction` reflect the detached manifest. A detached
version never appears in `Versions`/`Transactions` and can never become
latest. `Dataset.ListDetachedManifests` enumerates them as `DetachedManifest`
(version, path, size).

The full pattern (base write → two workers append → `ExecuteBatch`) is proven
in `TestTwoWorkerDistributedWriteBatch` and reproduced in the example.

### Reading transactions back

```go
txn, err := ds.ReadTransaction(ctx)                 // the current version's txn (nil if none)
txn, err = ds.TransactionByVersion(ctx, v)          // a specific version's txn
summaries, err := ds.Transactions(ctx, 10)          // recent JSON summaries (no pb bytes)
manifest, err := ds.Manifest(ctx)                   // current manifest summary
```

`ReadTransaction`/`TransactionByVersion` return the protobuf-backed
`*Transaction` (re-committable). `Transactions` returns read-only JSON
summaries only.

### Constructed operations

Some operations can be built directly on the Go side and committed without a
`WriteFragments` round-trip:

```go
_, err := lance.CommitOperation(ctx, uri,
	lance.UpdateConfigOperation{ConfigUpserts: map[string]string{"team": "lance"}},
	readVersion)
_, err = lance.CommitOperation(ctx, uri, lance.RestoreOperation{Version: 3}, readVersion)
```

## Distributed index build

The goal: build one logical vector index across a large dataset by having
each worker index a fragment subset, then merge the segments. For the segments
to be comparable and mergeable, **every worker must pin the same IVF
centroids**. The driver mints them once and passes them to each worker.

### 1. Workers build per-fragment segments

```go
// The driver's shared centroids: a FixedSizeList<float32, dim> Arrow array
// (C-allocated). Pass them through VectorOptions.Centroids.
seg, err := ds.CreateIndexUncommitted(ctx, "vec",
	lance.IvfFlat{
		Partitions:    k,
		VectorOptions: lance.VectorOptions{Centroids: centroids},
	},
	lance.WithFragments(fragID),          // this worker's fragment subset
	lance.WithUncommittedName("vec_idx"), // all segments share the logical name
)
// seg.Name(), seg.UUID(), seg.View(), seg.Bytes() (ship this)
```

`CreateIndexUncommitted` builds the segment but does not commit it.
`WithFragments(...)` restricts the build to a worker's slice.
`WithUncommittedName` sets the logical index name. Other options:
`WithIndexUUID`, `WithTrain(bool)`, `WithUncommittedTransactionProperties`.

> On `WithTrain`: when you supply shared centroids the segments are already
> comparable, and the proven path (`TestDistributedIVFIndex`, and the example)
> builds them **without** calling `WithTrain(false)`. Use `WithTrain(false)`
> only if you have a specific reason to force the provided centroids verbatim.
> If in doubt, omit it.

### 2. Driver merges and commits

```go
merged, err := ds.MergeIndexSegments(ctx, []*lance.IndexMetadata{seg1, seg2})
if err != nil { ... }
err = ds.CommitIndexSegments(ctx, "vec_idx", "vec", []*lance.IndexMetadata{merged})
```

`MergeIndexSegments` combines the per-fragment segments into one. Then
`CommitIndexSegments` commits it as a single logical index over the column,
advancing the dataset handle. After the commit the index covers all indexed
fragments. A normal vector search uses it:

```go
rdr, err := ds.Scan().Nearest("vec", query, 5).Nprobes(k).Reader(ctx)
```

### Reading an index partition

```go
reader, err := ds.ReadIndexPartition(ctx, "vec_idx", partition, withVector)
```

streams the rows of a single IVF partition (set `withVector` to include the
stored vectors), useful for inspecting or re-processing index contents.

## Compaction planning

`PlanCompaction` is the planning half of a distributed compaction (the
"driver" step). It takes the same typed `CompactionOptions` struct as
`CompactFiles` and returns a typed `CompactionPlan`: one independent
`CompactionTask` per group of fragments to rewrite, plus the dataset version
the plan was computed against.

```go
plan, err := ds.PlanCompaction(ctx, lance.CompactionOptions{
	TargetRowsPerFragment: 200,
})
// plan.Tasks       []lance.CompactionTask — one shippable unit of work each
// plan.ReadVersion uint64                — version the plan was computed against
// plan.Raw         json.RawMessage       — full plan JSON (escape hatch; also
//                                          carries the effective "options")
```

Each `CompactionTask` carries its contents as opaque JSON
(`task.Payload json.RawMessage`): the task data mirrors the engine's internal
fragment representation, which this package does not otherwise model. Tasks
marshal/unmarshal verbatim, so a task can be shipped to a worker and back
losslessly. `opts.DeferIndexRemap` is a `CompactFiles`-only knob; the planner
ignores it.

> **Deferred:** distributed compaction *task execution* and *result commit*
> are not yet exposed. For non-distributed compaction use
> `Dataset.CompactFiles` (see [usage.md](usage.md)).

## See also

- [usage.md](usage.md): single-node writes, versioning, `CompactFiles`.
- [indexes.md](indexes.md): index types and the vector build parameters
  (`VectorOptions`, centroids/codebook).
- [memory.md](memory.md): the C-allocator rule for the writer/centroids
  arrays and the ownership of returned byte buffers.
