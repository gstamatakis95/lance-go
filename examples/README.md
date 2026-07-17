# lance-go examples

Small runnable programs demonstrating the lance-go API. You need the native
library first. From a checkout of this repo, build it from source:

```sh
make rust   # from the repository root
```

Use `make rust`, not `make artifacts` / `scripts/download-artifacts.sh`, to
run the in-repo examples: prebuilt release artifacts only match published
releases, so on a development checkout they can lack this branch's new
exports, and the download script overwrites the checked-in
`include/lance_go.h`, which then fails `make header-check`. The prebuilt-
artifacts path (`make artifacts`) is for *external* consumers of the
published `lance-go` module — see the
[main README](../README.md#quickstart-prebuilt-artifacts).

Then run any example with `go run ./examples/<name>`.

- `write_scan`: write a dataset to the local filesystem, scan it back, and run a filtered scan.
- `vector_search`: build an IVF_PQ vector index and run nearest-neighbor searches, including a prefiltered search.
- `fts`: build an inverted (full-text search) index and query it with MatchQuery and PhraseQuery.
- `versioning`: append new versions, time-travel with checkout, and manage tags.
- `object_store`: write/scan a dataset on any object store. Takes `LANCE_URI` and `LANCE_STORAGE_OPTIONS` from the environment (works with the emulators from `make object-store-up`).
- `sql_query`: run SQL queries (filter/project and aggregate) over a dataset with `Dataset.SQL`.
- `take_blobs`: write a Lance blob column (`LargeBinary` tagged `lance-encoding:blob=true`) and read blob bytes back with `TakeBlobs`: whole blobs, sub-ranges, and cursor seeks.
- `distributed_index`: distributed write (two workers `WriteFragments` → batch commit) and distributed index build (per-fragment segments → merge → commit), then a vector search over the committed index.
- `plugincache`: the cache building blocks: a `CacheBackend` (external index cache on a `Session`) and an `ObjectStoreCache` (byte-range cache), both as swappable Go interfaces. See [docs/caching.md](../docs/caching.md).
- `observability`: attach OpenTelemetry providers via `WithWriteObservability`/`WithObservability` and print the emitted spans and metrics with the stdout exporters. Swap the exporter for OTLP to ship the same telemetry to Datadog with no change to lance-go.
- `schema_evolution`: evolve a dataset's schema with `AddColumnsSQL`, `AlterColumns` (rename + cast), `DropColumns`, and `Merge`, printing the schema after each step.
- `maintenance`: create a dataset with several small appends (many fragments), then compact them with `CompactFiles` and reclaim old versions with `CleanupOldVersions`, printing fragment/version counts before and after.

Records written through `lance.Write` must be allocated with a C-backed
Arrow allocator (`lance.Allocator()`), because their buffers are
exported across the Arrow C Data Interface. All examples do this.
