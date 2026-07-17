# Indexes and search

lance-go exposes every Lance index type through typed configs passed to
`CreateIndex`, plus vector and full-text search through the `Scan` builder.

```go
err := ds.CreateIndex(ctx, "column", cfg,
	lance.WithIndexName("my_idx"), // default: "<column>_idx"
	lance.WithReplace(true),       // default false: fail if the name is taken
)
```

`CreateIndex` commits a new dataset version and the handle tracks it.
Zero-valued config fields always mean "use the Lance default".

## Scalar indexes

Accelerate SQL filters (`Filter`, `CountRows`, delete/update predicates).

| Config | Index | Use for |
| --- | --- | --- |
| `lance.BTree{}` | BTREE | high-cardinality columns, range and equality predicates |
| `lance.Bitmap{}` | BITMAP | low-cardinality columns (enums, flags) |
| `lance.LabelList{}` | LABEL_LIST | list-of-labels columns (`array_has_any` / `array_has_all`) |
| `lance.NGram{}` | NGRAM | substring `contains()` predicates on strings |
| `lance.FM{}` | FM | exact substring `contains()` matches (FM-index) |
| `lance.ZoneMap{}` | ZONEMAP | per-zone min/max pruning on large scans |
| `lance.BloomFilter{}` | BLOOMFILTER | equality point-lookups without storing values |
| `lance.RTree{}` | RTREE | spatial predicates on a geometry (geoarrow) column |
| `lance.JSONIndex{}` | JSON | index a path within a JSON column via a target scalar type |

`Bitmap`, `LabelList`, `NGram` and `FM` take no parameters. The zero
struct is the whole config. The rest have optional knobs (zero = Lance
default):

| Config | Fields |
| --- | --- |
| `BTree` | `ZoneSize` (rows per BTree zone) |
| `ZoneMap` | `RowsPerZone` (rows per zone) |
| `BloomFilter` | `NumberOfItems` (default 8192), `Probability` (target FPR, default 0.00057) |
| `RTree` | `PageSize` (rows per index page, default 4096), requires a geometry-typed column |
| `JSONIndex` | `Path` (required, e.g. `"$.user.name"`), `TargetIndexType` (required, e.g. `"btree"`), `TargetParams` (optional JSON string) |

```go
err := ds.CreateIndex(ctx, "user_id", lance.BTree{})
err = ds.CreateIndex(ctx, "status", lance.Bitmap{})
err = ds.CreateIndex(ctx, "geom", lance.RTree{})
err = ds.CreateIndex(ctx, "doc", lance.JSONIndex{Path: "$.user.id", TargetIndexType: "btree"})
```

## Vector indexes

All vector configs share `Partitions` (IVF partition count, `0` = auto:
roughly `sqrt(num_rows)`) and `Distance` (`lance.L2` default, `lance.Cosine`,
`lance.Dot`, `lance.Hamming` for binary vectors).

| Config | Index | Extra fields (defaults) |
| --- | --- | --- |
| `lance.IvfFlat{}` | IVF_FLAT | none (exact distances within probed partitions) |
| `lance.IvfPq{}` | IVF_PQ | `Bits` (8) per PQ centroid, `SubVectors` (16, must divide the vector dimension), `MaxIterations` (50) k-means cap |
| `lance.IvfSq{}` | IVF_SQ | `Bits` (8) scaling range, `SampleRate` (256) |
| `lance.IvfRq{}` | IVF_RQ | `Bits` (1) per dimension, `RotationType` (`"fast"`, or `"matrix"`) |
| `lance.IvfHnswFlat{}` | IVF_HNSW_FLAT | `M` (20) connections/node, `EfConstruction` (150) |
| `lance.IvfHnswPq{}` | IVF_HNSW_PQ | PQ fields + HNSW fields as above |
| `lance.IvfHnswSq{}` | IVF_HNSW_SQ | SQ `Bits` + HNSW fields as above |

**IVF_RQ note:** the default `Bits` is 1 (RaBitQ binary codes). With
`num_bits = 1` the vector dimension must be a multiple of 8.

```go
err := ds.CreateIndex(ctx, "embedding", lance.IvfPq{
	Partitions: 256,
	SubVectors: 16, // dim % 16 == 0
	Distance:   lance.Cosine,
})
```

Indexes train on the existing rows. Build them after loading a
representative sample (k-means needs enough rows per partition).

#### Advanced vector build knobs (`VectorOptions`)

Every vector config embeds `VectorOptions` for shared advanced knobs (each
field's zero value = Lance default):

```go
err := ds.CreateIndex(ctx, "embedding", lance.IvfPq{
	SubVectors: 16,
	VectorOptions: lance.VectorOptions{
		TargetPartitionSize: 500,   // derive partition count from a target size
		Centroids:           cents, // pre-computed IVF centroids (Arrow array, C-allocated)
		Codebook:            cb,     // pre-computed PQ codebook (PQ variants)
		Retrain:             false,  // retrain the provided centroids vs. use verbatim
	},
})
```

`Centroids` / `Codebook` are Arrow arrays. Supplying shared centroids is the
basis of the distributed index build (see [distributed.md](distributed.md)).
Like all data crossing the boundary, they must be C-allocated
([memory.md](memory.md)).

### Vector search

```go
rdr, err := ds.Scan().
	Columns("id", "title").
	Filter("category = 'news'"). // combine with Prefilter(true) to filter before ANN
	Prefilter(true).
	Nearest("embedding", queryVec, 10). // []float32, k
	Metric(lance.Cosine).      // default: the metric the index was built with (L2 without an index)
	Nprobes(32).               // exact partitions to probe (sets min and max)
	MinimumNprobes(16).        // or bound them separately
	MaximumNprobes(64).
	Refine(2).                 // re-rank factor*k candidates with exact distances
	Ef(64).                    // HNSW search list size
	UseIndex(true).            // false forces a flat (exact) scan
	FastSearch().              // search only indexed rows, skip unindexed tail
	Reader(ctx)
```

Results gain a `_distance` column and arrive in ascending-distance order.
`NearestArrow` accepts an arbitrary Arrow query array
(Float16/Float32/Float64/UInt8, or a (FixedSize)List of those for
multivector/batch queries). Its buffers must be C-allocated and stay valid
until the terminal call returns.

To see whether a search actually used the index (and what it cost),
`Explain`/`AnalyzePlan` show the plan, and `WithScanStats(fn)` reports
per-scan execution counters — index partitions loaded, index comparisons,
I/O — when the search runs via `Reader`/`All` or `Batch` (in lance 8.0.0
`CountRows`/`Explain`/`AnalyzePlan`/`AnalyzeCountPlan` accept the option but
deliver no report; see
[observability.md](observability.md#per-scan-execution-stats-withscanstats)).

## Full-text search (Inverted index)

```go
err := ds.CreateIndex(ctx, "body", lance.Inverted{
	WithPosition: true, // store token positions, required by PhraseQuery
})
```

`lance.Inverted` fields (zero value = Lance default):

| Field | Default | Meaning |
| --- | --- | --- |
| `BaseTokenizer` | `"simple"` | `"simple"`, `"whitespace"`, `"raw"`, `"ngram"`, `"icu"`, `"lindera/*"`, `"jieba/*"` |
| `Language` | `"English"` | stemming / stop-word language |
| `WithPosition` | `false` | store positions (enables phrase queries, larger index) |
| `LowerCase` | `true` | lower-case tokens (`*bool`) |
| `Stem` | `true` | apply stemming (`*bool`) |
| `RemoveStopWords` | `true` | remove stop words (`*bool`) |
| `AsciiFolding` | `true` | fold accented chars to ASCII (`*bool`) |
| `MaxTokenLength` | `40` | drop longer tokens (`*int`, point at 0 for no limit) |
| `CustomStopWords` | built-in list | replaces the language's stop words |
| `NgramMinLength` / `NgramMaxLength` | `3` / `3` | `"ngram"` tokenizer only |
| `NgramPrefixOnly` | `false` | `"ngram"` tokenizer: prefix n-grams only |

The `"icu"` tokenizer (Unicode dictionary-based word segmentation, good
for CJK and mixed-script text) is always available: lance compiles the
icu4x segmenter in unconditionally, with its data embedded (no dictionary
download needed). With `"icu"`, `RemoveStopWords` uses the union of all
built-in stop-word lists rather than the single `Language`.

The shim is additionally compiled with the `tokenizer-lindera` (Japanese)
and `tokenizer-jieba` (Chinese) features, so `"lindera/<dict>"` and
`"jieba/<dict>"` tokenizers are available. They load their dictionaries at
runtime from Lance's language-model home directory: the
`LANCE_LANGUAGE_MODEL_HOME` environment variable if set, otherwise
`<user data dir>/lance/language_models` (`~/.local/share/lance/language_models`
on Linux, `~/Library/Application Support/lance/language_models` on macOS).

### FTS queries

```go
// Simple match (terms OR'd by default):
rdr, err := ds.Scan().
	FullTextSearch(lance.MatchQuery{Terms: "quick brown fox"}).
	Reader(ctx)

// All options:
q := lance.MatchQuery{
	Column:        "body",              // empty: resolved via WithFtsColumns / the only indexed column
	Terms:         "quick brwn fox",
	Operator:      lance.FtsOperatorAnd, // default Or
	Fuzziness:     1,                    // max edit distance; lance.FuzzinessAuto adapts per term
	MaxExpansions: 50,                   // fuzzy expansion cap (default 50)
	PrefixLength:  2,                    // leading chars exempt from fuzzing
	Boost:         2.0,                  // score multiplier (default 1.0)
}

// Phrase (index must have WithPosition: true):
p := lance.PhraseQuery{Column: "body", Terms: "brown fox", Slop: 1}

// Compose:
b := lance.BooleanQuery{
	Must:    []lance.FtsQuery{q},
	MustNot: []lance.FtsQuery{lance.MatchQuery{Terms: "lazy"}},
	Should:  []lance.FtsQuery{p},
}

// Demote instead of exclude:
boost := lance.BoostQuery{Positive: q, Negative: p, NegativeBoost: 0.3} // default 0.5

// Same terms across several columns with per-column boosts:
mm := lance.MultiMatchQuery{Queries: []lance.MatchQuery{
	{Column: "title", Terms: "fox", Boost: 3},
	{Column: "body", Terms: "fox"},
}}

rdr, err = ds.Scan().
	FullTextSearch(b,
		lance.WithFtsColumns("body"), // when queries don't name columns
		lance.WithFtsLimit(100),
		lance.WithWandFactor(1.5),    // >1 trades recall for speed (default 1.0)
	).
	Reader(ctx)
```

FTS results gain a `_score` column and arrive in descending-relevance
order.

## Managing indexes

```go
infos, err := ds.ListIndices(ctx)      // name, UUID, covered fields, versions
stats, err := ds.IndexStatistics(ctx, "body_idx") // Lance-defined JSON document
err = ds.DropIndex(ctx, "body_idx")    // commits a new version
err = ds.PrewarmIndex(ctx, "vec_idx")  // load into memory ahead of queries
```

### Rich descriptions and loading

`DescribeIndices` returns per-type details, coverage and size **without**
loading indices into memory. The `LoadIndex*` family loads and returns index
info by UUID or name:

```go
descs, err := ds.DescribeIndices(ctx, nil) // all indices; or pass an *IndexCriteria
// descs[i]: Name, IndexType, RowsIndexed, FieldIDs, Details (JSON), Segments, ...
descs, err = ds.DescribeIndices(ctx, &lance.IndexCriteria{MustSupportFTS: true})

info, err := ds.LoadIndexByName(ctx, "vec_idx")
infos, err := ds.LoadIndicesByName(ctx, "vec_idx") // all segments of the logical index
info, err = ds.LoadIndex(ctx, uuid)
```

### Prewarm with options, remap

```go
// Prewarm an FTS index including token positions (needed for phrase queries):
err := ds.PrewarmIndexWithOptions(ctx, "body_idx", lance.PrewarmFTS{WithPosition: true})

// Remap an index after row-address changes (e.g. post-compaction):
err = ds.RemapIndex(ctx, "vec", "vec_idx")
```

### Keeping indexes fresh

Rows written after an index build are not covered by it (searches still see
them via the unindexed tail unless `FastSearch()` is set). Periodically:

```go
err := ds.OptimizeIndices(ctx,
	lance.WithMergeIndices(1),               // merge deltas into the newest index (default)
	lance.WithIndexNames("vec_idx"),         // default: all indexes
)
// or fully retrain (v3 vector indexes only):
err = ds.OptimizeIndices(ctx, lance.WithRetrain())
```

`IndexStatistics` reports indexed vs. unindexed row counts. Use it to
decide when to optimize.

## Column encoding and compression

Independent of indexing, you can control how a column is encoded and
compressed on disk by attaching Lance encoder hints to a column's Arrow field
metadata before `Write`. The `encoding.go` helpers set these keys for you and
preserve any existing field metadata. The keys are exported as constants (they
mirror the `lance-encoding` crate's metadata keys and lance-arrow's blob key):

| Constant | Key | Values |
| --- | --- | --- |
| `CompressionMetaKey` | `lance-encoding:compression` | `CompressionNone` (`"none"`), `CompressionLZ4` (`"lz4"`), `CompressionZstd` (`"zstd"`), `CompressionFsst` (`"fsst"`) |
| `CompressionLevelMetaKey` | `lance-encoding:compression-level` | an integer level |
| `StructuralEncodingMetaKey` | `lance-encoding:structural-encoding` | `StructuralEncodingMiniblock` (`"miniblock"`), `StructuralEncodingFullzip` (`"fullzip"`) |
| `BSSMetaKey` | `lance-encoding:bss` | `BSSOff` (`"off"`), `BSSOn` (`"on"`), `BSSAuto` (`"auto"`) |
| `BlobMetaKey` | `lance-encoding:blob` | `"true"` to mark a `large_binary` column as a blob column |

The helpers return a new `arrow.Field` with the hint applied:

```go
// Zstd-compress a string column at level 3:
f := lance.SetFieldCompression(field, lance.CompressionZstd,
	lance.WithCompressionLevel(3))

// Pick a structural encoding (miniblock for small values, fullzip for large):
f = lance.SetFieldStructuralEncoding(f, lance.StructuralEncodingFullzip)

// Store a large_binary column's values as external blobs (see take_blobs):
blobField := lance.MarkBlobColumn(blobField)
```

`SetFieldCompression` also accepts `WithRLEThreshold` and other
`CompressionOption`s. To set arbitrary encoder metadata directly, use
`SetFieldMetadata(field, map[string]string{...})`. Build the schema from the
returned fields, then `Write` as usual. The hints are advisory. The Lance
encoder falls back to its defaults for any column without them.
