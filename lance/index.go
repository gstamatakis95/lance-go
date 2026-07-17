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
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"go.opentelemetry.io/otel/attribute"
)

// DistanceType selects the metric used to compare vectors.
type DistanceType int

const (
	// L2 is Euclidean distance (the default).
	L2 DistanceType = iota
	// Cosine is cosine distance.
	Cosine
	// Dot is negative dot-product distance.
	Dot
	// Hamming is Hamming distance (for binary vectors).
	Hamming
)

// String returns the metric's wire representation ("l2", "cosine", "dot", or
// "hamming").
func (d DistanceType) String() string {
	switch d {
	case L2:
		return "l2"
	case Cosine:
		return "cosine"
	case Dot:
		return "dot"
	case Hamming:
		return "hamming"
	default:
		return fmt.Sprintf("DistanceType(%d)", int(d))
	}
}

// IndexConfig selects the type of index to create and its build parameters.
// Implementations: BTree, Bitmap, LabelList, NGram, ZoneMap, BloomFilter,
// FM, RTree, JSONIndex, Inverted, IvfFlat, IvfPq, IvfSq, IvfRq, IvfHnswFlat,
// IvfHnswPq, IvfHnswSq. Zero values map to the Lance defaults.
//
// The interface methods are unexported, so IndexConfig cannot be implemented
// outside this package, so the set of index types is closed.
type IndexConfig interface {
	// indexTypeName returns the FFI string index-type identifier (a builtin
	// name like "btree"/"ivf_pq", or a scalar plugin name like "json").
	indexTypeName() string
	// indexParams returns the value marshaled into the FFI params_json
	// document, or nil for "all defaults".
	indexParams() (any, error)
	// arrowParams returns the optional pre-computed IVF centroids and PQ
	// codebook arrays passed across the Arrow C Data Interface (both nil for
	// non-vector index types). The returned arrays are borrowed, not
	// retained.
	arrowParams() (centroids, codebook arrow.Array)
}

// noArrowParams is embedded by index configs that pass no Arrow arrays.
type noArrowParams struct{}

func (noArrowParams) arrowParams() (arrow.Array, arrow.Array) { return nil, nil }

// BTree configures a BTree index, suited to high-cardinality scalar columns.
type BTree struct {
	// ZoneSize is the number of rows per BTree zone (0 = Lance default).
	ZoneSize uint64
	noArrowParams
}

func (BTree) indexTypeName() string { return "btree" }

func (c BTree) indexParams() (any, error) {
	return scalarParams(map[string]any{"zone_size": c.ZoneSize}), nil
}

// Bitmap configures a Bitmap index, suited to low-cardinality columns.
type Bitmap struct{ noArrowParams }

func (Bitmap) indexTypeName() string     { return "bitmap" }
func (Bitmap) indexParams() (any, error) { return nil, nil }

// LabelList configures a LabelList index over list-of-labels columns.
type LabelList struct{ noArrowParams }

func (LabelList) indexTypeName() string     { return "labellist" }
func (LabelList) indexParams() (any, error) { return nil, nil }

// NGram configures an NGram index, which accelerates `contains` filters.
type NGram struct{ noArrowParams }

func (NGram) indexTypeName() string     { return "ngram" }
func (NGram) indexParams() (any, error) { return nil, nil }

// ZoneMap configures a ZoneMap index.
type ZoneMap struct {
	// RowsPerZone is the number of rows per zone (0 = Lance default).
	RowsPerZone uint64
	noArrowParams
}

func (ZoneMap) indexTypeName() string { return "zonemap" }

func (c ZoneMap) indexParams() (any, error) {
	return scalarParams(map[string]any{"rows_per_zone": c.RowsPerZone}), nil
}

// BloomFilter configures a BloomFilter index.
type BloomFilter struct {
	// NumberOfItems is the expected number of distinct items the filter is
	// sized for (0 = Lance default, 8192).
	NumberOfItems uint64
	// Probability is the target false-positive rate (0 = Lance default,
	// 0.00057).
	Probability float64
	noArrowParams
}

func (BloomFilter) indexTypeName() string { return "bloomfilter" }

func (c BloomFilter) indexParams() (any, error) {
	m := map[string]any{}
	if c.NumberOfItems != 0 {
		m["number_of_items"] = c.NumberOfItems
	}
	if c.Probability != 0 {
		m["probability"] = c.Probability
	}
	if len(m) == 0 {
		return nil, nil
	}
	return map[string]any{"params": m}, nil
}

// FM configures an FM-index, which accelerates substring `contains` filters
// with exact matches.
type FM struct{ noArrowParams }

func (FM) indexTypeName() string     { return "fm" }
func (FM) indexParams() (any, error) { return nil, nil }

// RTree configures an RTree spatial index over a geometry column. Requires a
// geometry-typed column (geoarrow). Creation fails on a non-geometry column.
type RTree struct {
	// PageSize is the number of rows per index page (0 = Lance default,
	// 4096).
	PageSize uint32
	noArrowParams
}

func (RTree) indexTypeName() string { return "rtree" }

func (c RTree) indexParams() (any, error) {
	m := map[string]any{}
	if c.PageSize != 0 {
		m["page_size"] = c.PageSize
	}
	if len(m) == 0 {
		return nil, nil
	}
	return map[string]any{"params": m}, nil
}

// JSONIndex configures a JSON scalar index: it indexes the value at a JSON
// path within a JSON column, delegating to a target scalar index type. The
// column must be a JSON column (an arrow.json / lance.json extension field,
// i.e. LargeBinary JSONB storage).
type JSONIndex struct {
	// Path is the JSON path to index, e.g. "x" or "$.user.name".
	Path string
	// TargetIndexType is the underlying scalar index type built over the
	// extracted values, e.g. "btree" or "bitmap".
	TargetIndexType string
	// TargetParams is an optional JSON parameter document (as a string)
	// passed to the target index type, empty for its defaults.
	TargetParams string
	noArrowParams
}

func (JSONIndex) indexTypeName() string { return "json" }

func (c JSONIndex) indexParams() (any, error) {
	if c.Path == "" {
		return nil, fmt.Errorf("%w: JSONIndex.Path must be set", ErrInvalidArgument)
	}
	if c.TargetIndexType == "" {
		return nil, fmt.Errorf("%w: JSONIndex.TargetIndexType must be set", ErrInvalidArgument)
	}
	inner := map[string]any{
		"target_index_type": c.TargetIndexType,
		"path":              c.Path,
	}
	if c.TargetParams != "" {
		inner["target_index_parameters"] = c.TargetParams
	}
	return map[string]any{"params": inner}, nil
}

// scalarParams wraps a scalar-index params sub-document, dropping zero-valued
// uint64 fields so the Lance defaults apply. Returns nil when nothing is set.
func scalarParams(fields map[string]any) any {
	inner := map[string]any{}
	for k, v := range fields {
		if n, ok := v.(uint64); ok {
			if n != 0 {
				inner[k] = n
			}
			continue
		}
		inner[k] = v
	}
	if len(inner) == 0 {
		return nil
	}
	return map[string]any{"params": inner}
}

// Inverted configures an inverted (full-text search) index. The zero value
// uses the Lance defaults: "simple" tokenizer, English, lower-casing,
// stemming, stop-word removal and ascii folding enabled, no positions.
type Inverted struct {
	// BaseTokenizer is "simple" (default), "whitespace", "raw", "ngram",
	// "icu", "lindera/*" or "jieba/*".
	BaseTokenizer string
	// Language for stemming and stop words, e.g. "English" (default).
	Language string
	// WithPosition stores token positions, enabling phrase queries.
	WithPosition bool
	// LowerCase lower-cases tokens (default true).
	LowerCase *bool
	// Stem applies stemming (default true).
	Stem *bool
	// RemoveStopWords removes stop words (default true).
	RemoveStopWords *bool
	// AsciiFolding converts accented characters to their ascii equivalent
	// (default true).
	AsciiFolding *bool
	// MaxTokenLength removes tokens longer than the limit (default 40).
	// Point at 0 for "no limit".
	MaxTokenLength *int
	// CustomStopWords replaces the language's built-in stop-word list.
	CustomStopWords []string
	// NgramMinLength is the minimum n-gram length ("ngram" tokenizer only,
	// default 3).
	NgramMinLength uint32
	// NgramMaxLength is the maximum n-gram length ("ngram" tokenizer only,
	// default 3).
	NgramMaxLength uint32
	// NgramPrefixOnly generates only prefix n-grams ("ngram" tokenizer
	// only).
	NgramPrefixOnly bool
	noArrowParams
}

func (Inverted) indexTypeName() string { return "inverted" }

func (c Inverted) indexParams() (any, error) {
	m := map[string]any{}
	if c.BaseTokenizer != "" {
		m["base_tokenizer"] = c.BaseTokenizer
	}
	if c.Language != "" {
		m["language"] = c.Language
	}
	if c.WithPosition {
		m["with_position"] = true
	}
	if c.LowerCase != nil {
		m["lower_case"] = *c.LowerCase
	}
	if c.Stem != nil {
		m["stem"] = *c.Stem
	}
	if c.RemoveStopWords != nil {
		m["remove_stop_words"] = *c.RemoveStopWords
	}
	if c.AsciiFolding != nil {
		m["ascii_folding"] = *c.AsciiFolding
	}
	if c.MaxTokenLength != nil {
		m["max_token_length"] = *c.MaxTokenLength
	}
	if c.CustomStopWords != nil {
		m["custom_stop_words"] = c.CustomStopWords
	}
	if c.NgramMinLength != 0 {
		m["ngram_min_length"] = c.NgramMinLength
	}
	if c.NgramMaxLength != 0 {
		m["ngram_max_length"] = c.NgramMaxLength
	}
	if c.NgramPrefixOnly {
		m["ngram_prefix_only"] = true
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// VectorOptions holds advanced build knobs shared by every vector index
// type. Embedded (zero-valued) in each vector config. Every field's zero
// value means "use the Lance default". Set fields via the embedded struct,
// e.g. lance.IvfPq{VectorOptions: lance.VectorOptions{TargetPartitionSize: 500}}.
type VectorOptions struct {
	// TargetPartitionSize derives the IVF partition count from a target
	// per-partition size, preferred over Partitions when set (>0).
	TargetPartitionSize uint
	// Retrain retrains the provided Centroids instead of using them verbatim.
	Retrain bool
	// StreamingSampleRate sets the per-step sample rate for streaming IVF
	// k-means training (>0 enables streaming training).
	StreamingSampleRate uint
	// StreamingCoresetRate sets the coreset rate for streaming IVF training.
	StreamingCoresetRate uint
	// StreamingRefinePasses adds extra streaming Lloyd refinement passes.
	StreamingRefinePasses uint
	// ShufflePartitionBatches controls shuffle batches per partition
	// (advanced).
	ShufflePartitionBatches uint
	// ShufflePartitionConcurrency controls concurrent shuffle partitions
	// (advanced).
	ShufflePartitionConcurrency uint
	// PrecomputedPartitionsFile is a precomputed row_id -> partition_id file.
	PrecomputedPartitionsFile string
	// StorageOptions configures the object store used to load precomputed
	// partitions.
	StorageOptions map[string]string
	// KmeansRedos runs PQ k-means this many times and keeps the best result
	// (PQ variants only, 0 = default 1).
	KmeansRedos uint
	// PrefetchDistance sets the HNSW graph-build prefetch distance
	// (IVF_HNSW_* variants only).
	PrefetchDistance uint
	// IndexFileVersion selects the index file format: "legacy" or "v3"
	// (empty = the index type's default).
	IndexFileVersion string
	// SkipTranspose skips transpose/packing for PQ and RQ storage when set.
	SkipTranspose *bool
	// RuntimeHints are optional build preferences stored in the manifest
	// (reverse-DNS keys, e.g. "lance.ivf.max_iters").
	RuntimeHints map[string]string
	// Centroids is an optional pre-computed IVF centroids array
	// (FixedSizeList of the column's vector type/dim). Passed across the
	// Arrow C Data Interface.
	Centroids arrow.Array
	// Codebook is an optional pre-computed PQ codebook array (PQ variants
	// only). Passed across the Arrow C Data Interface.
	Codebook arrow.Array
}

// apply merges the non-default VectorOptions fields into the params map.
func (o VectorOptions) apply(m map[string]any) {
	if o.TargetPartitionSize != 0 {
		m["target_partition_size"] = o.TargetPartitionSize
	}
	if o.Retrain {
		m["retrain"] = true
	}
	if o.StreamingSampleRate != 0 {
		m["streaming_sample_rate"] = o.StreamingSampleRate
	}
	if o.StreamingCoresetRate != 0 {
		m["streaming_coreset_rate"] = o.StreamingCoresetRate
	}
	if o.StreamingRefinePasses != 0 {
		m["streaming_refine_passes"] = o.StreamingRefinePasses
	}
	if o.ShufflePartitionBatches != 0 {
		m["shuffle_partition_batches"] = o.ShufflePartitionBatches
	}
	if o.ShufflePartitionConcurrency != 0 {
		m["shuffle_partition_concurrency"] = o.ShufflePartitionConcurrency
	}
	if o.PrecomputedPartitionsFile != "" {
		m["precomputed_partitions_file"] = o.PrecomputedPartitionsFile
	}
	if len(o.StorageOptions) != 0 {
		m["storage_options"] = o.StorageOptions
	}
	if o.KmeansRedos != 0 {
		m["kmeans_redos"] = o.KmeansRedos
	}
	if o.IndexFileVersion != "" {
		m["index_file_version"] = o.IndexFileVersion
	}
	if o.SkipTranspose != nil {
		m["skip_transpose"] = *o.SkipTranspose
	}
	if len(o.RuntimeHints) != 0 {
		m["runtime_hints"] = o.RuntimeHints
	}
}

func (o VectorOptions) arrowParams() (arrow.Array, arrow.Array) {
	return o.Centroids, o.Codebook
}

// vectorParams assembles the shared vector-index params_json document,
// omitting zero fields so the Lance defaults apply.
func vectorParams(distance DistanceType, opts VectorOptions, fields map[string]any) map[string]any {
	m := map[string]any{"metric": distance.String()}
	for k, v := range fields {
		switch n := v.(type) {
		case int:
			if n > 0 {
				m[k] = n
			}
		case string:
			if n != "" {
				m[k] = n
			}
		default:
			m[k] = v
		}
	}
	opts.apply(m)
	return m
}

// hnswParams assembles the optional "hnsw" sub-document.
func hnswParams(m, efConstruction int, prefetchDistance uint) map[string]any {
	h := map[string]any{}
	if m > 0 {
		h["m"] = m
	}
	if efConstruction > 0 {
		h["ef_construction"] = efConstruction
	}
	if prefetchDistance != 0 {
		h["prefetch_distance"] = prefetchDistance
	}
	if len(h) == 0 {
		return nil
	}
	return h
}

// IvfFlat configures an IVF_FLAT vector index (exact distances within
// probed partitions).
type IvfFlat struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	VectorOptions
}

func (IvfFlat) indexTypeName() string { return "ivf_flat" }

func (c IvfFlat) indexParams() (any, error) {
	return vectorParams(c.Distance, c.VectorOptions, map[string]any{"num_partitions": c.Partitions}), nil
}

// IvfPq configures an IVF_PQ vector index (product quantization).
type IvfPq struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Bits per PQ centroid (default 8).
	Bits int
	// SubVectors is the number of PQ sub-vectors (default 16). Must divide
	// the vector dimension.
	SubVectors int
	// MaxIterations caps k-means training iterations (default 50).
	MaxIterations int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	VectorOptions
}

func (IvfPq) indexTypeName() string { return "ivf_pq" }

func (c IvfPq) indexParams() (any, error) {
	return vectorParams(c.Distance, c.VectorOptions, map[string]any{
		"num_partitions":  c.Partitions,
		"num_bits":        c.Bits,
		"num_sub_vectors": c.SubVectors,
		"max_iterations":  c.MaxIterations,
	}), nil
}

// IvfSq configures an IVF_SQ vector index (scalar quantization).
type IvfSq struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Bits of scaling range (default 8).
	Bits int
	// SampleRate for training (default 256).
	SampleRate int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	VectorOptions
}

func (IvfSq) indexTypeName() string { return "ivf_sq" }

func (c IvfSq) indexParams() (any, error) {
	return vectorParams(c.Distance, c.VectorOptions, map[string]any{
		"num_partitions": c.Partitions,
		"num_bits":       c.Bits,
		"sample_rate":    c.SampleRate,
	}), nil
}

// IvfRq configures an IVF_RQ vector index (RaBitQ quantization).
type IvfRq struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Bits per dimension (default 1).
	Bits int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	// RotationType is "fast" (default) or "matrix".
	RotationType string
	VectorOptions
}

func (IvfRq) indexTypeName() string { return "ivf_rq" }

func (c IvfRq) indexParams() (any, error) {
	return vectorParams(c.Distance, c.VectorOptions, map[string]any{
		"num_partitions": c.Partitions,
		"num_bits":       c.Bits,
		"rotation_type":  c.RotationType,
	}), nil
}

// IvfHnswFlat configures an IVF_HNSW_FLAT vector index.
type IvfHnswFlat struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	// M is the number of HNSW connections per node (default 20).
	M int
	// EfConstruction is the HNSW build-time candidate list size
	// (default 150).
	EfConstruction int
	VectorOptions
}

func (IvfHnswFlat) indexTypeName() string { return "ivf_hnsw_flat" }

func (c IvfHnswFlat) indexParams() (any, error) {
	m := vectorParams(c.Distance, c.VectorOptions, map[string]any{"num_partitions": c.Partitions})
	if h := hnswParams(c.M, c.EfConstruction, c.PrefetchDistance); h != nil {
		m["hnsw"] = h
	}
	return m, nil
}

// IvfHnswPq configures an IVF_HNSW_PQ vector index.
type IvfHnswPq struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Bits per PQ centroid (default 8).
	Bits int
	// SubVectors is the number of PQ sub-vectors (default 16).
	SubVectors int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	// M is the number of HNSW connections per node (default 20).
	M int
	// EfConstruction is the HNSW build-time candidate list size
	// (default 150).
	EfConstruction int
	VectorOptions
}

func (IvfHnswPq) indexTypeName() string { return "ivf_hnsw_pq" }

func (c IvfHnswPq) indexParams() (any, error) {
	m := vectorParams(c.Distance, c.VectorOptions, map[string]any{
		"num_partitions":  c.Partitions,
		"num_bits":        c.Bits,
		"num_sub_vectors": c.SubVectors,
	})
	if h := hnswParams(c.M, c.EfConstruction, c.PrefetchDistance); h != nil {
		m["hnsw"] = h
	}
	return m, nil
}

// IvfHnswSq configures an IVF_HNSW_SQ vector index.
type IvfHnswSq struct {
	// Partitions is the number of IVF partitions (0 = auto).
	Partitions int
	// Bits of scaling range (default 8).
	Bits int
	// Distance is the metric to index with (default L2).
	Distance DistanceType
	// M is the number of HNSW connections per node (default 20).
	M int
	// EfConstruction is the HNSW build-time candidate list size
	// (default 150).
	EfConstruction int
	VectorOptions
}

func (IvfHnswSq) indexTypeName() string { return "ivf_hnsw_sq" }

func (c IvfHnswSq) indexParams() (any, error) {
	m := vectorParams(c.Distance, c.VectorOptions, map[string]any{
		"num_partitions": c.Partitions,
		"num_bits":       c.Bits,
	})
	if h := hnswParams(c.M, c.EfConstruction, c.PrefetchDistance); h != nil {
		m["hnsw"] = h
	}
	return m, nil
}

// IndexInfo describes an index of a dataset.
type IndexInfo struct {
	// Name is the index name.
	Name string `json:"name"`
	// UUID uniquely identifies this index build.
	UUID string `json:"uuid"`
	// Fields are the dataset field ids covered by the index.
	Fields []int32 `json:"fields"`
	// DatasetVersion is the dataset version the index was last updated on.
	DatasetVersion uint64 `json:"dataset_version"`
	// IndexVersion is the index format version.
	IndexVersion int32 `json:"index_version"`
	// CreatedAt is the index creation time, if recorded.
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

// indexOptions collects CreateIndex options.
type indexOptions struct {
	name    string
	replace bool
}

// IndexOption configures CreateIndex.
type IndexOption func(*indexOptions)

// WithIndexName names the index (default "<column>_idx").
func WithIndexName(name string) IndexOption {
	return func(o *indexOptions) { o.name = name }
}

// WithReplace controls whether an existing index with the same name is
// replaced (default false: CreateIndex fails if the name is taken).
func WithReplace(replace bool) IndexOption {
	return func(o *indexOptions) { o.replace = replace }
}

// cStringArray marshals strs into a NULL-terminated C-string array. The
// caller must call the returned cleanup function.
func cStringArray(strs []string) (**C.char, func()) {
	n := len(strs) + 1
	arr := (**C.char)(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	slots := unsafe.Slice(arr, n)
	for i, s := range strs {
		slots[i] = C.CString(s)
	}
	// slots[n-1] is already NULL from calloc.
	return arr, func() {
		for _, p := range slots[:n-1] {
			C.free(unsafe.Pointer(p))
		}
		C.free(unsafe.Pointer(arr))
	}
}

// exportArrayPair exports arr across the Arrow C Data Interface into cVec /
// cSchema, returning them (or nil, nil for a nil array) plus a cleanup that
// releases the Go array. The C structs must be zero-initialized by the
// caller. The native side always takes ownership of the exported pair, even
// on error, so the producer is released exactly once.
func exportArrayPair(arr arrow.Array, cVec *C.struct_ArrowArray, cSchema *C.struct_ArrowSchema) (*C.struct_ArrowArray, *C.struct_ArrowSchema, func()) {
	if arr == nil {
		return nil, nil, func() {}
	}
	arr.Retain()
	cdata.ExportArrowArray(arr,
		(*cdata.CArrowArray)(unsafe.Pointer(cVec)),
		(*cdata.CArrowSchema)(unsafe.Pointer(cSchema)))
	return cVec, cSchema, arr.Release
}

// CreateIndex builds an index of the configured type on column. Upon
// success a new dataset version is committed and the handle tracks it.
func (d *Dataset) CreateIndex(ctx context.Context, column string, cfg IndexConfig, opts ...IndexOption) error {
	var o indexOptions
	for _, opt := range opts {
		opt(&o)
	}
	return datasetDo(ctx, d, "Dataset.CreateIndex", fmt.Sprintf("create index on %q", column),
		func(ctx context.Context, ptr *C.LanceDataset) error {
			params, err := cfg.indexParams()
			if err != nil {
				return err
			}
			var paramsJSON string
			if params != nil {
				data, err := json.Marshal(params)
				if err != nil {
					return fmt.Errorf("marshal index params: %w", err)
				}
				paramsJSON = string(data)
			}
			centroids, codebook := cfg.arrowParams()

			cColumns, freeColumns := cStringArray([]string{column})
			defer freeColumns()
			cType, freeType := cString(cfg.indexTypeName())
			defer freeType()
			cName, freeName := cString(o.name)
			defer freeName()
			cParams, freeParams := cString(paramsJSON)
			defer freeParams()

			// Export the optional Arrow training arrays. The C structs must be
			// zero-initialized (Go zeroes them). The native side always takes
			// ownership of the exported pair, even on error.
			var cCentVec, cCbVec C.struct_ArrowArray
			var cCentSchema, cCbSchema C.struct_ArrowSchema
			centVec, centSchema, freeCent := exportArrayPair(centroids, &cCentVec, &cCentSchema)
			defer freeCent()
			cbVec, cbSchema, freeCb := exportArrayPair(codebook, &cCbVec, &cCbSchema)
			defer freeCb()

			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_create_index_v2(ptr, cColumns, cType, cName, cParams,
					centVec, centSchema, cbVec, cbSchema, C.bool(o.replace), nil)
			})
		}, attribute.String("lance.column", column))
}

// ListIndices returns metadata for every index of the dataset.
func (d *Dataset) ListIndices(ctx context.Context) ([]IndexInfo, error) {
	return datasetOp(ctx, d, "Dataset.ListIndices", "list indices",
		func(ctx context.Context, ptr *C.LanceDataset) ([]IndexInfo, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_list_indices(ptr, &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var infos []IndexInfo
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &infos); err != nil {
				return nil, fmt.Errorf("decode index list: %w", err)
			}
			return infos, nil
		})
}

// IndexStatistics returns a Lance-defined JSON document describing the
// index named name (type, number of indexed/unindexed rows, ...).
func (d *Dataset) IndexStatistics(ctx context.Context, name string) (string, error) {
	return datasetOp(ctx, d, "Dataset.IndexStatistics", fmt.Sprintf("index statistics for %q", name),
		func(ctx context.Context, ptr *C.LanceDataset) (string, error) {
			cName, freeName := cString(name)
			defer freeName()
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_index_statistics(ptr, cName, &cJSON)
			}); err != nil {
				return "", err
			}
			defer C.lance_string_free(cJSON)
			return C.GoString(cJSON), nil
		}, attribute.String("lance.index_name", name))
}

// DropIndex removes the index named name. Upon success a new dataset
// version is committed and the handle tracks it. Fails with ErrNotFound if
// no index named name exists.
func (d *Dataset) DropIndex(ctx context.Context, name string) error {
	return datasetDo(ctx, d, "Dataset.DropIndex", fmt.Sprintf("drop index %q", name),
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cName, freeName := cString(name)
			defer freeName()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_drop_index(ptr, cName)
			})
		}, attribute.String("lance.index_name", name))
}

// optimizeConfig mirrors the options_json contract of
// lance_dataset_optimize_indices.
type optimizeConfig struct {
	NumIndicesToMerge     *int              `json:"num_indices_to_merge,omitempty"`
	IndexNames            []string          `json:"index_names,omitempty"`
	Retrain               bool              `json:"retrain,omitempty"`
	TransactionProperties map[string]string `json:"transaction_properties,omitempty"`
}

// OptimizeOption configures OptimizeIndices.
type OptimizeOption func(*optimizeConfig)

// WithMergeIndices merges the delta updates and the latest n indices into a
// single index (default 1: merge deltas into the most recent index).
func WithMergeIndices(n int) OptimizeOption {
	return func(cfg *optimizeConfig) { cfg.NumIndicesToMerge = &n }
}

// WithIndexNames restricts optimization to the named indices (default all).
func WithIndexNames(names ...string) OptimizeOption {
	return func(cfg *optimizeConfig) { cfg.IndexNames = names }
}

// WithRetrain retrains the whole index on the current data instead of
// merging deltas (v3 vector indices only).
func WithRetrain() OptimizeOption {
	return func(cfg *optimizeConfig) { cfg.Retrain = true }
}

// WithOptimizeTransactionProperties attaches key-value properties to the
// commit that optimization produces (stored in the transaction file, e.g. a
// job id).
func WithOptimizeTransactionProperties(props map[string]string) OptimizeOption {
	return func(cfg *optimizeConfig) { cfg.TransactionProperties = props }
}

// OptimizeIndices updates the dataset's indices to cover data written since
// they were built. Upon success a new dataset version is committed and the
// handle tracks it.
func (d *Dataset) OptimizeIndices(ctx context.Context, opts ...OptimizeOption) error {
	var cfg optimizeConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return datasetDo(ctx, d, "Dataset.OptimizeIndices", "optimize indices",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			// marshalOptions (dataset.go) returns an already-prefixed
			// "lance: marshal options: %w" error, which would double-prefix
			// if returned from inside this fn, so marshal inline instead.
			data, err := json.Marshal(&cfg)
			if err != nil {
				return fmt.Errorf("marshal options: %w", err)
			}
			cOpts, freeOpts := cString(string(data))
			defer freeOpts()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_optimize_indices(ptr, cOpts)
			})
		})
}

// PrewarmIndex loads the index named name into memory ahead of queries.
// Fails with ErrNotFound if no index named name exists.
func (d *Dataset) PrewarmIndex(ctx context.Context, name string) error {
	return datasetDo(ctx, d, "Dataset.PrewarmIndex", fmt.Sprintf("prewarm index %q", name),
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cName, freeName := cString(name)
			defer freeName()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_prewarm_index(ptr, cName)
			})
		}, attribute.String("lance.index_name", name))
}

// PrewarmOption selects the index-type-specific prewarm behavior for
// PrewarmIndexWithOptions. Implementations: PrewarmFTS.
type PrewarmOption interface {
	prewarmOptions() any
}

// PrewarmFTS prewarms an inverted (full-text) index.
type PrewarmFTS struct {
	// WithPosition additionally prewarms token positions along with the
	// posting lists (needed for phrase queries).
	WithPosition bool
}

func (o PrewarmFTS) prewarmOptions() any {
	return map[string]any{"fts": map[string]any{"with_position": o.WithPosition}}
}

// PrewarmIndexWithOptions loads the index named name into memory ahead of
// queries, applying index-type-specific prewarm options (e.g. PrewarmFTS).
// Fails with ErrNotFound if no index named name exists.
func (d *Dataset) PrewarmIndexWithOptions(ctx context.Context, name string, opt PrewarmOption) error {
	return datasetDo(ctx, d, "Dataset.PrewarmIndexWithOptions", fmt.Sprintf("prewarm index %q", name),
		func(ctx context.Context, ptr *C.LanceDataset) error {
			optsJSON, err := json.Marshal(opt.prewarmOptions())
			if err != nil {
				return fmt.Errorf("marshal prewarm options: %w", err)
			}
			cName, freeName := cString(name)
			defer freeName()
			cOpts, freeOpts := cString(string(optsJSON))
			defer freeOpts()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_prewarm_index_with_options(ptr, cName, cOpts)
			})
		}, attribute.String("lance.index_name", name))
}

// IndexDescription describes an index of a dataset (its type, coverage and
// details), as returned by DescribeIndices. It mirrors the lance_index
// IndexDescription trait.
type IndexDescription struct {
	// Name is the index name.
	Name string `json:"name"`
	// IndexType is a short type identifier (e.g. "BTree", "IVF_PQ", "Json"),
	// or "Unknown" if no plugin recognizes the index details.
	IndexType string `json:"index_type"`
	// TypeURL is the protobuf type URL of the index details.
	TypeURL string `json:"type_url"`
	// RowsIndexed is the approximate number of rows the index covers across
	// all segments (may include deleted rows).
	RowsIndexed uint64 `json:"rows_indexed"`
	// FieldIDs are the dataset field ids the index is built on.
	FieldIDs []int32 `json:"field_ids"`
	// Details is an index-type-specific JSON document, or empty when it could
	// not be decoded (e.g. no plugin for the type).
	Details string `json:"details,omitempty"`
	// TotalSizeBytes is the total on-disk size of the index across all
	// segments, or nil when size information is unavailable.
	TotalSizeBytes *uint64 `json:"total_size_bytes,omitempty"`
	// Segments are the physical index segments that make up this logical
	// index.
	Segments []IndexInfo `json:"segments"`
}

// IndexCriteria filters the indices considered by DescribeIndices.
type IndexCriteria struct {
	// ForColumn restricts to indices over this single column.
	ForColumn string `json:"for_column,omitempty"`
	// HasName restricts to the index with this name.
	HasName string `json:"has_name,omitempty"`
	// MustSupportFTS restricts to indices that support full-text search.
	MustSupportFTS bool `json:"must_support_fts,omitempty"`
	// MustSupportExactEquality restricts to indices that answer exact
	// equality (excludes ngram/inverted/bloom-filter).
	MustSupportExactEquality bool `json:"must_support_exact_equality,omitempty"`
}

// DescribeIndices returns rich descriptions of the dataset's indices. When
// criteria is nil, every index is described. Otherwise only matching indices
// are returned. Unlike ListIndices, this does not load indices into memory
// but surfaces per-type details, coverage and size.
func (d *Dataset) DescribeIndices(ctx context.Context, criteria *IndexCriteria) ([]IndexDescription, error) {
	return datasetOp(ctx, d, "Dataset.DescribeIndices", "describe indices",
		func(ctx context.Context, ptr *C.LanceDataset) ([]IndexDescription, error) {
			var criteriaJSON string
			if criteria != nil {
				data, err := json.Marshal(criteria)
				if err != nil {
					return nil, fmt.Errorf("marshal index criteria: %w", err)
				}
				criteriaJSON = string(data)
			}
			cCriteria, freeCriteria := cString(criteriaJSON)
			defer freeCriteria()
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_describe_indices(ptr, cCriteria, &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var descs []IndexDescription
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &descs); err != nil {
				return nil, fmt.Errorf("decode index descriptions: %w", err)
			}
			return descs, nil
		})
}

// loadIndicesByCriteria is the shared FFI wrapper behind the LoadIndex* APIs.
func (d *Dataset) loadIndicesByCriteria(ctx context.Context, criteria map[string]any) ([]IndexInfo, error) {
	var criteriaJSON string
	if len(criteria) != 0 {
		data, err := json.Marshal(criteria)
		if err != nil {
			return nil, fmt.Errorf("lance: marshal load-index criteria: %w", err)
		}
		criteriaJSON = string(data)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	cCriteria, freeCriteria := cString(criteriaJSON)
	defer freeCriteria()
	var cJSON *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_load_indices_by_criteria(ptr, cCriteria, &cJSON)
	}); err != nil {
		return nil, err
	}
	defer C.lance_string_free(cJSON)
	var infos []IndexInfo
	if err := json.Unmarshal([]byte(C.GoString(cJSON)), &infos); err != nil {
		return nil, fmt.Errorf("lance: decode index list: %w", err)
	}
	return infos, nil
}

// LoadIndex returns the index segment with the given build UUID, or nil if no
// index has that UUID.
func (d *Dataset) LoadIndex(ctx context.Context, uuid string) (res *IndexInfo, err error) {
	ctx, end := d.obs().start(ctx, "Dataset.LoadIndex", attribute.String("lance.index_uuid", uuid))
	defer func() { end(&err) }()
	infos, err := d.loadIndicesByCriteria(ctx, map[string]any{"uuid": uuid})
	if err != nil {
		return nil, fmt.Errorf("lance: load index %q: %w", uuid, err)
	}
	if len(infos) == 0 {
		return nil, nil
	}
	return &infos[0], nil
}

// LoadIndicesByName returns every (delta) segment of the index with the given
// name.
func (d *Dataset) LoadIndicesByName(ctx context.Context, name string) (res []IndexInfo, err error) {
	ctx, end := d.obs().start(ctx, "Dataset.LoadIndicesByName", attribute.String("lance.index_name", name))
	defer func() { end(&err) }()
	infos, err := d.loadIndicesByCriteria(ctx, map[string]any{"name": name})
	if err != nil {
		return nil, fmt.Errorf("lance: load indices by name %q: %w", name, err)
	}
	return infos, nil
}

// LoadIndexByName returns the unique index with the given name, or nil if no
// such index exists. Fails wrapping ErrIndex if more than one index shares
// the name (use LoadIndicesByName for delta indices).
func (d *Dataset) LoadIndexByName(ctx context.Context, name string) (*IndexInfo, error) {
	infos, err := d.LoadIndicesByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, nil
	}
	if len(infos) > 1 {
		return nil, fmt.Errorf("lance: load index by name %q: %w: found %d indices, use LoadIndicesByName",
			name, ErrIndex, len(infos))
	}
	return &infos[0], nil
}

// RemapIndex remaps the index on column to the row addresses moved by a prior
// compaction that ran with DeferIndexRemap (whose moves are recorded in the
// fragment reuse index). Upon success a new dataset version may be committed
// and the handle tracks it. When there is nothing to remap it is a no-op.
// name is the index name, or empty for the Lance default ("<column>_idx").
func (d *Dataset) RemapIndex(ctx context.Context, column, name string) error {
	return datasetDo(ctx, d, "Dataset.RemapIndex", fmt.Sprintf("remap index on %q", column),
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cColumns, freeColumns := cStringArray([]string{column})
			defer freeColumns()
			cName, freeName := cString(name)
			defer freeName()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_remap_column_index(ptr, cColumns, cName)
			})
		}, attribute.String("lance.column", column))
}
