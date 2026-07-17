package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// Scanner is a builder for reads over a dataset. Configure it with the
// chained methods, then call one of the terminal methods (Reader, CountRows,
// Explain). A Scanner is cheap. Each terminal call snapshots the dataset, so
// the Scanner itself holds no native resources.
//
// A Scanner must not be used concurrently while it is being configured, but
// the terminal methods are safe to call repeatedly.
type Scanner struct {
	ds  *Dataset
	cfg scanConfig
	// vector is the []float32 query for Nearest, vectorArr the arrow query
	// for NearestArrow. At most one is set.
	vector    []float32
	vectorArr arrow.Array
	// scanStats is the callback registered by WithScanStats, if any.
	scanStats func(ScanStats)
	// err defers configuration errors to the terminal methods.
	err error
}

// scanConfig mirrors the scan_json contract of lance_scanner_new.
type scanConfig struct {
	Columns         []string    `json:"columns,omitempty"`
	ProjectionExprs [][2]string `json:"projection_exprs,omitempty"`
	Filter          string      `json:"filter,omitempty"`
	FilterSubstrait string      `json:"filter_substrait,omitempty"`
	Prefilter       *bool       `json:"prefilter,omitempty"`
	Limit           *int64      `json:"limit,omitempty"`
	Offset          *int64      `json:"offset,omitempty"`
	WithRowID       bool        `json:"with_row_id,omitempty"`
	WithRowAddress  bool        `json:"with_row_address,omitempty"`
	BatchSize       uint64      `json:"batch_size,omitempty"`
	BatchSizeBytes  uint64      `json:"batch_size_bytes,omitempty"`
	StrictBatchSize *bool       `json:"strict_batch_size,omitempty"`
	ScanInOrder     *bool       `json:"scan_in_order,omitempty"`
	OrderBy         []orderBy   `json:"order_by,omitempty"`
	Aggregate       *AggSpec    `json:"aggregate,omitempty"`

	FragmentIDs                  []uint32 `json:"fragment_ids,omitempty"`
	IncludeDeletedRows           bool     `json:"include_deleted_rows,omitempty"`
	UseStats                     *bool    `json:"use_stats,omitempty"`
	UseScalarIndex               *bool    `json:"use_scalar_index,omitempty"`
	MaterializationStyle         string   `json:"materialization_style,omitempty"`
	BlobHandling                 string   `json:"blob_handling,omitempty"`
	DisableScoringAutoprojection bool     `json:"disable_scoring_autoprojection,omitempty"`

	// Performance knobs.
	IOBufferSize      uint64 `json:"io_buffer_size,omitempty"`
	BatchReadahead    *int   `json:"batch_readahead,omitempty"`
	FragmentReadahead *int   `json:"fragment_readahead,omitempty"`
	TargetParallelism *int   `json:"target_parallelism,omitempty"`

	// Vector search (only sent together with a query vector).
	Column           string   `json:"column,omitempty"`
	K                int      `json:"k,omitempty"`
	Metric           string   `json:"metric,omitempty"`
	Nprobes          int      `json:"nprobes,omitempty"`
	MinimumNprobes   int      `json:"minimum_nprobes,omitempty"`
	MaximumNprobes   int      `json:"maximum_nprobes,omitempty"`
	Refine           uint32   `json:"refine,omitempty"`
	Ef               int      `json:"ef,omitempty"`
	UseIndex         *bool    `json:"use_index,omitempty"`
	FastSearch       bool     `json:"fast_search,omitempty"`
	DistanceLower    *float32 `json:"distance_lower,omitempty"`
	DistanceUpper    *float32 `json:"distance_upper,omitempty"`
	QueryParallelism *int32   `json:"query_parallelism,omitempty"`
	ApproxMode       string   `json:"approx_mode,omitempty"`
	IndexSegments    []string `json:"index_segments,omitempty"`

	// Full-text search.
	Fts        json.RawMessage `json:"fts,omitempty"`
	FtsColumns []string        `json:"fts_columns,omitempty"`
	FtsLimit   *int64          `json:"fts_limit,omitempty"`
	WandFactor *float32        `json:"wand_factor,omitempty"`

	// ScanStatsPlugin is a Go plugin handle (registerPlugin) invoked once
	// after the scan executes with summary statistics. 0/omitted means no
	// callback. Set from Scanner.scanStats in newNative, never directly.
	ScanStatsPlugin uint64 `json:"scan_stats_plugin,omitempty"`
}

// orderBy mirrors one entry of the scan_json "order_by" array.
type orderBy struct {
	Column     string `json:"column"`
	Ascending  *bool  `json:"ascending,omitempty"`
	NullsFirst *bool  `json:"nulls_first,omitempty"`
}

// Scan starts building a scan over the dataset.
func (d *Dataset) Scan() *Scanner {
	return &Scanner{ds: d}
}

// obs returns the instrumentation handle for this scanner, inherited from its
// dataset (nil-safe).
func (s *Scanner) obs() *obs { return s.ds.obs() }

// Columns restricts the scan to the given columns (projection). By default
// all columns are returned.
func (s *Scanner) Columns(columns ...string) *Scanner {
	s.cfg.Columns = columns
	return s
}

// Filter applies an SQL predicate, e.g. "id < 50".
func (s *Scanner) Filter(filter string) *Scanner {
	s.cfg.Filter = filter
	return s
}

// Prefilter controls whether the filter runs before (true) or after any
// vector search stage.
func (s *Scanner) Prefilter(prefilter bool) *Scanner {
	s.cfg.Prefilter = &prefilter
	return s
}

// Limit caps the number of rows returned.
func (s *Scanner) Limit(limit int64) *Scanner {
	s.cfg.Limit = &limit
	return s
}

// Offset skips the first offset rows.
func (s *Scanner) Offset(offset int64) *Scanner {
	s.cfg.Offset = &offset
	return s
}

// WithRowID includes the internal _rowid column in the results.
func (s *Scanner) WithRowID() *Scanner {
	s.cfg.WithRowID = true
	return s
}

// BatchSize caps the number of rows per emitted record batch.
func (s *Scanner) BatchSize(n uint64) *Scanner {
	s.cfg.BatchSize = n
	return s
}

// ScanInOrder controls whether batches are returned in stable row order
// (the default) or in whatever order fragments finish reading.
func (s *Scanner) ScanInOrder(ordered bool) *Scanner {
	s.cfg.ScanInOrder = &ordered
	return s
}

// WithRowAddress includes the internal _rowaddr column in the results.
func (s *Scanner) WithRowAddress() *Scanner {
	s.cfg.WithRowAddress = true
	return s
}

// ProjectExpr adds a computed output column: name is the output column name
// and expr an SQL expression evaluated per row (e.g. "id * 2"). Repeatable.
// The projection then consists of exactly the ProjectExpr columns. Mutually
// exclusive with Columns.
func (s *Scanner) ProjectExpr(name, expr string) *Scanner {
	s.cfg.ProjectionExprs = append(s.cfg.ProjectionExprs, [2]string{name, expr})
	return s
}

// FilterSubstrait applies a filter given as a serialized Substrait
// ExtendedExpression message containing exactly one boolean expression.
// Mutually exclusive with Filter.
func (s *Scanner) FilterSubstrait(expr []byte) *Scanner {
	s.cfg.FilterSubstrait = base64.StdEncoding.EncodeToString(expr)
	return s
}

// OrderOption configures one OrderBy/OrderByDesc column.
type OrderOption func(*orderBy)

// NullsFirst controls where NULL values sort: first (true) or last (false,
// the default).
func NullsFirst(first bool) OrderOption {
	return func(o *orderBy) { o.NullsFirst = &first }
}

// OrderBy sorts the results by column, ascending. Repeated OrderBy /
// OrderByDesc calls add secondary sort columns. Sorting requires reading all
// data before the first batch is returned.
func (s *Scanner) OrderBy(column string, opts ...OrderOption) *Scanner {
	return s.orderBy(column, true, opts)
}

// OrderByDesc sorts the results by column, descending. See OrderBy.
func (s *Scanner) OrderByDesc(column string, opts ...OrderOption) *Scanner {
	return s.orderBy(column, false, opts)
}

func (s *Scanner) orderBy(column string, ascending bool, opts []OrderOption) *Scanner {
	o := orderBy{Column: column, Ascending: &ascending}
	for _, opt := range opts {
		opt(&o)
	}
	s.cfg.OrderBy = append(s.cfg.OrderBy, o)
	return s
}

// Aggregate function names accepted by AggFunc.Func.
const (
	// AggCount counts rows (or a column's non-NULL values).
	AggCount = "count"
	// AggSum sums a column's values.
	AggSum = "sum"
	// AggAvg averages a column's values.
	AggAvg = "avg"
	// AggMin takes the minimum of a column's values.
	AggMin = "min"
	// AggMax takes the maximum of a column's values.
	AggMax = "max"
)

// AggFunc is one aggregate function application within an AggSpec.
type AggFunc struct {
	// Func is one of AggCount, AggSum, AggAvg, AggMin, AggMax.
	Func string `json:"func"`
	// Column is the column to aggregate. For AggCount an empty Column (or
	// "*") counts all rows. A named column counts its non-NULL values.
	Column string `json:"column,omitempty"`
	// Alias renames the output column (default: the DataFusion name, e.g.
	// "count(1)" or "sum(score)").
	Alias string `json:"alias,omitempty"`
}

// AggSpec configures Scanner.Aggregate: the scan result is replaced by one
// row per GroupBy group (a single row when GroupBy is empty), with one
// column per GroupBy column followed by one column per aggregate.
type AggSpec struct {
	// GroupBy lists the columns to group by (may be empty).
	GroupBy []string `json:"group_by,omitempty"`
	// Aggregates lists the aggregate functions to compute.
	Aggregates []AggFunc `json:"aggregates"`
}

// Aggregate aggregates the scan (after any filter). The aggregated result
// comes back through the normal terminals (Reader, Batch).
func (s *Scanner) Aggregate(spec AggSpec) *Scanner {
	s.cfg.Aggregate = &spec
	return s
}

// DistanceRange keeps only Nearest results whose _distance is at least
// lower (inclusive) and less than upper (exclusive). Either bound may be
// nil for unbounded. Only takes effect together with Nearest/NearestArrow.
func (s *Scanner) DistanceRange(lower, upper *float32) *Scanner {
	s.cfg.DistanceLower = lower
	s.cfg.DistanceUpper = upper
	return s
}

// IOBufferSize sets the amount of RAM (bytes) reserved for buffered I/O
// (default 2 GiB).
func (s *Scanner) IOBufferSize(n uint64) *Scanner {
	s.cfg.IOBufferSize = n
	return s
}

// BatchReadahead sets how many batches to read ahead (legacy v1 data files
// only, ignored for v2+).
func (s *Scanner) BatchReadahead(n int) *Scanner {
	s.cfg.BatchReadahead = &n
	return s
}

// FragmentReadahead sets how many fragments to read ahead (only used when
// ScanInOrder(false)).
func (s *Scanner) FragmentReadahead(n int) *Scanner {
	s.cfg.FragmentReadahead = &n
	return s
}

// TargetParallelism overrides the target number of partitions used by the
// physical query optimizer (default: the number of compute CPUs).
func (s *Scanner) TargetParallelism(n int) *Scanner {
	s.cfg.TargetParallelism = &n
	return s
}

// QueryParallelism sets the partition-search concurrency of a Nearest
// query: 0 automatic (default), -1 the CPU pool size, 1 sequential, >= 2
// partition-parallel. Only takes effect together with Nearest/NearestArrow.
func (s *Scanner) QueryParallelism(n int32) *Scanner {
	s.cfg.QueryParallelism = &n
	return s
}

// BatchSizeBytes targets a batch size in bytes instead of rows, overriding
// BatchSize.
func (s *Scanner) BatchSizeBytes(n uint64) *Scanner {
	s.cfg.BatchSizeBytes = n
	return s
}

// StrictBatchSize forces every emitted batch (except the last) to have
// exactly BatchSize rows, at the cost of a data copy.
func (s *Scanner) StrictBatchSize(strict bool) *Scanner {
	s.cfg.StrictBatchSize = &strict
	return s
}

// IncludeDeletedRows also returns rows deleted from the dataset but still
// present in storage. Their _rowid is NULL. Fully deleted fragments are not
// returned.
func (s *Scanner) IncludeDeletedRows() *Scanner {
	s.cfg.IncludeDeletedRows = true
	return s
}

// UseStats controls whether statistics are used to optimize the scan
// (default true), mainly for debugging and benchmarking.
func (s *Scanner) UseStats(use bool) *Scanner {
	s.cfg.UseStats = &use
	return s
}

// UseScalarIndex controls whether scalar indices may be used to optimize
// the query (default true).
func (s *Scanner) UseScalarIndex(use bool) *Scanner {
	s.cfg.UseScalarIndex = &use
	return s
}

// Materialization styles accepted by Scanner.MaterializationStyle.
const (
	// MaterializationHeuristic picks early/late per column based on column
	// size and storage type (the default).
	MaterializationHeuristic = "heuristic"
	// MaterializationAllLate fetches filtered columns only for matching rows.
	MaterializationAllLate = "all_late"
	// MaterializationAllEarly fetches whole columns before filtering.
	MaterializationAllEarly = "all_early"
)

// MaterializationStyle controls when non-filter columns are fetched from
// storage: MaterializationHeuristic, MaterializationAllLate or
// MaterializationAllEarly.
func (s *Scanner) MaterializationStyle(style string) *Scanner {
	s.cfg.MaterializationStyle = style
	return s
}

// ScanFragments restricts the scan to the given fragment ids (see
// Version/fragment metadata for ids). Unknown ids fail with ErrNotFound.
func (s *Scanner) ScanFragments(ids ...uint32) *Scanner {
	s.cfg.FragmentIDs = ids
	return s
}

// IndexSegments restricts a Nearest search to the given vector index
// segment UUIDs.
func (s *Scanner) IndexSegments(uuids ...string) *Scanner {
	s.cfg.IndexSegments = uuids
	return s
}

// Approximation modes accepted by Scanner.ApproxMode.
const (
	// ApproxModeFast prefers lower latency over recall.
	ApproxModeFast = "fast"
	// ApproxModeNormal is the default latency/recall balance.
	ApproxModeNormal = "normal"
	// ApproxModeAccurate prefers recall over latency.
	ApproxModeAccurate = "accurate"
)

// ApproxMode sets the speed/accuracy tradeoff of a Nearest search on
// RQ-quantized indexes (other index types ignore it): ApproxModeFast,
// ApproxModeNormal or ApproxModeAccurate. Only takes effect together with
// Nearest/NearestArrow.
func (s *Scanner) ApproxMode(mode string) *Scanner {
	s.cfg.ApproxMode = mode
	return s
}

// Blob handling modes accepted by Scanner.BlobHandling.
const (
	// BlobsAllBinary reads all blob columns as binary data.
	BlobsAllBinary = "all_binary"
	// BlobsDescriptions reads blob columns as descriptions and other binary
	// columns as binary (the default).
	BlobsDescriptions = "blobs_descriptions"
	// BlobsAllDescriptions reads all binary columns as descriptions.
	BlobsAllDescriptions = "all_descriptions"
)

// BlobHandling controls how blob columns are projected: BlobsAllBinary,
// BlobsDescriptions or BlobsAllDescriptions.
func (s *Scanner) BlobHandling(handling string) *Scanner {
	s.cfg.BlobHandling = handling
	return s
}

// DisableScoringAutoprojection stops the _score/_distance column from being
// added automatically when an explicit projection is set. Include it in the
// projection to keep it.
func (s *Scanner) DisableScoringAutoprojection() *Scanner {
	s.cfg.DisableScoringAutoprojection = true
	return s
}

// WithScanStats registers fn to receive summary I/O statistics once after
// the scan executes. A nil fn clears any previously registered callback.
//
// In lance 8.0.0, the report is delivered ONLY by the streaming terminals
// (Reader, and therefore the Scanner.All iterator) and by Batch. CountRows,
// Explain, AnalyzePlan, and AnalyzeCountPlan accept the option without
// error, but upstream does not propagate the callback on those code paths,
// so they deliver no report.
//
// For Reader/All, fn fires on the consuming goroutine's thread when the
// stream is exhausted, while the reader's internal lock is held: fn must
// not touch that reader (Next, RecordBatch, Release), or it will deadlock.
// If the reader is released before exhaustion (early break), no stats are
// delivered. For Batch, fn fires before the call returns.
//
// fn is fire-and-forget (its return value, if any, is ignored and errors
// are swallowed) and it MUST NOT re-enter lance-go (opening/scanning a
// dataset, CountRows, etc.). A re-entrant call is rejected with
// ErrReentrantCall inside the native layer rather than crashing the
// process, but since reporting is best-effort that failure is silently
// dropped and the scan still completes.
func (s *Scanner) WithScanStats(fn func(ScanStats)) *Scanner {
	s.scanStats = fn
	return s
}

// Nearest configures a k-nearest-neighbor search on a vector column. The
// query vector's length must match the column dimension (or a multiple of
// it, for batch queries against fixed-size-list columns). Combine with
// Filter and Prefilter(true) to restrict the searched rows. Results gain a
// "_distance" column and are ordered by ascending distance.
func (s *Scanner) Nearest(column string, vector []float32, k int) *Scanner {
	s.cfg.Column = column
	s.cfg.K = k
	s.vector = vector
	s.vectorArr = nil
	return s
}

// NearestArrow is Nearest with an arbitrary Arrow query array (a
// Float16/Float32/Float64/UInt8 values array, or a (FixedSize)List of those
// for multivector/batch queries).
//
// The Scanner borrows the array: the caller must keep it valid (not
// Released) until the terminal methods it wants to run have returned. The
// array's buffers are exported across the Arrow C Data Interface on each
// terminal call, so they must live outside the Go heap (allocate with a
// C-backed allocator returned by Allocator, enforced under
// GOEXPERIMENT=cgocheck2).
func (s *Scanner) NearestArrow(column string, vector arrow.Array, k int) *Scanner {
	s.cfg.Column = column
	s.cfg.K = k
	s.vector = nil
	s.vectorArr = vector
	return s
}

// Metric overrides the distance metric for a Nearest search (default: the
// metric the vector index was built with, or L2 without an index).
func (s *Scanner) Metric(metric DistanceType) *Scanner {
	s.cfg.Metric = metric.String()
	return s
}

// Nprobes sets the exact number of IVF partitions to probe (both the
// minimum and the maximum).
func (s *Scanner) Nprobes(n int) *Scanner {
	s.cfg.Nprobes = n
	return s
}

// MinimumNprobes sets the minimum number of IVF partitions to probe.
func (s *Scanner) MinimumNprobes(n int) *Scanner {
	s.cfg.MinimumNprobes = n
	return s
}

// MaximumNprobes sets the maximum number of IVF partitions to probe.
func (s *Scanner) MaximumNprobes(n int) *Scanner {
	s.cfg.MaximumNprobes = n
	return s
}

// Refine re-ranks factor*k approximate candidates with exact distances.
func (s *Scanner) Refine(factor uint32) *Scanner {
	s.cfg.Refine = factor
	return s
}

// Ef sets the HNSW search list size.
func (s *Scanner) Ef(ef int) *Scanner {
	s.cfg.Ef = ef
	return s
}

// UseIndex controls whether a Nearest search may use a vector index
// (default true). Pass false to force a flat (exact) search.
func (s *Scanner) UseIndex(use bool) *Scanner {
	s.cfg.UseIndex = &use
	return s
}

// FastSearch searches only indexed data, skipping rows not yet covered by
// the vector index.
func (s *Scanner) FastSearch() *Scanner {
	s.cfg.FastSearch = true
	return s
}

// FullTextSearch configures a full-text search over one or more inverted-
// indexed columns. Results gain a "_score" column and are ordered by
// descending relevance.
func (s *Scanner) FullTextSearch(q FtsQuery, opts ...FtsOption) *Scanner {
	data, err := marshalFtsQuery(q)
	if err != nil {
		s.err = fmt.Errorf("lance: full text search: %w", err)
		return s
	}
	s.cfg.Fts = data
	for _, opt := range opts {
		opt(&s.cfg)
	}
	return s
}

// FtsOption configures FullTextSearch.
type FtsOption func(*scanConfig)

// WithFtsColumns sets the columns to search when the query itself does not
// name them.
func WithFtsColumns(columns ...string) FtsOption {
	return func(cfg *scanConfig) { cfg.FtsColumns = columns }
}

// WithFtsLimit caps the number of full-text search results.
func WithFtsLimit(limit int64) FtsOption {
	return func(cfg *scanConfig) { cfg.FtsLimit = &limit }
}

// WithWandFactor tunes the WAND ranking factor (default 1.0). Larger values
// trade recall for speed.
func WithWandFactor(factor float32) FtsOption {
	return func(cfg *scanConfig) { cfg.WandFactor = &factor }
}

// queryArray materializes the configured query vector as an Arrow array
// whose buffers live in C memory, as required to export it across the C
// Data Interface. The caller must Release the result. Returns nil when no
// query vector is configured.
func (s *Scanner) queryArray() arrow.Array {
	if s.vectorArr != nil {
		s.vectorArr.Retain()
		return s.vectorArr
	}
	if s.vector == nil {
		return nil
	}
	mem := Allocator()
	bld := array.NewFloat32Builder(mem)
	defer bld.Release()
	bld.AppendValues(s.vector, nil)
	return bld.NewArray()
}

// newNative constructs the native scanner. The caller must release the
// returned handle with C.lance_scanner_close. It holds no reference to the
// Go Dataset: the native scanner snapshots the dataset internally.
func (s *Scanner) newNative(ctx context.Context) (*C.LanceScanner, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.ds.mu.RLock()
	defer s.ds.mu.RUnlock()
	ptr, err := s.ds.checkOpen(ctx)
	if err != nil {
		return nil, err
	}

	// Register a fresh plugin handle for this native scanner, if requested.
	// On success the native scanner takes ownership of the registration
	// (released when it, and any streams cloned from it, drop); on failure
	// to construct the scanner we must release it ourselves. The handle is
	// injected into a per-call copy of cfg, never the shared field: terminal
	// methods are documented as safe to call concurrently, so newNative must
	// not write to Scanner state (and a stale handle from a previous call
	// must not leak into a later marshal).
	cfg := s.cfg
	cfg.ScanStatsPlugin = 0
	var scanStatsHandle uintptr
	if s.scanStats != nil {
		scanStatsHandle = registerPlugin(&scanStatsAdapter{fn: s.scanStats})
		cfg.ScanStatsPlugin = uint64(scanStatsHandle)
	}

	cJSON, freeJSON, err := marshalOptions(&cfg)
	if err != nil {
		if scanStatsHandle != 0 {
			releasePlugin(scanStatsHandle)
		}
		return nil, err
	}
	defer freeJSON()

	// Export the query vector, if any. The C structs must be zero-initialized
	// (Go zeroes them). The native side always takes ownership of the
	// exported pair, even on error, so the producer is released exactly once.
	var cVec *C.struct_ArrowArray
	var cVecSchema *C.struct_ArrowSchema
	if query := s.queryArray(); query != nil {
		defer query.Release()
		var vec C.struct_ArrowArray
		var vecSchema C.struct_ArrowSchema
		cdata.ExportArrowArray(query,
			(*cdata.CArrowArray)(unsafe.Pointer(&vec)),
			(*cdata.CArrowSchema)(unsafe.Pointer(&vecSchema)))
		cVec, cVecSchema = &vec, &vecSchema
	}

	var sc *C.LanceScanner
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_scanner_new(ptr, cJSON, cVec, cVecSchema, &sc)
	}); err != nil {
		if scanStatsHandle != 0 {
			releasePlugin(scanStatsHandle)
		}
		return nil, fmt.Errorf("lance: scan: %w", err)
	}
	return sc, nil
}

// Reader executes the scan and returns a reader over the result batches.
//
// The returned reader and every record obtained from it must be Released by
// the caller. Records returned by Next/RecordBatch are only valid until the
// next call to Next. Retain them to keep them longer. The reader stays valid
// after the Dataset is closed.
func (s *Scanner) Reader(ctx context.Context) (reader array.RecordReader, err error) {
	cfg := observedStream(ctx, s.obs(), "Scanner.Reader")
	defer cfg.endOnError(&err)
	ctx = cfg.ctx
	sc, err := s.newNative(ctx)
	if err != nil {
		return nil, err
	}
	defer C.lance_scanner_close(sc)

	// Must be zero-initialized (Go zeroes it). On success the native side
	// writes a self-contained stream into it. importRecordReader then moves the
	// stream contents into a deterministically released owner.
	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_scanner_to_stream(sc, &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: scan stream: %w", err)
	}
	return importRecordReader(&stream, "scan", cfg)
}

// CountRows returns the number of rows the scan would produce, respecting
// the configured filter.
func (s *Scanner) CountRows(ctx context.Context) (n uint64, err error) {
	ctx, end := s.obs().start(ctx, "Scanner.CountRows")
	defer func() { end(&err) }()

	sc, err := s.newNative(ctx)
	if err != nil {
		return 0, err
	}
	defer C.lance_scanner_close(sc)
	var count C.uint64_t
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_scanner_count_rows(sc, &count)
	}); err != nil {
		return 0, fmt.Errorf("lance: scan count rows: %w", err)
	}
	return uint64(count), nil
}

// Batch executes the scan and returns all results combined into a single
// record. The caller must Release the record. Prefer Reader for large
// results: Batch materializes the whole result in memory.
func (s *Scanner) Batch(ctx context.Context) (res arrow.RecordBatch, err error) {
	ctx, end := s.obs().start(ctx, "Scanner.Batch")
	defer func() { end(&err) }()

	sc, err := s.newNative(ctx)
	if err != nil {
		return nil, err
	}
	defer C.lance_scanner_close(sc)

	// Must be zero-initialized (Go zeroes it). On success the native side
	// writes a self-contained one-batch stream into it.
	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_scanner_to_batch(sc, &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: scan batch: %w", err)
	}
	reader, err := importRecordReader(&stream, "scan batch")
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("lance: scan batch: %w", err)
		}
		return nil, fmt.Errorf("lance: scan batch produced no record: %w", ErrInternal)
	}
	rec := reader.RecordBatch()
	rec.Retain()
	return rec, nil
}

// Explain returns a human-readable description of the scan execution plan.
func (s *Scanner) Explain(ctx context.Context, verbose bool) (res string, err error) {
	ctx, end := s.obs().start(ctx, "Scanner.Explain")
	defer func() { end(&err) }()

	sc, err := s.newNative(ctx)
	if err != nil {
		return "", err
	}
	defer C.lance_scanner_close(sc)
	var cPlan *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_scanner_explain(sc, C.bool(verbose), &cPlan)
	}); err != nil {
		return "", fmt.Errorf("lance: explain: %w", err)
	}
	defer C.lance_string_free(cPlan)
	return C.GoString(cPlan), nil
}

// analyze runs lance_scanner_analyze in the given mode (0 = plan, 1 =
// count plan) and returns the annotated plan text.
func (s *Scanner) analyze(ctx context.Context, mode int32, what string) (string, error) {
	sc, err := s.newNative(ctx)
	if err != nil {
		return "", err
	}
	defer C.lance_scanner_close(sc)
	var cText *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_scanner_analyze(sc, C.int32_t(mode), &cText)
	}); err != nil {
		return "", fmt.Errorf("lance: %s: %w", what, err)
	}
	defer C.lance_string_free(cText)
	return C.GoString(cText), nil
}

// AnalyzePlan executes the scan (EXPLAIN ANALYZE) and returns the execution
// plan annotated with runtime metrics. Note this runs the full scan.
func (s *Scanner) AnalyzePlan(ctx context.Context) (res string, err error) {
	ctx, end := s.obs().start(ctx, "Scanner.AnalyzePlan")
	defer func() { end(&err) }()
	return s.analyze(ctx, 0, "analyze plan")
}

// AnalyzeCountPlan executes the COUNT(*) form of the scan (the plan
// CountRows runs) and returns it annotated with runtime metrics.
func (s *Scanner) AnalyzeCountPlan(ctx context.Context) (res string, err error) {
	ctx, end := s.obs().start(ctx, "Scanner.AnalyzeCountPlan")
	defer func() { end(&err) }()
	return s.analyze(ctx, 1, "analyze count plan")
}

// FtsQuery is a full-text search query node. Implementations: MatchQuery,
// PhraseQuery, BoostQuery, MultiMatchQuery, BooleanQuery.
type FtsQuery interface {
	// ftsNode returns the query as the tagged JSON object understood by the
	// native side (the serde form of lance_index's FtsQuery enum).
	ftsNode() (map[string]any, error)
}

// marshalFtsQuery renders q as the fts JSON document.
func marshalFtsQuery(q FtsQuery) (json.RawMessage, error) {
	if q == nil {
		return nil, fmt.Errorf("query must not be nil: %w", ErrInvalidArgument)
	}
	node, err := q.ftsNode()
	if err != nil {
		return nil, err
	}
	return json.Marshal(node)
}

// FtsOperator combines the terms of a MatchQuery.
type FtsOperator string

const (
	// FtsOperatorOr matches rows containing at least one term (default).
	FtsOperatorOr FtsOperator = "Or"
	// FtsOperatorAnd matches only rows containing all terms.
	FtsOperatorAnd FtsOperator = "And"
)

// FuzzinessAuto selects the edit distance automatically from each term's
// length (0 for length <= 2, 1 for length <= 5, 2 above).
const FuzzinessAuto = -1

// MatchQuery matches rows whose column contains the query terms.
type MatchQuery struct {
	// Column to search. Empty means "determined by the search" (set via
	// WithFtsColumns or the only indexed column).
	Column string
	// Terms is the query text. It is tokenized like the indexed documents.
	Terms string
	// Boost scales the score contribution (default 1.0).
	Boost float32
	// Fuzziness is the max edit distance per term: 0 exact (default), n > 0
	// for that distance, or FuzzinessAuto.
	Fuzziness int
	// MaxExpansions caps the number of terms considered for fuzzy matching
	// (default 50).
	MaxExpansions int
	// Operator combines the terms: FtsOperatorOr (default) or
	// FtsOperatorAnd.
	Operator FtsOperator
	// PrefixLength is the number of leading characters kept unchanged by
	// fuzzy matching.
	PrefixLength uint32
}

func (q MatchQuery) ftsNode() (map[string]any, error) {
	if q.Terms == "" {
		return nil, fmt.Errorf("match query terms must not be empty: %w", ErrInvalidArgument)
	}
	boost := q.Boost
	if boost == 0 {
		boost = 1.0
	}
	var fuzziness any
	switch {
	case q.Fuzziness < 0:
		fuzziness = nil // auto
	default:
		fuzziness = q.Fuzziness
	}
	maxExpansions := q.MaxExpansions
	if maxExpansions == 0 {
		maxExpansions = 50
	}
	operator := q.Operator
	if operator == "" {
		operator = FtsOperatorOr
	}
	if operator != FtsOperatorOr && operator != FtsOperatorAnd {
		return nil, fmt.Errorf("invalid operator %q: %w", operator, ErrInvalidArgument)
	}
	return map[string]any{"match": map[string]any{
		"column":         nullableString(q.Column),
		"terms":          q.Terms,
		"boost":          boost,
		"fuzziness":      fuzziness,
		"max_expansions": maxExpansions,
		"operator":       string(operator),
		"prefix_length":  q.PrefixLength,
	}}, nil
}

// PhraseQuery matches rows whose column contains the terms as a contiguous
// phrase. The inverted index must be built with position information
// (Inverted{WithPosition: true}).
type PhraseQuery struct {
	// Column to search. Empty means "determined by the search".
	Column string
	// Terms is the phrase text.
	Terms string
	// Slop is the maximum number of positions the terms may be apart.
	Slop uint32
}

func (q PhraseQuery) ftsNode() (map[string]any, error) {
	if q.Terms == "" {
		return nil, fmt.Errorf("phrase query terms must not be empty: %w", ErrInvalidArgument)
	}
	return map[string]any{"phrase": map[string]any{
		"column": nullableString(q.Column),
		"terms":  q.Terms,
		"slop":   q.Slop,
	}}, nil
}

// BoostQuery scores rows matching Positive higher and rows matching
// Negative lower.
type BoostQuery struct {
	// Positive is the query whose matches are wanted.
	Positive FtsQuery
	// Negative is the query whose matches are penalized.
	Negative FtsQuery
	// NegativeBoost scales the negative contribution (default 0.5).
	NegativeBoost float32
}

func (q BoostQuery) ftsNode() (map[string]any, error) {
	if q.Positive == nil || q.Negative == nil {
		return nil, fmt.Errorf("boost query needs positive and negative sub-queries: %w", ErrInvalidArgument)
	}
	positive, err := q.Positive.ftsNode()
	if err != nil {
		return nil, err
	}
	negative, err := q.Negative.ftsNode()
	if err != nil {
		return nil, err
	}
	negativeBoost := q.NegativeBoost
	if negativeBoost == 0 {
		negativeBoost = 0.5
	}
	return map[string]any{"boost": map[string]any{
		"positive":       positive,
		"negative":       negative,
		"negative_boost": negativeBoost,
	}}, nil
}

// MultiMatchQuery runs the same terms against several columns. Every
// sub-query must name a Column and share the same Terms. Per-column Boost
// values are honored.
type MultiMatchQuery struct {
	Queries []MatchQuery
}

func (q MultiMatchQuery) ftsNode() (map[string]any, error) {
	if len(q.Queries) == 0 {
		return nil, fmt.Errorf("multi-match query needs at least one sub-query: %w", ErrInvalidArgument)
	}
	columns := make([]string, len(q.Queries))
	boosts := make([]float32, len(q.Queries))
	for i, sub := range q.Queries {
		if sub.Column == "" {
			return nil, fmt.Errorf("multi-match sub-queries must name a column: %w", ErrInvalidArgument)
		}
		if sub.Terms != q.Queries[0].Terms {
			return nil, fmt.Errorf("multi-match sub-queries must share the same terms: %w", ErrInvalidArgument)
		}
		columns[i] = sub.Column
		boosts[i] = sub.Boost
		if boosts[i] == 0 {
			boosts[i] = 1.0
		}
	}
	return map[string]any{"multi_match": map[string]any{
		"query":   q.Queries[0].Terms,
		"columns": columns,
		"boost":   boosts,
	}}, nil
}

// BooleanQuery combines sub-queries: rows must match all Must queries, must
// not match any MustNot query, and Should queries raise the score (at least
// one of Should/Must must be non-empty).
type BooleanQuery struct {
	Should  []FtsQuery
	Must    []FtsQuery
	MustNot []FtsQuery
}

func (q BooleanQuery) ftsNode() (map[string]any, error) {
	toNodes := func(queries []FtsQuery) ([]map[string]any, error) {
		nodes := make([]map[string]any, 0, len(queries))
		for _, sub := range queries {
			if sub == nil {
				return nil, fmt.Errorf("boolean sub-queries must not be nil: %w", ErrInvalidArgument)
			}
			node, err := sub.ftsNode()
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
		return nodes, nil
	}
	should, err := toNodes(q.Should)
	if err != nil {
		return nil, err
	}
	must, err := toNodes(q.Must)
	if err != nil {
		return nil, err
	}
	mustNot, err := toNodes(q.MustNot)
	if err != nil {
		return nil, err
	}
	return map[string]any{"boolean": map[string]any{
		"should":   should,
		"must":     must,
		"must_not": mustNot,
	}}, nil
}

// nullableString maps "" to JSON null.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
