//! Dataset scanner: projection / filter / limit configuration and Arrow
//! stream export.

use std::ffi::{CString, c_char};
use std::sync::{Arc, Mutex};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{ArrayRef, RecordBatchIterator, make_array};
use base64::Engine as _;
use lance::dataset::scanner::{
    AggregateExpr, ColumnOrdering, ExecutionSummaryCounts, MaterializationStyle, Scanner,
};
use lance::datatypes::BlobHandling;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::FtsQuery;
use lance_index::vector::ApproxMode;
use lance_linalg::distance::DistanceType;
use serde::Deserialize;
use uuid::Uuid;

use crate::arrow_bridge;
use crate::callbacks::OwnedGoPlugin;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// Opaque handle to a configured scan over a Lance dataset. Created by
/// `lance_scanner_new`. Must be released with `lance_scanner_close`. The
/// scanner snapshots the dataset internally, so it stays valid even if the
/// originating dataset handle is closed first.
pub struct LanceScanner(Mutex<Scanner>);

/// Method discriminator for the Go scan-stats plugin (see `lance/scanner.go`).
/// The payload is the JSON encoding of the scan's [`ExecutionSummaryCounts`]
/// (`{"iops", "requests", "bytes_read", "indices_loaded", "parts_loaded",
/// "index_comparisons", "all_counts": {..}, "all_times": {..}}`).
const SCAN_STATS_REPORT: i32 = 0;

/// JSON payload sent to the Go scan-stats plugin: the public fields of
/// [`ExecutionSummaryCounts`]. `all_counts` / `all_times` (nanoseconds) are
/// debugging metrics whose keys are subject to change upstream.
fn scan_stats_json(stats: &ExecutionSummaryCounts) -> serde_json::Value {
    serde_json::json!({
        "iops": stats.iops as u64,
        "requests": stats.requests as u64,
        "bytes_read": stats.bytes_read as u64,
        "indices_loaded": stats.indices_loaded as u64,
        "parts_loaded": stats.parts_loaded as u64,
        "index_comparisons": stats.index_comparisons as u64,
        "all_counts": stats.all_counts,
        "all_times": stats.all_times,
    })
}

/// One column of a scan ordering: `{"column": string, "ascending"?: bool
/// (default true), "nulls_first"?: bool (default false)}`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderBySpec {
    column: String,
    ascending: Option<bool>,
    nulls_first: Option<bool>,
}

/// One aggregate function application: `{"func": "count"|"sum"|"avg"|"min"|
/// "max", "column"?: string, "alias"?: string}`. `"count"` without a column
/// (or with `"*"`) is COUNT(*), and with a column it counts non-NULL values.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateFnSpec {
    func: String,
    column: Option<String>,
    alias: Option<String>,
}

/// Aggregation over the scan: `{"group_by"?: [string], "aggregates":
/// [AggregateFnSpec, ...]}`. The aggregated result comes back through the
/// normal stream terminals.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateSpec {
    group_by: Option<Vec<String>>,
    aggregates: Vec<AggregateFnSpec>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ScanOptions {
    columns: Option<Vec<String>>,
    /// Projection with SQL transforms: `[[output_name, sql_expr], ...]`.
    /// Mutually exclusive with `columns`.
    projection_exprs: Option<Vec<(String, String)>>,
    filter: Option<String>,
    /// A base64-encoded Substrait ExtendedExpression message containing a
    /// single boolean expression. Mutually exclusive with `filter`.
    filter_substrait: Option<String>,
    prefilter: Option<bool>,
    limit: Option<i64>,
    offset: Option<i64>,
    with_row_id: Option<bool>,
    /// Include the `_rowaddr` meta column in the results.
    with_row_address: Option<bool>,
    batch_size: Option<u64>,
    /// Target batch size in bytes, overrides the row-based `batch_size`.
    batch_size_bytes: Option<u64>,
    /// Emit exactly `batch_size` rows per batch (except the last one).
    strict_batch_size: Option<bool>,
    scan_in_order: Option<bool>,
    /// Sort the results: `[{"column": string, "ascending"?: bool,
    /// "nulls_first"?: bool}, ...]`.
    order_by: Option<Vec<OrderBySpec>>,
    /// Aggregate the scan (see [`AggregateSpec`]).
    aggregate: Option<AggregateSpec>,
    /// Restrict the scan to these fragment ids.
    fragment_ids: Option<Vec<u64>>,
    /// Include rows deleted from the dataset but still present in storage
    /// (their `_rowid` is NULL).
    include_deleted_rows: Option<bool>,
    /// Use statistics to optimize the scan (default true).
    use_stats: Option<bool>,
    /// Use scalar indices to optimize the query (default true).
    use_scalar_index: Option<bool>,
    /// "heuristic" (default), "all_late" or "all_early".
    materialization_style: Option<String>,
    /// "all_binary", "blobs_descriptions" (default) or "all_descriptions".
    blob_handling: Option<String>,
    /// Do not auto-project the `_score`/`_distance` column when an explicit
    /// projection is given.
    disable_scoring_autoprojection: Option<bool>,

    // --- performance knobs ---
    /// RAM reserved for buffering I/O (default 2 GiB).
    io_buffer_size: Option<u64>,
    /// Batch readahead (legacy v1 format only).
    batch_readahead: Option<usize>,
    /// Fragment readahead (only used when `scan_in_order` is false).
    fragment_readahead: Option<usize>,
    /// Target number of partitions for the physical optimizer.
    target_parallelism: Option<usize>,

    // --- vector (nearest-neighbor) search, only used when a query vector
    // is passed to lance_scanner_new ---
    /// Vector column to search. Required when a query vector is given.
    column: Option<String>,
    /// Number of nearest neighbors to return (default 10).
    k: Option<usize>,
    /// "l2", "cosine", "dot" or "hamming".
    metric: Option<String>,
    /// Sets both minimum and maximum nprobes.
    nprobes: Option<usize>,
    minimum_nprobes: Option<usize>,
    maximum_nprobes: Option<usize>,
    /// Refine factor: re-rank `refine * k` candidates with exact distances.
    refine: Option<u32>,
    /// HNSW search list size.
    ef: Option<usize>,
    /// Whether to use a vector index if available (default true).
    use_index: Option<bool>,
    /// Search only indexed data (skips unindexed fragments).
    fast_search: Option<bool>,
    /// Lower bound (exclusive) on the `_distance` of returned results.
    distance_lower: Option<f32>,
    /// Upper bound (inclusive) on the `_distance` of returned results.
    distance_upper: Option<f32>,
    /// Partition-search concurrency per vector query (0 = automatic,
    /// -1 = CPU pool size).
    query_parallelism: Option<i32>,
    /// "fast", "normal" (default) or "accurate" (RQ-quantized indexes only).
    approx_mode: Option<String>,
    /// Restrict the vector index search to these index segment UUIDs.
    index_segments: Option<Vec<String>>,

    // --- full-text search ---
    /// An `FtsQuery` JSON document (serde representation of
    /// lance_index::scalar::inverted::query::FtsQuery), e.g.
    /// `{"match": {"column": null, "terms": "...", "boost": 1.0,
    /// "fuzziness": 0, "max_expansions": 50, "operator": "Or",
    /// "prefix_length": 0}}`.
    fts: Option<serde_json::Value>,
    /// Columns to search when the query does not name them itself.
    fts_columns: Option<Vec<String>>,
    /// Maximum number of FTS results.
    fts_limit: Option<i64>,
    /// WAND factor for FTS ranking (default 1.0).
    wand_factor: Option<f32>,

    // --- callbacks ---
    /// Go plugin handle (from the Go handle registry) invoked once after the
    /// scan executes with summary statistics. Ownership of the registration
    /// transfers to the scanner: it is released when the scanner (and any
    /// streams cloned from it) drop. 0/absent means no callback.
    scan_stats_plugin: Option<u64>,
}

type ScanError = (ErrorCode, String);

fn invalid(msg: impl Into<String>) -> ScanError {
    (ErrorCode::InvalidArgument, msg.into())
}

fn from_lance(e: lance::Error) -> ScanError {
    (map_lance_error(&e), e.to_string())
}

/// Imports the optional query vector, taking ownership of both C structs so
/// the producer side is released exactly once on every path.
///
/// # Safety
///
/// `query_vector` and `query_schema` must each be NULL or a valid pointer to
/// an initialized, unmoved Arrow C struct.
unsafe fn import_query_vector(
    query_vector: *mut FFI_ArrowArray,
    query_schema: *mut FFI_ArrowSchema,
) -> Result<Option<ArrayRef>, String> {
    // Take ownership FIRST (even of a mismatched pair) so producer resources
    // are always released.
    let array =
        (!query_vector.is_null()).then(|| unsafe { FFI_ArrowArray::from_raw(query_vector) });
    let schema =
        (!query_schema.is_null()).then(|| unsafe { FFI_ArrowSchema::from_raw(query_schema) });
    match (array, schema) {
        (None, None) => Ok(None),
        (Some(array), Some(schema)) => {
            // SAFETY: both structs were produced by an Arrow C Data Interface
            // exporter per this function's contract.
            let data = unsafe { arrow::ffi::from_ffi(array, &schema) }
                .map_err(|e| format!("failed to import query vector: {e}"))?;
            Ok(Some(make_array(data)))
        }
        _ => Err("query_vector and query_schema must both be NULL or both non-NULL".to_string()),
    }
}

/// Builds a lance `AggregateExpr` from the JSON aggregate spec.
fn build_aggregate(spec: &AggregateSpec) -> Result<AggregateExpr, ScanError> {
    if spec.aggregates.is_empty() {
        return Err(invalid(
            "\"aggregate\" needs at least one entry in \"aggregates\"",
        ));
    }
    let mut builder = AggregateExpr::builder();
    if let Some(group_by) = &spec.group_by {
        builder = builder.group_by_columns(group_by.iter().cloned());
    }
    for agg in &spec.aggregates {
        let column = agg.column.as_deref().filter(|c| !c.is_empty() && *c != "*");
        let pending = match (agg.func.as_str(), column) {
            ("count", None) => builder.count_star(),
            ("count", Some(c)) => builder.count(c),
            ("sum", Some(c)) => builder.sum(c),
            ("avg", Some(c)) => builder.avg(c),
            ("min", Some(c)) => builder.min(c),
            ("max", Some(c)) => builder.max(c),
            (func @ ("sum" | "avg" | "min" | "max"), None) => {
                return Err(invalid(format!(
                    "aggregate function \"{func}\" requires a \"column\""
                )));
            }
            (other, _) => {
                return Err(invalid(format!(
                    "unknown aggregate function {other:?} (expected \"count\", \"sum\", \"avg\", \"min\" or \"max\")"
                )));
            }
        };
        builder = match &agg.alias {
            Some(alias) => pending.alias(alias.clone()),
            // No alias: keep DataFusion's default output column name.
            // `group_by_columns` over an empty iterator is a no-op that
            // returns the builder to the "no pending aggregate" state.
            None => pending.group_by_columns(std::iter::empty::<String>()),
        };
    }
    Ok(builder.build())
}

fn parse_materialization_style(style: &str) -> Result<MaterializationStyle, ScanError> {
    match style {
        "heuristic" => Ok(MaterializationStyle::Heuristic),
        "all_late" => Ok(MaterializationStyle::AllLate),
        "all_early" => Ok(MaterializationStyle::AllEarly),
        other => Err(invalid(format!(
            "invalid materialization_style {other:?} (expected \"heuristic\", \"all_late\" or \"all_early\")"
        ))),
    }
}

fn parse_blob_handling(handling: &str) -> Result<BlobHandling, ScanError> {
    match handling {
        "all_binary" => Ok(BlobHandling::AllBinary),
        "blobs_descriptions" => Ok(BlobHandling::BlobsDescriptions),
        "all_descriptions" => Ok(BlobHandling::AllDescriptions),
        other => Err(invalid(format!(
            "invalid blob_handling {other:?} (expected \"all_binary\", \"blobs_descriptions\" or \"all_descriptions\")"
        ))),
    }
}

fn parse_approx_mode(mode: &str) -> Result<ApproxMode, ScanError> {
    match mode {
        "fast" => Ok(ApproxMode::Fast),
        "normal" => Ok(ApproxMode::Normal),
        "accurate" => Ok(ApproxMode::Accurate),
        other => Err(invalid(format!(
            "invalid approx_mode {other:?} (expected \"fast\", \"normal\" or \"accurate\")"
        ))),
    }
}

/// Applies every non-search scan option to `scanner`. `dataset` is the same
/// snapshot the scanner was created over (used to resolve fragment ids).
fn apply_scan_options(
    scanner: &mut Scanner,
    options: &ScanOptions,
    dataset: &lance::Dataset,
) -> Result<(), ScanError> {
    // Fragment-scoped scan first: it narrows what the rest operates on.
    if let Some(ids) = &options.fragment_ids {
        let all = dataset.fragments();
        let fragments = ids
            .iter()
            .map(|id| {
                all.iter().find(|f| f.id == *id).cloned().ok_or((
                    ErrorCode::NotFound,
                    format!("fragment {id} not found in dataset"),
                ))
            })
            .collect::<Result<Vec<_>, ScanError>>()?;
        scanner.with_fragments(fragments);
    }

    if options.columns.is_some() && options.projection_exprs.is_some() {
        return Err(invalid(
            "\"columns\" and \"projection_exprs\" are mutually exclusive",
        ));
    }
    if let Some(columns) = &options.columns {
        scanner.project(columns).map_err(from_lance)?;
    }
    if let Some(exprs) = &options.projection_exprs {
        scanner.project_with_transform(exprs).map_err(from_lance)?;
    }

    if options.filter.is_some() && options.filter_substrait.is_some() {
        return Err(invalid(
            "\"filter\" and \"filter_substrait\" are mutually exclusive",
        ));
    }
    if let Some(filter) = &options.filter {
        scanner.filter(filter).map_err(from_lance)?;
    }
    if let Some(encoded) = &options.filter_substrait {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| invalid(format!("invalid base64 in \"filter_substrait\": {e}")))?;
        scanner.filter_substrait(&bytes).map_err(from_lance)?;
    }

    if let Some(prefilter) = options.prefilter {
        scanner.prefilter(prefilter);
    }
    if options.limit.is_some() || options.offset.is_some() {
        scanner
            .limit(options.limit, options.offset)
            .map_err(from_lance)?;
    }
    if options.with_row_id.unwrap_or(false) {
        scanner.with_row_id();
    }
    if options.with_row_address.unwrap_or(false) {
        scanner.with_row_address();
    }
    if let Some(batch_size) = options.batch_size {
        scanner.batch_size(batch_size as usize);
    }
    if let Some(bytes) = options.batch_size_bytes {
        scanner.batch_size_bytes(bytes);
    }
    if let Some(strict) = options.strict_batch_size {
        scanner.strict_batch_size(strict);
    }
    if let Some(ordered) = options.scan_in_order {
        scanner.scan_in_order(ordered);
    }
    if let Some(order_by) = &options.order_by {
        let ordering = order_by
            .iter()
            .map(|spec| ColumnOrdering {
                ascending: spec.ascending.unwrap_or(true),
                nulls_first: spec.nulls_first.unwrap_or(false),
                column_name: spec.column.clone(),
            })
            .collect();
        scanner.order_by(Some(ordering)).map_err(from_lance)?;
    }
    if options.include_deleted_rows.unwrap_or(false) {
        scanner.include_deleted_rows();
    }
    if let Some(use_stats) = options.use_stats {
        scanner.use_stats(use_stats);
    }
    if let Some(use_scalar_index) = options.use_scalar_index {
        scanner.use_scalar_index(use_scalar_index);
    }
    if let Some(style) = &options.materialization_style {
        scanner.materialization_style(parse_materialization_style(style)?);
    }
    if let Some(handling) = &options.blob_handling {
        scanner.blob_handling(parse_blob_handling(handling)?);
    }
    if options.disable_scoring_autoprojection.unwrap_or(false) {
        scanner.disable_scoring_autoprojection();
    }

    if let Some(size) = options.io_buffer_size {
        scanner.io_buffer_size(size);
    }
    if let Some(n) = options.batch_readahead {
        scanner.batch_readahead(n);
    }
    if let Some(n) = options.fragment_readahead {
        scanner.fragment_readahead(n);
    }
    if let Some(n) = options.target_parallelism {
        scanner.target_parallelism(n);
    }

    if let Some(segments) = &options.index_segments {
        let uuids = segments
            .iter()
            .map(|s| {
                Uuid::parse_str(s)
                    .map_err(|e| invalid(format!("invalid index segment UUID {s:?}: {e}")))
            })
            .collect::<Result<Vec<_>, ScanError>>()?;
        scanner.with_index_segments(uuids).map_err(from_lance)?;
    }

    if let Some(aggregate) = &options.aggregate {
        scanner
            .aggregate(build_aggregate(aggregate)?)
            .map_err(from_lance)?;
    }
    Ok(())
}

/// Applies the vector-search and FTS options to `scanner`. `query` is the
/// imported query vector, if any.
fn apply_search_options(
    scanner: &mut Scanner,
    options: &ScanOptions,
    query: Option<ArrayRef>,
) -> Result<(), ScanError> {
    if let Some(query) = query {
        let column = options.column.as_deref().ok_or_else(|| {
            invalid("\"column\" must be set in the scan options when a query vector is given")
        })?;
        let k = options.k.unwrap_or(10);
        scanner
            .nearest(column, query.as_ref(), k)
            .map_err(from_lance)?;

        // All of the following knobs are no-ops unless nearest() succeeded,
        // so they must come after it.
        if let Some(metric) = &options.metric {
            let metric =
                DistanceType::try_from(metric.as_str()).map_err(|e| invalid(e.to_string()))?;
            scanner.distance_metric(metric);
        }
        if let Some(n) = options.nprobes {
            scanner.nprobes(n);
        }
        if let Some(n) = options.minimum_nprobes {
            scanner.minimum_nprobes(n);
        }
        if let Some(n) = options.maximum_nprobes {
            scanner.maximum_nprobes(n);
        }
        if let Some(factor) = options.refine {
            scanner.refine(factor);
        }
        if let Some(ef) = options.ef {
            scanner.ef(ef);
        }
        if let Some(use_index) = options.use_index {
            scanner.use_index(use_index);
        }
        if options.fast_search.unwrap_or(false) {
            scanner.fast_search();
        }
        if options.distance_lower.is_some() || options.distance_upper.is_some() {
            scanner.distance_range(options.distance_lower, options.distance_upper);
        }
        if let Some(parallelism) = options.query_parallelism {
            scanner.query_parallelism(parallelism);
        }
        if let Some(mode) = &options.approx_mode {
            scanner.approx_mode(parse_approx_mode(mode)?);
        }
    }

    if let Some(fts) = &options.fts {
        let query: FtsQuery = serde_json::from_value(fts.clone())
            .map_err(|e| invalid(format!("invalid \"fts\" query JSON: {e}")))?;
        let mut fts_query = FullTextSearchQuery::new_query(query)
            .limit(options.fts_limit)
            .wand_factor(options.wand_factor);
        if let Some(columns) = &options.fts_columns {
            fts_query = fts_query.with_columns(columns).map_err(from_lance)?;
        }
        scanner.full_text_search(fts_query).map_err(from_lance)?;
    }
    Ok(())
}

/// Creates a scanner over `ds`.
///
/// - `scan_json`: optional JSON object
///   `{"columns"?: [string], "projection_exprs"?: [[name, sql_expr]],
///   "filter"?: string, "filter_substrait"?: base64 string,
///   "prefilter"?: bool, "limit"?: int, "offset"?: int,
///   "with_row_id"?: bool, "with_row_address"?: bool, "batch_size"?: uint,
///   "batch_size_bytes"?: uint, "strict_batch_size"?: bool,
///   "scan_in_order"?: bool,
///   "order_by"?: [{"column": string, "ascending"?: bool,
///   "nulls_first"?: bool}],
///   "aggregate"?: {"group_by"?: [string], "aggregates": [{"func":
///   "count"|"sum"|"avg"|"min"|"max", "column"?: string, "alias"?: string}]},
///   "fragment_ids"?: [uint], "include_deleted_rows"?: bool,
///   "use_stats"?: bool, "use_scalar_index"?: bool,
///   "materialization_style"?: "heuristic"|"all_late"|"all_early",
///   "blob_handling"?: "all_binary"|"blobs_descriptions"|"all_descriptions",
///   "disable_scoring_autoprojection"?: bool, "io_buffer_size"?: uint,
///   "batch_readahead"?: uint, "fragment_readahead"?: uint,
///   "target_parallelism"?: uint,
///   "column"?: string, "k"?: uint, "metric"?: string, "nprobes"?: uint,
///   "minimum_nprobes"?: uint, "maximum_nprobes"?: uint, "refine"?: uint,
///   "ef"?: uint, "use_index"?: bool, "fast_search"?: bool,
///   "distance_lower"?: float, "distance_upper"?: float,
///   "query_parallelism"?: int, "approx_mode"?: "fast"|"normal"|"accurate",
///   "index_segments"?: [uuid string],
///   "fts"?: object, "fts_columns"?: [string], "fts_limit"?: int,
///   "wand_factor"?: float, "scan_stats_plugin"?: uint}`, or NULL for a full
///   scan. Unknown fields are ignored. `distance_lower`/`distance_upper`,
///   `query_parallelism` and `approx_mode` only take effect when a query
///   vector is given. `scan_stats_plugin` is a Go plugin handle invoked once
///   after the scan executes with a JSON summary-statistics payload (see
///   `SCAN_STATS_REPORT`); its registration is released when the scanner
///   drops.
/// - `query_vector` / `query_schema`: optional Arrow C Data Interface query
///   vector for nearest-neighbor search (a Float16/32/64 or UInt8 values
///   array, or a (FixedSize)List thereof). Both NULL or both non-NULL.
///   Ownership of both structs is always taken, even on error. When given,
///   `"column"` must be set in `scan_json` (`"k"` defaults to 10).
/// - `out`: receives the scanner handle on success. Release it with
///   `lance_scanner_close`.
///
/// # Safety
///
/// `ds` must be a valid dataset handle, `scan_json` NULL or a valid C
/// string, `query_vector`/`query_schema` NULL or valid unmoved Arrow C
/// structs, and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_new(
    ds: *const LanceDataset,
    scan_json: *const c_char,
    query_vector: *mut FFI_ArrowArray,
    query_schema: *mut FFI_ArrowSchema,
    out: *mut *mut LanceScanner,
) -> i32 {
    // Import the query vector FIRST so its producer resources are owned (and
    // thus released) on every subsequent error path.
    let query = match unsafe { import_query_vector(query_vector, query_schema) } {
        Ok(query) => query,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let scan_json = match unsafe { storage::optional_str(scan_json, "scan_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: ScanOptions = match scan_json {
        None => ScanOptions::default(),
        Some(s) if s.trim().is_empty() => ScanOptions::default(),
        Some(s) => match serde_json::from_str(s) {
            Ok(options) => options,
            Err(e) => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    format!("invalid scan JSON: {e}"),
                );
            }
        },
    };

    let dataset = Arc::new(unsafe { &*ds }.dataset());
    let mut scanner = Scanner::new(dataset.clone());

    if let Err((code, msg)) = apply_scan_options(&mut scanner, &options, &dataset) {
        return set_error(code, msg);
    }
    if let Err((code, msg)) = apply_search_options(&mut scanner, &options, query) {
        return set_error(code, msg);
    }

    if let Some(handle) = options.scan_stats_plugin.filter(|h| *h != 0) {
        // The Arc-backed lease releases the Go registration when the last
        // native clone of the callback drops (mirroring the write-progress
        // and cache-backend plugins).
        let plugin = OwnedGoPlugin::new(handle as usize);
        scanner.scan_stats_callback(Arc::new(move |stats: &ExecutionSummaryCounts| {
            // Fire-and-forget: stats reporting is best-effort, so plugin
            // errors are ignored (the Go shim already contains panics, and
            // GoPlugin::call never panics on its own).
            let payload = scan_stats_json(stats).to_string();
            let _ = plugin.call(SCAN_STATS_REPORT, payload.as_bytes());
        }));
    }

    let handle = Box::into_raw(Box::new(LanceScanner(Mutex::new(scanner))));
    // SAFETY: `out` is non-NULL and valid for writes.
    unsafe { *out = handle };
    ok()
}

/// Executes the scan and exports the results into `out` as an Arrow C
/// stream. The caller owns the stream and must call its `release` callback.
/// The stream is self-contained: the scanner (and dataset) handles may be
/// closed while it is still being consumed.
///
/// # Safety
///
/// `sc` must be a valid scanner handle and `out` valid for writes (any
/// previous contents are overwritten without being released).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_to_stream(
    sc: *const LanceScanner,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if sc.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "sc and out must not be NULL");
    }
    let scanner = unsafe { &*sc }.0.lock().unwrap_or_else(|e| e.into_inner());
    let stream = match block_on_cc!(scanner.try_into_stream()) {
        Ok(stream) => stream,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match unsafe { arrow_bridge::export_stream(stream, out) } {
        Ok(()) => ok(),
        Err(msg) => set_error(ErrorCode::Internal, msg),
    }
}

/// Executes the scan, collects every result batch and exports the combined
/// single batch into `out` as a one-batch Arrow C stream. The caller owns
/// the stream and must call its `release` callback. Prefer
/// `lance_scanner_to_stream` for large results: this call materializes the
/// whole result in memory.
///
/// # Safety
///
/// `sc` must be a valid scanner handle and `out` valid for writes (any
/// previous contents are overwritten without being released).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_to_batch(
    sc: *const LanceScanner,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if sc.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "sc and out must not be NULL");
    }
    let scanner = unsafe { &*sc }.0.lock().unwrap_or_else(|e| e.into_inner());
    let batch = match block_on_cc!(scanner.try_into_batch()) {
        Ok(batch) => batch,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);
    let stream = FFI_ArrowArrayStream::new(Box::new(reader));
    // SAFETY: `out` is non-NULL and valid for writes per the contract. The
    // write is unaligned-safe because the caller may hand us arbitrarily
    // aligned memory.
    unsafe { std::ptr::write_unaligned(out, stream) };
    ok()
}

/// Counts the rows the scan would produce (respecting the scanner's filter).
///
/// # Safety
///
/// `sc` must be a valid scanner handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_count_rows(sc: *const LanceScanner, out: *mut u64) -> i32 {
    if sc.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "sc and out must not be NULL");
    }
    let scanner = unsafe { &*sc }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(scanner.count_rows()) {
        Ok(count) => {
            // SAFETY: `out` is non-NULL and valid for writes.
            unsafe { *out = count };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns a human-readable description of the scan's execution plan. The
/// caller owns `*out_plan` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `sc` must be a valid scanner handle and `out_plan` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_explain(
    sc: *const LanceScanner,
    verbose: bool,
    out_plan: *mut *mut c_char,
) -> i32 {
    if sc.is_null() || out_plan.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "sc and out_plan must not be NULL",
        );
    }
    let scanner = unsafe { &*sc }.0.lock().unwrap_or_else(|e| e.into_inner());
    let plan = match block_on_cc!(scanner.explain_plan(verbose)) {
        Ok(plan) => plan,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match CString::new(plan) {
        Ok(cstr) => {
            // SAFETY: `out_plan` is non-NULL and valid for writes.
            unsafe { *out_plan = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// EXPLAIN ANALYZE: executes the scan and returns the execution plan
/// annotated with runtime metrics. `mode` 0 analyzes the scan's own plan.
/// `mode` 1 analyzes the equivalent COUNT(*) plan (the plan `count_rows`
/// would execute). Note this call runs the full scan. The caller owns
/// `*out_text` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `sc` must be a valid scanner handle and `out_text` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_analyze(
    sc: *const LanceScanner,
    mode: i32,
    out_text: *mut *mut c_char,
) -> i32 {
    if sc.is_null() || out_text.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "sc and out_text must not be NULL",
        );
    }
    let scanner = unsafe { &*sc }.0.lock().unwrap_or_else(|e| e.into_inner());
    let result = match mode {
        0 => block_on_cc!(scanner.analyze_plan()),
        1 => block_on_cc!(scanner.analyze_count_plan()),
        other => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid analyze mode {other} (expected 0=plan or 1=count)"),
            );
        }
    };
    let text = match result {
        Ok(text) => text,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match CString::new(text) {
        Ok(cstr) => {
            // SAFETY: `out_text` is non-NULL and valid for writes.
            unsafe { *out_text = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Releases a scanner handle. NULL is a no-op. The handle must not be used
/// again afterwards. Streams previously exported from this scanner remain
/// valid.
///
/// # Safety
///
/// `sc` must be NULL or a handle obtained from `lance_scanner_new` that has
/// not already been closed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_scanner_close(sc: *mut LanceScanner) {
    if sc.is_null() {
        return;
    }
    // SAFETY: per the contract, `sc` came from Box::into_raw and is closed
    // at most once.
    drop(unsafe { Box::from_raw(sc) });
}
