//! Index management: create, list, describe, statistics, drop, optimize,
//! prewarm, remap.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::str::FromStr;
use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{ArrayRef, FixedSizeListArray, make_array};
use lance::dataset::optimize::remapping;
use lance::index::DatasetIndexExt;
use lance::index::vector::{IndexFileVersion, VectorIndexParams};
use lance::table::format::IndexMetadata;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::vector::bq::{RQBuildParams, RQRotationType};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::{FtsPrewarmOptions, IndexCriteria, IndexParams, IndexType, PrewarmOptions};
use lance_linalg::distance::DistanceType;
use serde::{Deserialize, Serialize};

use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// JSON body accepted for scalar (non-inverted) index types:
/// `{"params": <index-type-specific JSON>}`, everything optional.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ScalarParamsJson {
    params: Option<serde_json::Value>,
}

/// JSON body accepted for the inverted (full-text) index type. All fields
/// are optional. Omitted fields keep the Lance defaults.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct InvertedParamsJson {
    /// "simple" (default), "whitespace", "raw", "ngram", "icu",
    /// "lindera/*" or "jieba/*".
    base_tokenizer: Option<String>,
    /// Stemming / stop-word language, e.g. "English" (default).
    language: Option<String>,
    with_position: Option<bool>,
    lower_case: Option<bool>,
    stem: Option<bool>,
    remove_stop_words: Option<bool>,
    ascii_folding: Option<bool>,
    /// Maximum token length, `0` means "no limit".
    max_token_length: Option<usize>,
    custom_stop_words: Option<Vec<String>>,
    ngram_min_length: Option<u32>,
    ngram_max_length: Option<u32>,
    ngram_prefix_only: Option<bool>,
}

/// JSON body accepted for vector index types. All fields are optional.
/// Omitted fields keep the Lance defaults.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct VectorParamsJson {
    /// "l2" (default), "cosine", "dot" or "hamming".
    metric: Option<String>,
    num_partitions: Option<usize>,
    /// Target IVF partition size, preferred over `num_partitions` (the
    /// partition count is derived from it).
    target_partition_size: Option<usize>,
    /// Retrain the provided `centroids` instead of using them verbatim.
    retrain: Option<bool>,
    /// PQ: bits per centroid (default 8), SQ: scaling-range bits (default 8),
    /// RQ: bits per dimension (default 1).
    num_bits: Option<u32>,
    /// PQ only: number of sub-vectors (default 16).
    num_sub_vectors: Option<usize>,
    /// Max k-means iterations for IVF and PQ training (default 50).
    max_iterations: Option<usize>,
    /// Sample rate for IVF / PQ / SQ training (default 256).
    sample_rate: Option<usize>,
    /// Per-step sample rate for streaming IVF k-means training.
    streaming_sample_rate: Option<usize>,
    /// Coreset rate for streaming IVF k-means training.
    streaming_coreset_rate: Option<usize>,
    /// Extra streaming Lloyd refinement passes (default 0).
    streaming_refine_passes: Option<usize>,
    /// Shuffle: batches per partition (advanced).
    shuffle_partition_batches: Option<usize>,
    /// Shuffle: concurrent partitions (advanced).
    shuffle_partition_concurrency: Option<usize>,
    /// Precomputed row_id -> partition_id file.
    precomputed_partitions_file: Option<String>,
    /// Storage options used to load precomputed partitions.
    storage_options: Option<HashMap<String, String>>,
    /// PQ only: run k-means `kmeans_redos` times and keep the best result
    /// (default 1).
    kmeans_redos: Option<usize>,
    /// RQ only: "fast" (default) or "matrix".
    rotation_type: Option<String>,
    /// HNSW graph parameters (IVF_HNSW_* types only).
    hnsw: Option<HnswParamsJson>,
    /// Index file format: "legacy" or "v3" (default depends on index type).
    index_file_version: Option<String>,
    /// Skip transpose / packing for PQ and RQ storage.
    skip_transpose: Option<bool>,
    /// Optional build preferences stored in the index manifest
    /// (reverse-DNS keys, e.g. "lance.ivf.max_iters").
    runtime_hints: Option<HashMap<String, String>>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct HnswParamsJson {
    m: Option<usize>,
    ef_construction: Option<usize>,
    max_level: Option<u16>,
    prefetch_distance: Option<usize>,
}

/// Options accepted by `lance_dataset_optimize_indices`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct OptimizeOptionsJson {
    num_indices_to_merge: Option<usize>,
    index_names: Option<Vec<String>>,
    retrain: Option<bool>,
    transaction_properties: Option<HashMap<String, String>>,
}

/// Criteria accepted by `lance_dataset_describe_indices`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct IndexCriteriaJson {
    for_column: Option<String>,
    has_name: Option<String>,
    must_support_fts: Option<bool>,
    must_support_exact_equality: Option<bool>,
}

/// Criteria accepted by `lance_dataset_load_indices_by_criteria`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LoadIndexCriteriaJson {
    /// Load the index (delta) segments with this UUID.
    uuid: Option<String>,
    /// Load all segments of the index with this name.
    name: Option<String>,
}

/// Options accepted by `lance_dataset_prewarm_index_with_options`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PrewarmOptionsJson {
    fts: Option<FtsPrewarmOptionsJson>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FtsPrewarmOptionsJson {
    with_position: Option<bool>,
}

/// The subset of `IndexMetadata` returned across the FFI as JSON.
#[derive(Serialize)]
struct IndexInfoJson {
    name: String,
    uuid: String,
    fields: Vec<i32>,
    dataset_version: u64,
    index_version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
}

impl From<&IndexMetadata> for IndexInfoJson {
    fn from(meta: &IndexMetadata) -> Self {
        Self {
            name: meta.name.clone(),
            uuid: meta.uuid.to_string(),
            fields: meta.fields.clone(),
            dataset_version: meta.dataset_version,
            index_version: meta.index_version,
            created_at: meta.created_at.map(|t| t.to_rfc3339()),
        }
    }
}

/// The subset of `IndexDescription` (lance_index::traits) returned across
/// the FFI as JSON by `lance_dataset_describe_indices`.
#[derive(Serialize)]
struct IndexDescriptionJson {
    name: String,
    index_type: String,
    type_url: String,
    rows_indexed: u64,
    field_ids: Vec<u32>,
    /// Index-type-specific details as a JSON document, omitted when the
    /// details cannot be decoded (e.g. no plugin for the type).
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_size_bytes: Option<u64>,
    segments: Vec<IndexInfoJson>,
}

/// Maps a scalar index type to the `ScalarIndexParams::index_type` string.
fn scalar_index_type_str(index_type: IndexType) -> Option<&'static str> {
    match index_type {
        IndexType::Scalar | IndexType::BTree => Some("btree"),
        IndexType::Bitmap => Some("bitmap"),
        IndexType::LabelList => Some("labellist"),
        IndexType::NGram => Some("ngram"),
        IndexType::ZoneMap => Some("zonemap"),
        IndexType::BloomFilter => Some("bloomfilter"),
        IndexType::RTree => Some("rtree"),
        IndexType::Fm => Some("fm"),
        _ => None,
    }
}

/// The index family/route resolved from the string form of an index type
/// (see `lance_dataset_create_index_v2`).
enum ResolvedIndexType {
    /// A builtin index type, routed by the `IndexType` enum discriminant.
    Builtin(IndexType),
    /// A scalar index plugin (e.g. "json"), routed through
    /// `IndexType::Scalar` with the plugin name in
    /// `ScalarIndexParams::index_type`.
    ScalarPlugin(String),
}

/// Parses the string form of an index type. Builtin names (case-insensitive)
/// select the corresponding `IndexType`. `"scalar:<plugin>"` (or any
/// unrecognized bare name) selects the scalar-plugin route.
fn resolve_index_type_str(s: &str) -> Result<ResolvedIndexType, String> {
    let trimmed = s.trim();
    if let Some(plugin) = trimmed.strip_prefix("scalar:") {
        let plugin = plugin.trim();
        if plugin.is_empty() {
            return Err("index type \"scalar:\" is missing the plugin name".to_string());
        }
        return Ok(ResolvedIndexType::ScalarPlugin(plugin.to_string()));
    }
    let norm = trimmed.to_lowercase().replace('-', "_");
    let builtin = match norm.as_str() {
        "" => return Err("index type must not be empty".to_string()),
        "btree" => IndexType::BTree,
        "bitmap" => IndexType::Bitmap,
        "labellist" | "label_list" => IndexType::LabelList,
        "inverted" => IndexType::Inverted,
        "ngram" => IndexType::NGram,
        "zonemap" => IndexType::ZoneMap,
        "bloomfilter" | "bloom_filter" => IndexType::BloomFilter,
        "rtree" => IndexType::RTree,
        "fm" => IndexType::Fm,
        "ivf_flat" => IndexType::IvfFlat,
        "ivf_sq" => IndexType::IvfSq,
        "ivf_pq" => IndexType::IvfPq,
        "ivf_rq" => IndexType::IvfRq,
        "ivf_hnsw_flat" => IndexType::IvfHnswFlat,
        "ivf_hnsw_pq" => IndexType::IvfHnswPq,
        "ivf_hnsw_sq" => IndexType::IvfHnswSq,
        // Unknown names fall through to the scalar-plugin registry, which
        // produces a clear error if no such plugin exists.
        _ => return Ok(ResolvedIndexType::ScalarPlugin(norm)),
    };
    Ok(ResolvedIndexType::Builtin(builtin))
}

fn build_inverted_params(json: InvertedParamsJson) -> Result<InvertedIndexParams, String> {
    let mut params = InvertedIndexParams::default();
    if let Some(tokenizer) = json.base_tokenizer {
        params = params.base_tokenizer(tokenizer);
    }
    if let Some(language) = &json.language {
        params = params
            .language(language)
            .map_err(|e| format!("invalid language {language:?}: {e}"))?;
    }
    if let Some(with_position) = json.with_position {
        params = params.with_position(with_position);
    }
    if let Some(lower_case) = json.lower_case {
        params = params.lower_case(lower_case);
    }
    if let Some(stem) = json.stem {
        params = params.stem(stem);
    }
    if let Some(remove_stop_words) = json.remove_stop_words {
        params = params.remove_stop_words(remove_stop_words);
    }
    if let Some(ascii_folding) = json.ascii_folding {
        params = params.ascii_folding(ascii_folding);
    }
    if let Some(max_token_length) = json.max_token_length {
        // 0 means "no limit" on the wire (Option is already spent on
        // "keep the default").
        let limit = (max_token_length > 0).then_some(max_token_length);
        params = params.max_token_length(limit);
    }
    if let Some(custom_stop_words) = json.custom_stop_words {
        params = params.custom_stop_words(Some(custom_stop_words));
    }
    if let Some(min) = json.ngram_min_length {
        params = params.ngram_min_length(min);
    }
    if let Some(max) = json.ngram_max_length {
        params = params.ngram_max_length(max);
    }
    if let Some(prefix_only) = json.ngram_prefix_only {
        params = params.ngram_prefix_only(prefix_only);
    }
    Ok(params)
}

fn build_vector_params(
    index_type: IndexType,
    json: VectorParamsJson,
    centroids: Option<ArrayRef>,
    codebook: Option<ArrayRef>,
) -> Result<VectorIndexParams, String> {
    let metric = match &json.metric {
        None => DistanceType::L2,
        Some(s) => DistanceType::try_from(s.as_str()).map_err(|e| e.to_string())?,
    };

    let mut ivf = IvfBuildParams::default();
    if let Some(n) = json.num_partitions {
        ivf.num_partitions = Some(n);
    }
    if let Some(n) = json.target_partition_size {
        ivf.target_partition_size = Some(n);
    }
    if let Some(n) = json.max_iterations {
        ivf.max_iters = n;
    }
    if let Some(n) = json.sample_rate {
        ivf.sample_rate = n;
    }
    if let Some(retrain) = json.retrain {
        ivf.retrain = retrain;
    }
    if let Some(n) = json.streaming_sample_rate {
        ivf.streaming_sample_rate = Some(n);
    }
    if let Some(n) = json.streaming_coreset_rate {
        ivf.streaming_coreset_rate = Some(n);
    }
    if let Some(n) = json.streaming_refine_passes {
        ivf.streaming_refine_passes = n;
    }
    if let Some(n) = json.shuffle_partition_batches {
        ivf.shuffle_partition_batches = n;
    }
    if let Some(n) = json.shuffle_partition_concurrency {
        ivf.shuffle_partition_concurrency = n;
    }
    if let Some(file) = &json.precomputed_partitions_file {
        ivf.precomputed_partitions_file = Some(file.clone());
    }
    if let Some(options) = &json.storage_options {
        ivf.storage_options = Some(options.clone());
    }
    if let Some(centroids) = centroids {
        let fsl = centroids
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                format!(
                    "centroids must be a FixedSizeList array, got {}",
                    centroids.data_type()
                )
            })?;
        ivf.centroids = Some(Arc::new(fsl.clone()));
    }

    let has_codebook = codebook.is_some();
    let pq = || {
        let mut pq = PQBuildParams::default();
        if let Some(n) = json.num_bits {
            pq.num_bits = n as usize;
        }
        if let Some(n) = json.num_sub_vectors {
            pq.num_sub_vectors = n;
        }
        if let Some(n) = json.max_iterations {
            pq.max_iters = n;
        }
        if let Some(n) = json.kmeans_redos {
            pq.kmeans_redos = n;
        }
        if let Some(n) = json.sample_rate {
            pq.sample_rate = n;
        }
        pq.codebook = codebook.clone();
        pq
    };
    let sq = || {
        let mut sq = SQBuildParams::default();
        if let Some(n) = json.num_bits {
            sq.num_bits = n as u16;
        }
        if let Some(n) = json.sample_rate {
            sq.sample_rate = n;
        }
        sq
    };
    let rq = || -> Result<RQBuildParams, String> {
        let mut rq = RQBuildParams::default();
        if let Some(n) = json.num_bits {
            rq.num_bits = n as u8;
        }
        if let Some(rotation) = &json.rotation_type {
            rq.rotation_type = RQRotationType::from_str(rotation).map_err(|e| e.to_string())?;
        }
        Ok(rq)
    };
    let hnsw = || {
        let mut hnsw = HnswBuildParams::default();
        if let Some(p) = &json.hnsw {
            if let Some(m) = p.m {
                hnsw.m = m;
            }
            if let Some(ef) = p.ef_construction {
                hnsw.ef_construction = ef;
            }
            if let Some(level) = p.max_level {
                hnsw.max_level = level;
            }
            if let Some(distance) = p.prefetch_distance {
                hnsw.prefetch_distance = Some(distance);
            }
        }
        hnsw
    };

    let uses_pq = matches!(
        index_type,
        IndexType::Vector | IndexType::IvfPq | IndexType::IvfHnswPq
    );
    if has_codebook && !uses_pq {
        return Err(format!(
            "codebook is only supported for PQ index types, not {index_type}"
        ));
    }

    let mut params = match index_type {
        IndexType::IvfFlat => VectorIndexParams::with_ivf_flat_params(metric, ivf),
        IndexType::IvfSq => VectorIndexParams::with_ivf_sq_params(metric, ivf, sq()),
        IndexType::Vector | IndexType::IvfPq => {
            VectorIndexParams::with_ivf_pq_params(metric, ivf, pq())
        }
        IndexType::IvfRq => VectorIndexParams::with_ivf_rq_params(metric, ivf, rq()?),
        IndexType::IvfHnswFlat => VectorIndexParams::ivf_hnsw(metric, ivf, hnsw()),
        IndexType::IvfHnswPq => {
            VectorIndexParams::with_ivf_hnsw_pq_params(metric, ivf, hnsw(), pq())
        }
        IndexType::IvfHnswSq => {
            VectorIndexParams::with_ivf_hnsw_sq_params(metric, ivf, hnsw(), sq())
        }
        other => return Err(format!("index type {other} is not a vector index")),
    };

    if let Some(version) = &json.index_file_version {
        params.version = IndexFileVersion::try_from(version).map_err(|e| e.to_string())?;
    }
    if let Some(skip_transpose) = json.skip_transpose {
        params.skip_transpose = skip_transpose;
    }
    if let Some(hints) = json.runtime_hints {
        params.runtime_hints = hints;
    }
    Ok(params)
}

fn parse_params_json<T: Default + for<'de> Deserialize<'de>>(
    json: Option<&str>,
    what: &str,
) -> Result<T, String> {
    match json {
        None => Ok(T::default()),
        Some(s) if s.trim().is_empty() => Ok(T::default()),
        Some(s) => serde_json::from_str(s).map_err(|e| format!("invalid {what} JSON: {e}")),
    }
}

/// Builds the `IndexParams` implementation for `index_type` from the
/// optional JSON parameter document and the optional Arrow-passed
/// `centroids` / `codebook` arrays (vector index types only).
///
/// Shared with `distributed.rs` (uncommitted-index build path).
pub(crate) fn build_index_params(
    index_type: IndexType,
    params_json: Option<&str>,
    centroids: Option<ArrayRef>,
    codebook: Option<ArrayRef>,
) -> Result<Box<dyn IndexParams>, String> {
    if index_type == IndexType::Inverted || scalar_index_type_str(index_type).is_some() {
        if centroids.is_some() || codebook.is_some() {
            return Err(format!(
                "centroids/codebook are only supported for vector index types, not {index_type}"
            ));
        }
        if index_type == IndexType::Inverted {
            let json: InvertedParamsJson = parse_params_json(params_json, "inverted index params")?;
            return Ok(Box::new(build_inverted_params(json)?));
        }
        let type_str = scalar_index_type_str(index_type).unwrap();
        let json: ScalarParamsJson = parse_params_json(params_json, "scalar index params")?;
        return Ok(Box::new(ScalarIndexParams {
            index_type: type_str.to_string(),
            params: json.params.map(|v| v.to_string()),
        }));
    }
    let json: VectorParamsJson = parse_params_json(params_json, "vector index params")?;
    Ok(Box::new(build_vector_params(
        index_type, json, centroids, codebook,
    )?))
}

/// Imports an optional Arrow array crossing the C Data Interface. Both
/// pointers must be NULL or both non-NULL. Ownership of both structs is
/// always taken, even on error, so the producer side is released exactly
/// once.
///
/// # Safety
///
/// `array` and `schema` must each be NULL or a valid pointer to an
/// initialized, unmoved Arrow C struct.
unsafe fn import_optional_array(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    what: &str,
) -> Result<Option<ArrayRef>, String> {
    // Take ownership FIRST (even of a mismatched pair) so producer resources
    // are always released.
    let array = (!array.is_null()).then(|| unsafe { FFI_ArrowArray::from_raw(array) });
    let schema = (!schema.is_null()).then(|| unsafe { FFI_ArrowSchema::from_raw(schema) });
    match (array, schema) {
        (None, None) => Ok(None),
        (Some(array), Some(schema)) => {
            // SAFETY: both structs were produced by an Arrow C Data Interface
            // exporter per this function's contract.
            let data = unsafe { arrow::ffi::from_ffi(array, &schema) }
                .map_err(|e| format!("failed to import {what}: {e}"))?;
            Ok(Some(make_array(data)))
        }
        _ => Err(format!(
            "{what} array and schema must both be NULL or both non-NULL"
        )),
    }
}

/// Parses a NULL-terminated array of C strings into a Vec.
///
/// # Safety
///
/// `columns` must be a valid array of valid C strings terminated by a NULL
/// entry (a NULL array is rejected, not treated as empty).
unsafe fn parse_columns(columns: *const *const c_char) -> Result<Vec<String>, String> {
    if columns.is_null() {
        return Err("columns must not be NULL".to_string());
    }
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
        // SAFETY: caller guarantees the array is NULL-terminated.
        let ptr = unsafe { *columns.add(i) };
        if ptr.is_null() {
            break;
        }
        out.push(unsafe { storage::required_str(ptr, "column name") }?.to_owned());
        i += 1;
    }
    if out.is_empty() {
        return Err("columns must contain at least one column name".to_string());
    }
    Ok(out)
}

/// Writes `json` into `*out_json` as an owned C string (caller frees it with
/// `lance_string_free`).
unsafe fn emit_json(json: String, out_json: *mut *mut c_char) -> i32 {
    match CString::new(json) {
        Ok(cstr) => {
            // SAFETY: callers validated `out_json` is non-NULL and writable.
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Creates an index on `columns` of the dataset. Upon success a new dataset
/// version is committed and the handle is updated to it.
///
/// - `columns`: NULL-terminated array of column names (currently Lance
///   supports exactly one column per index). Must not be NULL.
/// - `index_type`: an `IndexType` discriminant: BTree=1, Bitmap=2,
///   LabelList=3, Inverted=4, NGram=5, ZoneMap=8, BloomFilter=9,
///   IvfFlat=101, IvfSq=102, IvfPq=103, IvfHnswSq=104, IvfHnswPq=105,
///   IvfHnswFlat=106, IvfRq=107.
/// - `name`: optional index name, or NULL for the Lance default
///   (`<column>_idx`).
/// - `params_json`: optional JSON parameter document, or NULL for defaults.
///   Shape depends on the index family:
///   - scalar types: `{"params"?: <index-specific JSON>}`
///   - inverted: `{"base_tokenizer"?: string, "language"?: string,
///     "with_position"?: bool, "lower_case"?: bool, "stem"?: bool,
///     "remove_stop_words"?: bool, "ascii_folding"?: bool,
///     "max_token_length"?: uint (0 = unlimited),
///     "custom_stop_words"?: [string], "ngram_min_length"?: uint,
///     "ngram_max_length"?: uint, "ngram_prefix_only"?: bool}`
///   - vector types: `{"metric"?: "l2"|"cosine"|"dot"|"hamming",
///     "num_partitions"?: uint, "num_bits"?: uint,
///     "num_sub_vectors"?: uint, "max_iterations"?: uint,
///     "sample_rate"?: uint, "rotation_type"?: "fast"|"matrix",
///     "hnsw"?: {"m"?: uint, "ef_construction"?: uint, "max_level"?: uint}}`
/// - `replace`: whether an existing index with the same name is replaced
///   (false makes the call fail if the name is taken).
/// - `out_json`: if non-NULL, receives the created index metadata as JSON
///   `{"name", "uuid", "fields", "dataset_version", "index_version",
///   "created_at"?}`. Free with `lance_string_free`.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_create_index(
    ds: *mut LanceDataset,
    columns: *const *const c_char,
    index_type: i32,
    name: *const c_char,
    params_json: *const c_char,
    replace: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (columns, name, params_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                parse_columns(columns)?,
                storage::optional_str(name, "name")?.map(str::to_owned),
                storage::optional_str(params_json, "params_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let index_type = match IndexType::try_from(index_type) {
        Ok(t) => t,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid index type {index_type}: {e}"),
            );
        }
    };
    let params = match build_index_params(index_type, params_json, None, None) {
        Ok(params) => params,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    unsafe {
        run_create_index(
            ds,
            &columns,
            index_type,
            name,
            params.as_ref(),
            replace,
            out_json,
        )
    }
}

/// Shared tail of the index-creation entry points: runs `create_index` on
/// the dataset and optionally emits the created index metadata as JSON.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` NULL or valid for writes.
unsafe fn run_create_index(
    ds: *mut LanceDataset,
    columns: &[String],
    index_type: IndexType,
    name: Option<String>,
    params: &dyn IndexParams,
    replace: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let metadata =
        match block_on_cc!(guard.create_index(&column_refs, index_type, name, params, replace)) {
            Ok(metadata) => metadata,
            Err(e) => return set_error(map_lance_error(&e), e),
        };
    drop(guard);

    if out_json.is_null() {
        return ok();
    }
    match serde_json::to_string(&IndexInfoJson::from(&metadata)) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Creates an index on `columns` of the dataset, selecting the index type by
/// string. Upon success a new dataset version is committed and the handle is
/// updated to it.
///
/// This is the v2 form of `lance_dataset_create_index`. It additionally
/// supports scalar index plugins and Arrow-passed training data.
///
/// - `columns`: NULL-terminated array of column names (currently Lance
///   supports exactly one column per index). Must not be NULL.
/// - `index_type`: the index type, case-insensitive. One of the builtin
///   names `"btree"`, `"bitmap"`, `"labellist"`, `"inverted"`, `"ngram"`,
///   `"zonemap"`, `"bloomfilter"`, `"rtree"`, `"fm"`, `"ivf_flat"`,
///   `"ivf_sq"`, `"ivf_pq"`, `"ivf_rq"`, `"ivf_hnsw_flat"`, `"ivf_hnsw_pq"`,
///   `"ivf_hnsw_sq"`, or a scalar index plugin name, optionally prefixed
///   with `"scalar:"` (e.g. `"json"` or `"scalar:json"`), routed through the
///   scalar index plugin registry.
/// - `name`: optional index name, or NULL for the Lance default.
/// - `params_json`: optional JSON parameter document, or NULL for defaults.
///   Shape depends on the index family:
///   - scalar builtin and plugin types: `{"params"?: <index-specific JSON>}`
///     (e.g. for the "json" plugin:
///     `{"params": {"target_index_type": "btree",
///     "target_index_parameters"?: "<JSON string>", "path": "x"}}`)
///   - inverted: same shape as `lance_dataset_create_index`
///   - vector types: the `lance_dataset_create_index` shape plus
///     `"target_partition_size"?: uint, "retrain"?: bool,
///     "streaming_sample_rate"?: uint, "streaming_coreset_rate"?: uint,
///     "streaming_refine_passes"?: uint, "shuffle_partition_batches"?: uint,
///     "shuffle_partition_concurrency"?: uint,
///     "precomputed_partitions_file"?: string,
///     "storage_options"?: {string: string}, "kmeans_redos"?: uint,
///     "hnsw"?: {..., "prefetch_distance"?: uint},
///     "index_file_version"?: "legacy"|"v3", "skip_transpose"?: bool,
///     "runtime_hints"?: {string: string}`
/// - `centroids` / `centroids_schema`: optional Arrow C Data Interface
///   FixedSizeList array of pre-computed IVF centroids (vector types only).
///   Both NULL or both non-NULL. Ownership of both structs is always taken,
///   even on error.
/// - `codebook` / `codebook_schema`: optional Arrow C Data Interface array
///   holding a pre-computed PQ codebook (PQ types only). Same ownership
///   contract as `centroids`.
/// - `replace`: whether an existing index with the same name is replaced.
/// - `out_json`: if non-NULL, receives the created index metadata as JSON
///   (same shape as `lance_dataset_create_index`). Free with
///   `lance_string_free`.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_create_index_v2(
    ds: *mut LanceDataset,
    columns: *const *const c_char,
    index_type: *const c_char,
    name: *const c_char,
    params_json: *const c_char,
    centroids: *mut FFI_ArrowArray,
    centroids_schema: *mut FFI_ArrowSchema,
    codebook: *mut FFI_ArrowArray,
    codebook_schema: *mut FFI_ArrowSchema,
    replace: bool,
    out_json: *mut *mut c_char,
) -> i32 {
    // Take ownership of the Arrow payloads first so they are released on
    // every path, including argument-validation failures.
    let imported = (|| -> Result<_, String> {
        let centroids = unsafe { import_optional_array(centroids, centroids_schema, "centroids") };
        let codebook = unsafe { import_optional_array(codebook, codebook_schema, "codebook") };
        Ok((centroids?, codebook?))
    })();
    let (centroids, codebook) = match imported {
        Ok(pair) => pair,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let parsed = unsafe {
        (|| -> Result<_, String> {
            Ok((
                parse_columns(columns)?,
                storage::required_str(index_type, "index_type")?,
                storage::optional_str(name, "name")?.map(str::to_owned),
                storage::optional_str(params_json, "params_json")?,
            ))
        })()
    };
    let (columns, index_type, name, params_json) = match parsed {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let resolved = match resolve_index_type_str(index_type) {
        Ok(resolved) => resolved,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    match resolved {
        ResolvedIndexType::Builtin(index_type) => {
            let params = match build_index_params(index_type, params_json, centroids, codebook) {
                Ok(params) => params,
                Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
            };
            unsafe {
                run_create_index(
                    ds,
                    &columns,
                    index_type,
                    name,
                    params.as_ref(),
                    replace,
                    out_json,
                )
            }
        }
        ResolvedIndexType::ScalarPlugin(plugin) => {
            if centroids.is_some() || codebook.is_some() {
                return set_error(
                    ErrorCode::InvalidArgument,
                    "centroids/codebook are only supported for vector index types",
                );
            }
            let json: ScalarParamsJson = match parse_params_json(params_json, "scalar index params")
            {
                Ok(json) => json,
                Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
            };
            let params = ScalarIndexParams {
                index_type: plugin,
                params: json.params.map(|v| v.to_string()),
            };
            unsafe {
                run_create_index(
                    ds,
                    &columns,
                    IndexType::Scalar,
                    name,
                    &params,
                    replace,
                    out_json,
                )
            }
        }
    }
}

/// Lists the indices of the dataset as a JSON array of
/// `{"name", "uuid", "fields", "dataset_version", "index_version",
/// "created_at"?}` objects. The caller owns `*out_json` and must free it
/// with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_list_indices(
    ds: *const LanceDataset,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let dataset = unsafe { &*ds }.dataset();
    let indices = match block_on_cc!(dataset.load_indices()) {
        Ok(indices) => indices,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let infos: Vec<IndexInfoJson> = indices.iter().map(IndexInfoJson::from).collect();
    match serde_json::to_string(&infos) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Returns index statistics (a Lance-defined JSON document) for the index
/// named `name`. The caller owns `*out_json` and must free it with
/// `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle, `name` a valid C string, and `out_json`
/// valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_index_statistics(
    ds: *const LanceDataset,
    name: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let name = match unsafe { storage::required_str(name, "name") } {
        Ok(name) => name,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.index_statistics(name)) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Drops the index named `name`. Upon success a new dataset version is
/// committed and the handle is updated to it.
///
/// # Safety
///
/// `ds` must be a valid handle and `name` a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_drop_index(
    ds: *mut LanceDataset,
    name: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let name = match unsafe { storage::required_str(name, "name") } {
        Ok(name) => name,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.drop_index(name)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Optimizes (merges/updates) the dataset's indices to cover newly written
/// data. Upon success a new dataset version is committed and the handle is
/// updated to it.
///
/// - `options_json`: optional JSON object `{"num_indices_to_merge"?: uint,
///   "index_names"?: [string], "retrain"?: bool,
///   "transaction_properties"?: {string: string}}`, or NULL for the Lance
///   defaults (merge into 1 index, all indices, no retrain).
///
/// # Safety
///
/// `ds` must be a valid handle and `options_json` NULL or a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_optimize_indices(
    ds: *mut LanceDataset,
    options_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let options_json = match unsafe { storage::optional_str(options_json, "options_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let parsed: OptimizeOptionsJson = match parse_params_json(options_json, "optimize options") {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    // OptimizeOptions is #[non_exhaustive]: start from Default and only
    // override what the caller provided.
    let mut options = OptimizeOptions::default();
    if let Some(n) = parsed.num_indices_to_merge {
        options.num_indices_to_merge = Some(n);
    }
    if let Some(names) = parsed.index_names {
        options.index_names = Some(names);
    }
    if let Some(retrain) = parsed.retrain {
        options.retrain = retrain;
    }
    if let Some(properties) = parsed.transaction_properties {
        options.transaction_properties = Some(Arc::new(properties));
    }

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.optimize_indices(&options)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Loads the index named `name` into memory ahead of queries.
///
/// # Safety
///
/// `ds` must be a valid handle and `name` a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_prewarm_index(
    ds: *const LanceDataset,
    name: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let name = match unsafe { storage::required_str(name, "name") } {
        Ok(name) => name,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.prewarm_index(name)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Loads the index named `name` into memory ahead of queries, with
/// index-type-specific options.
///
/// - `options_json`: JSON object selecting the options variant. Currently
///   the only variant is FTS (inverted indices):
///   `{"fts": {"with_position"?: bool}}`. `with_position` additionally
///   prewarms token positions along with the posting lists.
///
/// # Safety
///
/// `ds` must be a valid handle, `name` a valid C string, and `options_json`
/// NULL or a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_prewarm_index_with_options(
    ds: *const LanceDataset,
    name: *const c_char,
    options_json: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (name, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(name, "name")?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let parsed: PrewarmOptionsJson = match parse_params_json(options_json, "prewarm options") {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let Some(fts) = parsed.fts else {
        return set_error(
            ErrorCode::InvalidArgument,
            "prewarm options must select a variant (currently only \"fts\")",
        );
    };
    let options = PrewarmOptions::Fts(
        FtsPrewarmOptions::new().with_position(fts.with_position.unwrap_or(false)),
    );
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.prewarm_index_with_options(name, &options)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Describes the dataset's indices as a JSON array of
/// `{"name", "index_type", "type_url", "rows_indexed", "field_ids",
/// "details"?, "total_size_bytes"?, "segments": [...]}` objects, where
/// `"segments"` entries have the `lance_dataset_list_indices` shape and
/// `"details"` is an index-type-specific JSON document (omitted when it
/// cannot be decoded). The caller owns `*out_json` and must free it with
/// `lance_string_free`.
///
/// - `criteria_json`: optional JSON object `{"for_column"?: string,
///   "has_name"?: string, "must_support_fts"?: bool,
///   "must_support_exact_equality"?: bool}`, or NULL for all indices.
///
/// # Safety
///
/// `ds` must be a valid handle, `criteria_json` NULL or a valid C string,
/// and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_describe_indices(
    ds: *const LanceDataset,
    criteria_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let criteria_json = match unsafe { storage::optional_str(criteria_json, "criteria_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let parsed: Option<IndexCriteriaJson> = match criteria_json {
        None => None,
        Some(s) if s.trim().is_empty() => None,
        Some(s) => match serde_json::from_str(s) {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    format!("invalid index criteria JSON: {e}"),
                );
            }
        },
    };
    let criteria = parsed.as_ref().map(|json| {
        let mut criteria = IndexCriteria::default();
        if let Some(column) = &json.for_column {
            criteria = criteria.for_column(column);
        }
        if let Some(name) = &json.has_name {
            criteria = criteria.with_name(name);
        }
        if json.must_support_fts.unwrap_or(false) {
            criteria = criteria.supports_fts();
        }
        if json.must_support_exact_equality.unwrap_or(false) {
            criteria = criteria.supports_exact_equality();
        }
        criteria
    });

    let dataset = unsafe { &*ds }.dataset();
    let descriptions = match block_on_cc!(dataset.describe_indices(criteria)) {
        Ok(descriptions) => descriptions,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let infos: Vec<IndexDescriptionJson> = descriptions
        .iter()
        .map(|desc| IndexDescriptionJson {
            name: desc.name().to_string(),
            index_type: desc.index_type().to_string(),
            type_url: desc.type_url().to_string(),
            rows_indexed: desc.rows_indexed(),
            field_ids: desc.field_ids().to_vec(),
            details: desc.details().ok(),
            total_size_bytes: desc.total_size_bytes(),
            segments: desc.segments().iter().map(IndexInfoJson::from).collect(),
        })
        .collect();
    match serde_json::to_string(&infos) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Loads index metadata matching the criteria as a JSON array of
/// `lance_dataset_list_indices`-shaped objects (an empty array when nothing
/// matches). The caller owns `*out_json` and must free it with
/// `lance_string_free`.
///
/// - `criteria_json`: optional JSON object `{"uuid"?: string,
///   "name"?: string}`. `uuid` and `name` are mutually exclusive: `uuid`
///   selects the index segment with that build UUID, `name` selects all
///   (delta) segments of the index with that name, and NULL / `{}` selects
///   every index.
///
/// # Safety
///
/// `ds` must be a valid handle, `criteria_json` NULL or a valid C string,
/// and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_load_indices_by_criteria(
    ds: *const LanceDataset,
    criteria_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let criteria_json = match unsafe { storage::optional_str(criteria_json, "criteria_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let criteria: LoadIndexCriteriaJson =
        match parse_params_json(criteria_json, "load index criteria") {
            Ok(criteria) => criteria,
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        };
    if criteria.uuid.is_some() && criteria.name.is_some() {
        return set_error(
            ErrorCode::InvalidArgument,
            "load index criteria \"uuid\" and \"name\" are mutually exclusive",
        );
    }

    let dataset = unsafe { &*ds }.dataset();
    let metadata: Vec<IndexMetadata> = if let Some(name) = &criteria.name {
        match block_on_cc!(dataset.load_indices_by_name(name)) {
            Ok(metadata) => metadata,
            Err(e) => return set_error(map_lance_error(&e), e),
        }
    } else {
        let indices = match block_on_cc!(dataset.load_indices()) {
            Ok(indices) => indices,
            Err(e) => return set_error(map_lance_error(&e), e),
        };
        match &criteria.uuid {
            Some(uuid) => {
                let wanted = uuid.to_lowercase();
                indices
                    .iter()
                    .filter(|idx| idx.uuid.to_string() == wanted)
                    .cloned()
                    .collect()
            }
            None => indices.iter().cloned().collect(),
        }
    };
    let infos: Vec<IndexInfoJson> = metadata.iter().map(IndexInfoJson::from).collect();
    match serde_json::to_string(&infos) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Remaps the index on `columns` to row addresses moved by a compaction
/// that ran with `defer_index_remap` (which records the moves in the
/// fragment reuse index). Upon success a new dataset version may be
/// committed and the handle is updated to it. When there is nothing to
/// remap the call is a no-op. Fails if no fragment reuse index exists.
///
/// - `columns`: NULL-terminated array of column names (currently exactly
///   one). Must not be NULL.
/// - `name`: optional index name, or NULL for the Lance default
///   (`<column>_idx`).
///
/// # Safety
///
/// `ds` must be a valid handle, `columns` a valid NULL-terminated array of
/// C strings, and `name` NULL or a valid C string.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_remap_column_index(
    ds: *mut LanceDataset,
    columns: *const *const c_char,
    name: *const c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let (columns, name) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                parse_columns(columns)?,
                storage::optional_str(name, "name")?.map(str::to_owned),
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(remapping::remap_column_index(
        &mut guard,
        &column_refs,
        name,
    )) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
