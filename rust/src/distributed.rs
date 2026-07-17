//! Distributed index build and distributed compaction.
//!
//! Mirrors the "driver + workers" pattern of Lance's distributed index build
//! (see `python/examples/distributed_ivf_rq_100_segments.py`): a driver mints
//! shared IVF centroids, each worker builds a per-fragment *uncommitted*
//! index segment pinned to those centroids, the driver merges the segments
//! and commits one logical index.
//!
//! Like transactions, [`IndexMetadata`] is protobuf-backed (not serde), so a
//! segment crosses the FFI boundary as `pb::IndexMetadata` bytes (lossless,
//! preserving `index_details`/`fragment_bitmap`), freed with
//! `lance_bytes_free`, plus a JSON summary for inspection.
//!
//! Index parameters are built via `crate::index::build_index_params` (shared
//! `pub(crate)` helper). The few small JSON structs / helpers still declared
//! here are the ones this module also uses directly.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::str::FromStr;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, make_array};
use futures::TryStreamExt;
use lance::index::DatasetIndexExt;
use lance::table::format::{IndexMetadata, pb};
use lance_index::scalar::ScalarIndexParams;
use lance_index::{IndexParams, IndexType};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

// Owned buffer / JSON emit helpers

/// Hands `bytes` to the caller as an owned `(ptr, len)` buffer (freed with
/// `lance_bytes_free`). Empty input yields a NULL pointer with length 0.
///
/// # Safety
///
/// `out_ptr` and `out_len` must be non-NULL and valid for writes.
unsafe fn emit_bytes(bytes: Vec<u8>, out_ptr: *mut *mut u8, out_len: *mut usize) {
    if bytes.is_empty() {
        unsafe {
            *out_ptr = std::ptr::null_mut();
            *out_len = 0;
        }
        return;
    }
    let mut boxed = bytes.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    let len = boxed.len();
    std::mem::forget(boxed);
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
}

/// Writes `json` into `*out_json` as an owned C string.
///
/// # Safety
///
/// `out_json` must be non-NULL and valid for writes.
unsafe fn emit_json(json: String, out_json: *mut *mut c_char) -> i32 {
    match CString::new(json) {
        Ok(cstr) => {
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

// IndexMetadata <-> protobuf bytes / JSON view

fn encode_index_metadata(meta: &IndexMetadata) -> Vec<u8> {
    let pb_meta: pb::IndexMetadata = meta.into();
    pb_meta.encode_to_vec()
}

fn decode_index_metadata(bytes: &[u8]) -> Result<IndexMetadata, String> {
    let pb_meta = pb::IndexMetadata::decode(bytes)
        .map_err(|e| format!("failed to decode index metadata: {e}"))?;
    IndexMetadata::try_from(pb_meta).map_err(|e| format!("failed to rebuild index metadata: {e}"))
}

fn index_metadata_view(meta: &IndexMetadata) -> Value {
    json!({
        "name": meta.name,
        "uuid": meta.uuid.to_string(),
        "fields": meta.fields,
        "dataset_version": meta.dataset_version,
        "index_version": meta.index_version,
        "created_at": meta.created_at.map(|t| t.to_rfc3339()),
    })
}

/// Emits an index segment both as protobuf bytes (lossless) and JSON summary.
///
/// # Safety
///
/// Output pointers must be non-NULL and valid for writes.
unsafe fn emit_index_metadata(
    meta: &IndexMetadata,
    out_pb: *mut *mut u8,
    out_pb_len: *mut usize,
    out_json: *mut *mut c_char,
) -> i32 {
    unsafe { emit_bytes(encode_index_metadata(meta), out_pb, out_pb_len) };
    unsafe { emit_json(index_metadata_view(meta).to_string(), out_json) }
}

/// Decodes a parallel `(ptrs, lens, count)` array of protobuf buffers into a
/// `Vec<IndexMetadata>`.
///
/// # Safety
///
/// Both arrays must hold `count` valid entries, and `ptrs[i]` must point to
/// `lens[i]` valid bytes.
unsafe fn decode_segments(
    ptrs: *const *const u8,
    lens: *const usize,
    count: usize,
) -> Result<Vec<IndexMetadata>, String> {
    if ptrs.is_null() || lens.is_null() {
        return Err("segment arrays must not be NULL".to_string());
    }
    let mut segments = Vec::with_capacity(count);
    for i in 0..count {
        let ptr = unsafe { *ptrs.add(i) };
        let len = unsafe { *lens.add(i) };
        if ptr.is_null() {
            return Err(format!("segment {i} pointer is NULL"));
        }
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
        segments.push(decode_index_metadata(bytes)?);
    }
    Ok(segments)
}

// Index-type resolution + JSON param parsing (module-local helpers)

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ScalarParamsJson {
    params: Option<serde_json::Value>,
}

enum ResolvedIndexType {
    Builtin(IndexType),
    ScalarPlugin(String),
}

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
        _ => return Ok(ResolvedIndexType::ScalarPlugin(norm)),
    };
    Ok(ResolvedIndexType::Builtin(builtin))
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

unsafe fn import_optional_array(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    what: &str,
) -> Result<Option<ArrayRef>, String> {
    let array = (!array.is_null()).then(|| unsafe { FFI_ArrowArray::from_raw(array) });
    let schema = (!schema.is_null()).then(|| unsafe { FFI_ArrowSchema::from_raw(schema) });
    match (array, schema) {
        (None, None) => Ok(None),
        (Some(array), Some(schema)) => {
            let data = unsafe { arrow::ffi::from_ffi(array, &schema) }
                .map_err(|e| format!("failed to import {what}: {e}"))?;
            Ok(Some(make_array(data)))
        }
        _ => Err(format!(
            "{what} array and schema must both be NULL or both non-NULL"
        )),
    }
}

unsafe fn parse_columns(columns: *const *const c_char) -> Result<Vec<String>, String> {
    if columns.is_null() {
        return Err("columns must not be NULL".to_string());
    }
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
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

// lance_dataset_create_index_uncommitted

/// Options accepted by `lance_dataset_create_index_uncommitted`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CreateUncommittedOptions {
    /// Whether to train the IVF/quantizer model (default true). Set false when
    /// supplying shared centroids so every segment stays comparable.
    train: Option<bool>,
    /// Restrict the build to these fragment ids (a distributed worker's slice).
    fragments: Option<Vec<u32>>,
    /// Pin the segment's index UUID.
    index_uuid: Option<String>,
    /// Explicit index name (defaults to `<column>_idx`).
    name: Option<String>,
    transaction_properties: Option<HashMap<String, String>>,
}

/// Builds an *uncommitted* index segment over an optional fragment subset and
/// returns it as protobuf bytes (`out_pb`/`out_pb_len`, freed with
/// `lance_bytes_free`) plus a JSON summary (`out_json`).
///
/// - `columns`: NULL-terminated array of column names (exactly one).
/// - `index_type`: same string contract as `lance_dataset_create_index_v2`.
/// - `params_json`: same shape as `lance_dataset_create_index_v2`.
/// - `options_json`: `{"train"?, "fragments"?: [uint], "index_uuid"?: string,
///   "name"?: string, "transaction_properties"?: {string: string}}`.
/// - `centroids`/`codebook` (+ schemas): optional shared training arrays,
///   same ownership contract as `lance_dataset_create_index_v2`.
/// - `preprocessed_stream`: optional preprocessed-data Arrow stream (BTree).
///   NULL for none. Ownership is always taken when non-NULL.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn lance_dataset_create_index_uncommitted(
    ds: *mut LanceDataset,
    columns: *const *const c_char,
    index_type: *const c_char,
    params_json: *const c_char,
    options_json: *const c_char,
    centroids: *mut FFI_ArrowArray,
    centroids_schema: *mut FFI_ArrowSchema,
    codebook: *mut FFI_ArrowArray,
    codebook_schema: *mut FFI_ArrowSchema,
    preprocessed_stream: *mut FFI_ArrowArrayStream,
    out_pb: *mut *mut u8,
    out_pb_len: *mut usize,
    out_json: *mut *mut c_char,
) -> i32 {
    // Take ownership of all foreign payloads first so they are released on
    // every path.
    let imported = (|| -> Result<_, String> {
        let centroids = unsafe { import_optional_array(centroids, centroids_schema, "centroids") };
        let codebook = unsafe { import_optional_array(codebook, codebook_schema, "codebook") };
        Ok((centroids?, codebook?))
    })();
    let preprocessed = if preprocessed_stream.is_null() {
        None
    } else {
        match unsafe { arrow_bridge::import_stream(preprocessed_stream) } {
            Ok(reader) => Some(reader),
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        }
    };
    let (centroids, codebook) = match imported {
        Ok(pair) => pair,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if ds.is_null() || out_pb.is_null() || out_pb_len.is_null() || out_json.is_null() {
        return set_error(ErrorCode::InvalidArgument, "arguments must not be NULL");
    }
    let parsed = unsafe {
        (|| -> Result<_, String> {
            Ok((
                parse_columns(columns)?,
                storage::required_str(index_type, "index_type")?,
                storage::optional_str(params_json, "params_json")?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    };
    let (columns, index_type, params_json, options_json) = match parsed {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: CreateUncommittedOptions =
        match parse_params_json(options_json, "create-uncommitted options") {
            Ok(options) => options,
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        };
    let index_uuid = match options
        .index_uuid
        .as_deref()
        .map(Uuid::from_str)
        .transpose()
    {
        Ok(uuid) => uuid,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid index_uuid: {e}"),
            );
        }
    };
    let (resolved_type, params) = match resolve_index_type_str(index_type) {
        Ok(ResolvedIndexType::Builtin(t)) => {
            match crate::index::build_index_params(t, params_json, centroids, codebook) {
                Ok(params) => (t, params),
                Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
            }
        }
        Ok(ResolvedIndexType::ScalarPlugin(plugin)) => {
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
            let params: Box<dyn IndexParams> = Box::new(ScalarIndexParams {
                index_type: plugin,
                params: json.params.map(|v| v.to_string()),
            });
            (IndexType::Scalar, params)
        }
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut builder = guard.create_index_builder(&column_refs, resolved_type, params.as_ref());
    if let Some(train) = options.train {
        builder = builder.train(train);
    }
    if let Some(fragments) = options.fragments {
        builder = builder.fragments(fragments);
    }
    if let Some(uuid) = index_uuid {
        builder = builder.index_uuid(uuid);
    }
    if let Some(name) = options.name {
        builder = builder.name(name);
    }
    if let Some(properties) = options.transaction_properties {
        builder = builder.transaction_properties(properties);
    }
    if let Some(reader) = preprocessed {
        builder = builder.preprocessed_data(Box::new(reader));
    }
    let meta = match block_on_cc!(builder.execute_uncommitted()) {
        Ok(meta) => meta,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    unsafe { emit_index_metadata(&meta, out_pb, out_pb_len, out_json) }
}

// lance_dataset_merge_index_segments

/// Merges uncommitted index segments (each `pb::IndexMetadata` bytes) into a
/// single uncommitted segment, returned as protobuf bytes + JSON summary.
///
/// # Safety
///
/// The two arrays must each hold `num_segments` valid entries, and `segments_pb[i]`
/// must point to `segments_pb_lens[i]` valid bytes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_merge_index_segments(
    ds: *const LanceDataset,
    segments_pb: *const *const u8,
    segments_pb_lens: *const usize,
    num_segments: usize,
    out_pb: *mut *mut u8,
    out_pb_len: *mut usize,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_pb.is_null() || out_pb_len.is_null() || out_json.is_null() {
        return set_error(ErrorCode::InvalidArgument, "arguments must not be NULL");
    }
    if num_segments == 0 {
        return set_error(ErrorCode::InvalidArgument, "num_segments must be > 0");
    }
    let segments = match unsafe { decode_segments(segments_pb, segments_pb_lens, num_segments) } {
        Ok(segments) => segments,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.merge_existing_index_segments(segments)) {
        Ok(meta) => unsafe { emit_index_metadata(&meta, out_pb, out_pb_len, out_json) },
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

// lance_dataset_commit_index_segments

/// Commits one or more physical index segments (each `pb::IndexMetadata`
/// bytes) as a single logical index named `index_name` over `column`, and
/// updates the dataset handle to the resulting version. Returns the new
/// version as JSON `{"version": uint}`.
///
/// # Safety
///
/// Pointer contracts as on `lance_dataset_merge_index_segments`. `name` and
/// `column` must be valid C strings.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_commit_index_segments(
    ds: *mut LanceDataset,
    index_name: *const c_char,
    column: *const c_char,
    segments_pb: *const *const u8,
    segments_pb_lens: *const usize,
    num_segments: usize,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    if num_segments == 0 {
        return set_error(ErrorCode::InvalidArgument, "num_segments must be > 0");
    }
    let (index_name, column) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(index_name, "index_name")?.to_owned(),
                storage::required_str(column, "column")?.to_owned(),
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let segments = match unsafe { decode_segments(segments_pb, segments_pb_lens, num_segments) } {
        Ok(segments) => segments,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.commit_existing_index_segments(&index_name, &column, segments)) {
        Ok(()) => {
            let version = guard.version().version;
            unsafe { emit_json(json!({ "version": version }).to_string(), out_json) }
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

// lance_dataset_read_index_partition

/// Streams the rows of an index partition into the caller-provided
/// `ArrowArrayStream`. The partition is materialized before export (partitions
/// are small). The caller owns the exported stream.
///
/// # Safety
///
/// `ds` must be a valid handle, `name` a valid C string, and `out` valid for
/// writes (previous contents overwritten without release).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_read_index_partition(
    ds: *const LanceDataset,
    name: *const c_char,
    partition: u64,
    with_vector: bool,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let name = match unsafe { storage::required_str(name, "name") } {
        Ok(name) => name,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let dataset = unsafe { &*ds }.dataset();
    let stream =
        match block_on_cc!(dataset.read_index_partition(name, partition as usize, with_vector)) {
            Ok(stream) => stream,
            Err(e) => return set_error(map_lance_error(&e), e),
        };
    let schema = stream.schema();
    let batches: Vec<RecordBatch> = match block_on_cc!(stream.try_collect()) {
        Ok(batches) => batches,
        Err(e) => return set_error(ErrorCode::Internal, format!("read index partition: {e}")),
    };
    let reader = RecordBatchIterator::new(
        batches
            .into_iter()
            .map(Ok::<RecordBatch, arrow_schema::ArrowError>),
        schema,
    );
    let ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));
    // SAFETY: `out` is valid for writes per the contract above.
    unsafe { std::ptr::write_unaligned(out, ffi_stream) };
    ok()
}

// lance_dataset_plan_compaction (distributed compaction, planning)

/// Optional overrides for a compaction plan. Unset fields keep the Lance
/// defaults.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CompactionOptionsJson {
    target_rows_per_fragment: Option<usize>,
    max_rows_per_group: Option<usize>,
    max_bytes_per_file: Option<usize>,
    materialize_deletions: Option<bool>,
    materialize_deletions_threshold: Option<f32>,
    num_threads: Option<usize>,
    batch_size: Option<usize>,
}

/// Plans a compaction and returns the plan as JSON (the plan's task list is
/// `serde`-serializable, unlike transactions). This is the planning half of a
/// distributed compaction. Executing tasks and committing their results are
/// deferred (see the module note and the wrap-up report).
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_plan_compaction(
    ds: *const LanceDataset,
    options_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let options_json = match unsafe { storage::optional_str(options_json, "options_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let parsed: CompactionOptionsJson = match parse_params_json(options_json, "compaction options")
    {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let mut options = lance::dataset::optimize::CompactionOptions::default();
    if let Some(n) = parsed.target_rows_per_fragment {
        options.target_rows_per_fragment = n;
    }
    if let Some(n) = parsed.max_rows_per_group {
        options.max_rows_per_group = n;
    }
    if parsed.max_bytes_per_file.is_some() {
        options.max_bytes_per_file = parsed.max_bytes_per_file;
    }
    if let Some(b) = parsed.materialize_deletions {
        options.materialize_deletions = b;
    }
    if let Some(t) = parsed.materialize_deletions_threshold {
        options.materialize_deletions_threshold = t;
    }
    if parsed.num_threads.is_some() {
        options.num_threads = parsed.num_threads;
    }
    if parsed.batch_size.is_some() {
        options.batch_size = parsed.batch_size;
    }

    let dataset = unsafe { &*ds }.dataset();
    let plan = match block_on_cc!(lance::dataset::optimize::plan_compaction(
        &dataset, &options,
    )) {
        Ok(plan) => plan,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match serde_json::to_string(&plan) {
        Ok(json) => unsafe { emit_json(json, out_json) },
        Err(e) => set_error(
            ErrorCode::Internal,
            format!("serialize compaction plan: {e}"),
        ),
    }
}
