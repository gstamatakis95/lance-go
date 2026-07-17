//! Uncommitted writes, commits, and transaction/manifest reads (the
//! building blocks of a distributed write pipeline).
//!
//! Unlike most of this shim, [`Transaction`], [`Operation`], `Manifest` and
//! [`IndexMetadata`] are **not** serde-serializable in Lance: they are
//! protobuf-backed (`prost::Message`). We therefore cross the FFI boundary
//! two ways:
//!
//! - **Lossless wire form**: a `Transaction` is encoded to `pb::Transaction`
//!   protobuf bytes (the exact form Lance writes to its own transaction
//!   files) and handed back as an owned `(ptr, len)` buffer, freed with
//!   [`lance_bytes_free`]. `lance_commit` takes those bytes back, decodes,
//!   and commits, a perfect round trip, including variants this shim does
//!   not otherwise understand.
//! - **Inspection view**: a hand-built JSON summary (operation type + salient
//!   fields) for human/typed access on the Go side, mirroring the
//!   `IndexInfoJson` pattern in `index.rs`.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::transaction::{Operation, Transaction, UpdateMap, UpdateMapEntry};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteParams};
use lance::table::format::pb;
use lance_file::version::LanceFileVersion;
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

// Owned byte-buffer plumbing

/// Hands `bytes` to the caller as an owned `(ptr, len)` buffer. The caller
/// must release it with [`lance_bytes_free`]. Empty input yields a NULL
/// pointer with length 0.
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

/// Frees a `(ptr, len)` byte buffer previously handed out by this crate (e.g.
/// `lance_write_fragments` or the blob-read functions). NULL is a no-op. `len`
/// must be the exact length that was returned alongside `ptr`.
///
/// # Safety
///
/// `ptr` must be NULL or a pointer previously returned by this crate with the
/// matching `len`, not already freed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_bytes_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    // SAFETY: reconstitutes the boxed slice leaked by `emit_bytes`.
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    drop(unsafe { Box::from_raw(slice) });
}

/// Writes `json` into `*out_json` as an owned C string (freed with
/// `lance_string_free`).
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

/// Boxes `dataset` into an opaque handle written to `out`.
///
/// # Safety
///
/// `out` must be non-NULL and valid for writes.
unsafe fn emit_dataset(dataset: lance::Dataset, out: *mut *mut LanceDataset) {
    let handle = Box::into_raw(Box::new(LanceDataset(Mutex::new(dataset))));
    unsafe { *out = handle };
}

// Transaction <-> protobuf bytes / JSON view

/// Encodes a `Transaction` to its `pb::Transaction` protobuf bytes (the same
/// form Lance persists in its transaction files).
pub(crate) fn encode_transaction(txn: &Transaction) -> Vec<u8> {
    let pb_txn: pb::Transaction = txn.into();
    pb_txn.encode_to_vec()
}

/// Decodes `pb::Transaction` protobuf bytes back into a `Transaction`.
pub(crate) fn decode_transaction(bytes: &[u8]) -> Result<Transaction, String> {
    let pb_txn =
        pb::Transaction::decode(bytes).map_err(|e| format!("failed to decode transaction: {e}"))?;
    Transaction::try_from(pb_txn).map_err(|e| format!("failed to rebuild transaction: {e}"))
}

fn fragment_ids(fragments: &[lance::table::format::Fragment]) -> Vec<u64> {
    fragments.iter().map(|f| f.id).collect()
}

/// A JSON summary of an [`Operation`]: a `{"type": <name>, ...}` object with
/// the operation's salient scalar fields. Not lossless (the protobuf bytes
/// are the lossless form) but enough for inspection and the common variants.
fn operation_view(op: &Operation) -> Value {
    match op {
        Operation::Append { fragments } => json!({
            "type": "Append",
            "num_fragments": fragments.len(),
            "fragment_ids": fragment_ids(fragments),
        }),
        Operation::Delete {
            updated_fragments,
            deleted_fragment_ids,
            predicate,
        } => json!({
            "type": "Delete",
            "predicate": predicate,
            "deleted_fragment_ids": deleted_fragment_ids,
            "updated_fragment_ids": fragment_ids(updated_fragments),
        }),
        Operation::Overwrite { fragments, .. } => json!({
            "type": "Overwrite",
            "num_fragments": fragments.len(),
            "fragment_ids": fragment_ids(fragments),
        }),
        Operation::CreateIndex {
            new_indices,
            removed_indices,
        } => json!({
            "type": "CreateIndex",
            "new_indices": new_indices.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
            "removed_indices": removed_indices.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
        }),
        Operation::Rewrite { groups, .. } => json!({
            "type": "Rewrite",
            "num_groups": groups.len(),
        }),
        Operation::Merge { fragments, .. } => json!({
            "type": "Merge",
            "num_fragments": fragments.len(),
        }),
        Operation::Restore { version } => json!({"type": "Restore", "version": version}),
        Operation::ReserveFragments { num_fragments } => {
            json!({"type": "ReserveFragments", "num_fragments": num_fragments})
        }
        Operation::Project { .. } => json!({"type": "Project"}),
        Operation::Update { .. } => json!({"type": "Update"}),
        Operation::UpdateConfig { .. } => json!({"type": "UpdateConfig"}),
        Operation::DataReplacement { replacements } => json!({
            "type": "DataReplacement",
            "num_replacements": replacements.len(),
        }),
        // Clone / UpdateBases / UpdateMemWalState and any future variant: the
        // protobuf bytes still round-trip losslessly. The view is minimal.
        other => json!({"type": format!("{other}")}),
    }
}

/// A JSON summary of a whole [`Transaction`].
fn transaction_view(txn: &Transaction) -> Value {
    json!({
        "read_version": txn.read_version,
        "uuid": txn.uuid,
        "tag": txn.tag,
        "operation": operation_view(&txn.operation),
        "transaction_properties": txn
            .transaction_properties
            .as_ref()
            .map(|p| (**p).clone()),
    })
}

// lance_write_fragments

/// Subset of `WriteParams` relevant to a standalone fragment write.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WriteFragmentsOptions {
    /// "create" (default) writes an Overwrite transaction that creates the
    /// dataset. "append" writes an Append transaction against the existing
    /// dataset at `uri` (the distributed-append case).
    mode: Option<String>,
    max_rows_per_file: Option<usize>,
    max_rows_per_group: Option<usize>,
    max_bytes_per_file: Option<usize>,
    data_storage_version: Option<String>,
    enable_stable_row_ids: Option<bool>,
    enable_v2_manifest_paths: Option<bool>,
    transaction_properties: Option<HashMap<String, String>>,
}

fn parse_json_options<T: Default + for<'de> Deserialize<'de>>(
    json: Option<&str>,
    what: &str,
) -> Result<T, String> {
    match json {
        None => Ok(T::default()),
        Some(s) if s.trim().is_empty() => Ok(T::default()),
        Some(s) => serde_json::from_str(s).map_err(|e| format!("invalid {what} JSON: {e}")),
    }
}

/// Writes `stream`'s batches as new, uncommitted fragments at `uri` and
/// returns the resulting [`Transaction`] both as lossless protobuf bytes
/// (`out_txn_pb` / `out_txn_pb_len`, freed with [`lance_bytes_free`]) and as
/// a JSON summary (`out_txn_json`, freed with `lance_string_free`).
///
/// This is the distributed-write primitive: run it on N workers over disjoint
/// data, ship the N transactions to a driver, and commit them with
/// `lance_commit` / `lance_commit_batch`.
///
/// - `stream`: caller-produced Arrow C stream, ownership is always taken.
/// - `uri`: destination dataset URI. Must not be NULL.
/// - `options_json`: optional `{"max_rows_per_file"?, "max_rows_per_group"?,
///   "max_bytes_per_file"?, "data_storage_version"?, "enable_stable_row_ids"?,
///   "enable_v2_manifest_paths"?, "transaction_properties"?}`.
/// - `storage_kv`: storage options, same format as `lance_dataset_open`.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above, and the `out_*`
/// pointers must be non-NULL and valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_write_fragments(
    stream: *mut FFI_ArrowArrayStream,
    uri: *const c_char,
    options_json: *const c_char,
    storage_kv: *const *const c_char,
    out_txn_pb: *mut *mut u8,
    out_txn_pb_len: *mut usize,
    out_txn_json: *mut *mut c_char,
) -> i32 {
    // Import the stream FIRST so its producer resources are owned on every
    // error path.
    let reader = match unsafe { arrow_bridge::import_stream(stream) } {
        Ok(reader) => reader,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if out_txn_pb.is_null() || out_txn_pb_len.is_null() || out_txn_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "output pointers must not be NULL",
        );
    }
    let (uri, storage_options, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(uri, "uri")?,
                storage::parse_storage_kv(storage_kv)?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: WriteFragmentsOptions =
        match parse_json_options(options_json, "write fragments options") {
            Ok(options) => options,
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        };
    let data_storage_version = match options
        .data_storage_version
        .as_deref()
        .map(LanceFileVersion::from_str)
        .transpose()
    {
        Ok(version) => version,
        Err(e) => return set_error(ErrorCode::InvalidArgument, e),
    };
    let mode = match options.mode.as_deref() {
        None | Some("create") => lance::dataset::WriteMode::Create,
        Some("append") => lance::dataset::WriteMode::Append,
        Some("overwrite") => lance::dataset::WriteMode::Overwrite,
        Some(other) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid mode {other:?}, expected \"create\", \"append\" or \"overwrite\""),
            );
        }
    };
    let mut params = WriteParams {
        mode,
        data_storage_version,
        store_params: storage::object_store_params(storage_options),
        ..Default::default()
    };
    if let Some(n) = options.max_rows_per_file {
        params.max_rows_per_file = n;
    }
    if let Some(n) = options.max_rows_per_group {
        params.max_rows_per_group = n;
    }
    if let Some(n) = options.max_bytes_per_file {
        params.max_bytes_per_file = n;
    }
    if let Some(enable) = options.enable_stable_row_ids {
        params.enable_stable_row_ids = enable;
    }
    if let Some(enable) = options.enable_v2_manifest_paths {
        params.enable_v2_manifest_paths = enable;
    }
    if let Some(properties) = options.transaction_properties {
        params.transaction_properties = Some(Arc::new(properties));
    }

    // `write_fragments` is deprecated in favor of the builder. This is exactly
    // what it does under the hood.
    let txn = match block_on_cc!(
        InsertBuilder::new(uri)
            .with_params(&params)
            .execute_uncommitted_stream(reader),
    ) {
        Ok(txn) => txn,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let view = transaction_view(&txn).to_string();
    unsafe { emit_bytes(encode_transaction(&txn), out_txn_pb, out_txn_pb_len) };
    unsafe { emit_json(view, out_txn_json) }
}

// lance_commit / lance_commit_batch

/// Options accepted by `lance_commit` / `lance_commit_batch`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CommitOptions {
    use_stable_row_ids: Option<bool>,
    enable_v2_manifest_paths: Option<bool>,
    detached: Option<bool>,
    max_retries: Option<u32>,
    skip_auto_cleanup: Option<bool>,
    transaction_properties: Option<HashMap<String, String>>,
    storage_format: Option<String>,
}

fn build_commit_builder<'a>(
    uri: &'a str,
    opts: &CommitOptions,
    storage_options: HashMap<String, String>,
) -> Result<CommitBuilder<'a>, String> {
    let mut builder = CommitBuilder::new(uri);
    if let Some(v) = opts.use_stable_row_ids {
        builder = builder.use_stable_row_ids(v);
    }
    if let Some(v) = opts.enable_v2_manifest_paths {
        builder = builder.enable_v2_manifest_paths(v);
    }
    if let Some(v) = opts.detached {
        builder = builder.with_detached(v);
    }
    if let Some(n) = opts.max_retries {
        builder = builder.with_max_retries(n);
    }
    if let Some(v) = opts.skip_auto_cleanup {
        builder = builder.with_skip_auto_cleanup(v);
    }
    if let Some(props) = &opts.transaction_properties {
        builder = builder.with_transaction_properties(props.clone());
    }
    if let Some(fmt) = &opts.storage_format {
        let version = LanceFileVersion::from_str(fmt)
            .map_err(|e| format!("invalid storage_format {fmt:?}: {e}"))?;
        builder = builder.with_storage_format(version);
    }
    if let Some(store_params) = storage::object_store_params(storage_options) {
        builder = builder.with_store_params(store_params);
    }
    Ok(builder)
}

/// Commits a single [`Transaction`] (as `pb::Transaction` bytes produced by
/// `lance_write_fragments`) against the dataset at `uri`, returning a handle
/// to the resulting dataset version in `out`.
///
/// With `"detached": true` in `options_json` the version is committed
/// OUTSIDE the dataset's lineage (it never appears in the history and can
/// never become latest; requires V2 manifest paths and an existing dataset).
/// The full transaction — uuid, tag, transaction properties — is preserved,
/// and the returned handle is checked out at the detached version (its
/// version number carries the detached high bit).
///
/// # Safety
///
/// `txn_pb` must point to `txn_pb_len` valid bytes. All other pointer
/// contracts as documented on the sibling functions.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_commit(
    uri: *const c_char,
    txn_pb: *const u8,
    txn_pb_len: usize,
    options_json: *const c_char,
    storage_kv: *const *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    if txn_pb.is_null() || out.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "txn_pb and out must not be NULL",
        );
    }
    let (uri, storage_options, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(uri, "uri")?,
                storage::parse_storage_kv(storage_kv)?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: CommitOptions = match parse_json_options(options_json, "commit options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    // SAFETY: caller guarantees `txn_pb` is valid for `txn_pb_len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(txn_pb, txn_pb_len) };
    let txn = match decode_transaction(bytes) {
        Ok(txn) => txn,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let builder = match build_commit_builder(uri, &options, storage_options) {
        Ok(builder) => builder,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    match block_on_cc!(builder.execute(txn)) {
        Ok(dataset) => {
            unsafe { emit_dataset(dataset, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Commits a batch of append [`Transaction`]s (each as `pb::Transaction`
/// bytes) in a single commit against `uri`. Lance merges compatible appends
/// into one manifest, so the result is a single new version, returned as JSON
/// `{"version": uint}` in `out_json`.
///
/// - `txns_pb`: array of `num_txns` pointers to protobuf buffers.
/// - `txns_pb_lens`: array of `num_txns` matching lengths.
///
/// # Safety
///
/// The two arrays must each hold `num_txns` valid entries, and `txns_pb[i]` must
/// point to `txns_pb_lens[i]` valid bytes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_commit_batch(
    uri: *const c_char,
    txns_pb: *const *const u8,
    txns_pb_lens: *const usize,
    num_txns: usize,
    options_json: *const c_char,
    storage_kv: *const *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if txns_pb.is_null() || txns_pb_lens.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "txns_pb, txns_pb_lens and out_json must not be NULL",
        );
    }
    if num_txns == 0 {
        return set_error(ErrorCode::InvalidArgument, "num_txns must be > 0");
    }
    let (uri, storage_options, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(uri, "uri")?,
                storage::parse_storage_kv(storage_kv)?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: CommitOptions = match parse_json_options(options_json, "commit options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let mut txns = Vec::with_capacity(num_txns);
    for i in 0..num_txns {
        // SAFETY: caller guarantees both arrays hold `num_txns` valid entries.
        let ptr = unsafe { *txns_pb.add(i) };
        let len = unsafe { *txns_pb_lens.add(i) };
        if ptr.is_null() {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("transaction {i} pointer is NULL"),
            );
        }
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
        match decode_transaction(bytes) {
            Ok(txn) => txns.push(txn),
            Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
        }
    }
    let builder = match build_commit_builder(uri, &options, storage_options) {
        Ok(builder) => builder,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    match block_on_cc!(builder.execute_batch(txns)) {
        Ok(result) => {
            let version = result.dataset.version().version;
            unsafe { emit_json(json!({ "version": version }).to_string(), out_json) }
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Emits a [`ManifestLocation`](lance_table::io::commit::ManifestLocation) as
/// JSON `{"version": uint, "path": string, "size": uint|null,
/// "naming_scheme": "V1"|"V2", "e_tag": string|null}`.
fn manifest_location_view(location: &lance_table::io::commit::ManifestLocation) -> Value {
    json!({
        "version": location.version,
        "path": location.path.to_string(),
        "size": location.size,
        "naming_scheme": format!("{:?}", location.naming_scheme),
        "e_tag": location.e_tag,
    })
}

/// Returns the location of the manifest backing the currently checked-out
/// version as JSON `{"version": uint, "path": string, "size": uint|null,
/// "naming_scheme": "V1"|"V2", "e_tag": string|null}`. The caller owns
/// `*out_json` and must free it with `lance_string_free`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_manifest_location(
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
    unsafe {
        emit_json(
            manifest_location_view(dataset.manifest_location()).to_string(),
            out_json,
        )
    }
}

// Transaction / manifest reads

/// Emits an optional transaction as protobuf bytes + JSON view. `None`
/// becomes an empty buffer and the JSON literal `null`.
///
/// # Safety
///
/// Output pointers must be non-NULL and valid for writes.
unsafe fn emit_optional_transaction(
    txn: Option<Transaction>,
    out_pb: *mut *mut u8,
    out_pb_len: *mut usize,
    out_json: *mut *mut c_char,
) -> i32 {
    match txn {
        Some(txn) => {
            let view = transaction_view(&txn).to_string();
            unsafe { emit_bytes(encode_transaction(&txn), out_pb, out_pb_len) };
            unsafe { emit_json(view, out_json) }
        }
        None => {
            unsafe { emit_bytes(Vec::new(), out_pb, out_pb_len) };
            unsafe { emit_json("null".to_string(), out_json) }
        }
    }
}

/// Reads the transaction that produced the currently checked-out version.
/// See [`emit_optional_transaction`] for the `None` encoding.
///
/// # Safety
///
/// `ds` must be a valid handle, and the `out_*` pointers valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_read_transaction(
    ds: *const LanceDataset,
    out_pb: *mut *mut u8,
    out_pb_len: *mut usize,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_pb.is_null() || out_pb_len.is_null() || out_json.is_null() {
        return set_error(ErrorCode::InvalidArgument, "arguments must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.read_transaction()) {
        Ok(txn) => unsafe { emit_optional_transaction(txn, out_pb, out_pb_len, out_json) },
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Reads the transaction that produced `version`.
///
/// # Safety
///
/// As [`lance_dataset_read_transaction`].
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_read_transaction_by_version(
    ds: *const LanceDataset,
    version: u64,
    out_pb: *mut *mut u8,
    out_pb_len: *mut usize,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_pb.is_null() || out_pb_len.is_null() || out_json.is_null() {
        return set_error(ErrorCode::InvalidArgument, "arguments must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    match block_on_cc!(dataset.read_transaction_by_version(version)) {
        Ok(txn) => unsafe { emit_optional_transaction(txn, out_pb, out_pb_len, out_json) },
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Returns up to `n` most recent transactions as a JSON array of transaction
/// views (missing/unreadable entries appear as `null`).
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_get_transactions(
    ds: *const LanceDataset,
    n: usize,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "ds and out_json must not be NULL",
        );
    }
    let dataset = unsafe { &*ds }.dataset();
    let txns = match block_on_cc!(dataset.get_transactions(n)) {
        Ok(txns) => txns,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let views: Vec<Value> = txns
        .iter()
        .map(|t| t.as_ref().map(transaction_view).unwrap_or(Value::Null))
        .collect();
    unsafe { emit_json(Value::Array(views).to_string(), out_json) }
}

/// Returns a JSON summary of the current manifest:
/// `{"version", "fields": [{"id", "name"}], "num_fragments", "config",
/// "table_metadata", "tag", "branch"}`.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_manifest(
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
    let manifest = dataset.manifest();
    let fields: Vec<Value> = manifest
        .schema
        .fields
        .iter()
        .map(|f| json!({"id": f.id, "name": f.name}))
        .collect();
    let view = json!({
        "version": manifest.version,
        "fields": fields,
        "num_fragments": manifest.fragments.len(),
        "config": manifest.config,
        "table_metadata": manifest.table_metadata,
        "tag": manifest.tag,
        "branch": manifest.branch,
    });
    unsafe { emit_json(view.to_string(), out_json) }
}

/// Returns detached manifests as a JSON array of
/// `{"version", "path", "size"?}` objects.
///
/// # Safety
///
/// `ds` must be a valid handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_list_detached_manifests(
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
    let locations = match block_on_cc!(dataset.list_detached_manifests()) {
        Ok(locations) => locations,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    let views: Vec<Value> = locations
        .iter()
        .map(|loc| json!({"version": loc.version, "path": loc.path.to_string(), "size": loc.size}))
        .collect();
    unsafe { emit_json(Value::Array(views).to_string(), out_json) }
}

// lance_dataset_commit_operation (build an Operation from JSON)

/// JSON form of an `UpdateMap`: `{"entries": {k: v|null}, "replace"?: bool}`.
/// A `null` value deletes the key.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct UpdateMapJson {
    #[serde(default)]
    entries: HashMap<String, Option<String>>,
    #[serde(default)]
    replace: bool,
}

impl From<UpdateMapJson> for UpdateMap {
    fn from(json: UpdateMapJson) -> Self {
        UpdateMap {
            update_entries: json
                .entries
                .into_iter()
                .map(|(key, value)| UpdateMapEntry { key, value })
                .collect(),
            replace: json.replace,
        }
    }
}

/// JSON form of the [`Operation`]s that can be constructed on the Go side.
/// Only the tractable, verifiable variants are supported here. Richer
/// operations (Append, Rewrite, ...) travel losslessly as protobuf bytes via
/// `lance_write_fragments` -> `lance_commit`.
#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum OperationJson {
    UpdateConfig {
        config_updates: Option<UpdateMapJson>,
        table_metadata_updates: Option<UpdateMapJson>,
        schema_metadata_updates: Option<UpdateMapJson>,
    },
    Restore {
        version: u64,
    },
}

impl From<OperationJson> for Operation {
    fn from(json: OperationJson) -> Self {
        match json {
            OperationJson::UpdateConfig {
                config_updates,
                table_metadata_updates,
                schema_metadata_updates,
            } => Operation::UpdateConfig {
                config_updates: config_updates.map(Into::into),
                table_metadata_updates: table_metadata_updates.map(Into::into),
                schema_metadata_updates: schema_metadata_updates.map(Into::into),
                field_metadata_updates: HashMap::new(),
            },
            OperationJson::Restore { version } => Operation::Restore { version },
        }
    }
}

/// Builds an [`Operation`] from JSON, wraps it in a fresh [`Transaction`] at
/// `read_version`, and commits it against `uri`.
///
/// - `operation_json`: a tagged object, e.g.
///   `{"type": "UpdateConfig", "config_updates": {"entries": {"k": "v"}}}`
///   or `{"type": "Restore", "version": 1}`.
/// - `read_version`: the dataset version the operation is based on.
///
/// # Safety
///
/// Pointer contracts as documented. `out` must be valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_commit_operation(
    uri: *const c_char,
    operation_json: *const c_char,
    read_version: u64,
    options_json: *const c_char,
    storage_kv: *const *const c_char,
    out: *mut *mut LanceDataset,
) -> i32 {
    if out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "out must not be NULL");
    }
    let (uri, operation_json, storage_options, options_json) = match unsafe {
        (|| -> Result<_, String> {
            Ok((
                storage::required_str(uri, "uri")?,
                storage::required_str(operation_json, "operation_json")?,
                storage::parse_storage_kv(storage_kv)?,
                storage::optional_str(options_json, "options_json")?,
            ))
        })()
    } {
        Ok(parsed) => parsed,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let op_json: OperationJson = match serde_json::from_str(operation_json) {
        Ok(op) => op,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid operation JSON: {e} (supported types: UpdateConfig, Restore)"),
            );
        }
    };
    let options: CommitOptions = match parse_json_options(options_json, "commit options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let transaction = Transaction {
        read_version,
        uuid: uuid::Uuid::new_v4().hyphenated().to_string(),
        operation: op_json.into(),
        tag: None,
        transaction_properties: options.transaction_properties.clone().map(Arc::new),
    };
    let builder = match build_commit_builder(uri, &options, storage_options) {
        Ok(builder) => builder,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    match block_on_cc!(builder.execute(transaction)) {
        Ok(dataset) => {
            unsafe { emit_dataset(dataset, out) };
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
