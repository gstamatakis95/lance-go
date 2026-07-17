//! Session: a shared index/metadata cache (optionally backed by a pluggable
//! Go cache backend) reused across dataset opens, plus session-scoped open
//! and write entry points.
//!
//! These are additive FFI functions layered on top of the existing
//! `lance_dataset_open` / `lance_dataset_write` path: they do not alter those
//! signatures. `lance_dataset_open_with_session` / `_write_with_session`
//! attach an [`Arc<Session>`] so the caches survive across opens, and the
//! write variant also carries an optional Go write-progress callback.

use std::ffi::{CString, c_char};
use std::sync::{Arc, Mutex};

use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{WriteProgressFn, WriteStats};
use lance::session::Session;
use serde::{Deserialize, Serialize};

use crate::arrow_bridge;
use crate::cache::GoCacheBackend;
use crate::callbacks::{GoPlugin, OwnedGoPlugin};
use crate::dataset::{LanceDataset, build_write_params};
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// In-process byte budget of the embedded moka cache that holds the
/// non-serializable ("live object", `codec == None`) index-cache entries when
/// a Go cache backend is attached. Serializable (`codec == Some`) entries are
/// delegated to the Go backend instead, see `crate::cache`.
const DEFAULT_LIVE_OBJECT_CACHE_BYTES: usize = 512 * 1024 * 1024;

/// Method discriminator for the Go write-progress plugin (see
/// `lance/session.go`). The payload is a fixed 20-byte little-endian encoding
/// of [`WriteStats`]: `bytes_written: u64 | rows_written: u64 |
/// files_written: u32`.
const WRITE_PROGRESS_REPORT: i32 = 0;

/// Opaque handle to a [`Session`]. Created by `lance_session_new` /
/// `lance_session_new_with_cache_backend`. Released with
/// `lance_session_close`.
pub struct LanceSession(pub(crate) Arc<Session>);

/// JSON form of [`lance::dataset::builder::DatasetBuilder`] open options,
/// mirroring the private one in `crate::dataset`. Cache-size fields are
/// intentionally omitted: the session owns the caches.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct OpenOptions {
    version: Option<u64>,
    tag: Option<String>,
    branch: Option<String>,
}

/// Serializable mirror of [`lance_core::cache::CacheStats`].
#[derive(Serialize)]
struct CacheStatsJson {
    hits: u64,
    misses: u64,
    num_entries: u64,
    size_bytes: u64,
}

impl From<lance_core::cache::CacheStats> for CacheStatsJson {
    fn from(s: lance_core::cache::CacheStats) -> Self {
        CacheStatsJson {
            hits: s.hits,
            misses: s.misses,
            num_entries: s.num_entries as u64,
            size_bytes: s.size_bytes as u64,
        }
    }
}

/// JSON returned by `lance_session_stats`.
#[derive(Serialize)]
struct SessionStatsJson {
    index_cache: CacheStatsJson,
    metadata_cache: CacheStatsJson,
    size_bytes: u64,
    approx_num_items: u64,
}

fn parse_json<T: Default + for<'de> Deserialize<'de>>(
    json: Option<&str>,
    what: &str,
) -> Result<T, String> {
    match json {
        None => Ok(T::default()),
        Some(s) if s.trim().is_empty() => Ok(T::default()),
        Some(s) => serde_json::from_str(s).map_err(|e| format!("invalid {what} JSON: {e}")),
    }
}

/// Emits a freshly opened dataset as an owned opaque handle.
fn emit_dataset(dataset: lance::Dataset, out: *mut *mut LanceDataset) {
    let handle = Box::into_raw(Box::new(LanceDataset(Mutex::new(dataset))));
    // SAFETY: callers validated `out` is non-NULL and writable.
    unsafe { *out = handle };
}

/// Creates a session with in-memory index and metadata caches of the given
/// byte budgets. `out` receives the handle. Release it with
/// `lance_session_close`.
///
/// # Safety
///
/// `out` must be valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_session_new(
    index_cache_bytes: u64,
    metadata_cache_bytes: u64,
    out: *mut *mut LanceSession,
) -> i32 {
    if out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "out must not be NULL");
    }
    let session = Session::new(
        index_cache_bytes as usize,
        metadata_cache_bytes as usize,
        Default::default(),
    );
    let handle = Box::into_raw(Box::new(LanceSession(Arc::new(session))));
    // SAFETY: `out` is non-NULL and valid for writes.
    unsafe { *out = handle };
    ok()
}

/// Creates a session whose index cache is backed by the Go plugin registered
/// under `plugin_handle` (a `CacheBackend`). Only serializable, codec-bearing
/// index entries reach the Go backend, and live (non-serializable) objects stay
/// in an embedded in-process cache. The metadata cache stays in-memory
/// (Lance 8.0 exposes no metadata-cache backend hook).
///
/// # Safety
///
/// `plugin_handle` must be a live Go plugin handle, and `out` must be valid for
/// writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_session_new_with_cache_backend(
    plugin_handle: usize,
    metadata_cache_bytes: u64,
    out: *mut *mut LanceSession,
) -> i32 {
    if out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "out must not be NULL");
    }
    let backend = Arc::new(GoCacheBackend::new(
        OwnedGoPlugin::new(plugin_handle),
        DEFAULT_LIVE_OBJECT_CACHE_BYTES,
    ));
    let session = Session::with_index_cache_backend(
        backend,
        metadata_cache_bytes as usize,
        Default::default(),
    );
    let handle = Box::into_raw(Box::new(LanceSession(Arc::new(session))));
    // SAFETY: `out` is non-NULL and valid for writes.
    unsafe { *out = handle };
    ok()
}

/// Writes session cache statistics into `*out_json` as a JSON object
/// `{"index_cache": CacheStats, "metadata_cache": CacheStats,
/// "size_bytes": uint, "approx_num_items": uint}` where each `CacheStats` is
/// `{"hits","misses","num_entries","size_bytes"}`. The caller owns the string
/// and must free it with `lance_string_free`.
///
/// # Safety
///
/// `sess` must be a valid session handle and `out_json` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_session_stats(
    sess: *const LanceSession,
    out_json: *mut *mut c_char,
) -> i32 {
    if sess.is_null() || out_json.is_null() {
        return set_error(
            ErrorCode::InvalidArgument,
            "sess and out_json must not be NULL",
        );
    }
    let session = unsafe { &*sess }.0.clone();
    let stats = SessionStatsJson {
        index_cache: block_on_cc!(session.index_cache_stats()).into(),
        metadata_cache: block_on_cc!(session.metadata_cache_stats()).into(),
        size_bytes: session.size_bytes(),
        approx_num_items: session.approx_num_items() as u64,
    };
    let json = match serde_json::to_string(&stats) {
        Ok(json) => json,
        Err(e) => return set_error(ErrorCode::Internal, e),
    };
    match CString::new(json) {
        Ok(cstr) => {
            // SAFETY: `out_json` is non-NULL and valid for writes.
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Releases a session handle. NULL is a no-op.
///
/// # Safety
///
/// `sess` must be NULL or a handle from `lance_session_new` /
/// `lance_session_new_with_cache_backend` not already closed.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_session_close(sess: *mut LanceSession) {
    if sess.is_null() {
        return;
    }
    // SAFETY: per the contract, from Box::into_raw and closed at most once.
    drop(unsafe { Box::from_raw(sess) });
}

/// Opens a dataset sharing `sess`'s caches. Same `uri`/`storage_kv`/
/// `options_json` contract as `lance_dataset_open` (minus the cache-size
/// options, which the session owns). `out` receives the dataset handle.
///
/// # Safety
///
/// All pointer arguments must satisfy the usual FFI contracts. `sess` must be
/// a valid session handle and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_open_with_session(
    uri: *const c_char,
    storage_kv: *const *const c_char,
    options_json: *const c_char,
    sess: *const LanceSession,
    out: *mut *mut LanceDataset,
) -> i32 {
    if out.is_null() || sess.is_null() {
        return set_error(ErrorCode::InvalidArgument, "sess and out must not be NULL");
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
    let options: OpenOptions = match parse_json(options_json, "open options") {
        Ok(options) => options,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if let Err(msg) = crate::dataset::validate_open_ref(
        options.version,
        options.tag.as_deref(),
        options.branch.as_deref(),
    ) {
        return set_error(ErrorCode::InvalidArgument, msg);
    }
    let session = unsafe { &*sess }.0.clone();

    let mut builder = DatasetBuilder::from_uri(uri).with_session(session);
    if !storage_options.is_empty() {
        builder = builder.with_storage_options(storage_options);
    }
    if let Some(branch) = &options.branch {
        builder = builder.with_branch(branch, options.version);
    } else if let Some(version) = options.version {
        builder = builder.with_version(version);
    }
    if let Some(tag) = &options.tag {
        builder = builder.with_tag(tag);
    }

    match block_on_cc!(builder.load()) {
        Ok(dataset) => {
            emit_dataset(dataset, out);
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Writes an Arrow stream to a dataset, optionally sharing a session's caches
/// and/or reporting progress to a Go callback.
///
/// - `stream`: Arrow C stream of record batches, ownership always taken.
/// - `options_json`: same complete write-options object accepted by
///   `lance_dataset_write`, or NULL.
/// - `sess`: session handle to attach (NULL for none).
/// - `progress_handle`: Go write-progress plugin handle (0 for none). The
///   plugin's method `WRITE_PROGRESS_REPORT` is invoked after each batch with
///   a 20-byte little-endian `WriteStats` payload.
/// - `out`: receives the resulting dataset handle (NULL to discard).
///
/// # Safety
///
/// All pointer arguments must satisfy the usual FFI contracts.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_write_with_session(
    stream: *mut FFI_ArrowArrayStream,
    uri: *const c_char,
    options_json: *const c_char,
    storage_kv: *const *const c_char,
    sess: *const LanceSession,
    progress_handle: usize,
    out: *mut *mut LanceDataset,
) -> i32 {
    // Import the stream FIRST so its producer is owned on every error path.
    let reader = match unsafe { arrow_bridge::import_stream(stream) } {
        Ok(reader) => reader,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
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
    let mut params = match build_write_params(options_json, storage_options) {
        Ok(params) => params,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    if !sess.is_null() {
        params.session = Some(unsafe { &*sess }.0.clone());
    }
    if progress_handle != 0 {
        let plugin = GoPlugin::new(progress_handle);
        params.write_progress = Some(WriteProgressFn::new(move |stats: WriteStats| {
            let mut payload = Vec::with_capacity(20);
            payload.extend_from_slice(&stats.bytes_written.to_le_bytes());
            payload.extend_from_slice(&stats.rows_written.to_le_bytes());
            payload.extend_from_slice(&stats.files_written.to_le_bytes());
            // Progress is best-effort: a callback error/panic must not abort
            // the write (the Go shim already contains panics).
            let _ = plugin.call(WRITE_PROGRESS_REPORT, &payload);
        }));
    }

    match block_on_cc!(lance::Dataset::write(reader, uri, Some(params))) {
        Ok(dataset) => {
            if !out.is_null() {
                emit_dataset(dataset, out);
            }
            ok()
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
