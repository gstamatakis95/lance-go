//! Go-backed caches.
//!
//! Two independent hooks live here, both bridged to Go plugins through the
//! generic callback vtable (`crate::callbacks`):
//!
//! 1. [`GoCacheBackend`], a [`lance_core::cache::CacheBackend`] whose
//!    serializable (codec-bearing) index entries are delegated to a Go
//!    key/value store, while non-serializable "live object" entries stay in
//!    an embedded in-process moka cache. Attached to a [`Session`] via
//!    `lance_session_new_with_cache_backend`.
//!
//! 2. [`CachingObjectStore`], a `WrappingObjectStore` that consults a Go
//!    byte-range cache for immutable file reads. Attached post-open via
//!    `lance_dataset_wrap_object_store_cache`.
//!
//! [`Session`]: lance::session::Session

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use futures::stream::{self, BoxStream, StreamExt};
use lance_core::Result;
use lance_core::cache::{
    CacheBackend, CacheCodec, CacheDecode, CacheEntry, InternalCacheKey, MokaCacheBackend,
};
use lance_io::object_store::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore as OSObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as OSResult,
};
use std::ops::Range;

use crate::callbacks::OwnedGoPlugin;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, ok, set_error};

// Wire encoding helpers (shared conventions with lance/cache.go)

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Length-prefixed UTF-8 string: `len: u32 | bytes`.
fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

/// Encodes an [`InternalCacheKey`] as `prefix | key | type_name`, each
/// length-prefixed. A cache PUT appends the value bytes after this.
fn encode_cache_key(key: &InternalCacheKey) -> Vec<u8> {
    let mut buf = Vec::new();
    put_str(&mut buf, key.prefix());
    put_str(&mut buf, key.key());
    put_str(&mut buf, key.type_name());
    buf
}

/// Decodes a little-endian u64 response (for the Len / SizeBytes methods).
fn decode_u64(bytes: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    let n = bytes.len().min(8);
    a[..n].copy_from_slice(&bytes[..n]);
    u64::from_le_bytes(a)
}

// 1. CacheBackend bridge

/// Method discriminators for the Go `CacheBackend` plugin (see
/// `lance/cache.go`).
const CACHE_GET: i32 = 0;
const CACHE_PUT: i32 = 1;
const CACHE_INVALIDATE_PREFIX: i32 = 2;
const CACHE_CLEAR: i32 = 3;
const CACHE_LEN: i32 = 4;
const CACHE_SIZE_BYTES: i32 = 5;

/// A [`CacheBackend`] that delegates serializable (codec-bearing) index
/// entries to a Go plugin and keeps non-serializable live objects in an
/// embedded moka cache.
///
/// The codec boundary is the whole point: entries whose [`CacheKey`] declares
/// a [`CacheCodec`] (IVF/PQ/RQ partitions, FTS posting lists, scalar-index
/// states, ...) can be serialized to bytes and therefore leave the process to
/// the Go store. Entries with `codec == None` are opaque `Arc<dyn Any>` live
/// objects that cannot cross the boundary, so they stay in `mem`.
///
/// [`CacheKey`]: lance_core::cache::CacheKey
#[derive(Debug)]
pub(crate) struct GoCacheBackend {
    plugin: OwnedGoPlugin,
    /// In-process cache for `codec == None` entries.
    mem: MokaCacheBackend,
}

impl GoCacheBackend {
    pub(crate) fn new(plugin: OwnedGoPlugin, live_object_capacity_bytes: usize) -> Self {
        Self {
            plugin,
            mem: MokaCacheBackend::with_capacity(live_object_capacity_bytes),
        }
    }

    /// Fetches raw bytes for `key` from the Go store (`None` on miss/error).
    ///
    /// A transport error is treated as a miss so the loader recomputes
    /// (self-healing, matching the codec version-mismatch policy).
    async fn go_get(&self, key: &InternalCacheKey) -> Option<Vec<u8>> {
        self.plugin
            .call_blocking(CACHE_GET, encode_cache_key(key))
            .await
            .unwrap_or_default()
    }

    /// Serializes `entry` via `codec` and stores it in the Go store.
    async fn go_put(&self, key: &InternalCacheKey, entry: &CacheEntry, codec: CacheCodec) {
        let mut buf = encode_cache_key(key);
        // serialize appends the codec envelope + body after the key prefix.
        if codec.serialize(entry, &mut buf).is_ok() {
            let _ = self.plugin.call_blocking(CACHE_PUT, buf).await;
        }
    }
}

#[async_trait]
impl CacheBackend for GoCacheBackend {
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        let Some(codec) = codec else {
            return self.mem.get(key, None).await;
        };
        let bytes = self.go_get(key).await?;
        match codec.deserialize(&Bytes::from(bytes)) {
            CacheDecode::Hit(entry) => Some(entry),
            CacheDecode::Miss(_) => None,
        }
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        codec: Option<CacheCodec>,
    ) {
        match codec {
            None => self.mem.insert(key, entry, size_bytes, None).await,
            Some(codec) => self.go_put(key, &entry, codec).await,
        }
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)> {
        let Some(codec) = codec else {
            return self.mem.get_or_insert(key, loader, None).await;
        };
        // The loader is a Rust future, so get_or_insert cannot be delegated
        // wholesale to Go: consult the store, and on a miss run the loader
        // here and write the serialized result back.
        if let Some(bytes) = self.go_get(key).await
            && let CacheDecode::Hit(entry) = codec.deserialize(&Bytes::from(bytes))
        {
            return Ok((entry, true));
        }
        let (entry, _size) = loader.await?;
        self.go_put(key, &entry, codec).await;
        Ok((entry, false))
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        self.mem.invalidate_prefix(prefix).await;
        let _ = self
            .plugin
            .call_blocking(CACHE_INVALIDATE_PREFIX, prefix.as_bytes().to_vec())
            .await;
    }

    async fn clear(&self) {
        self.mem.clear().await;
        let _ = self.plugin.call_blocking(CACHE_CLEAR, Vec::new()).await;
    }

    async fn num_entries(&self) -> usize {
        let go = self
            .plugin
            .call_blocking(CACHE_LEN, Vec::new())
            .await
            .ok()
            .flatten()
            .map(|b| decode_u64(&b))
            .unwrap_or(0);
        self.mem.num_entries().await + go as usize
    }

    async fn size_bytes(&self) -> usize {
        let go = self
            .plugin
            .call_blocking(CACHE_SIZE_BYTES, Vec::new())
            .await
            .ok()
            .flatten()
            .map(|b| decode_u64(&b))
            .unwrap_or(0);
        self.mem.size_bytes().await + go as usize
    }
}

// 2. Object-store byte-range cache

/// Method discriminators for the Go `ObjectStoreCache` plugin.
const OSC_GET: i32 = 0;
const OSC_PUT: i32 = 1;

/// Encodes an object-store range key as `path | start: u64 | end: u64`. A PUT
/// appends the data bytes.
fn encode_range_key(path: &Path, start: u64, end: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    put_str(&mut buf, path.as_ref());
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf
}

/// A `WrappingObjectStore` that installs a [`CachingObjectStore`] over the
/// backend store.
#[derive(Debug)]
struct CachingWrapper {
    plugin: OwnedGoPlugin,
}

impl WrappingObjectStore for CachingWrapper {
    fn wrap(
        &self,
        _store_prefix: &str,
        original: Arc<dyn OSObjectStore>,
    ) -> Arc<dyn OSObjectStore> {
        Arc::new(CachingObjectStore {
            target: original,
            plugin: self.plugin.clone(),
        })
    }
}

/// Wraps an object store, consulting a Go byte-range cache for immutable file
/// reads (`get_ranges` and bounded `get_opts`). Lance data files are
/// write-once, so no invalidation is needed. All other operations delegate to
/// the inner store.
#[derive(Debug)]
struct CachingObjectStore {
    target: Arc<dyn OSObjectStore>,
    plugin: OwnedGoPlugin,
}

impl std::fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore({})", self.target)
    }
}

impl CachingObjectStore {
    async fn cache_get(&self, path: &Path, start: u64, end: u64) -> Option<Bytes> {
        match self
            .plugin
            .call_blocking(OSC_GET, encode_range_key(path, start, end))
            .await
        {
            Ok(Some(bytes)) => Some(Bytes::from(bytes)),
            _ => None,
        }
    }

    async fn cache_put(&self, path: &Path, start: u64, end: u64, data: &[u8]) {
        let mut payload = encode_range_key(path, start, end);
        payload.extend_from_slice(data);
        let _ = self.plugin.call_blocking(OSC_PUT, payload).await;
    }
}

#[async_trait]
impl OSObjectStore for CachingObjectStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        // Only cache plain bounded range reads of immutable data: skip HEAD,
        // conditional requests, versioned reads, and open-ended ranges.
        let cacheable = !options.head
            && options.version.is_none()
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none();
        let bounded = match &options.range {
            Some(GetRange::Bounded(r)) => Some(r.clone()),
            _ => None,
        };
        if let (true, Some(range)) = (cacheable, bounded) {
            if let Some(bytes) = self.cache_get(location, range.start, range.end).await {
                return Ok(cached_get_result(location, range, bytes));
            }
            let res = self.target.get_opts(location, options).await?;
            let served = res.range.clone();
            let meta = res.meta.clone();
            let attributes = res.attributes.clone();
            let bytes = res.bytes().await?;
            self.cache_put(location, served.start, served.end, &bytes)
                .await;
            return Ok(GetResult {
                payload: bytes_payload(bytes),
                meta,
                range: served,
                attributes,
            });
        }
        self.target.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
        let mut out = Vec::with_capacity(ranges.len());
        let mut misses = Vec::new();
        for (i, r) in ranges.iter().enumerate() {
            match self.cache_get(location, r.start, r.end).await {
                Some(bytes) => out.push(Some(bytes)),
                None => {
                    out.push(None);
                    misses.push(i);
                }
            }
        }
        if !misses.is_empty() {
            let miss_ranges: Vec<Range<u64>> = misses.iter().map(|&i| ranges[i].clone()).collect();
            let fetched = self.target.get_ranges(location, &miss_ranges).await?;
            for (slot, bytes) in misses.iter().zip(fetched.into_iter()) {
                let r = &ranges[*slot];
                self.cache_put(location, r.start, r.end, &bytes).await;
                out[*slot] = Some(bytes);
            }
        }
        Ok(out
            .into_iter()
            .map(|b| b.expect("all slots filled"))
            .collect())
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.target.put_opts(location, bytes, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.target.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
        self.target.copy_opts(from, to, opts).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> OSResult<()> {
        self.target.rename_opts(from, to, opts).await
    }
}

/// Wraps materialized `bytes` as a streaming `GetResultPayload`.
fn bytes_payload(bytes: Bytes) -> GetResultPayload {
    GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed())
}

/// Builds a `GetResult` for a cache hit over `range`. The `ObjectMeta` is
/// best-effort (Lance range readers consume the bytes, not the metadata):
/// `size` is set to the range end and no e-tag/version is available.
fn cached_get_result(location: &Path, range: Range<u64>, bytes: Bytes) -> GetResult {
    GetResult {
        payload: bytes_payload(bytes),
        meta: ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size: range.end,
            e_tag: None,
            version: None,
        },
        range,
        attributes: Attributes::default(),
    }
}

/// Returns a clone of the dataset wrapped so that immutable file reads are
/// served through the Go `ObjectStoreCache` behind `plugin_handle`. `out`
/// receives a NEW dataset handle. The caller should close the original.
///
/// # Safety
///
/// `ds` must be a valid dataset handle, `plugin_handle` a live Go plugin
/// handle, and `out` valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_wrap_object_store_cache(
    ds: *const LanceDataset,
    plugin_handle: usize,
    out: *mut *mut LanceDataset,
) -> i32 {
    if ds.is_null() || out.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds and out must not be NULL");
    }
    let dataset = unsafe { &*ds }.dataset();
    let wrapper: Arc<dyn WrappingObjectStore> = Arc::new(CachingWrapper {
        plugin: OwnedGoPlugin::new(plugin_handle),
    });
    let wrapped = dataset.with_object_store_wrappers([wrapper]);
    let handle = Box::into_raw(Box::new(LanceDataset(Mutex::new(wrapped))));
    // SAFETY: `out` is non-NULL and valid for writes.
    unsafe { *out = handle };
    ok()
}
