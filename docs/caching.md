# Caching

lance-go exposes Lance's two caching seams as plain Go interfaces, so you can
externalize its caches (Redis, disk, a shared byte store) **without changing
lance-go**. This page explains the two seams, what actually crosses each
boundary, and walks through a Redis-backed cache built entirely on the public
API. The runnable reference implementation is
[`examples/plugincache`](../examples/plugincache). Read it alongside this
page.

The two seams are independent and address different layers:

| Seam | Go type | Attached via | Caches | Keyed by |
| --- | --- | --- | --- | --- |
| Byte-range cache | `ObjectStoreCache` | `OpenWithObjectStoreCache` | raw immutable file byte ranges | `(path, start, end)` |
| Decoded-object cache | `CacheBackend` | `NewSessionWithCacheBackend` | serialized index cache entries | `CacheKey{Prefix, Key, TypeName}` |

## Seam 1: byte-level `ObjectStoreCache`

This wraps the object store so that immutable Lance file reads are served
through your cache. It sits *below* Lance: it sees opaque byte ranges of
files, not decoded objects. Lance data files are write-once, so ranges never
need invalidation. The interface has only `Get`/`Put`:

```go
type ObjectStoreCache interface {
	Get(path string, start, end uint64) (data []byte, found bool)
	Put(path string, start, end uint64, data []byte)
}
```

Attach it when opening a dataset:

```go
ds, err := lance.OpenWithObjectStoreCache(ctx, uri, myByteCache /* opts... */)
if err != nil { ... }
defer ds.Close()
```

The returned `*Dataset` behaves like any other. Its native cache lease is
retained by derived scanners and readers, so `Dataset.Close` is safe while a
reader is still live. The final native clone releases the Go registration.

### The `file://` caveat

The plain local-filesystem path (`/path` or `file:///path`) uses an
optimized `LocalObjectReader` that **bypasses object-store wrappers**, so an
`ObjectStoreCache` attached to a `file://` dataset never sees reads. Use it
against a real object store (`s3://`, `az://`, `gs://`) where the wrapper is
always on the read path, or (in tests/examples) against the
`file-object-store://` scheme, which routes local reads through the
object-store API so the wrapper engages:

```go
// Exercises the cache locally (tests/examples):
cds, err := lance.OpenWithObjectStoreCache(ctx, "file-object-store://"+uri, byteCache)
```

`examples/plugincache` scans twice through a disk-backed byte cache over
`file-object-store://` and shows the second scan served from cache
(`hits > 0`).

## Seam 2: decoded-object `CacheBackend`

This backs Lance's **index cache** with an external key/value store, plugged
into a `Session`. It sits *above* the decode path: only serializable,
codec-bearing entries reach it, already serialized to `[]byte`.

```go
type CacheKey struct {
	Prefix   string // scopes the entry (e.g. a dataset/index location)
	Key      string // entry name within the prefix
	TypeName string // Rust type of the cached value (opaque discriminator)
}

type CacheBackend interface {
	Get(key CacheKey) (val []byte, found bool)
	Put(key CacheKey, val []byte, sizeHint int)
	InvalidatePrefix(prefix string)
	Clear()
	Len() int
	SizeBytes() int64
}
```

Attach it by constructing a `Session` with it:

```go
sess, err := lance.NewSessionWithCacheBackend(myBackend, 64<<20 /* metadata cache bytes */)
if err != nil { ... }
defer sess.Close()

ds, err := sess.Open(ctx, uri)  // datasets opened on the session use the backend
```

### The codec boundary: what actually reaches a `CacheBackend`

This is the crucial detail. Lance's index cache holds two kinds of entries:

- **Codec-bearing (serializable) entries:** these have a byte codec and are
  delegated to your `CacheBackend` as `[]byte`. They are:
  - IVF / PQ / RQ vector-index partitions,
  - FTS (inverted-index) posting lists,
  - BTree / Bitmap / LabelList scalar-index states,
  - index metadata.
- **Live objects:** non-serializable in-process values (open readers,
  handles, decoded structures). These **never cross the boundary**. They stay
  in an in-process cache regardless of the backend.

Two more limits worth internalizing:

- **The metadata cache has no backend hook in Lance 8.0.** Only the index
  cache can be externalized. The metadata cache is always in-process (its
  byte budget is the second argument to `NewSessionWithCacheBackend`). This
  is an upstream limitation, not a lance-go choice.
- Because entries arrive as bytes, a real backend is just a `[]byte` KV
  store. No Arrow or Lance types are involved.

`examples/plugincache` builds and prewarms an IVF_PQ index on a session whose
index cache is a Go map backend, then runs a search served from it, printing
Put/Get/hit counters and `Session.Stats()`.

## Building a Redis-backed index cache (public API only)

No lance-go change is required: implement `CacheBackend` over a Redis client
and hand it to `NewSessionWithCacheBackend`. The `CacheKey` triple is stable
and opaque. Concatenate its parts for the Redis key.

```go
type redisCacheBackend struct {
	ctx context.Context
	rdb *redis.Client
	ns  string // key namespace, e.g. "lance:idx:"
}

func (r *redisCacheBackend) rkey(k lance.CacheKey) string {
	return r.ns + k.Prefix + "|" + k.Key + "|" + k.TypeName
}

func (r *redisCacheBackend) Get(k lance.CacheKey) ([]byte, bool) {
	v, err := r.rdb.Get(r.ctx, r.rkey(k)).Bytes()
	if err != nil {
		return nil, false // redis.Nil (miss) or a transient error
	}
	return v, true
}

func (r *redisCacheBackend) Put(k lance.CacheKey, v []byte, sizeHint int) {
	r.rdb.Set(r.ctx, r.rkey(k), v, 0)
}

func (r *redisCacheBackend) InvalidatePrefix(prefix string) {
	iter := r.rdb.Scan(r.ctx, 0, r.ns+prefix+"*", 0).Iterator()
	for iter.Next(r.ctx) {
		r.rdb.Del(r.ctx, iter.Val())
	}
}

func (r *redisCacheBackend) Clear()           { r.rdb.FlushDB(r.ctx) }
func (r *redisCacheBackend) Len() int         { n, _ := r.rdb.DBSize(r.ctx).Result(); return int(n) }
func (r *redisCacheBackend) SizeBytes() int64 { /* track via MEMORY USAGE or a counter */ return 0 }

// Wire it up:
sess, _ := lance.NewSessionWithCacheBackend(
	&redisCacheBackend{ctx: ctx, rdb: rdb, ns: "lance:idx:"}, 64<<20)
```

`Get`/`Put` are called concurrently from many threads. Your implementation
must be safe for concurrent use (a Redis client already is). Returning
`found=false` from `Get` (a miss) makes Lance recompute and `Put` the entry.
A disk backend is the same shape: one file per key, hashed for the filename.

Because entries survive as bytes, the cache is externalizable across process
restarts: persist the map (or point Redis at durable storage) and a fresh
`Session` warm-starts from it. The repo's `TestCacheBackendExternalizable`
demonstrates exactly this.

## Session sharing

A `Session` holds in-process index and metadata caches (plus, optionally, the
`CacheBackend` above). Reusing **one** `Session` across `Open`/`Write` calls
lets a second open of the same dataset serve schema, manifest, and index data
from cache instead of re-reading storage. The object store is reused too.

```go
sess, err := lance.NewSession(lance.SessionConfig{
	IndexCacheBytes:    128 << 20,
	MetadataCacheBytes: 64 << 20,
})
if err != nil { ... }
defer sess.Close()

a, _ := sess.Open(ctx, uri)
b, _ := sess.Open(ctx, uri) // warm: served from the shared caches
```

`NewSession` gives a purely in-process session. `NewSessionWithCacheBackend`
additionally externalizes the index cache. Both are safe for concurrent use.
`Close` is idempotent. Datasets already opened through the session retain the
native cache state and backend lease they need; the final native clone releases
the registration.

### Cache statistics

`Session.Stats()` returns a snapshot of hit/miss and size counters:

```go
stats, _ := sess.Stats()
fmt.Printf("index cache: hits=%d misses=%d entries=%d bytes=%d\n",
	stats.IndexCache.Hits, stats.IndexCache.Misses,
	stats.IndexCache.NumEntries, stats.IndexCache.SizeBytes)
fmt.Printf("metadata cache: hits=%d misses=%d\n",
	stats.MetadataCache.Hits, stats.MetadataCache.Misses)
```

A per-dataset view is also available without a session via
`Dataset.CacheStats(ctx)`.

## API shape (composable options)

`Open` and `Write` now accept composable options for the cache seams, so a
session and a byte cache can be attached to the same handle in one call:

- `WithSession(s)` is an `OpenOption` that attaches a shared session to an
  `Open`. `WithWriteSession(s)` is the `WriteOption` counterpart for `Write`.
- `WithObjectStoreCache(cache)` is an `Open`-only option that wraps the opened
  dataset's object store with the byte-range cache.
- `WithWriteProgress(fn)` is a `WriteOption` that attaches a progress callback.

Because these are ordinary options, the session and the object-store cache
now combine on a single dataset:

```go
ds, err := lance.Open(ctx, uri,
	lance.WithSession(s),
	lance.WithObjectStoreCache(myByteCache))
```

The dedicated constructors remain as convenience wrappers over these options
and attach the same native cache machinery:

- `NewSession(SessionConfig)` / `NewSessionWithCacheBackend(backend, metadataBytes)`
- `Session.Open(ctx, uri, opts...)`: accepts the same `OpenOption`s as the
  package-level `Open` (`WithVersion`/`WithTag`/`WithStorageOptions`/
  `WithObjectStoreCache`).
- `Session.Write` / `Session.WriteWithProgress`: accept the core
  `WriteOption`s (the advanced blob/base/auto-cleanup options remain on the
  package-level `Write`).
- `OpenWithObjectStoreCache(ctx, uri, cache, opts...)`: a thin wrapper over
  `Open(ctx, uri, append(opts, WithObjectStoreCache(cache))...)`.

Standalone `lance.WriteWithProgress(...)` (no session) is also available for
the progress callback alone. See [callbacks.md](callbacks.md).

## See also

- [storage.md](storage.md): object-store URI schemes and provider options
  (the `ObjectStoreCache` sits over these stores).
- [callbacks.md](callbacks.md): the plugin/callback mechanism both cache
  seams are built on (panic containment, payload ownership).
- [memory.md](memory.md): ownership rules for buffers crossing the boundary.
