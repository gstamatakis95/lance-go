# plugincache: external cache building blocks

Runnable reference implementations for the v0.2 cache hooks. Both are simple
**templates**: they show the interfaces working end to end, and a production
implementation swaps the storage without touching lance-go.

```sh
go run ./examples/plugincache
```

## What it shows

1. **`lance.CacheBackend`**: an external store for Lance's *index* cache,
   attached to a `Session` via `lance.NewSessionWithCacheBackend`. The example
   builds and prewarms an IVF_PQ vector index on a dataset opened with that
   session and prints the Put/Get/hit counters, then runs a search served from
   the backend.

   Only **serializable, codec-bearing** index entries reach the backend
   (already serialized to `[]byte`): IVF/PQ partition data, the IVF index
   state, and index metadata. Non-serializable "live object" entries stay in
   an in-process cache and never cross the boundary, and the metadata cache
   has no backend hook in Lance 8.0. Because entries arrive as bytes, a real
   backend is just a `[]byte` KV store:

   - **Redis**: `Get` → `GET`, `Put` → `SET`, `InvalidatePrefix` → `SCAN`+`DEL`.
   - **Disk**: one file per key (hash the key parts for the filename).

   The `memCacheBackend` here is a mutex-guarded map. The
   `TestCacheBackendExternalizable` test persists such a map to disk and loads
   it into a fresh session, proving the cache survives a process restart.

2. **`lance.ObjectStoreCache`**: a byte-range cache for immutable Lance file
   reads, attached with `lance.OpenWithObjectStoreCache`. The example scans a
   dataset twice through a disk-backed cache and shows the second scan served
   from cache (hits > 0). Keys are `(path, start, end)`. Lance data files are
   write-once, so no invalidation is needed.

   > The plain `file://` scheme uses an optimized local reader that bypasses
   > object-store wrappers, so the example opens through `file-object-store://`
   > to route local reads through the object-store API. Against a real object
   > store (`s3://`, `az://`, `gs://`) the wrapper is always on the read path.

## Externalizing to Redis (sketch)

No lance-go change is required, implement the same interface:

```go
type redisCacheBackend struct{ rdb *redis.Client }

func (r *redisCacheBackend) Get(k lance.CacheKey) ([]byte, bool) {
	v, err := r.rdb.Get(ctx, k.Prefix+"|"+k.Key+"|"+k.TypeName).Bytes()
	return v, err == nil
}
func (r *redisCacheBackend) Put(k lance.CacheKey, v []byte, sizeHint int) {
	r.rdb.Set(ctx, k.Prefix+"|"+k.Key+"|"+k.TypeName, v, 0)
}
// ... InvalidatePrefix, Clear, Len, SizeBytes

sess, _ := lance.NewSessionWithCacheBackend(&redisCacheBackend{rdb}, 64<<20)
```
