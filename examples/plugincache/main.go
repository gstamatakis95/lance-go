// Command plugincache demonstrates the v0.2 cache building blocks: a Go
// lance.CacheBackend (the external index-cache store attached to a Session)
// and a lance.ObjectStoreCache (a byte-range cache for immutable file reads).
//
// Both implementations here are deliberately simple TEMPLATES:
//   - memCacheBackend is an in-memory map. Swap the map for Redis or a disk
//     KV store and you have an externalized index cache, with no change to
//     lance-go required.
//   - diskObjectStoreCache stores byte ranges as files under a temp dir. The
//     same interface backs an S3/Redis byte cache.
//
// Usage: go run ./examples/plugincache
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/gstamatakis95/lance-go/lance"
)

// ---- (1) An in-memory CacheBackend template ----

// memCacheBackend is a toy lance.CacheBackend backed by a mutex-guarded map.
// Only serializable, codec-bearing index entries reach it (already bytes), so
// a real implementation just needs a []byte KV store (Redis: GET/SET, disk:
// files). Counters make the cache traffic visible.
type memCacheBackend struct {
	mu         sync.Mutex
	m          map[string][]byte
	gets, puts int
	hits       int
}

func newMemCacheBackend() *memCacheBackend {
	return &memCacheBackend{m: map[string][]byte{}}
}

func (b *memCacheBackend) key(k lance.CacheKey) string {
	return k.Prefix + "\x00" + k.Key + "\x00" + k.TypeName
}

func (b *memCacheBackend) Get(k lance.CacheKey) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets++
	v, ok := b.m[b.key(k)]
	if ok {
		b.hits++
	}
	return v, ok
}

func (b *memCacheBackend) Put(k lance.CacheKey, v []byte, sizeHint int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts++
	b.m[b.key(k)] = v
}

func (b *memCacheBackend) InvalidatePrefix(prefix string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for k := range b.m {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(b.m, k)
		}
	}
}

func (b *memCacheBackend) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m = map[string][]byte{}
}

func (b *memCacheBackend) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.m)
}

func (b *memCacheBackend) SizeBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for _, v := range b.m {
		n += int64(len(v))
	}
	return n
}

// ---- (2) A disk ObjectStoreCache template ----

// diskObjectStoreCache stores each cached byte range as a file named by a hash
// of (path, start, end) under dir. Swap the file I/O for S3 or Redis and you
// have a shared byte cache.
type diskObjectStoreCache struct {
	dir        string
	mu         sync.Mutex
	gets, puts int
	hits       int
}

func (c *diskObjectStoreCache) file(path string, start, end uint64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d", path, start, end)))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:])+".range")
}

func (c *diskObjectStoreCache) Get(path string, start, end uint64) ([]byte, bool) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	data, err := os.ReadFile(c.file(path, start, end))
	if err != nil {
		return nil, false
	}
	c.mu.Lock()
	c.hits++
	c.mu.Unlock()
	return data, true
}

func (c *diskObjectStoreCache) Put(path string, start, end uint64, data []byte) {
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	_ = os.WriteFile(c.file(path, start, end), data, 0o600)
}

// ---- driver ----

func newVectorReader(rows int64) array.RecordReader {
	const dim = 32
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	lb := b.Field(1).(*array.FixedSizeListBuilder)
	vb := lb.ValueBuilder().(*array.Float32Builder)
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
		lb.Append(true)
		for j := 0; j < dim; j++ {
			vb.Append(float32(i) + float32(j))
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("new record reader: %v", err)
	}
	return rdr
}

func queryVec() []float32 {
	const dim = 32
	v := make([]float32, dim)
	for j := range v {
		v[j] = 42 + float32(j)
	}
	return v
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "plugincache")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "vectors.lance")

	// Write a small vector dataset.
	rdr := newVectorReader(2048)
	ds0, err := lance.Write(ctx, uri, rdr)
	rdr.Release()
	if err != nil {
		log.Fatalf("write: %v", err)
	}
	ds0.Close()

	// (1) CacheBackend: build + prewarm a vector index on a session whose
	// index cache is the Go backend.
	backend := newMemCacheBackend()
	sess, err := lance.NewSessionWithCacheBackend(backend, 64<<20)
	if err != nil {
		log.Fatalf("new session: %v", err)
	}
	defer sess.Close()

	ds, err := sess.Open(ctx, uri)
	if err != nil {
		log.Fatalf("session open: %v", err)
	}
	if err := ds.CreateIndex(ctx, "vec",
		lance.IvfPq{Partitions: 8, SubVectors: 4, Bits: 8},
		lance.WithIndexName("vec_idx")); err != nil {
		log.Fatalf("create index: %v", err)
	}
	if err := ds.PrewarmIndex(ctx, "vec_idx"); err != nil {
		log.Fatalf("prewarm: %v", err)
	}
	fmt.Printf("CacheBackend after index build + prewarm: entries=%d puts=%d gets=%d hits=%d\n",
		backend.Len(), backend.puts, backend.gets, backend.hits)

	// Search served from the Go backend.
	if _, err := search(ctx, ds); err != nil {
		log.Fatalf("search: %v", err)
	}
	ds.Close()
	fmt.Printf("CacheBackend after search:            entries=%d puts=%d gets=%d hits=%d\n",
		backend.Len(), backend.puts, backend.gets, backend.hits)

	stats, err := sess.Stats()
	if err != nil {
		log.Fatalf("stats: %v", err)
	}
	fmt.Printf("Session index-cache stats: hits=%d misses=%d entries=%d\n",
		stats.IndexCache.Hits, stats.IndexCache.Misses, stats.IndexCache.NumEntries)

	// (2) ObjectStoreCache: scan twice through a disk byte cache. The
	// "file-object-store" scheme routes local reads through the object_store
	// API (the plain "file" scheme uses an optimized local reader that
	// bypasses wrappers).
	osCache := &diskObjectStoreCache{dir: dir}
	cds, err := lance.OpenWithObjectStoreCache(ctx, "file-object-store://"+uri, osCache)
	if err != nil {
		log.Fatalf("open with object-store cache: %v", err)
	}
	defer cds.Close()
	scanCount(ctx, cds)
	fmt.Printf("ObjectStoreCache after 1st scan: puts=%d gets=%d hits=%d\n", osCache.puts, osCache.gets, osCache.hits)
	scanCount(ctx, cds)
	fmt.Printf("ObjectStoreCache after 2nd scan: puts=%d gets=%d hits=%d\n", osCache.puts, osCache.gets, osCache.hits)

	fmt.Println("\nBoth caches are plain Go interfaces: point them at Redis or disk to")
	fmt.Println("externalize Lance's index and byte caches with no change to lance-go.")
}

func search(ctx context.Context, ds *lance.Dataset) (int, error) {
	rdr, err := ds.Scan().Nearest("vec", queryVec(), 10).Nprobes(8).Refine(4).Reader(ctx)
	if err != nil {
		return 0, err
	}
	defer rdr.Release()
	rows := 0
	for rdr.Next() {
		rows += int(rdr.RecordBatch().NumRows())
	}
	return rows, rdr.Err()
}

func scanCount(ctx context.Context, ds *lance.Dataset) {
	rdr, err := ds.Scan().Reader(ctx)
	if err != nil {
		log.Fatalf("scan: %v", err)
	}
	defer rdr.Release()
	for rdr.Next() {
	}
	if err := rdr.Err(); err != nil {
		log.Fatalf("scan read: %v", err)
	}
}
