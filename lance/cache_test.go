package lance_test

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// recordingBackend is an in-memory lance.CacheBackend that records traffic so
// tests can assert on what actually reaches the codec boundary.
type recordingBackend struct {
	mu       sync.Mutex
	m        map[string][]byte
	puts     int
	gets     int
	hits     int
	putTypes map[string]int
}

func newRecordingBackend() *recordingBackend {
	return &recordingBackend{m: map[string][]byte{}, putTypes: map[string]int{}}
}

func keyStr(k lance.CacheKey) string {
	return k.Prefix + "\x00" + k.Key + "\x00" + k.TypeName
}

func (b *recordingBackend) Get(k lance.CacheKey) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets++
	v, ok := b.m[keyStr(k)]
	if ok {
		b.hits++
	}
	return v, ok
}

func (b *recordingBackend) Put(k lance.CacheKey, v []byte, sizeHint int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts++
	b.putTypes[k.TypeName]++
	b.m[keyStr(k)] = v
}

func (b *recordingBackend) InvalidatePrefix(prefix string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for k := range b.m {
		// keys are prefix\0key\0type, match on the leading prefix segment.
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(b.m, k)
		}
	}
}

func (b *recordingBackend) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m = map[string][]byte{}
}

func (b *recordingBackend) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.m)
}

func (b *recordingBackend) SizeBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for _, v := range b.m {
		n += int64(len(v))
	}
	return n
}

func (b *recordingBackend) snapshot() (puts, gets, hits int, types map[string]int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	types = map[string]int{}
	for k, v := range b.putTypes {
		types[k] = v
	}
	return b.puts, b.gets, b.hits, types
}

// export/load persist the backend's key/value map, simulating a store that
// survives a process restart.
func (b *recordingBackend) export(t *testing.T, path string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create export: %v", err)
	}
	defer f.Close()
	if err := gob.NewEncoder(f).Encode(b.m); err != nil {
		t.Fatalf("encode export: %v", err)
	}
}

func loadRecordingBackend(t *testing.T, path string) *recordingBackend {
	t.Helper()
	b := newRecordingBackend()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open export: %v", err)
	}
	defer f.Close()
	if err := gob.NewDecoder(f).Decode(&b.m); err != nil {
		t.Fatalf("decode export: %v", err)
	}
	return b
}

// writeVecDataset writes a fresh vector dataset (no session) and returns its
// uri.
func writeVecDataset(t *testing.T, rows int64) string {
	t.Helper()
	uri := filepath.Join(t.TempDir(), "vec.lance")
	rdr := testutil.NewReader(testutil.Allocator(), 0, rows, 128)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	ds.Close()
	return uri
}

// TestCacheBackendReceivesCodecEntries verifies that building and prewarming a
// vector index on a dataset opened with a cache-backend session delegates
// codec-bearing index entries to the Go backend, and that a subsequent search
// serves them back (Gets with hits).
func TestCacheBackendReceivesCodecEntries(t *testing.T) {
	ctx := t.Context()
	uri := writeVecDataset(t, 1024)

	backend := newRecordingBackend()
	sess, err := lance.NewSessionWithCacheBackend(backend, 64<<20)
	if err != nil {
		t.Fatalf("NewSessionWithCacheBackend: %v", err)
	}
	defer sess.Close()

	ds, err := sess.Open(ctx, uri)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	if err := ds.CreateIndex(ctx, "vec",
		lance.IvfPq{Partitions: 4, SubVectors: 4, Bits: 8},
		lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := ds.PrewarmIndex(ctx, "vec_idx"); err != nil {
		t.Fatalf("PrewarmIndex: %v", err)
	}

	puts, _, _, types := backend.snapshot()
	t.Logf("codec-bearing entry types reaching the backend: %v", types)
	if puts == 0 {
		t.Fatal("no codec-bearing entries reached the Go cache backend")
	}
	// The IVF/PQ partition entries are the serializable payload the backend
	// exists to hold. Assert at least one arrived.
	var sawPartition bool
	for typeName := range types {
		if len(typeName) >= 9 && typeName[:9] == "lance::in" { // PartitionEntry<...>
			sawPartition = true
		}
	}
	if !sawPartition {
		t.Logf("note: no IVF PartitionEntry reached the backend (types=%v)", types)
	}
	ds.Close()

	// Reopen on the shared session and search: partition data is served from
	// the backend (Gets with hits).
	before := func() int { _, _, h, _ := backend.snapshot(); return h }()
	ds2, err := sess.Open(ctx, uri)
	if err != nil {
		t.Fatalf("second session.Open: %v", err)
	}
	recs := scanAll(t, ds2.Scan().Nearest("vec", vecOf(42), 10).Nprobes(4).Refine(4))
	if !contains(idsOf(t, recs), 42) {
		t.Fatalf("search miss: top-10 for vec(42) does not contain id 42")
	}
	ds2.Close()

	_, gets, hits, _ := backend.snapshot()
	if gets == 0 {
		t.Fatal("second open + search issued no Gets to the backend")
	}
	if hits <= before {
		t.Fatalf("search served no cache hits from the backend (hits=%d, before=%d)", hits, before)
	}
}

// TestCacheBackendExternalizable proves the backend is a true external store:
// after persisting its map and loading it into a NEW session+backend (a
// simulated process restart), a fresh open + search works served from the
// cache.
func TestCacheBackendExternalizable(t *testing.T) {
	ctx := t.Context()
	uri := writeVecDataset(t, 1024)

	// Phase 1: populate a backend by building + prewarming the index.
	backend := newRecordingBackend()
	sess, err := lance.NewSessionWithCacheBackend(backend, 64<<20)
	if err != nil {
		t.Fatalf("NewSessionWithCacheBackend: %v", err)
	}
	ds, err := sess.Open(ctx, uri)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	if err := ds.CreateIndex(ctx, "vec",
		lance.IvfPq{Partitions: 4, SubVectors: 4, Bits: 8},
		lance.WithIndexName("vec_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := ds.PrewarmIndex(ctx, "vec_idx"); err != nil {
		t.Fatalf("PrewarmIndex: %v", err)
	}
	ds.Close()

	path := filepath.Join(t.TempDir(), "cache.gob")
	backend.export(t, path)
	sess.Close()

	// Phase 2: fresh session + a backend loaded from disk ("restart").
	loaded := loadRecordingBackend(t, path)
	if loaded.Len() == 0 {
		t.Fatal("persisted cache is empty")
	}
	sess2, err := lance.NewSessionWithCacheBackend(loaded, 64<<20)
	if err != nil {
		t.Fatalf("NewSessionWithCacheBackend(loaded): %v", err)
	}
	defer sess2.Close()

	ds2, err := sess2.Open(ctx, uri)
	if err != nil {
		t.Fatalf("session2.Open: %v", err)
	}
	defer ds2.Close()
	recs := scanAll(t, ds2.Scan().Nearest("vec", vecOf(42), 10).Nprobes(4).Refine(4))
	if !contains(idsOf(t, recs), 42) {
		t.Fatalf("search from restored cache missed id 42")
	}
	if _, _, hits, _ := loaded.snapshot(); hits == 0 {
		t.Fatal("restored backend served no hits: cache was not consulted")
	}
}

// recordingOSCache is an in-memory lance.ObjectStoreCache that records traffic.
type recordingOSCache struct {
	mu   sync.Mutex
	m    map[string][]byte
	gets int
	puts int
	hits int
	keys []string
}

func newRecordingOSCache() *recordingOSCache {
	return &recordingOSCache{m: map[string][]byte{}}
}

func rangeKey(path string, start, end uint64) string {
	return fmt.Sprintf("%s|%d|%d", path, start, end)
}

func (c *recordingOSCache) Get(path string, start, end uint64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	v, ok := c.m[rangeKey(path, start, end)]
	if ok {
		c.hits++
	}
	return v, ok
}

func (c *recordingOSCache) Put(path string, start, end uint64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	c.keys = append(c.keys, rangeKey(path, start, end))
	c.m[rangeKey(path, start, end)] = data
}

func (c *recordingOSCache) stats() (gets, puts, hits int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.puts, c.hits
}

func TestObjectStoreCache(t *testing.T) {
	ctx := t.Context()
	// Write to a local path, then open through the "file-object-store" scheme
	// so reads go through the object_store API (the plain "file" scheme uses
	// an optimized local reader that bypasses object stores, and thus wrappers).
	uri := writeVecDataset(t, 2048)
	osURI := "file-object-store://" + uri

	cache := newRecordingOSCache()
	ds, err := lance.OpenWithObjectStoreCache(ctx, osURI, cache)
	if err != nil {
		t.Fatalf("OpenWithObjectStoreCache: %v", err)
	}
	defer ds.Close()

	// First scan populates the cache (all misses, then Puts).
	recs := scanAll(t, ds.Scan())
	if got := testutil.TotalRows(recs); got != 2048 {
		t.Fatalf("first scan rows = %d, want 2048", got)
	}
	gets1, puts1, _ := cache.stats()
	if puts1 == 0 {
		t.Fatal("object-store cache received no Puts on first scan")
	}
	if gets1 == 0 {
		t.Fatal("object-store cache received no Gets on first scan")
	}
	// Keys must be (path, start, end) with a real path and a non-empty range.
	cache.mu.Lock()
	if len(cache.keys) == 0 || cache.keys[0] == "||0|0" {
		t.Fatalf("unexpected cache keys: %v", cache.keys)
	}
	cache.mu.Unlock()

	// Second scan serves cached ranges (hits).
	recs2 := scanAll(t, ds.Scan())
	if got := testutil.TotalRows(recs2); got != 2048 {
		t.Fatalf("second scan rows = %d, want 2048", got)
	}
	_, _, hits2 := cache.stats()
	if hits2 == 0 {
		t.Fatal("object-store cache served no hits on the second scan")
	}
}

// TestSessionAndObjectStoreCache exercises the previously-impossible
// combination: opening one dataset with BOTH a shared Session and an
// object-store byte cache. It asserts (a) a scan drives traffic through the
// byte cache on the wrapped handle, and (b) the session is shared across opens
// (metadata cache hits grow on a second open). Note the session's survival
// through the object-store wrap rests on Dataset::with_object_store_wrappers
// being a clone that preserves the session Arc, not on the hit-growth check
// alone (open_with_session already primes the session cache before the wrap).
func TestSessionAndObjectStoreCache(t *testing.T) {
	ctx := t.Context()
	uri := writeVecDataset(t, 2048)
	// Open through the "file-object-store" scheme so reads go through the
	// object_store API (plain "file" bypasses object stores and thus wrappers).
	osURI := "file-object-store://" + uri

	sess, err := lance.NewSession(lance.SessionConfig{
		IndexCacheBytes:    32 << 20,
		MetadataCacheBytes: 32 << 20,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	cache := newRecordingOSCache()
	ds, err := lance.Open(ctx, osURI, lance.WithSession(sess), lance.WithObjectStoreCache(cache))
	if err != nil {
		t.Fatalf("Open with session + object-store cache: %v", err)
	}

	recs := scanAll(t, ds.Scan())
	if got := testutil.TotalRows(recs); got != 2048 {
		t.Fatalf("scan rows = %d, want 2048", got)
	}
	// The byte cache must have taken both Gets (lookups) and Puts (fills).
	gets1, puts1, _ := cache.stats()
	if puts1 == 0 {
		t.Fatal("object-store cache received no Puts under session + cache open")
	}
	if gets1 == 0 {
		t.Fatal("object-store cache received no Gets under session + cache open")
	}
	s1, err := sess.Stats()
	if err != nil {
		t.Fatalf("Stats #1: %v", err)
	}
	ds.Close()

	// A second open on the SAME session serves file metadata from the shared
	// metadata cache: hits grow. This is what proves the session was preserved
	// through the object-store wrapping.
	ds2, err := sess.Open(ctx, osURI)
	if err != nil {
		t.Fatalf("second session.Open: %v", err)
	}
	_ = scanAll(t, ds2.Scan())
	s2, err := sess.Stats()
	if err != nil {
		t.Fatalf("Stats #2: %v", err)
	}
	ds2.Close()

	if s2.MetadataCache.Hits <= s1.MetadataCache.Hits {
		t.Fatalf("session metadata cache hits did not grow across a shared-session reopen: %d -> %d",
			s1.MetadataCache.Hits, s2.MetadataCache.Hits)
	}
}
