package lance

import (
	"path/filepath"
	"testing"

	"github.com/gstamatakis95/lance-go/internal/testutil"
)

type lifetimeObjectStoreCache struct{}

func (lifetimeObjectStoreCache) Get(string, uint64, uint64) ([]byte, bool) { return nil, false }
func (lifetimeObjectStoreCache) Put(string, uint64, uint64, []byte)        {}

type lifetimeCacheBackend struct{}

func (lifetimeCacheBackend) Get(CacheKey) ([]byte, bool) { return nil, false }
func (lifetimeCacheBackend) Put(CacheKey, []byte, int)   {}
func (lifetimeCacheBackend) InvalidatePrefix(string)     {}
func (lifetimeCacheBackend) Clear()                      {}
func (lifetimeCacheBackend) Len() int                    { return 0 }
func (lifetimeCacheBackend) SizeBytes() int64            { return 0 }

func pluginCount() int {
	pluginRegistry.RLock()
	defer pluginRegistry.RUnlock()
	return len(pluginRegistry.m)
}

func writeCacheLifetimeDataset(t *testing.T) string {
	t.Helper()
	uri := filepath.Join(t.TempDir(), "cache-lifetime.lance")
	rdr := testutil.NewReader(testutil.Allocator(), 0, 8, 2)
	defer rdr.Release()
	ds, err := Write(t.Context(), uri, rdr)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close written dataset: %v", err)
	}
	return uri
}

func TestObjectStoreCacheRegistrationFollowsNativeReader(t *testing.T) {
	uri := writeCacheLifetimeDataset(t)
	before := pluginCount()
	ds, err := OpenWithObjectStoreCache(t.Context(), uri, lifetimeObjectStoreCache{})
	if err != nil {
		t.Fatalf("OpenWithObjectStoreCache: %v", err)
	}
	if got := pluginCount(); got != before+1 {
		t.Fatalf("plugin count after open = %d, want %d", got, before+1)
	}
	rdr, err := ds.Scan().BatchSize(1).Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Dataset.Close: %v", err)
	}
	if got := pluginCount(); got != before+1 {
		t.Fatalf("plugin released while native reader was alive: got %d, want %d", got, before+1)
	}
	rdr.Release()
	if got := pluginCount(); got != before {
		t.Fatalf("plugin count after reader release = %d, want %d", got, before)
	}
}

func TestSessionCacheRegistrationFollowsNativeDataset(t *testing.T) {
	uri := writeCacheLifetimeDataset(t)
	before := pluginCount()
	session, err := NewSessionWithCacheBackend(lifetimeCacheBackend{}, 1<<20)
	if err != nil {
		t.Fatalf("NewSessionWithCacheBackend: %v", err)
	}
	ds, err := session.Open(t.Context(), uri)
	if err != nil {
		_ = session.Close()
		t.Fatalf("Session.Open: %v", err)
	}
	if got := pluginCount(); got != before+1 {
		t.Fatalf("plugin count after open = %d, want %d", got, before+1)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Session.Close: %v", err)
	}
	if got := pluginCount(); got != before+1 {
		t.Fatalf("plugin released while native dataset was alive: got %d, want %d", got, before+1)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Dataset.Close: %v", err)
	}
	if got := pluginCount(); got != before {
		t.Fatalf("plugin count after dataset close = %d, want %d", got, before)
	}
}
