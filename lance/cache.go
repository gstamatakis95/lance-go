package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/binary"
	"fmt"
)

// CacheKey identifies an entry in the Lance index cache. Prefix scopes the
// entry (typically a dataset/index location), Key is the entry name within
// the prefix, and TypeName is the Rust type of the cached value (a stable
// discriminator you can treat as opaque).
type CacheKey struct {
	Prefix   string
	Key      string
	TypeName string
}

// CacheBackend is an external key/value store for serializable Lance index
// cache entries, plugged into a Session via NewSessionWithCacheBackend. Only
// codec-bearing entries reach it (already serialized to bytes). Non-
// serializable live objects never cross the boundary. Implementations must be
// safe for concurrent use.
//
// This is a building block: the examples/plugincache reference implementation
// shows an in-memory backend, and the same interface backs a disk or Redis
// store with no changes to lance-go.
type CacheBackend interface {
	// Get returns the bytes stored under key, and whether they were found.
	Get(key CacheKey) (val []byte, found bool)
	// Put stores val under key. sizeHint is the value's size in bytes.
	Put(key CacheKey, val []byte, sizeHint int)
	// InvalidatePrefix drops every entry whose key prefix starts with prefix.
	InvalidatePrefix(prefix string)
	// Clear drops every entry.
	Clear()
	// Len returns the number of entries.
	Len() int
	// SizeBytes returns the total stored byte size.
	SizeBytes() int64
}

// ObjectStoreCache is a byte-range cache for immutable Lance file reads,
// attached to an opened dataset via OpenWithObjectStoreCache. Keys are
// (path, start, end). Lance data files are write-once so no invalidation is
// needed. Implementations must be safe for concurrent use.
type ObjectStoreCache interface {
	// Get returns the cached bytes for the exact (path, start, end) range and
	// whether they were found.
	Get(path string, start, end uint64) (data []byte, found bool)
	// Put stores data for the (path, start, end) range.
	Put(path string, start, end uint64, data []byte)
}

// --- cache backend adapter (Go side of the CacheBackend plugin) ---

// Method discriminators that must match rust/src/cache.rs.
const (
	cacheGet              int32 = 0
	cachePut              int32 = 1
	cacheInvalidatePrefix int32 = 2
	cacheClear            int32 = 3
	cacheLen              int32 = 4
	cacheSizeBytes        int32 = 5
)

type cacheBackendAdapter struct {
	b CacheBackend
}

// Invoke implements plugin by dispatching to the wrapped CacheBackend based
// on the cache* method discriminator.
func (a *cacheBackendAdapter) Invoke(method int32, payload []byte) ([]byte, error) {
	switch method {
	case cacheGet:
		key, _, err := decodeCacheKey(payload)
		if err != nil {
			return nil, err
		}
		val, found := a.b.Get(key)
		if !found {
			return nil, errPluginMiss
		}
		return val, nil
	case cachePut:
		key, val, err := decodeCacheKey(payload)
		if err != nil {
			return nil, err
		}
		// Copy: val aliases the (borrowed) payload buffer.
		a.b.Put(key, append([]byte(nil), val...), len(val))
		return nil, nil
	case cacheInvalidatePrefix:
		a.b.InvalidatePrefix(string(payload))
		return nil, nil
	case cacheClear:
		a.b.Clear()
		return nil, nil
	case cacheLen:
		return u64le(uint64(a.b.Len())), nil
	case cacheSizeBytes:
		return u64le(uint64(a.b.SizeBytes())), nil
	default:
		return nil, fmt.Errorf("lance: unknown cache backend method %d", method)
	}
}

// --- object-store cache adapter ---

// Method discriminators that must match rust/src/cache.rs.
const (
	oscGet int32 = 0
	oscPut int32 = 1
)

type objectStoreCacheAdapter struct {
	c ObjectStoreCache
}

// Invoke implements plugin by dispatching to the wrapped ObjectStoreCache
// based on the osc* method discriminator.
func (a *objectStoreCacheAdapter) Invoke(method int32, payload []byte) ([]byte, error) {
	path, start, end, data, err := decodeRangeKey(payload)
	if err != nil {
		return nil, err
	}
	switch method {
	case oscGet:
		d, found := a.c.Get(path, start, end)
		if !found {
			return nil, errPluginMiss
		}
		return d, nil
	case oscPut:
		a.c.Put(path, start, end, append([]byte(nil), data...))
		return nil, nil
	default:
		return nil, fmt.Errorf("lance: unknown object-store cache method %d", method)
	}
}

// WithObjectStoreCache wraps the opened dataset's object store so that
// immutable file byte-range reads are served through cache. It composes with
// the other Open options, including WithSession, so a dataset can be opened
// with a shared session AND a byte cache at once. A nil cache is a no-op.
//
// Native dataset/scanner/reader clones retain the cache registration until the
// last clone is released. This is an Open-only option: writes have no
// object-store wrapper hook.
func WithObjectStoreCache(cache ObjectStoreCache) OpenOption {
	return func(cfg *openConfig) { cfg.objectStoreCache = cache }
}

// OpenWithObjectStoreCache opens the dataset at uri (with the given
// OpenOptions) and wraps its object store so that immutable file byte-range
// reads are served through cache. The returned Dataset behaves like any
// other. Its Close releases the native handle; native children retain the
// cache registration until they are released. It is a thin wrapper over
// Open(ctx, uri, append(opts, WithObjectStoreCache(cache))...).
func OpenWithObjectStoreCache(ctx context.Context, uri string, cache ObjectStoreCache, opts ...OpenOption) (*Dataset, error) {
	if cache == nil {
		return nil, fmt.Errorf("lance: object-store cache must not be nil: %w", ErrInvalidArgument)
	}
	return Open(ctx, uri, append(opts, WithObjectStoreCache(cache))...)
}

// wrapObjectStoreCache wraps ds's object store with the Go byte-range cache and
// returns a NEW dataset handle carrying obs o. It closes ds (the unwrapped
// handle) and, on success, ties the cache-plugin registration to the wrapped
// dataset's lifetime. uri is used only for error context.
func wrapObjectStoreCache(ds *Dataset, cache ObjectStoreCache, o *obs, uri string) (*Dataset, error) {
	handle := registerPlugin(&objectStoreCacheAdapter{c: cache})

	var wrapped *C.LanceDataset
	callErr := ffiCall(context.Background(), func() C.int32_t {
		return C.lance_dataset_wrap_object_store_cache(ds.ptr, C.size_t(handle), &wrapped)
	})
	// The original (unwrapped) handle is no longer needed either way.
	ds.Close()
	if callErr != nil {
		releasePlugin(handle)
		return nil, fmt.Errorf("lance: wrap object-store cache for %q: %w", uri, callErr)
	}
	// The wrapped native store owns the plugin registration and releases it
	// after its last dataset/scanner/reader clone is dropped.
	return newDataset(wrapped, o), nil
}

// --- wire helpers (must match the encodings in rust/src/cache.rs) ---

// decodeCacheKey decodes a length-prefixed (prefix, key, type_name) triple and
// returns the key plus any trailing value bytes (empty for GET).
func decodeCacheKey(payload []byte) (CacheKey, []byte, error) {
	rd := payload
	readStr := func() (string, error) {
		if len(rd) < 4 {
			return "", fmt.Errorf("lance: truncated cache key")
		}
		n := binary.LittleEndian.Uint32(rd)
		rd = rd[4:]
		if uint32(len(rd)) < n {
			return "", fmt.Errorf("lance: truncated cache key string")
		}
		s := string(rd[:n])
		rd = rd[n:]
		return s, nil
	}
	prefix, err := readStr()
	if err != nil {
		return CacheKey{}, nil, err
	}
	key, err := readStr()
	if err != nil {
		return CacheKey{}, nil, err
	}
	typeName, err := readStr()
	if err != nil {
		return CacheKey{}, nil, err
	}
	return CacheKey{Prefix: prefix, Key: key, TypeName: typeName}, rd, nil
}

// decodeRangeKey decodes a length-prefixed path, start:u64, end:u64 and any
// trailing data bytes (empty for GET).
func decodeRangeKey(payload []byte) (path string, start, end uint64, data []byte, err error) {
	if len(payload) < 4 {
		return "", 0, 0, nil, fmt.Errorf("lance: truncated range key")
	}
	n := binary.LittleEndian.Uint32(payload)
	rd := payload[4:]
	if uint32(len(rd)) < n {
		return "", 0, 0, nil, fmt.Errorf("lance: truncated range key path")
	}
	path = string(rd[:n])
	rd = rd[n:]
	if len(rd) < 16 {
		return "", 0, 0, nil, fmt.Errorf("lance: truncated range key bounds")
	}
	start = binary.LittleEndian.Uint64(rd[0:8])
	end = binary.LittleEndian.Uint64(rd[8:16])
	data = rd[16:]
	return path, start, end, data, nil
}

// u64le encodes v as 8 little-endian bytes.
func u64le(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}
