package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Session holds index and metadata caches shared across dataset opens and
// writes. Reusing one Session across Open/Write calls lets a second open of
// the same dataset serve schema, manifest and index data from cache instead
// of re-reading storage. A Session is safe for concurrent use. Close releases
// it (idempotent).
type Session struct {
	mu  sync.Mutex
	ptr *C.LanceSession
	// withObs is the OpenTelemetry instrumentation for this session, inherited
	// by every Dataset opened or written through it (nil-safe).
	withObs *obs
	// cleanup is a leak safety net: if the caller drops every reference to
	// this Session without calling Close, the runtime eventually releases the
	// native handle anyway. Close stops it so the native pointer is never
	// closed twice.
	cleanup runtime.Cleanup
}

// obs returns the instrumentation handle for this session (nil-safe).
func (s *Session) obs() *obs { return s.withObs }

// newSession wraps a native session pointer, attaching the observability
// handle o. Both Session constructors (NewSession,
// NewSessionWithCacheBackend) go through here so every Session gets the GC
// leak safety net.
func newSession(ptr *C.LanceSession, o *obs) *Session {
	s := &Session{ptr: ptr, withObs: o}
	// The cleanup func must capture nothing but its argument: closing over s
	// or ptr would keep the Session reachable and defeat collection.
	s.cleanup = runtime.AddCleanup(s, func(p *C.LanceSession) { C.lance_session_close(p) }, ptr)
	return s
}

// SessionConfig configures the in-memory caches of a Session.
type SessionConfig struct {
	// IndexCacheBytes is the byte budget of the index cache.
	IndexCacheBytes uint64
	// MetadataCacheBytes is the byte budget of the metadata cache.
	MetadataCacheBytes uint64
}

// CacheCounts reports hit/miss and size counters for one cache.
type CacheCounts struct {
	// Hits is the number of lookups served from the cache.
	Hits uint64 `json:"hits"`
	// Misses is the number of lookups not found in the cache.
	Misses uint64 `json:"misses"`
	// NumEntries is the number of entries currently cached.
	NumEntries uint64 `json:"num_entries"`
	// SizeBytes is the approximate memory held by the cache.
	SizeBytes uint64 `json:"size_bytes"`
}

// SessionStats reports a snapshot of a Session's cache statistics.
type SessionStats struct {
	// IndexCache reports the index cache's hit/miss and size counters.
	IndexCache CacheCounts `json:"index_cache"`
	// MetadataCache reports the metadata cache's hit/miss and size counters.
	MetadataCache CacheCounts `json:"metadata_cache"`
	// SizeBytes is the approximate total memory held by both caches.
	SizeBytes uint64 `json:"size_bytes"`
	// ApproxNumItems is the approximate total number of entries across both
	// caches.
	ApproxNumItems uint64 `json:"approx_num_items"`
}

// NewSession creates a Session with in-memory index and metadata caches of the
// configured byte budgets. Close it when done.
func NewSession(cfg SessionConfig, opts ...ObsOption) (*Session, error) {
	var ptr *C.LanceSession
	if err := ffiCall(context.Background(), func() C.int32_t {
		return C.lance_session_new(
			C.uint64_t(cfg.IndexCacheBytes),
			C.uint64_t(cfg.MetadataCacheBytes),
			&ptr,
		)
	}); err != nil {
		return nil, fmt.Errorf("lance: new session: %w", err)
	}
	return newSession(ptr, newObsFromOptions(opts)), nil
}

// NewSessionWithCacheBackend creates a Session whose index cache is backed by
// b. Only serializable, codec-bearing index entries (IVF/PQ/RQ partitions, FTS
// posting lists, scalar-index states, ...) are delegated to b. Non-
// serializable "live object" entries stay in an in-process cache, and the
// metadata cache stays in-memory (Lance 8.0 has no metadata-cache backend
// hook). Close releases this Session handle; native datasets opened through it
// retain the backend registration until their final clone is released.
func NewSessionWithCacheBackend(b CacheBackend, metadataCacheBytes uint64, opts ...ObsOption) (*Session, error) {
	if b == nil {
		return nil, fmt.Errorf("lance: cache backend must not be nil: %w", ErrInvalidArgument)
	}
	handle := registerPlugin(&cacheBackendAdapter{b: b})
	var ptr *C.LanceSession
	if err := ffiCall(context.Background(), func() C.int32_t {
		return C.lance_session_new_with_cache_backend(
			C.size_t(handle),
			C.uint64_t(metadataCacheBytes),
			&ptr,
		)
	}); err != nil {
		releasePlugin(handle)
		return nil, fmt.Errorf("lance: new session with cache backend: %w", err)
	}
	// Native ownership now includes the Go registration. It is released only
	// after the last native Session clone (including datasets) is dropped.
	return newSession(ptr, newObsFromOptions(opts)), nil
}

// Stats returns a snapshot of the session's cache statistics.
func (s *Session) Stats() (SessionStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		return SessionStats{}, fmt.Errorf("lance: session is closed: %w", ErrInvalidArgument)
	}
	var cJSON *C.char
	if err := ffiCall(context.Background(), func() C.int32_t {
		return C.lance_session_stats(s.ptr, &cJSON)
	}); err != nil {
		return SessionStats{}, fmt.Errorf("lance: session stats: %w", err)
	}
	defer C.lance_string_free(cJSON)
	var stats SessionStats
	if err := json.Unmarshal([]byte(C.GoString(cJSON)), &stats); err != nil {
		return SessionStats{}, fmt.Errorf("lance: decode session stats: %w", err)
	}
	return stats, nil
}

// Close releases this session handle. A cache-backend registration remains
// alive while datasets opened with the session still hold native Session
// clones, and is released when the last clone is dropped. Close is idempotent.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr != nil {
		C.lance_session_close(s.ptr)
		s.cleanup.Stop()
		s.ptr = nil
	}
	return nil
}

// WithSession opens a dataset sharing s's index and metadata caches. It
// composes with the other Open options, including WithObjectStoreCache:
// Open(ctx, uri, WithSession(s), WithObjectStoreCache(c)) opens the dataset
// with a shared session AND a byte cache on the same handle.
func WithSession(s *Session) OpenOption {
	return func(cfg *openConfig) { cfg.session = s }
}

// WithWriteSession writes a dataset sharing s's index and metadata caches.
// It composes with the other Write options. This is the Write counterpart of
// WithSession.
func WithWriteSession(s *Session) WriteOption {
	return func(cfg *writeConfig) { cfg.session = s }
}

// Open opens a dataset sharing this session's caches. It accepts the same
// OpenOptions as the package-level Open (WithVersion/WithTag/
// WithStorageOptions/WithObjectStoreCache). The session owns the cache sizes.
// It is a thin wrapper over Open(ctx, uri, append(opts, WithSession(s))...).
func (s *Session) Open(ctx context.Context, uri string, opts ...OpenOption) (*Dataset, error) {
	return Open(ctx, uri, append(opts, WithSession(s))...)
}

// Write writes rdr to a dataset at uri with this session's caches attached.
// It accepts every package-level WriteOption, including blob, multi-base,
// manifest, auto-cleanup, and transaction-property options. It is a thin
// wrapper over Write(ctx, uri, rdr, append(opts, WithWriteSession(s))...).
func (s *Session) Write(ctx context.Context, uri string, rdr array.RecordReader, opts ...WriteOption) (*Dataset, error) {
	return Write(ctx, uri, rdr, append(opts, WithWriteSession(s))...)
}

// WriteWithProgress is like Session.Write but also reports cumulative
// WriteStats to progress after each batch is written. It is a thin wrapper
// over Write with WithWriteSession and WithWriteProgress.
//
// progress runs synchronously on the write path and MUST NOT re-enter lance-go
// (opening/scanning a dataset, CountRows, etc.). A re-entrant call is rejected
// with ErrReentrantCall rather than crashing the process. Because progress is
// best-effort, that error is ignored and the write still completes.
func (s *Session) WriteWithProgress(ctx context.Context, uri string, rdr array.RecordReader, progress func(WriteStats), opts ...WriteOption) (*Dataset, error) {
	return Write(ctx, uri, rdr, append(opts, WithWriteSession(s), WithWriteProgress(progress))...)
}

// WriteWithProgress writes rdr to a dataset at uri, reporting cumulative
// WriteStats to progress after each batch is written. It is a thin wrapper
// over Write with WithWriteProgress. The reader's batches must be C-allocated
// (see Write).
//
// progress runs synchronously on the write path and MUST NOT re-enter lance-go
// (opening/scanning a dataset, CountRows, etc.). A re-entrant call is rejected
// with ErrReentrantCall rather than crashing the process. Because progress is
// best-effort, that error is ignored and the write still completes.
func WriteWithProgress(ctx context.Context, uri string, rdr array.RecordReader, progress func(WriteStats), opts ...WriteOption) (*Dataset, error) {
	return Write(ctx, uri, rdr, append(opts, WithWriteProgress(progress))...)
}
