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
	"time"
)

// CompactionOptions configures CompactFiles. The zero value uses the
// engine's defaults (1M rows per fragment, 1024 rows per group,
// materialize deletions above a 10% threshold). Only explicitly set fields
// are sent to the engine.
type CompactionOptions struct {
	// TargetRowsPerFragment is the target number of rows per data file.
	// Fragments with fewer rows are candidates for compaction. 0 means the
	// default of 1 million.
	TargetRowsPerFragment uint64
	// MaxRowsPerGroup caps the number of rows per row group in rewritten
	// files. 0 means the default.
	MaxRowsPerGroup uint64
	// MaxBytesPerFile caps the (approximate) size of rewritten data files.
	MaxBytesPerFile *uint64
	// MaterializeDeletions controls whether fragments with deleted rows are
	// rewritten without the deletions (default true).
	MaterializeDeletions *bool
	// MaterializeDeletionsThreshold is the fraction of deleted rows above
	// which deletions are materialized (default 0.1).
	MaterializeDeletionsThreshold *float32
	// NumThreads is the number of compaction tasks to run in parallel
	// (default: number of CPUs).
	NumThreads *uint
	// BatchSize is the batch size used when scanning input fragments.
	BatchSize *uint64
	// DeferIndexRemap defers index remapping to a later optimize pass
	// (default false).
	DeferIndexRemap *bool
}

// compactionOptionsJSON mirrors the options_json contract of
// lance_dataset_compact_files (only set fields are marshaled).
type compactionOptionsJSON struct {
	TargetRowsPerFragment         *uint64  `json:"target_rows_per_fragment,omitempty"`
	MaxRowsPerGroup               *uint64  `json:"max_rows_per_group,omitempty"`
	MaxBytesPerFile               *uint64  `json:"max_bytes_per_file,omitempty"`
	MaterializeDeletions          *bool    `json:"materialize_deletions,omitempty"`
	MaterializeDeletionsThreshold *float32 `json:"materialize_deletions_threshold,omitempty"`
	NumThreads                    *uint    `json:"num_threads,omitempty"`
	BatchSize                     *uint64  `json:"batch_size,omitempty"`
	DeferIndexRemap               *bool    `json:"defer_index_remap,omitempty"`
}

func (o CompactionOptions) toJSON() compactionOptionsJSON {
	out := compactionOptionsJSON{
		MaxBytesPerFile:               o.MaxBytesPerFile,
		MaterializeDeletions:          o.MaterializeDeletions,
		MaterializeDeletionsThreshold: o.MaterializeDeletionsThreshold,
		NumThreads:                    o.NumThreads,
		BatchSize:                     o.BatchSize,
		DeferIndexRemap:               o.DeferIndexRemap,
	}
	if o.TargetRowsPerFragment != 0 {
		out.TargetRowsPerFragment = &o.TargetRowsPerFragment
	}
	if o.MaxRowsPerGroup != 0 {
		out.MaxRowsPerGroup = &o.MaxRowsPerGroup
	}
	return out
}

// CompactionMetrics reports what CompactFiles changed.
type CompactionMetrics struct {
	// FragmentsRemoved is the number of fragments that were replaced.
	FragmentsRemoved uint64 `json:"fragments_removed"`
	// FragmentsAdded is the number of new fragments written.
	FragmentsAdded uint64 `json:"fragments_added"`
	// FilesRemoved is the number of files removed, including deletion files.
	FilesRemoved uint64 `json:"files_removed"`
	// FilesAdded is the number of new files written.
	FilesAdded uint64 `json:"files_added"`
}

// RemovalStats reports what CleanupOldVersions removed.
type RemovalStats struct {
	// BytesRemoved is the total number of bytes freed.
	BytesRemoved uint64 `json:"bytes_removed"`
	// OldVersions is the number of dataset versions removed.
	OldVersions uint64 `json:"old_versions"`
	// DataFilesRemoved is the number of data files removed.
	DataFilesRemoved uint64 `json:"data_files_removed"`
	// TransactionFilesRemoved is the number of transaction files removed.
	TransactionFilesRemoved uint64 `json:"transaction_files_removed"`
	// IndexFilesRemoved is the number of index files removed.
	IndexFilesRemoved uint64 `json:"index_files_removed"`
	// DeletionFilesRemoved is the number of deletion files removed.
	DeletionFilesRemoved uint64 `json:"deletion_files_removed"`
}

// cleanupConfig mirrors the options_json contract of
// lance_dataset_cleanup_old_versions.
type cleanupConfig struct {
	OlderThanSeconds         int64 `json:"older_than_seconds"`
	DeleteUnverified         *bool `json:"delete_unverified,omitempty"`
	ErrorIfTaggedOldVersions *bool `json:"error_if_tagged_old_versions,omitempty"`
}

// CleanupOption configures CleanupOldVersions.
type CleanupOption func(*cleanupConfig)

// WithDeleteUnverified controls whether files not referenced by any manifest
// are deleted too. Such files may belong to an in-progress transaction, so
// only enable this when no other write is running (default false).
func WithDeleteUnverified(deleteUnverified bool) CleanupOption {
	return func(cfg *cleanupConfig) { cfg.DeleteUnverified = &deleteUnverified }
}

// WithErrorIfTaggedOldVersions controls whether the cleanup fails when an
// old version that would be removed is still referenced by a tag (default
// true in the engine). Set to false to silently keep tagged versions.
func WithErrorIfTaggedOldVersions(errorIfTagged bool) CleanupOption {
	return func(cfg *cleanupConfig) { cfg.ErrorIfTaggedOldVersions = &errorIfTagged }
}

// CompactFiles compacts small data files into larger ones and commits the
// result as a new version of the dataset. The handle is left at the new
// version.
func (d *Dataset) CompactFiles(ctx context.Context, opts CompactionOptions) (CompactionMetrics, error) {
	cOpts, freeOpts, err := marshalOptions(opts.toJSON())
	if err != nil {
		return CompactionMetrics{}, err
	}
	defer freeOpts()

	return datasetOp(ctx, d, "Dataset.CompactFiles", "compact files",
		func(ctx context.Context, ptr *C.LanceDataset) (CompactionMetrics, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_compact_files(ptr, cOpts, &cJSON)
			}); err != nil {
				return CompactionMetrics{}, err
			}
			defer C.lance_string_free(cJSON)
			var metrics CompactionMetrics
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &metrics); err != nil {
				return CompactionMetrics{}, fmt.Errorf("decode compaction metrics: %w", err)
			}
			return metrics, nil
		})
}

// CleanupOldVersions removes versions older than olderThan (and files unique
// to them) from storage. Removed versions can no longer be checked out or
// restored. The current version is never removed.
func (d *Dataset) CleanupOldVersions(ctx context.Context, olderThan time.Duration, opts ...CleanupOption) (RemovalStats, error) {
	cfg := cleanupConfig{OlderThanSeconds: int64(olderThan / time.Second)}
	for _, opt := range opts {
		opt(&cfg)
	}
	cOpts, freeOpts, err := marshalOptions(&cfg)
	if err != nil {
		return RemovalStats{}, err
	}
	defer freeOpts()

	return datasetOp(ctx, d, "Dataset.CleanupOldVersions", "cleanup old versions",
		func(ctx context.Context, ptr *C.LanceDataset) (RemovalStats, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_cleanup_old_versions(ptr, cOpts, &cJSON)
			}); err != nil {
				return RemovalStats{}, err
			}
			defer C.lance_string_free(cJSON)
			var stats RemovalStats
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &stats); err != nil {
				return RemovalStats{}, fmt.Errorf("decode removal stats: %w", err)
			}
			return stats, nil
		})
}
