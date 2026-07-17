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

// This file covers dataset config / metadata management plus miscellaneous
// dataset utilities (truncate, validate, statistics, paths, policy-based
// cleanup).

// getJSON runs fn to fetch a JSON payload from the native side and decodes
// it into out. Called from within a datasetOp fn closure, so it returns raw,
// unprefixed errors; the enclosing datasetOp applies the single
// "lance: <verb>: %w" wrap.
func getJSON(ctx context.Context, ptr *C.LanceDataset, what string, out any, fn func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t) error {
	var cJSON *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return fn(ptr, &cJSON)
	}); err != nil {
		return err
	}
	defer C.lance_string_free(cJSON)
	if err := json.Unmarshal([]byte(C.GoString(cJSON)), out); err != nil {
		return fmt.Errorf("decode %s: %w", what, err)
	}
	return nil
}

// updateMap runs the shared plumbing of the config/metadata/schema-metadata
// update calls: marshal + datasetOp + decode. updates maps keys to their new
// value, or to nil to delete the key.
func (d *Dataset) updateMap(ctx context.Context, name, verb string, updates map[string]*string, replace bool, fn func(ptr *C.LanceDataset, cUpdates *C.char, replace C.bool, out **C.char) C.int32_t) (map[string]string, error) {
	if updates == nil {
		updates = map[string]*string{}
	}
	cUpdates, freeUpdates, err := marshalOptions(updates)
	if err != nil {
		return nil, err
	}
	defer freeUpdates()

	return datasetOp(ctx, d, name, verb,
		func(ctx context.Context, ptr *C.LanceDataset) (map[string]string, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return fn(ptr, cUpdates, C.bool(replace), &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var result map[string]string
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &result); err != nil {
				return nil, fmt.Errorf("decode %s result: %w", verb, err)
			}
			return result, nil
		})
}

// values wraps a plain key-value map as non-deleting updates.
func values(kv map[string]string) map[string]*string {
	updates := make(map[string]*string, len(kv))
	for k, v := range kv {
		updates[k] = &v
	}
	return updates
}

// Config returns the dataset configuration key-value map from the manifest
// of the checked-out version.
func (d *Dataset) Config(ctx context.Context) (map[string]string, error) {
	return datasetOp(ctx, d, "Dataset.Config", "config",
		func(ctx context.Context, ptr *C.LanceDataset) (map[string]string, error) {
			var config map[string]string
			if err := getJSON(ctx, ptr, "config", &config, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_config(ptr, cJSON)
			}); err != nil {
				return nil, err
			}
			return config, nil
		})
}

// StorageOptions returns the object-store options this dataset handle was
// opened with, as given (an empty map when none were given).
func (d *Dataset) StorageOptions(ctx context.Context) (map[string]string, error) {
	return datasetOp(ctx, d, "Dataset.StorageOptions", "storage options",
		func(ctx context.Context, ptr *C.LanceDataset) (map[string]string, error) {
			var opts map[string]string
			if err := getJSON(ctx, ptr, "storage options", &opts, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_storage_options(ptr, cJSON)
			}); err != nil {
				return nil, err
			}
			return opts, nil
		})
}

// UpdateConfig merges updates into the dataset configuration, committing a
// new version, and returns the resulting config map.
func (d *Dataset) UpdateConfig(ctx context.Context, updates map[string]string) (map[string]string, error) {
	return d.updateMap(ctx, "Dataset.UpdateConfig", "update config", values(updates), false,
		func(ptr *C.LanceDataset, cUpdates *C.char, replace C.bool, out **C.char) C.int32_t {
			return C.lance_dataset_update_config(ptr, cUpdates, replace, out)
		})
}

// DeleteConfigKeys removes keys from the dataset configuration, committing
// a new version, via the dedicated lance_dataset_delete_config_keys export.
// Missing keys are ignored.
func (d *Dataset) DeleteConfigKeys(ctx context.Context, keys ...string) error {
	if keys == nil {
		keys = []string{}
	}
	cKeys, freeKeys, err := marshalOptions(keys)
	if err != nil {
		return err
	}
	defer freeKeys()

	return datasetDo(ctx, d, "Dataset.DeleteConfigKeys", "delete config keys",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_delete_config_keys(ptr, cKeys)
			})
		})
}

// Metadata returns the table metadata key-value map from the manifest of
// the checked-out version.
func (d *Dataset) Metadata(ctx context.Context) (map[string]string, error) {
	return datasetOp(ctx, d, "Dataset.Metadata", "metadata",
		func(ctx context.Context, ptr *C.LanceDataset) (map[string]string, error) {
			var metadata map[string]string
			if err := getJSON(ctx, ptr, "metadata", &metadata, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_metadata(ptr, cJSON)
			}); err != nil {
				return nil, err
			}
			return metadata, nil
		})
}

// UpdateMetadata merges updates into the table metadata, committing a new
// version, and returns the resulting metadata map.
func (d *Dataset) UpdateMetadata(ctx context.Context, updates map[string]string) (map[string]string, error) {
	return d.updateMap(ctx, "Dataset.UpdateMetadata", "update metadata", values(updates), false,
		func(ptr *C.LanceDataset, cUpdates *C.char, replace C.bool, out **C.char) C.int32_t {
			return C.lance_dataset_update_metadata(ptr, cUpdates, replace, out)
		})
}

// DeleteMetadataKeys removes keys from the table metadata, committing a new
// version. Missing keys are ignored.
func (d *Dataset) DeleteMetadataKeys(ctx context.Context, keys ...string) error {
	updates := make(map[string]*string, len(keys))
	for _, key := range keys {
		updates[key] = nil
	}
	_, err := d.updateMap(ctx, "Dataset.DeleteMetadataKeys", "delete metadata keys", updates, false,
		func(ptr *C.LanceDataset, cUpdates *C.char, replace C.bool, out **C.char) C.int32_t {
			return C.lance_dataset_update_metadata(ptr, cUpdates, replace, out)
		})
	return err
}

// UpdateSchemaMetadata merges updates into the schema metadata, committing
// a new version, and returns the resulting schema metadata map.
func (d *Dataset) UpdateSchemaMetadata(ctx context.Context, updates map[string]string) (map[string]string, error) {
	return d.updateMap(ctx, "Dataset.UpdateSchemaMetadata", "update schema metadata", values(updates), false,
		func(ptr *C.LanceDataset, cUpdates *C.char, replace C.bool, out **C.char) C.int32_t {
			return C.lance_dataset_update_schema_metadata(ptr, cUpdates, replace, out)
		})
}

// ReplaceSchemaMetadata replaces the entire schema metadata map with
// metadata, committing a new version.
func (d *Dataset) ReplaceSchemaMetadata(ctx context.Context, metadata map[string]string) error {
	_, err := d.updateMap(ctx, "Dataset.ReplaceSchemaMetadata", "replace schema metadata", values(metadata), true,
		func(ptr *C.LanceDataset, cUpdates *C.char, replace C.bool, out **C.char) C.int32_t {
			return C.lance_dataset_update_schema_metadata(ptr, cUpdates, replace, out)
		})
	return err
}

// fieldMetadata factors UpdateFieldMetadata / ReplaceFieldMetadata.
func (d *Dataset) fieldMetadata(ctx context.Context, name, verb, field string, updates map[string]string, replace bool) error {
	cUpdates, freeUpdates, err := marshalOptions(values(updates))
	if err != nil {
		return err
	}
	defer freeUpdates()

	return datasetDo(ctx, d, name, verb,
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cField, freeField := cString(field)
			defer freeField()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_update_field_metadata(ptr, cField, cUpdates, C.bool(replace))
			})
		})
}

// UpdateFieldMetadata merges updates into the metadata of the named field
// (nested fields use dotted paths), committing a new version.
func (d *Dataset) UpdateFieldMetadata(ctx context.Context, field string, updates map[string]string) error {
	return d.fieldMetadata(ctx, "Dataset.UpdateFieldMetadata", "update field metadata", field, updates, false)
}

// ReplaceFieldMetadata replaces the entire metadata map of the named field
// with metadata, committing a new version.
func (d *Dataset) ReplaceFieldMetadata(ctx context.Context, field string, metadata map[string]string) error {
	return d.fieldMetadata(ctx, "Dataset.ReplaceFieldMetadata", "replace field metadata", field, metadata, true)
}

// TruncateTable deletes all rows from the dataset, committing a new
// version. Older versions remain accessible via time travel.
func (d *Dataset) TruncateTable(ctx context.Context) error {
	return datasetDo(ctx, d, "Dataset.TruncateTable", "truncate table",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_truncate(ptr)
			})
		})
}

// CountDeletedRows returns the number of soft-deleted rows still present in
// the dataset's files (reclaimable by compaction).
func (d *Dataset) CountDeletedRows(ctx context.Context) (uint64, error) {
	return datasetOp(ctx, d, "Dataset.CountDeletedRows", "count deleted rows",
		func(ctx context.Context, ptr *C.LanceDataset) (uint64, error) {
			var count C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_count_deleted_rows(ptr, &count)
			}); err != nil {
				return 0, err
			}
			return uint64(count), nil
		})
}

// IsStale reports whether the dataset has a newer committed version than
// the one checked out on this handle (catch up with CheckoutLatest).
func (d *Dataset) IsStale(ctx context.Context) (bool, error) {
	return datasetOp(ctx, d, "Dataset.IsStale", "is stale",
		func(ctx context.Context, ptr *C.LanceDataset) (bool, error) {
			var stale C.bool
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_is_stale(ptr, &stale)
			}); err != nil {
				return false, err
			}
			return bool(stale), nil
		})
}

// HasSuccessorVersion reports whether the immediate successor version's
// manifest exists. This is a fast contiguous-history probe that may return
// false if intermediate manifests were cleaned up. Prefer IsStale for a
// general freshness check.
func (d *Dataset) HasSuccessorVersion(ctx context.Context) (bool, error) {
	return datasetOp(ctx, d, "Dataset.HasSuccessorVersion", "has successor version",
		func(ctx context.Context, ptr *C.LanceDataset) (bool, error) {
			var has C.bool
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_has_successor_version(ptr, &has)
			}); err != nil {
				return false, err
			}
			return bool(has), nil
		})
}

// Validate checks the dataset's internal consistency (unique fragment ids,
// consistent field ids, physical row counts) and returns an error when a
// check fails.
func (d *Dataset) Validate(ctx context.Context) error {
	return datasetDo(ctx, d, "Dataset.Validate", "validate",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_validate(ptr)
			})
		})
}

// MigrateManifestPathsV2 migrates the dataset to the V2 manifest naming
// scheme (constant-time latest-manifest lookups on object storage). The
// handle is re-pointed at the migrated latest version. New datasets already
// use V2 paths unless written with WithV2ManifestPaths(false).
//
// Unlike other operations, ctx cancellation does not abort the migration
// once it has started: it always runs to completion, because aborting
// mid-migration would leave a mix of V1 and V2 manifest names that Lance
// refuses to open.
func (d *Dataset) MigrateManifestPathsV2(ctx context.Context) error {
	return datasetDo(ctx, d, "Dataset.MigrateManifestPathsV2", "migrate manifest paths v2",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_migrate_manifest_paths_v2(ptr)
			})
		})
}

// NumSmallFiles returns the number of data files holding fewer rows than
// maxRowsPerGroup (candidates for CompactFiles).
func (d *Dataset) NumSmallFiles(ctx context.Context, maxRowsPerGroup uint64) (uint64, error) {
	return datasetOp(ctx, d, "Dataset.NumSmallFiles", "num small files",
		func(ctx context.Context, ptr *C.LanceDataset) (uint64, error) {
			var count C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_num_small_files(ptr, C.uint64_t(maxRowsPerGroup), &count)
			}); err != nil {
				return 0, err
			}
			return uint64(count), nil
		})
}

// CacheStats describes the dataset session's cache usage.
type CacheStats struct {
	// CacheSizeBytes is the approximate memory held by the session caches.
	CacheSizeBytes uint64 `json:"cache_size_bytes"`
	// IndexCacheEntryCount is the number of entries in the index cache.
	IndexCacheEntryCount uint64 `json:"index_cache_entry_count"`
	// IndexCacheHitRate is the index cache hit ratio in [0, 1].
	IndexCacheHitRate float32 `json:"index_cache_hit_rate"`
}

// CacheStats returns cache statistics of the dataset's session.
func (d *Dataset) CacheStats(ctx context.Context) (CacheStats, error) {
	return datasetOp(ctx, d, "Dataset.CacheStats", "cache stats",
		func(ctx context.Context, ptr *C.LanceDataset) (CacheStats, error) {
			var stats CacheStats
			if err := getJSON(ctx, ptr, "cache stats", &stats, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_cache_stats(ptr, cJSON)
			}); err != nil {
				return CacheStats{}, err
			}
			return stats, nil
		})
}

// DatasetPaths describes where a dataset stores its files. URI is fully
// qualified, the remaining values are object-store paths without a scheme.
type DatasetPaths struct {
	URI         string `json:"uri"`
	Base        string `json:"base"`
	DataDir     string `json:"data_dir"`
	IndicesDir  string `json:"indices_dir"`
	VersionsDir string `json:"versions_dir"`
	// Branch is the checked-out branch, or "" for the main branch.
	Branch string `json:"branch"`
}

// Paths returns the storage locations of the dataset.
func (d *Dataset) Paths(ctx context.Context) (DatasetPaths, error) {
	return datasetOp(ctx, d, "Dataset.Paths", "paths",
		func(ctx context.Context, ptr *C.LanceDataset) (DatasetPaths, error) {
			var paths DatasetPaths
			if err := getJSON(ctx, ptr, "paths", &paths, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_paths(ptr, cJSON)
			}); err != nil {
				return DatasetPaths{}, err
			}
			return paths, nil
		})
}

// CleanupPolicy selects which old dataset versions CleanupWithPolicy
// removes. The zero value removes nothing besides unreferenced files. Set
// BeforeTimestamp and/or BeforeVersion to select versions.
type CleanupPolicy struct {
	// BeforeTimestamp removes versions committed before this time (zero
	// means no time bound).
	BeforeTimestamp time.Time
	// BeforeVersion removes versions numbered below this version (zero
	// means no version bound).
	BeforeVersion uint64
	// DeleteUnverified also deletes recent unverified (possibly
	// in-progress) files. Only set this when no other writes are running.
	DeleteUnverified bool
	// ErrorIfTaggedOldVersions makes the cleanup fail if a tagged version
	// matches the policy. nil (the zero value) protects tagged versions,
	// the lance default. Set it to false to allow removing tagged versions.
	ErrorIfTaggedOldVersions *bool
	// CleanReferencedBranches also cleans up referenced branches.
	CleanReferencedBranches bool
	// DeleteRateLimit caps delete requests per second (0 = unlimited).
	DeleteRateLimit uint64
}

// cleanupPolicyJSON mirrors the policy_json contract of
// lance_dataset_cleanup_with_policy.
type cleanupPolicyJSON struct {
	BeforeTimestamp          string `json:"before_timestamp,omitempty"`
	BeforeVersion            uint64 `json:"before_version,omitempty"`
	DeleteUnverified         bool   `json:"delete_unverified"`
	ErrorIfTaggedOldVersions *bool  `json:"error_if_tagged_old_versions,omitempty"`
	CleanReferencedBranches  bool   `json:"clean_referenced_branches"`
	DeleteRateLimit          uint64 `json:"delete_rate_limit,omitempty"`
}

// CleanupWithPolicy removes old dataset versions (and files unique to them)
// according to policy, returning removal statistics. Removed versions can no
// longer be checked out or restored. For the common age-based cleanup see
// CleanupOldVersions.
func (d *Dataset) CleanupWithPolicy(ctx context.Context, policy CleanupPolicy) (RemovalStats, error) {
	cfg := cleanupPolicyJSON{
		BeforeVersion:            policy.BeforeVersion,
		DeleteUnverified:         policy.DeleteUnverified,
		ErrorIfTaggedOldVersions: policy.ErrorIfTaggedOldVersions,
		CleanReferencedBranches:  policy.CleanReferencedBranches,
		DeleteRateLimit:          policy.DeleteRateLimit,
	}
	if !policy.BeforeTimestamp.IsZero() {
		cfg.BeforeTimestamp = policy.BeforeTimestamp.UTC().Format(time.RFC3339Nano)
	}
	cPolicy, freePolicy, err := marshalOptions(&cfg)
	if err != nil {
		return RemovalStats{}, err
	}
	defer freePolicy()

	return datasetOp(ctx, d, "Dataset.CleanupWithPolicy", "cleanup with policy",
		func(ctx context.Context, ptr *C.LanceDataset) (RemovalStats, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_cleanup_with_policy(ptr, cPolicy, &cJSON)
			}); err != nil {
				return RemovalStats{}, err
			}
			defer C.lance_string_free(cJSON)
			var stats RemovalStats
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &stats); err != nil {
				return RemovalStats{}, fmt.Errorf("decode cleanup stats: %w", err)
			}
			return stats, nil
		})
}

// FieldStatistics describes the storage of a single field.
type FieldStatistics struct {
	// ID is the field id (matching the dataset schema).
	ID uint32 `json:"id"`
	// BytesOnDisk is the compressed size of the field's data. Zero for
	// datasets written with a data storage version below 2.
	BytesOnDisk uint64 `json:"bytes_on_disk"`
}

// DataStatistics describes the storage of the dataset's data.
type DataStatistics struct {
	// Fields holds per-field statistics.
	Fields []FieldStatistics `json:"fields"`
}

// DataStats computes per-field storage statistics for the dataset.
func (d *Dataset) DataStats(ctx context.Context) (DataStatistics, error) {
	return datasetOp(ctx, d, "Dataset.DataStats", "data stats",
		func(ctx context.Context, ptr *C.LanceDataset) (DataStatistics, error) {
			var stats DataStatistics
			if err := getJSON(ctx, ptr, "data stats", &stats, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_data_stats(ptr, cJSON)
			}); err != nil {
				return DataStatistics{}, err
			}
			return stats, nil
		})
}
