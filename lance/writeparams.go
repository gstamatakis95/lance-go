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

// extendedWriteConfig holds the extended WriteParams (multi-base, blob,
// auto-cleanup, and related options). It is embedded in writeConfig
// (lance/dataset.go) so its fields are inlined into the options_json
// contract of lance_dataset_write.
type extendedWriteConfig struct {
	EnableV2ManifestPaths         *bool                        `json:"enable_v2_manifest_paths,omitempty"`
	AutoCleanup                   *autoCleanupJSON             `json:"auto_cleanup,omitempty"`
	SkipAutoCleanup               bool                         `json:"skip_auto_cleanup,omitempty"`
	TransactionProperties         map[string]string            `json:"transaction_properties,omitempty"`
	ExternalBlobMode              string                       `json:"external_blob_mode,omitempty"`
	BlobPackFileSizeThreshold     uint64                       `json:"blob_pack_file_size_threshold,omitempty"`
	AllowExternalBlobOutsideBases bool                         `json:"allow_external_blob_outside_bases,omitempty"`
	InitialBases                  []BasePath                   `json:"initial_bases,omitempty"`
	TargetBases                   []uint32                     `json:"target_bases,omitempty"`
	TargetBaseNamesOrPaths        []string                     `json:"target_base_names_or_paths,omitempty"`
	BaseStoreParams               map[string]map[string]string `json:"base_store_params,omitempty"`
}

// autoCleanupJSON mirrors the auto_cleanup object of the write options.
type autoCleanupJSON struct {
	Interval         uint64 `json:"interval"`
	OlderThanSeconds int64  `json:"older_than_seconds"`
}

// BasePath registers an additional storage location (bucket/directory) in a
// dataset manifest, for multi-base datasets.
type BasePath struct {
	// ID uniquely identifies the base within the dataset, must be non-zero.
	ID uint32 `json:"id"`
	// Name is an optional human-readable name the base can be referenced
	// by (e.g. in WithTargetBaseNamesOrPaths).
	Name string `json:"name,omitempty"`
	// IsDatasetRoot marks the base holding the dataset root (as opposed to
	// a data-only base).
	IsDatasetRoot bool `json:"is_dataset_root,omitempty"`
	// Path is the full URI of the base, e.g. "s3://bucket/path" or a local
	// directory.
	Path string `json:"path"`
}

// ExternalBlobMode selects how external blob URIs are handled on write.
type ExternalBlobMode string

const (
	// ExternalBlobReference stores the URI as an external blob reference
	// (the default).
	ExternalBlobReference ExternalBlobMode = "reference"
	// ExternalBlobIngest reads the external bytes during the write and
	// stores them in Lance-managed storage.
	ExternalBlobIngest ExternalBlobMode = "ingest"
)

// WithV2ManifestPaths controls whether a NEW dataset uses the V2 manifest
// naming scheme (constant-time latest-manifest lookups on object storage,
// default true). It has no effect on existing datasets. Use
// Dataset.MigrateManifestPathsV2 to migrate those.
func WithV2ManifestPaths(enable bool) WriteOption {
	return func(cfg *writeConfig) { cfg.EnableV2ManifestPaths = &enable }
}

// WithAutoCleanup makes every interval-th commit of a NEW dataset
// automatically clean up versions older than olderThan. It has no effect on
// existing datasets (set the lance.auto_cleanup.* config keys instead).
// Auto-cleanup adds per-commit latency that grows with the version count.
// Prefer explicit CleanupOldVersions calls when reclaiming space matters.
func WithAutoCleanup(interval uint64, olderThan time.Duration) WriteOption {
	return func(cfg *writeConfig) {
		cfg.AutoCleanup = &autoCleanupJSON{
			Interval:         interval,
			OlderThanSeconds: int64(olderThan / time.Second),
		}
	}
}

// WithSkipAutoCleanup skips auto-cleanup during this write's commit (useful
// for high-frequency writers, or writers without delete permissions).
func WithSkipAutoCleanup(skip bool) WriteOption {
	return func(cfg *writeConfig) { cfg.SkipAutoCleanup = skip }
}

// WithTransactionProperties attaches key-value pairs (commit messages,
// engine information, ...) to the write's transaction. They are persisted
// as part of the transaction and readable via Delta().Transactions.
func WithTransactionProperties(properties map[string]string) WriteOption {
	return func(cfg *writeConfig) { cfg.TransactionProperties = properties }
}

// WithExternalBlobMode selects how external blob URIs are handled
// (ExternalBlobReference, the default, or ExternalBlobIngest).
func WithExternalBlobMode(mode ExternalBlobMode) WriteOption {
	return func(cfg *writeConfig) { cfg.ExternalBlobMode = string(mode) }
}

// WithBlobPackFileSizeThreshold caps the size in bytes of blob v2 pack
// (.blob) sidecar files. When a pack file reaches the threshold a new one
// is started (default 1 GiB).
func WithBlobPackFileSizeThreshold(bytes uint64) WriteOption {
	return func(cfg *writeConfig) { cfg.BlobPackFileSizeThreshold = bytes }
}

// WithAllowExternalBlobOutsideBases permits writing external blob URIs that
// cannot be mapped to any registered non-dataset-root base path (such rows
// are rejected by default).
func WithAllowExternalBlobOutsideBases(allow bool) WriteOption {
	return func(cfg *writeConfig) { cfg.AllowExternalBlobOutsideBases = allow }
}

// WithInitialBases registers additional storage base paths in the manifest
// of a NEW dataset (create/overwrite modes only). Each base must have a
// unique non-zero ID.
func WithInitialBases(bases ...BasePath) WriteOption {
	return func(cfg *writeConfig) { cfg.InitialBases = bases }
}

// WithTargetBases directs the write's new data files to the bases with the
// given IDs (registered via WithInitialBases or Dataset.AddBases).
func WithTargetBases(ids ...uint32) WriteOption {
	return func(cfg *writeConfig) { cfg.TargetBases = ids }
}

// WithTargetBaseNamesOrPaths directs the write's new data files to the
// bases with the given names or paths (resolved to IDs when the write
// executes).
func WithTargetBaseNamesOrPaths(namesOrPaths ...string) WriteOption {
	return func(cfg *writeConfig) { cfg.TargetBaseNamesOrPaths = namesOrPaths }
}

// WithBaseStoreParams sets storage options (credentials, endpoints, ...)
// for a single registered base path, keyed by base name or path. The
// write-level storage options remain the fallback for other bases. Call
// repeatedly for multiple bases.
func WithBaseStoreParams(base string, options map[string]string) WriteOption {
	return func(cfg *writeConfig) {
		if cfg.BaseStoreParams == nil {
			cfg.BaseStoreParams = map[string]map[string]string{}
		}
		cfg.BaseStoreParams[base] = options
	}
}

// AddBases registers additional storage base paths in the dataset manifest,
// committing a new version. The handle moves to that version. The bases can
// then be targeted by writes (WithTargetBases). properties, if non-nil, is
// attached to the transaction.
func (d *Dataset) AddBases(ctx context.Context, bases []BasePath, properties map[string]string) error {
	return datasetDo(ctx, d, "Dataset.AddBases", "add bases",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			// marshalOptions (dataset.go) returns an already-prefixed
			// "lance: marshal options: %w" error, which would double-prefix
			// if returned from inside this fn, so marshal inline instead.
			basesData, err := json.Marshal(bases)
			if err != nil {
				return fmt.Errorf("marshal bases: %w", err)
			}
			cBases, freeBases := cString(string(basesData))
			defer freeBases()

			var cProps *C.char
			if properties != nil {
				propsData, err := json.Marshal(properties)
				if err != nil {
					return fmt.Errorf("marshal properties: %w", err)
				}
				var freeProps func()
				cProps, freeProps = cString(string(propsData))
				defer freeProps()
			}
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_add_bases(ptr, cBases, cProps)
			})
		})
}
