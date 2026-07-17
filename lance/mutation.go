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
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"go.opentelemetry.io/otel/attribute"
)

// Mutations (Delete, Update, MergeInsert) commit a new dataset version and
// update the handle in place, so subsequent reads through the same Dataset
// observe the mutated data without reopening.

// DeleteResult reports the outcome of a Delete.
type DeleteResult struct {
	// NumDeletedRows is the number of rows removed by the predicate.
	NumDeletedRows uint64 `json:"num_deleted_rows"`
}

// UpdateResult reports the outcome of an Update.
type UpdateResult struct {
	// RowsUpdated is the number of rows modified.
	RowsUpdated uint64 `json:"rows_updated"`
}

// UpdateSpec describes an Update operation.
type UpdateSpec struct {
	// Set maps column names to SQL expressions producing the new value,
	// e.g. {"score": "score * 2"}. It must contain at least one entry.
	Set map[string]string
	// Where restricts the update to rows matching this SQL predicate.
	// Empty means "update every row".
	Where string
	// ConflictRetries overrides the number of times the operation is
	// retried on commit contention (nil keeps the Lance default of 10).
	ConflictRetries *uint32
	// RetryTimeout caps the total time spent on retries (zero keeps the
	// Lance default of 30 seconds).
	RetryTimeout time.Duration
}

// MergeStats reports the outcome of a MergeInsert.
type MergeStats struct {
	// NumInsertedRows is the number of source rows inserted.
	NumInsertedRows uint64 `json:"num_inserted_rows"`
	// NumUpdatedRows is the number of target rows updated from the source.
	NumUpdatedRows uint64 `json:"num_updated_rows"`
	// NumDeletedRows is the number of target rows deleted.
	NumDeletedRows uint64 `json:"num_deleted_rows"`
	// NumAttempts is the number of attempts performed (>1 on contention).
	NumAttempts uint32 `json:"num_attempts"`
	// BytesWritten is the total data-file bytes written to storage.
	BytesWritten uint64 `json:"bytes_written"`
	// NumFilesWritten is the number of data files written.
	NumFilesWritten uint64 `json:"num_files_written"`
	// NumSkippedDuplicates is the number of duplicate source rows skipped.
	NumSkippedDuplicates uint64 `json:"num_skipped_duplicates"`
}

// Delete removes all rows matching the SQL predicate and commits a new
// dataset version. The handle is updated in place to track the new version.
func (d *Dataset) Delete(ctx context.Context, predicate string) (res DeleteResult, err error) {
	ctx, end := d.obs().start(ctx, "Dataset.Delete")
	defer func() {
		end(&err, attribute.Int64("lance.rows_deleted", int64(res.NumDeletedRows)))
		d.obs().recordRows(ctx, "Dataset.Delete", "deleted", int64(res.NumDeletedRows))
	}()
	if predicate == "" {
		return DeleteResult{}, fmt.Errorf("lance: delete: predicate must not be empty: %w", ErrInvalidArgument)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return DeleteResult{}, err
	}
	cPredicate, freePredicate := cString(predicate)
	defer freePredicate()
	var cJSON *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_delete(ptr, cPredicate, &cJSON)
	}); err != nil {
		return DeleteResult{}, fmt.Errorf("lance: delete: %w", err)
	}
	defer C.lance_string_free(cJSON)
	if err := json.Unmarshal([]byte(C.GoString(cJSON)), &res); err != nil {
		return DeleteResult{}, fmt.Errorf("lance: decode delete result: %w", err)
	}
	return res, nil
}

// updateOptions mirrors the update_json contract of lance_dataset_update.
type updateOptions struct {
	Set             map[string]string `json:"set"`
	Where           string            `json:"where,omitempty"`
	ConflictRetries *uint32           `json:"conflict_retries,omitempty"`
	RetryTimeoutMS  uint64            `json:"retry_timeout_ms,omitempty"`
}

// Update modifies column values on rows matching spec.Where (or all rows if
// empty) and commits a new dataset version. The handle is updated in place
// to track the new version.
func (d *Dataset) Update(ctx context.Context, spec UpdateSpec) (res UpdateResult, err error) {
	ctx, end := d.obs().start(ctx, "Dataset.Update")
	defer func() {
		end(&err, attribute.Int64("lance.rows_updated", int64(res.RowsUpdated)))
		d.obs().recordRows(ctx, "Dataset.Update", "updated", int64(res.RowsUpdated))
	}()
	if len(spec.Set) == 0 {
		return UpdateResult{}, fmt.Errorf("lance: update: Set must not be empty: %w", ErrInvalidArgument)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return UpdateResult{}, err
	}
	opts := updateOptions{
		Set:             spec.Set,
		Where:           spec.Where,
		ConflictRetries: spec.ConflictRetries,
		RetryTimeoutMS:  uint64(spec.RetryTimeout / time.Millisecond),
	}
	cJSON, freeJSON, err := marshalOptions(&opts)
	if err != nil {
		return UpdateResult{}, err
	}
	defer freeJSON()
	var cResult *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_update(ptr, cJSON, &cResult)
	}); err != nil {
		return UpdateResult{}, fmt.Errorf("lance: update: %w", err)
	}
	defer C.lance_string_free(cResult)
	if err := json.Unmarshal([]byte(C.GoString(cResult)), &res); err != nil {
		return UpdateResult{}, fmt.Errorf("lance: decode update result: %w", err)
	}
	return res, nil
}

// mergeInsertConfig mirrors the options_json contract of
// lance_dataset_merge_insert. The when_* fields are either a plain string
// ("update_all", ...) or a one-key object ({"update_if": expr}).
type mergeInsertConfig struct {
	On                     []string `json:"on"`
	WhenMatched            any      `json:"when_matched,omitempty"`
	WhenNotMatched         string   `json:"when_not_matched,omitempty"`
	WhenNotMatchedBySource any      `json:"when_not_matched_by_source,omitempty"`
	ConflictRetries        *uint32  `json:"conflict_retries,omitempty"`
	RetryTimeoutMS         uint64   `json:"retry_timeout_ms,omitempty"`
	SourceDedupeBehavior   string   `json:"source_dedupe_behavior,omitempty"`
	UseIndex               *bool    `json:"use_index,omitempty"`
	CommitRetries          *uint32  `json:"commit_retries,omitempty"`
	SkipAutoCleanup        *bool    `json:"skip_auto_cleanup,omitempty"`
}

// SourceDedupeBehavior controls how a merge-insert handles duplicate source
// rows that match the same target row on the join key.
type SourceDedupeBehavior string

const (
	// SourceDedupeFail fails the operation when the source contains duplicate
	// keys that match the same target row. This is Lance's default.
	SourceDedupeFail SourceDedupeBehavior = "fail"
	// SourceDedupeFirstSeen keeps the first-encountered source row for a given
	// key and skips subsequent duplicates (counted in
	// MergeStats.NumSkippedDuplicates).
	SourceDedupeFirstSeen SourceDedupeBehavior = "first_seen"
)

// MergeInsertBuilder configures a merge-insert (upsert / find-or-create)
// operation joining source rows against the dataset on the given key
// columns. Configure it with the chained When* methods, then call Execute.
//
// The defaults mirror Lance's find-or-create semantics: matched target rows
// are kept as-is (WhenMatchedDoNothing), unmatched source rows are inserted
// (WhenNotMatchedInsertAll), and target rows missing from the source are
// kept (WhenNotMatchedBySourceKeep), with 10 conflict retries over at most
// 30 seconds.
//
// A MergeInsertBuilder must not be used concurrently while it is being
// configured.
type MergeInsertBuilder struct {
	ds  *Dataset
	cfg mergeInsertConfig
}

// obs returns the instrumentation handle for this builder, inherited from its
// dataset (nil-safe).
func (b *MergeInsertBuilder) obs() *obs { return b.ds.obs() }

// MergeInsert starts building a merge-insert of new source rows into the
// dataset, matching source and target rows on the given key columns.
func (d *Dataset) MergeInsert(on ...string) *MergeInsertBuilder {
	return &MergeInsertBuilder{ds: d, cfg: mergeInsertConfig{On: on}}
}

// WhenMatchedUpdateAll replaces matched target rows with the source row
// (upsert).
func (b *MergeInsertBuilder) WhenMatchedUpdateAll() *MergeInsertBuilder {
	b.cfg.WhenMatched = "update_all"
	return b
}

// WhenMatchedUpdateIf replaces matched target rows with the source row only
// where expr evaluates to true. The expression references the joined row via
// "target." and "source." qualifiers, e.g. "source.score > target.score".
func (b *MergeInsertBuilder) WhenMatchedUpdateIf(expr string) *MergeInsertBuilder {
	b.cfg.WhenMatched = map[string]string{"update_if": expr}
	return b
}

// WhenMatchedDelete removes matched target rows (bulk delete by key).
func (b *MergeInsertBuilder) WhenMatchedDelete() *MergeInsertBuilder {
	b.cfg.WhenMatched = "delete"
	return b
}

// WhenMatchedFail fails the operation if any source row matches an existing
// target row.
func (b *MergeInsertBuilder) WhenMatchedFail() *MergeInsertBuilder {
	b.cfg.WhenMatched = "fail"
	return b
}

// WhenMatchedDoNothing keeps matched target rows unchanged (the default,
// find-or-create).
func (b *MergeInsertBuilder) WhenMatchedDoNothing() *MergeInsertBuilder {
	b.cfg.WhenMatched = "do_nothing"
	return b
}

// WhenNotMatchedInsertAll inserts source rows that have no matching target
// row (the default).
func (b *MergeInsertBuilder) WhenNotMatchedInsertAll() *MergeInsertBuilder {
	b.cfg.WhenNotMatched = "insert_all"
	return b
}

// WhenNotMatchedDoNothing ignores source rows that have no matching target
// row.
func (b *MergeInsertBuilder) WhenNotMatchedDoNothing() *MergeInsertBuilder {
	b.cfg.WhenNotMatched = "do_nothing"
	return b
}

// WhenNotMatchedBySourceKeep keeps target rows that have no matching source
// row (the default).
func (b *MergeInsertBuilder) WhenNotMatchedBySourceKeep() *MergeInsertBuilder {
	b.cfg.WhenNotMatchedBySource = "keep"
	return b
}

// WhenNotMatchedBySourceDelete removes target rows that have no matching
// source row.
func (b *MergeInsertBuilder) WhenNotMatchedBySourceDelete() *MergeInsertBuilder {
	b.cfg.WhenNotMatchedBySource = "delete"
	return b
}

// WhenNotMatchedBySourceDeleteIf removes target rows that have no matching
// source row and for which expr (an SQL predicate over the target columns)
// evaluates to true.
func (b *MergeInsertBuilder) WhenNotMatchedBySourceDeleteIf(expr string) *MergeInsertBuilder {
	b.cfg.WhenNotMatchedBySource = map[string]string{"delete_if": expr}
	return b
}

// ConflictRetries overrides the number of times the operation is retried on
// commit contention (the Lance default is 10).
func (b *MergeInsertBuilder) ConflictRetries(n uint32) *MergeInsertBuilder {
	b.cfg.ConflictRetries = &n
	return b
}

// RetryTimeout caps the total time spent on retries (the Lance default is
// 30 seconds).
func (b *MergeInsertBuilder) RetryTimeout(d time.Duration) *MergeInsertBuilder {
	b.cfg.RetryTimeoutMS = uint64(d / time.Millisecond)
	return b
}

// SourceDedupeBehavior controls how duplicate source rows that match the same
// target row on the join key are handled. The Lance default is
// SourceDedupeFail, which errors on such duplicates. SourceDedupeFirstSeen
// keeps the first-seen source row and skips the rest, reporting the skipped
// count in MergeStats.NumSkippedDuplicates.
func (b *MergeInsertBuilder) SourceDedupeBehavior(behavior SourceDedupeBehavior) *MergeInsertBuilder {
	b.cfg.SourceDedupeBehavior = string(behavior)
	return b
}

// UseIndex controls whether a scalar index on the join key is used to find
// matches. The Lance default is true. Passing false forces a full table scan
// even when an index exists (useful for benchmarking or to sidestep a
// suboptimal plan).
func (b *MergeInsertBuilder) UseIndex(use bool) *MergeInsertBuilder {
	b.cfg.UseIndex = &use
	return b
}

// CommitRetries sets the number of inner commit retries for manifest version
// conflicts (the Lance default is 20). This is distinct from ConflictRetries,
// which retries the whole operation on semantic write contention.
func (b *MergeInsertBuilder) CommitRetries(n uint32) *MergeInsertBuilder {
	b.cfg.CommitRetries = &n
	return b
}

// SkipAutoCleanup controls whether automatic cleanup of old versions during
// the commit is skipped. The Lance default is false. Setting it to true can
// improve throughput for high-frequency writers or writers without delete
// permissions.
func (b *MergeInsertBuilder) SkipAutoCleanup(skip bool) *MergeInsertBuilder {
	b.cfg.SkipAutoCleanup = &skip
	return b
}

// Execute runs the merge-insert with rdr as the source rows and commits a
// new dataset version. The handle is updated in place to track the new
// version. The reader is fully consumed. The caller keeps its own reference
// and should still Release it as usual.
//
// As with Write, the reader's batches cross the Arrow C Data Interface, so
// their buffers must live outside the Go heap (use a C-backed allocator such
// as Allocator).
func (b *MergeInsertBuilder) Execute(ctx context.Context, rdr array.RecordReader) (res MergeStats, err error) {
	ctx, end := b.obs().start(ctx, "MergeInsertBuilder.Execute")
	defer func() {
		end(&err,
			attribute.Int64("lance.rows_inserted", int64(res.NumInsertedRows)),
			attribute.Int64("lance.rows_updated", int64(res.NumUpdatedRows)),
			attribute.Int64("lance.rows_deleted", int64(res.NumDeletedRows)),
			attribute.Int64("lance.bytes_written", int64(res.BytesWritten)),
		)
		b.obs().recordRows(ctx, "MergeInsertBuilder.Execute", "inserted", int64(res.NumInsertedRows))
		b.obs().recordRows(ctx, "MergeInsertBuilder.Execute", "updated", int64(res.NumUpdatedRows))
		b.obs().recordRows(ctx, "MergeInsertBuilder.Execute", "deleted", int64(res.NumDeletedRows))
		b.obs().recordBytes(ctx, "MergeInsertBuilder.Execute", int64(res.BytesWritten))
	}()
	b.ds.mu.RLock()
	defer b.ds.mu.RUnlock()
	ptr, err := b.ds.checkOpen(ctx)
	if err != nil {
		return MergeStats{}, err
	}
	cJSON, freeJSON, err := marshalOptions(&b.cfg)
	if err != nil {
		return MergeStats{}, err
	}
	defer freeJSON()

	// The stream struct must be zero-initialized (Go zeroes it). The native
	// side always takes ownership of the exported stream, even on error, so
	// the exported reader is released exactly once.
	var stream C.struct_ArrowArrayStream
	cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&stream)))

	var cStats *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_merge_insert(ptr, cJSON, &stream, &cStats)
	}); err != nil {
		return MergeStats{}, fmt.Errorf("lance: merge insert: %w", err)
	}
	defer C.lance_string_free(cStats)
	if err := json.Unmarshal([]byte(C.GoString(cStats)), &res); err != nil {
		return MergeStats{}, fmt.Errorf("lance: decode merge stats: %w", err)
	}
	return res, nil
}
