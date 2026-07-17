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

	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/otel/attribute"
)

// IndexMetadata is an uncommitted index segment produced by
// CreateIndexUncommitted or MergeIndexSegments. Like Transaction, it holds a
// lossless protobuf encoding (used to merge/commit the segment, preserving
// index details a JSON view would drop) plus a JSON summary for inspection.
type IndexMetadata struct {
	pb   []byte
	view IndexMetadataView

	// Raw is the full JSON summary of the segment.
	Raw json.RawMessage
}

// IndexMetadataView is a typed summary of an index segment.
type IndexMetadataView struct {
	// Name is the index name.
	Name string `json:"name"`
	// UUID is the segment's build UUID.
	UUID string `json:"uuid"`
	// Fields lists the field ids the index covers.
	Fields []int32 `json:"fields"`
	// DatasetVersion is the dataset version the segment was built against.
	DatasetVersion uint64 `json:"dataset_version"`
	// IndexVersion is the on-disk index format version.
	IndexVersion int32 `json:"index_version"`
	// CreatedAt is when the segment was created, RFC3339, if reported.
	CreatedAt *string `json:"created_at"`
}

// Name is the index name.
func (m *IndexMetadata) Name() string { return m.view.Name }

// UUID is the segment's build UUID.
func (m *IndexMetadata) UUID() string { return m.view.UUID }

// View returns a copy of the typed summary of the segment.
func (m *IndexMetadata) View() IndexMetadataView {
	view := m.view
	view.Fields = append([]int32(nil), m.view.Fields...)
	if m.view.CreatedAt != nil {
		createdAt := *m.view.CreatedAt
		view.CreatedAt = &createdAt
	}
	return view
}

// Bytes returns a copy of the opaque, lossless protobuf encoding of the
// segment.
func (m *IndexMetadata) Bytes() []byte { return append([]byte(nil), m.pb...) }

func newIndexMetadata(pb []byte, viewJSON []byte) (*IndexMetadata, error) {
	m := &IndexMetadata{pb: pb, Raw: append(json.RawMessage(nil), viewJSON...)}
	if err := json.Unmarshal(viewJSON, &m.view); err != nil {
		// Unprefixed: both callers run inside datasetOp fn closures, which
		// apply the single "lance: <verb>:" wrap.
		return nil, fmt.Errorf("decode index metadata view: %w", err)
	}
	return m, nil
}

// createUncommittedConfig mirrors the options_json of
// lance_dataset_create_index_uncommitted.
//
// Fragments is a pointer so the native side can distinguish "not provided"
// (nil pointer, omitted from JSON, None on the Rust side, meaning "whole
// dataset") from "provided but empty" (pointer to an empty slice, serialized
// as [], Some(vec![]) on the Rust side, meaning "restrict to zero fragments").
type createUncommittedConfig struct {
	Train                 *bool             `json:"train,omitempty"`
	Fragments             *[]uint32         `json:"fragments,omitempty"`
	IndexUUID             string            `json:"index_uuid,omitempty"`
	Name                  string            `json:"name,omitempty"`
	TransactionProperties map[string]string `json:"transaction_properties,omitempty"`
	// optErr records an option-construction error (e.g. an ambiguous
	// Fragments call) surfaced by CreateIndexUncommitted. Unexported, so
	// encoding/json never serializes it.
	optErr error
}

// UncommittedOption configures CreateIndexUncommitted.
type UncommittedOption func(*createUncommittedConfig)

// WithTrain controls whether the IVF/quantizer model is trained. Set false when
// supplying shared centroids (via the IndexConfig's VectorOptions.Centroids)
// so every distributed segment stays comparable.
func WithTrain(train bool) UncommittedOption {
	return func(c *createUncommittedConfig) { c.Train = &train }
}

// WithFragments restricts the build to the given fragment ids (a worker's
// slice).
//
// A variadic cannot tell "no fragments" from "not called", so calling
// WithFragments with no ids (or spreading a nil slice) is rejected with
// ErrInvalidArgument rather than silently meaning "all fragments". To restrict
// the build to zero fragments (an empty worker assignment), spread an explicit
// empty slice: WithFragments(emptySlice...) where emptySlice is []uint32{}.
func WithFragments(ids ...uint32) UncommittedOption {
	return func(c *createUncommittedConfig) {
		if ids == nil {
			c.optErr = fmt.Errorf("lance: WithFragments requires at least one fragment id, or an explicit empty slice spread with WithFragments(emptySlice...) to restrict to zero fragments: %w", ErrInvalidArgument)
			return
		}
		// Copy into a fresh non-nil slice so an empty selector marshals as []
		// (not null). make(len 0) is non-nil even when ids is empty.
		f := make([]uint32, len(ids))
		copy(f, ids)
		c.Fragments = &f
	}
}

// WithIndexUUID pins the segment's index UUID.
func WithIndexUUID(uuid string) UncommittedOption {
	return func(c *createUncommittedConfig) { c.IndexUUID = uuid }
}

// WithUncommittedName sets the index name (defaults to "<column>_idx").
func WithUncommittedName(name string) UncommittedOption {
	return func(c *createUncommittedConfig) { c.Name = name }
}

// WithUncommittedTransactionProperties attaches properties recorded with the
// eventual commit.
func WithUncommittedTransactionProperties(props map[string]string) UncommittedOption {
	return func(c *createUncommittedConfig) { c.TransactionProperties = props }
}

// CreateIndexUncommitted builds an uncommitted index segment over an optional
// fragment subset and returns it without committing. This is the distributed
// index-build primitive: a driver mints shared centroids (pass them via the
// IndexConfig's VectorOptions.Centroids with WithTrain(false)), each worker builds
// a per-fragment segment, then MergeIndexSegments + CommitIndexSegments finish
// the index.
func (d *Dataset) CreateIndexUncommitted(ctx context.Context, column string, cfg IndexConfig, opts ...UncommittedOption) (*IndexMetadata, error) {
	var o createUncommittedConfig
	for _, opt := range opts {
		opt(&o)
	}
	if o.optErr != nil {
		return nil, o.optErr
	}
	return datasetOp(ctx, d, "Dataset.CreateIndexUncommitted", fmt.Sprintf("create index uncommitted on %q", column),
		func(ctx context.Context, ptr *C.LanceDataset) (*IndexMetadata, error) {
			params, err := cfg.indexParams()
			if err != nil {
				return nil, err
			}
			var paramsJSON string
			if params != nil {
				data, err := json.Marshal(params)
				if err != nil {
					return nil, fmt.Errorf("marshal index params: %w", err)
				}
				paramsJSON = string(data)
			}
			centroids, codebook := cfg.arrowParams()

			cColumns, freeColumns := cStringArray([]string{column})
			defer freeColumns()
			cType, freeType := cString(cfg.indexTypeName())
			defer freeType()
			cParams, freeParams := cString(paramsJSON)
			defer freeParams()
			// marshalOptions (dataset.go) returns an already-prefixed
			// "lance: marshal options: %w" error, which would double-prefix
			// if returned from inside this fn, so marshal inline instead.
			optData, err := json.Marshal(&o)
			if err != nil {
				return nil, fmt.Errorf("marshal options: %w", err)
			}
			cOpts, freeOpts := cString(string(optData))
			defer freeOpts()

			var cCentVec, cCbVec C.struct_ArrowArray
			var cCentSchema, cCbSchema C.struct_ArrowSchema
			centVec, centSchema, freeCent := exportArrayPair(centroids, &cCentVec, &cCentSchema)
			defer freeCent()
			cbVec, cbSchema, freeCb := exportArrayPair(codebook, &cCbVec, &cCbSchema)
			defer freeCb()

			var pbPtr *C.uint8_t
			var pbLen C.size_t
			var viewJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_create_index_uncommitted(ptr, cColumns, cType, cParams, cOpts,
					centVec, centSchema, cbVec, cbSchema, nil, &pbPtr, &pbLen, &viewJSON)
			}); err != nil {
				if pbPtr != nil {
					C.lance_bytes_free(pbPtr, pbLen)
				}
				if viewJSON != nil {
					C.lance_string_free(viewJSON)
				}
				return nil, err
			}
			defer C.lance_string_free(viewJSON)
			pb, err := goBytesFree(pbPtr, pbLen)
			if err != nil {
				return nil, fmt.Errorf("copy index metadata bytes: %w", err)
			}
			return newIndexMetadata(pb, []byte(C.GoString(viewJSON)))
		}, attribute.String("lance.column", column))
}

// MergeIndexSegments merges uncommitted index segments into a single segment.
func (d *Dataset) MergeIndexSegments(ctx context.Context, segments []*IndexMetadata) (*IndexMetadata, error) {
	return datasetOp(ctx, d, "Dataset.MergeIndexSegments", "merge index segments",
		func(ctx context.Context, ptr *C.LanceDataset) (*IndexMetadata, error) {
			if len(segments) == 0 {
				return nil, fmt.Errorf("no segments: %w", ErrInvalidArgument)
			}
			blobs := make([][]byte, len(segments))
			for i, s := range segments {
				blobs[i] = s.pb
			}
			ptrs, lens, free := cByteArrays(blobs)
			defer free()

			var pbPtr *C.uint8_t
			var pbLen C.size_t
			var viewJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_merge_index_segments(ptr, ptrs, lens, C.size_t(len(segments)), &pbPtr, &pbLen, &viewJSON)
			}); err != nil {
				if pbPtr != nil {
					C.lance_bytes_free(pbPtr, pbLen)
				}
				if viewJSON != nil {
					C.lance_string_free(viewJSON)
				}
				return nil, err
			}
			defer C.lance_string_free(viewJSON)
			pb, err := goBytesFree(pbPtr, pbLen)
			if err != nil {
				return nil, fmt.Errorf("copy index metadata bytes: %w", err)
			}
			return newIndexMetadata(pb, []byte(C.GoString(viewJSON)))
		})
}

// CommitIndexSegments commits one or more physical index segments as a single
// logical index named name over column, advancing the dataset handle to the
// new version.
func (d *Dataset) CommitIndexSegments(ctx context.Context, name, column string, segments []*IndexMetadata) error {
	return datasetDo(ctx, d, "Dataset.CommitIndexSegments", fmt.Sprintf("commit index segments %q", name),
		func(ctx context.Context, ptr *C.LanceDataset) error {
			if len(segments) == 0 {
				return fmt.Errorf("no segments: %w", ErrInvalidArgument)
			}
			cName, freeName := cString(name)
			defer freeName()
			cColumn, freeColumn := cString(column)
			defer freeColumn()
			blobs := make([][]byte, len(segments))
			for i, s := range segments {
				blobs[i] = s.pb
			}
			ptrs, lens, free := cByteArrays(blobs)
			defer free()

			var viewJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_commit_index_segments(ptr, cName, cColumn, ptrs, lens, C.size_t(len(segments)), &viewJSON)
			}); err != nil {
				return err
			}
			C.lance_string_free(viewJSON)
			return nil
		}, attribute.String("lance.index_name", name))
}

// ReadIndexPartition streams the rows of one index partition. Set withVector
// to include the stored vectors. The caller must Release the reader.
func (d *Dataset) ReadIndexPartition(ctx context.Context, name string, partition uint64, withVector bool) (reader array.RecordReader, err error) {
	cfg := observedStream(ctx, d.obs(), "Dataset.ReadIndexPartition")
	defer cfg.endOnError(&err)
	ctx = cfg.ctx
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	cName, freeName := cString(name)
	defer freeName()

	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_read_index_partition(ptr, cName, C.uint64_t(partition), C.bool(withVector), &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: read index partition %q: %w", name, err)
	}
	return importRecordReader(&stream, "index partition", cfg)
}

// planCompactionOptionsJSON mirrors the options_json contract of
// lance_dataset_plan_compaction. It is deliberately narrower than
// compactionOptionsJSON (maintenance.go): the planner does not know about
// DeferIndexRemap (a CompactFiles-only knob) and rejects unrecognized
// fields, so that field is never marshaled here.
type planCompactionOptionsJSON struct {
	TargetRowsPerFragment         *uint64  `json:"target_rows_per_fragment,omitempty"`
	MaxRowsPerGroup               *uint64  `json:"max_rows_per_group,omitempty"`
	MaxBytesPerFile               *uint64  `json:"max_bytes_per_file,omitempty"`
	MaterializeDeletions          *bool    `json:"materialize_deletions,omitempty"`
	MaterializeDeletionsThreshold *float32 `json:"materialize_deletions_threshold,omitempty"`
	NumThreads                    *uint    `json:"num_threads,omitempty"`
	BatchSize                     *uint64  `json:"batch_size,omitempty"`
}

// planJSON renders o for lance_dataset_plan_compaction, dropping the
// DeferIndexRemap field the planner does not accept. CompactionOptions is
// defined in maintenance.go (shared with CompactFiles); reused here rather
// than duplicated per the package's typed-options convention.
func planJSON(o CompactionOptions) planCompactionOptionsJSON {
	full := o.toJSON()
	return planCompactionOptionsJSON{
		TargetRowsPerFragment:         full.TargetRowsPerFragment,
		MaxRowsPerGroup:               full.MaxRowsPerGroup,
		MaxBytesPerFile:               full.MaxBytesPerFile,
		MaterializeDeletions:          full.MaterializeDeletions,
		MaterializeDeletionsThreshold: full.MaterializeDeletionsThreshold,
		NumThreads:                    full.NumThreads,
		BatchSize:                     full.BatchSize,
	}
}

// CompactionTask is one group of fragments to compact, a standalone unit of
// work within a CompactionPlan. Its contents (the lance TaskData: the
// fragment list to rewrite) are kept opaque JSON: they mirror the engine's
// internal Fragment representation, which this package does not otherwise
// model. Ship Payload verbatim to a worker for round-trip execution once a
// distributed-execute FFI export exists (see the package notes; execution
// and commit are not yet exposed).
type CompactionTask struct {
	// Payload is the task's opaque JSON contents.
	Payload json.RawMessage
}

// UnmarshalJSON captures the task's raw JSON element verbatim.
func (t *CompactionTask) UnmarshalJSON(b []byte) error {
	t.Payload = append(json.RawMessage(nil), b...)
	return nil
}

// MarshalJSON re-emits Payload verbatim (round-trip support for shipping a
// task to a worker and back).
func (t CompactionTask) MarshalJSON() ([]byte, error) {
	if len(t.Payload) == 0 {
		return []byte("null"), nil
	}
	return t.Payload, nil
}

// CompactionPlan is the result of PlanCompaction: a set of independent
// CompactionTasks, plus the dataset version they were planned against.
type CompactionPlan struct {
	// Tasks is the set of independent compaction units the plan computed.
	Tasks []CompactionTask `json:"tasks"`
	// ReadVersion is the dataset version the plan was computed against.
	ReadVersion uint64 `json:"read_version"`

	// Raw is the plan's full JSON, an escape hatch that also carries
	// "options" (the effective options the planner used, echoing defaults
	// for anything opts left unset).
	Raw json.RawMessage `json:"-"`
}

// UnmarshalJSON captures the plan's raw JSON alongside the typed fields.
func (p *CompactionPlan) UnmarshalJSON(b []byte) error {
	p.Raw = append(json.RawMessage(nil), b...)
	type alias CompactionPlan
	return json.Unmarshal(b, (*alias)(p))
}

// PlanCompaction plans a compaction without executing it and returns the
// plan: an independent CompactionTask per group of fragments to rewrite,
// plus the dataset version the plan was computed against. This is the
// planning half of a distributed compaction (the "driver" step); task
// execution and result commit are not yet exposed (see the package notes).
// opts.DeferIndexRemap is not a planning knob and is ignored (see
// CompactFiles, which does honor it).
func (d *Dataset) PlanCompaction(ctx context.Context, opts CompactionOptions) (CompactionPlan, error) {
	cOpts, freeOpts, err := marshalOptions(planJSON(opts))
	if err != nil {
		return CompactionPlan{}, err
	}
	defer freeOpts()

	return datasetOp(ctx, d, "Dataset.PlanCompaction", "plan compaction",
		func(ctx context.Context, ptr *C.LanceDataset) (CompactionPlan, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_plan_compaction(ptr, cOpts, &cJSON)
			}); err != nil {
				return CompactionPlan{}, err
			}
			defer C.lance_string_free(cJSON)
			var plan CompactionPlan
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &plan); err != nil {
				return CompactionPlan{}, fmt.Errorf("decode compaction plan: %w", err)
			}
			return plan, nil
		})
}
