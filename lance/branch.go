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
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"go.opentelemetry.io/otel/attribute"
)

// WithBranch opens the dataset checked out on the named branch (at its
// latest version, or at a specific version when combined with WithVersion).
func WithBranch(name string) OpenOption {
	return func(cfg *openConfig) {
		cfg.Branch = name
		cfg.branchSet = true
	}
}

// BranchInfo describes a branch of a dataset.
type BranchInfo struct {
	// ParentBranch is the branch this one was created from, or "" for the
	// main branch.
	ParentBranch string `json:"parentBranch"`
	// ParentVersion is the version the branch was created at.
	ParentVersion uint64 `json:"parentVersion"`
	// CreatedAtUnix is the branch creation time as a Unix timestamp.
	CreatedAtUnix uint64 `json:"createAt"`
	// ManifestSize is the size in bytes of the branch head's manifest.
	ManifestSize uint64 `json:"manifestSize"`
	// Metadata holds key-value pairs attached to the branch.
	Metadata map[string]string `json:"metadata"`
}

// marshalOptionalRef renders ref as a JSON C string, mapping the zero Ref
// to NULL (which the native side resolves to the currently checked-out
// version). The caller must free the result.
func marshalOptionalRef(ref Ref) (*C.char, func(), error) {
	if ref.version == nil && ref.tag == "" {
		return nil, func() {}, nil
	}
	return marshalRef(ref)
}

// CreateBranch creates a branch named name pointing at the referenced
// version and returns a NEW Dataset handle checked out on the branch. The
// receiver stays on its current branch. The caller must Close both handles
// independently.
func (d *Dataset) CreateBranch(ctx context.Context, name string, ref Ref) (*Dataset, error) {
	return datasetOp(ctx, d, "Dataset.CreateBranch", "create branch",
		func(ctx context.Context, ptr *C.LanceDataset) (*Dataset, error) {
			cName, freeName := cString(name)
			defer freeName()
			cRef, freeRef, err := marshalOptionalRef(ref)
			if err != nil {
				return nil, err
			}
			defer freeRef()

			var out *C.LanceDataset
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_create_branch(ptr, cName, cRef, &out)
			}); err != nil {
				return nil, err
			}
			return newDataset(out, d.obs()), nil
		}, attribute.String("lance.branch", name))
}

// CheckoutBranch returns a NEW Dataset handle checked out at the latest
// version of the named branch. The receiver is left unchanged. The caller
// must Close both handles independently.
func (d *Dataset) CheckoutBranch(ctx context.Context, name string) (*Dataset, error) {
	return datasetOp(ctx, d, "Dataset.CheckoutBranch", "checkout branch",
		func(ctx context.Context, ptr *C.LanceDataset) (*Dataset, error) {
			cName, freeName := cString(name)
			defer freeName()

			var out *C.LanceDataset
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_checkout_branch(ptr, cName, &out)
			}); err != nil {
				return nil, err
			}
			return newDataset(out, d.obs()), nil
		}, attribute.String("lance.branch", name))
}

// DeleteBranch deletes the named branch. With force set, the branch's files
// are removed even if its metadata is missing (zombie-branch cleanup).
func (d *Dataset) DeleteBranch(ctx context.Context, name string, force bool) error {
	return datasetDo(ctx, d, "Dataset.DeleteBranch", "delete branch",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cName, freeName := cString(name)
			defer freeName()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_delete_branch(ptr, cName, C.bool(force))
			})
		}, attribute.String("lance.branch", name))
}

// ListBranches returns all branches of the dataset, keyed by branch name.
func (d *Dataset) ListBranches(ctx context.Context) (map[string]BranchInfo, error) {
	return datasetOp(ctx, d, "Dataset.ListBranches", "list branches",
		func(ctx context.Context, ptr *C.LanceDataset) (map[string]BranchInfo, error) {
			var branches map[string]BranchInfo
			err := getJSON(ctx, ptr, "list branches", &branches, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_list_branches(ptr, cJSON)
			})
			return branches, err
		})
}

// clone factors ShallowClone / DeepClone over datasetOp. name is the span
// name, what the human-readable verb (interpolated with targetURI to match
// the pre-migration "lance: <what> <uri>: %w" error text).
func (d *Dataset) clone(ctx context.Context, name, what, targetURI string, ref Ref, storageOptions map[string]string, fn func(ptr *C.LanceDataset, cURI, cRef *C.char, kv **C.char, out **C.LanceDataset) C.int32_t) (*Dataset, error) {
	return datasetOp(ctx, d, name, fmt.Sprintf("%s %q", what, targetURI),
		func(ctx context.Context, ptr *C.LanceDataset) (*Dataset, error) {
			cURI, freeURI := cString(targetURI)
			defer freeURI()
			cRef, freeRef, err := marshalOptionalRef(ref)
			if err != nil {
				return nil, err
			}
			defer freeRef()
			kv, freeKV := cStorageKV(storageOptions)
			defer freeKV()

			var out *C.LanceDataset
			if err := ffiCall(ctx, func() C.int32_t {
				return fn(ptr, cURI, cRef, kv, &out)
			}); err != nil {
				return nil, err
			}
			return newDataset(out, d.obs()), nil
		}, datasetURIAttribute(targetURI))
}

// ShallowClone clones the referenced version (or the currently checked-out
// version for the zero Ref) into a new dataset at targetURI without copying
// data files. The clone references the source dataset's files. Returns a
// handle to the clone, which the caller must Close. storageOptions
// configures the target's object store (nil for none).
func (d *Dataset) ShallowClone(ctx context.Context, targetURI string, ref Ref, storageOptions map[string]string) (*Dataset, error) {
	return d.clone(ctx, "Dataset.ShallowClone", "shallow clone", targetURI, ref, storageOptions,
		func(ptr *C.LanceDataset, cURI, cRef *C.char, kv **C.char, out **C.LanceDataset) C.int32_t {
			return C.lance_dataset_shallow_clone(ptr, cURI, cRef, kv, out)
		})
}

// DeepClone clones the referenced version (or the currently checked-out
// version for the zero Ref) into a fully independent dataset at targetURI,
// copying all data files. Fails if a dataset already exists at the target.
// Returns a handle to the clone, which the caller must Close.
func (d *Dataset) DeepClone(ctx context.Context, targetURI string, ref Ref, storageOptions map[string]string) (*Dataset, error) {
	return d.clone(ctx, "Dataset.DeepClone", "deep clone", targetURI, ref, storageOptions,
		func(ptr *C.LanceDataset, cURI, cRef *C.char, kv **C.char, out **C.LanceDataset) C.int32_t {
			return C.lance_dataset_deep_clone(ptr, cURI, cRef, kv, out)
		})
}

// Append appends all record batches from rdr to the dataset this handle is
// checked out on, committing a new version and advancing the handle to it.
// When the handle is on a branch (e.g. from CheckoutBranch), the append
// commits to that branch.
//
// The reader's batches are exported across the Arrow C Data Interface, so
// their buffers must live outside the Go heap (allocate with a C-backed
// allocator returned by Allocator, enforced under GOEXPERIMENT=cgocheck2).
func (d *Dataset) Append(ctx context.Context, rdr array.RecordReader) (err error) {
	ctx, end := d.obs().start(ctx, "Dataset.Append")
	defer func() { end(&err) }()
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return err
	}
	// The stream struct must be zero-initialized (Go zeroes it). The native
	// side always takes ownership of the exported stream, even on error.
	var stream C.struct_ArrowArrayStream
	cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&stream)))
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_append(ptr, &stream)
	}); err != nil {
		return fmt.Errorf("lance: append: %w", err)
	}
	return nil
}

// TagEntry is one element of Tags.ListOrdered: a tag name plus the tag's
// details.
type TagEntry struct {
	// Name is the tag name.
	Name string `json:"name"`
	TagInfo
}

// ListOrdered returns all tags ordered by the version they point at,
// newest first (or oldest first with ascending set). Ties are broken by
// tag name.
func (t *Tags) ListOrdered(ctx context.Context, ascending bool) ([]TagEntry, error) {
	return datasetOp(ctx, t.ds, "Tags.ListOrdered", "list tags ordered",
		func(ctx context.Context, ptr *C.LanceDataset) ([]TagEntry, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_tag_list_ordered(ptr, C.bool(ascending), &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var tags []TagEntry
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &tags); err != nil {
				return nil, fmt.Errorf("decode ordered tags: %w", err)
			}
			return tags, nil
		})
}

// ReplaceMetadata replaces the metadata key-value map attached to an
// existing tag.
func (t *Tags) ReplaceMetadata(ctx context.Context, name string, metadata map[string]string) error {
	return datasetDo(ctx, t.ds, "Tags.ReplaceMetadata", "replace tag metadata",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cName, freeName := cString(name)
			defer freeName()
			if metadata == nil {
				metadata = map[string]string{}
			}
			// marshalOptions (dataset.go) returns an already-prefixed
			// "lance: marshal options: %w" error, which would double-prefix
			// if returned from inside this fn, so marshal inline instead.
			data, err := json.Marshal(metadata)
			if err != nil {
				return fmt.Errorf("marshal metadata: %w", err)
			}
			cMeta, freeMeta := cString(string(data))
			defer freeMeta()
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_tag_replace_metadata(ptr, cName, cMeta)
			})
		}, attribute.String("lance.tag", name))
}
