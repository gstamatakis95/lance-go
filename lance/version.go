package lance

/*
#include <stdlib.h>
#include "lance_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// Ref identifies a dataset version, either by version number (VersionRef)
// or by tag name (TagRef). The zero value is invalid.
type Ref struct {
	version *uint64
	tag     string
}

// VersionRef references a dataset version by its version number.
func VersionRef(version uint64) Ref {
	return Ref{version: &version}
}

// TagRef references the dataset version a tag points at.
func TagRef(tag string) Ref {
	return Ref{tag: tag}
}

// refJSON mirrors the ref_json contract of lance_dataset_checkout and the
// tag functions.
type refJSON struct {
	Version *uint64 `json:"version,omitempty"`
	Tag     string  `json:"tag,omitempty"`
}

// marshalRef renders the ref as a JSON C string. The caller must free it.
// Its errors are unprefixed; callers flow them through the datasetOp/
// datasetDo single "lance: <verb>: %w" wrap.
func marshalRef(ref Ref) (*C.char, func(), error) {
	if ref.version == nil && ref.tag == "" {
		return nil, nil, fmt.Errorf("invalid Ref, use VersionRef or TagRef: %w", ErrInvalidArgument)
	}
	data, err := json.Marshal(refJSON{Version: ref.version, Tag: ref.tag})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal ref: %w", err)
	}
	cs, free := cString(string(data))
	return cs, free, nil
}

// TagInfo describes a tag of a dataset.
type TagInfo struct {
	// Version is the version number the tag points at.
	Version uint64 `json:"version"`
	// Branch is the branch the version belongs to, or "" for the main
	// branch.
	Branch string `json:"branch"`
	// ManifestSize is the size in bytes of the tagged version's manifest.
	ManifestSize uint64 `json:"manifestSize"`
	// Metadata holds key-value pairs attached to the tag.
	Metadata map[string]string `json:"metadata"`
}

// Checkout returns a NEW Dataset handle fixed at the referenced version.
// The receiver is left unchanged and stays usable. The caller must Close
// both handles independently. Fails with ErrNotFound if the version or tag
// does not exist.
func (d *Dataset) Checkout(ctx context.Context, ref Ref) (*Dataset, error) {
	ds, err := datasetOp(ctx, d, "Dataset.Checkout", "checkout",
		func(ctx context.Context, ptr *C.LanceDataset) (*Dataset, error) {
			cRef, freeRef, err := marshalRef(ref)
			if err != nil {
				return nil, err
			}
			defer freeRef()

			var out *C.LanceDataset
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_checkout(ptr, cRef, &out)
			}); err != nil {
				return nil, err
			}
			return newDataset(out, d.obs()), nil
		})
	if err != nil && errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w (version or tag does not exist; list with Versions or Tags().List)", err)
	}
	return ds, err
}

// CheckoutLatest moves this handle to the latest committed version of the
// dataset (e.g. after another handle committed new versions or restored an
// old one).
func (d *Dataset) CheckoutLatest(ctx context.Context) error {
	return datasetDo(ctx, d, "Dataset.CheckoutLatest", "checkout latest",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_checkout_latest(ptr)
			})
		})
}

// Restore commits the version currently checked out on this handle as the
// new latest version of the dataset. Typically used on a handle returned by
// Checkout to roll the dataset back. Other handles pick up the restored
// version via CheckoutLatest.
func (d *Dataset) Restore(ctx context.Context) error {
	return datasetDo(ctx, d, "Dataset.Restore", "restore",
		func(ctx context.Context, ptr *C.LanceDataset) error {
			return ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_restore(ptr)
			})
		})
}

// Versions lists all committed versions of the dataset, oldest first.
func (d *Dataset) Versions(ctx context.Context) ([]VersionInfo, error) {
	return datasetOp(ctx, d, "Dataset.Versions", "list versions",
		func(ctx context.Context, ptr *C.LanceDataset) ([]VersionInfo, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_list_versions(ptr, &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var versions []VersionInfo
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &versions); err != nil {
				return nil, fmt.Errorf("decode versions: %w", err)
			}
			return versions, nil
		})
}

// Tags returns the tag manager of the dataset. Tags are named references to
// versions. A tagged version is protected from CleanupOldVersions (unless
// that behavior is disabled with WithErrorIfTaggedOldVersions(false)).
func (d *Dataset) Tags() *Tags {
	return &Tags{ds: d}
}

// Tags manages the tags of a dataset. Obtain one with Dataset.Tags. It is
// only valid while the originating Dataset is open.
type Tags struct {
	ds *Dataset
}

// obs returns the instrumentation handle for these tags, inherited from the
// dataset (nil-safe).
func (t *Tags) obs() *obs { return t.ds.obs() }

// Create creates a new tag pointing at the referenced version. It fails
// with ErrAlreadyExists semantics if the tag already exists.
func (t *Tags) Create(ctx context.Context, name string, ref Ref) error {
	return t.mutate(ctx, "Tags.Create", "create tag", name, &ref,
		func(ptr *C.LanceDataset, cName, cRef *C.char) C.int32_t {
			return C.lance_dataset_tag_create(ptr, cName, cRef)
		})
}

// Update moves an existing tag to the referenced version.
func (t *Tags) Update(ctx context.Context, name string, ref Ref) error {
	return t.mutate(ctx, "Tags.Update", "update tag", name, &ref,
		func(ptr *C.LanceDataset, cName, cRef *C.char) C.int32_t {
			return C.lance_dataset_tag_update(ptr, cName, cRef)
		})
}

// Delete removes a tag. The version it pointed at is unaffected.
func (t *Tags) Delete(ctx context.Context, name string) error {
	return t.mutate(ctx, "Tags.Delete", "delete tag", name, nil,
		func(ptr *C.LanceDataset, cName, _ *C.char) C.int32_t {
			return C.lance_dataset_tag_delete(ptr, cName)
		})
}

// mutate factors the shared plumbing of the tag mutations over datasetDo.
// ref may be nil for operations without a version reference.
func (t *Tags) mutate(ctx context.Context, name, verb, tag string, ref *Ref, fn func(ptr *C.LanceDataset, cName, cRef *C.char) C.int32_t) error {
	return datasetDo(ctx, t.ds, name, verb,
		func(ctx context.Context, ptr *C.LanceDataset) error {
			cName, freeName := cString(tag)
			defer freeName()
			var cRef *C.char
			if ref != nil {
				marshaled, freeRef, err := marshalRef(*ref)
				if err != nil {
					return err
				}
				defer freeRef()
				cRef = marshaled
			}
			return ffiCall(ctx, func() C.int32_t {
				return fn(ptr, cName, cRef)
			})
		}, attribute.String("lance.tag", tag))
}

// List returns all tags of the dataset, keyed by tag name.
func (t *Tags) List(ctx context.Context) (map[string]TagInfo, error) {
	return datasetOp(ctx, t.ds, "Tags.List", "list tags",
		func(ctx context.Context, ptr *C.LanceDataset) (map[string]TagInfo, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_tag_list(ptr, &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var tags map[string]TagInfo
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &tags); err != nil {
				return nil, fmt.Errorf("decode tags: %w", err)
			}
			return tags, nil
		})
}

// TagContents is the full detail of a single tag, as returned by Tags.Get
// (the same per-tag shape List returns for every tag, plus timestamps).
type TagContents struct {
	// Version is the version number the tag points at.
	Version uint64 `json:"version"`
	// Branch is the branch the version belongs to, or "" for the main
	// branch.
	Branch string `json:"branch"`
	// CreatedAt is when the tag was created, RFC3339, if reported.
	CreatedAt string `json:"createdAt,omitempty"`
	// UpdatedAt is when the tag was last moved, RFC3339, if reported.
	UpdatedAt string `json:"updatedAt,omitempty"`
	// ManifestSize is the size in bytes of the tagged version's manifest.
	ManifestSize uint64 `json:"manifestSize"`
	// Metadata holds key-value pairs attached to the tag.
	Metadata map[string]string `json:"metadata"`
}

// Get returns the full contents of a single tag. Fails with ErrNotFound if
// the tag does not exist.
func (t *Tags) Get(ctx context.Context, name string) (TagContents, error) {
	contents, err := datasetOp(ctx, t.ds, "Tags.Get", "get tag contents",
		func(ctx context.Context, ptr *C.LanceDataset) (TagContents, error) {
			cName, freeName := cString(name)
			defer freeName()
			var contents TagContents
			if err := getJSON(ctx, ptr, "tag contents", &contents, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_tag_contents(ptr, cName, cJSON)
			}); err != nil {
				return TagContents{}, err
			}
			return contents, nil
		}, attribute.String("lance.tag", name))
	if err != nil && errors.Is(err, ErrNotFound) {
		return TagContents{}, fmt.Errorf("%w (tag does not exist)", err)
	}
	return contents, err
}

// GetVersion resolves a tag to its version number. Fails with ErrNotFound if
// the tag does not exist.
func (t *Tags) GetVersion(ctx context.Context, name string) (uint64, error) {
	version, err := datasetOp(ctx, t.ds, "Tags.GetVersion", "get tag version",
		func(ctx context.Context, ptr *C.LanceDataset) (uint64, error) {
			cName, freeName := cString(name)
			defer freeName()
			var version C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_tag_get_version(ptr, cName, &version)
			}); err != nil {
				return 0, err
			}
			return uint64(version), nil
		}, attribute.String("lance.tag", name))
	if err != nil && errors.Is(err, ErrNotFound) {
		return 0, fmt.Errorf("%w (tag does not exist)", err)
	}
	return version, err
}

// ManifestLocation describes where a manifest is stored.
type ManifestLocation struct {
	// Version is the dataset version the manifest backs.
	Version uint64 `json:"version"`
	// Path is the object-store path of the manifest file.
	Path string `json:"path"`
	// Size is the manifest file's size in bytes, if known.
	Size *uint64 `json:"size"`
	// NamingScheme is the manifest path naming scheme: "V1" or "V2".
	NamingScheme string `json:"naming_scheme"`
	// ETag is the object-store entity tag of the manifest file, if reported.
	ETag *string `json:"e_tag"`
}

// ManifestLocation returns the location of the manifest backing the
// currently checked-out version.
func (d *Dataset) ManifestLocation(ctx context.Context) (ManifestLocation, error) {
	return datasetOp(ctx, d, "Dataset.ManifestLocation", "manifest location",
		func(ctx context.Context, ptr *C.LanceDataset) (ManifestLocation, error) {
			var loc ManifestLocation
			if err := getJSON(ctx, ptr, "manifest location", &loc, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_manifest_location(ptr, cJSON)
			}); err != nil {
				return ManifestLocation{}, err
			}
			return loc, nil
		})
}
