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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// FragmentInfo summarizes one fragment of a dataset (see Dataset.Fragments).
type FragmentInfo struct {
	// ID is the fragment id.
	ID uint32 `json:"id"`
	// NumRows is the number of live (non-deleted) rows in the fragment.
	NumRows uint64 `json:"num_rows"`
	// PhysicalRows is the number of rows stored before deletions.
	PhysicalRows uint64 `json:"physical_rows"`
	// NumDeletions is the number of rows deleted from the fragment.
	NumDeletions uint64 `json:"num_deletions"`
	// NumDataFiles is the number of data files backing the fragment.
	NumDataFiles uint32 `json:"num_data_files"`
	// DataFiles lists the fragment's data file paths, relative to the
	// dataset root.
	DataFiles []string `json:"data_files"`
}

// DataFileInfo describes one data file of a fragment (see FragmentMetadata).
type DataFileInfo struct {
	// Path is the data file path relative to the dataset root.
	Path string `json:"path"`
	// Fields are the field ids stored in this file.
	Fields []int32 `json:"fields"`
	// FileMajorVersion is the Lance file format major version.
	FileMajorVersion uint32 `json:"file_major_version"`
	// FileMinorVersion is the Lance file format minor version.
	FileMinorVersion uint32 `json:"file_minor_version"`
}

// DeletionFileInfo describes a fragment's deletion file, if any.
type DeletionFileInfo struct {
	// ReadVersion is the dataset version the deletion file was written at.
	ReadVersion uint64 `json:"read_version"`
	// ID is the deletion file id.
	ID uint64 `json:"id"`
	// NumDeletedRows is the number of deleted rows, if recorded.
	NumDeletedRows *uint64 `json:"num_deleted_rows"`
}

// FragmentMetadata is the decoded metadata of a fragment (see
// Fragment.Metadata). Fields not modeled here are ignored on decode.
type FragmentMetadata struct {
	// ID is the fragment id.
	ID uint64 `json:"id"`
	// PhysicalRows is the stored row count, if recorded in the manifest.
	PhysicalRows *uint64 `json:"physical_rows"`
	// Files lists the fragment's data files.
	Files []DataFileInfo `json:"files"`
	// DeletionFile describes the fragment's deletion file, if any.
	DeletionFile *DeletionFileInfo `json:"deletion_file"`
}

// Fragment is a handle to a single fragment of a dataset. It is self-contained
// (it snapshots the dataset), so it stays valid after the originating Dataset
// is closed. Close must be called to release native resources. It is
// idempotent and safe for concurrent use.
type Fragment struct {
	mu  sync.Mutex
	ptr *C.LanceFragment
	// withObs is inherited from the originating Dataset (nil-safe).
	withObs *obs
	// cleanup is a leak safety net: if the caller drops every reference to
	// this Fragment without calling Close, the runtime eventually releases
	// the native handle anyway. Close stops it so the native pointer is never
	// closed twice.
	cleanup runtime.Cleanup
}

// obs returns the instrumentation handle for this fragment (nil-safe).
func (f *Fragment) obs() *obs { return f.withObs }

// newFragment wraps a native fragment pointer, attaching the observability
// handle o. All &Fragment construction in this package goes through here so
// every handle gets the GC leak safety net.
func newFragment(ptr *C.LanceFragment, o *obs) *Fragment {
	f := &Fragment{ptr: ptr, withObs: o}
	// The cleanup func must capture nothing but its argument: closing over f
	// or ptr would keep the Fragment reachable and defeat collection.
	f.cleanup = runtime.AddCleanup(f, func(p *C.LanceFragment) { C.lance_fragment_close(p) }, ptr)
	return f
}

// Fragments returns metadata for every fragment of the dataset. The fragments
// are returned in dataset order, and the sum of their NumRows equals the dataset's
// live row count.
func (d *Dataset) Fragments(ctx context.Context) ([]FragmentInfo, error) {
	return datasetOp(ctx, d, "Dataset.Fragments", "fragments",
		func(ctx context.Context, ptr *C.LanceDataset) ([]FragmentInfo, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_fragments(ptr, &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var infos []FragmentInfo
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &infos); err != nil {
				return nil, fmt.Errorf("decode fragments: %w", err)
			}
			return infos, nil
		})
}

// Fragment opens the fragment with the given id as a handle. It fails with
// ErrNotFound if no fragment has that id. The caller must Close the result.
func (d *Dataset) Fragment(ctx context.Context, id uint32) (*Fragment, error) {
	return datasetOp(ctx, d, "Dataset.Fragment", fmt.Sprintf("get fragment %d", id),
		func(ctx context.Context, ptr *C.LanceDataset) (*Fragment, error) {
			var fPtr *C.LanceFragment
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_get_fragment(ptr, C.uint32_t(id), &fPtr)
			}); err != nil {
				return nil, err
			}
			return newFragment(fPtr, d.obs()), nil
		})
}

// Close releases the native fragment handle. It is idempotent. Streams from a
// fragment Scan remain valid after Close.
func (f *Fragment) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ptr != nil {
		C.lance_fragment_close(f.ptr)
		f.cleanup.Stop()
		f.ptr = nil
	}
	return nil
}

// checkOpen returns the native handle, or an error if the fragment is closed
// or ctx is done. Callers must hold f.mu.
func (f *Fragment) checkOpen(ctx context.Context) (*C.LanceFragment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.ptr == nil {
		return nil, fmt.Errorf("lance: fragment is closed: %w", ErrInvalidArgument)
	}
	return f.ptr, nil
}

// CountRows returns the number of live rows in the fragment. A non-empty
// filter restricts the count to rows matching the SQL predicate.
func (f *Fragment) CountRows(ctx context.Context, filter string) (uint64, error) {
	return fragmentOp(ctx, f, "Fragment.CountRows", "fragment count rows",
		func(ctx context.Context, ptr *C.LanceFragment) (uint64, error) {
			cFilter, freeFilter := cString(filter)
			defer freeFilter()
			var count C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_fragment_count_rows(ptr, cFilter, &count)
			}); err != nil {
				return 0, err
			}
			return uint64(count), nil
		})
}

// CountDeletions returns the number of rows deleted from the fragment.
func (f *Fragment) CountDeletions(ctx context.Context) (uint64, error) {
	return fragmentOp(ctx, f, "Fragment.CountDeletions", "fragment count deletions",
		func(ctx context.Context, ptr *C.LanceFragment) (uint64, error) {
			var count C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_fragment_count_deletions(ptr, &count)
			}); err != nil {
				return 0, err
			}
			return uint64(count), nil
		})
}

// PhysicalRows returns the number of physical rows stored in the fragment
// (before deletions).
func (f *Fragment) PhysicalRows(ctx context.Context) (uint64, error) {
	return fragmentOp(ctx, f, "Fragment.PhysicalRows", "fragment physical rows",
		func(ctx context.Context, ptr *C.LanceFragment) (uint64, error) {
			var count C.uint64_t
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_fragment_physical_rows(ptr, &count)
			}); err != nil {
				return 0, err
			}
			return uint64(count), nil
		})
}

// Metadata returns the fragment's decoded metadata.
func (f *Fragment) Metadata(ctx context.Context) (FragmentMetadata, error) {
	return fragmentOp(ctx, f, "Fragment.Metadata", "fragment metadata",
		func(ctx context.Context, ptr *C.LanceFragment) (FragmentMetadata, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_fragment_metadata(ptr, &cJSON)
			}); err != nil {
				return FragmentMetadata{}, err
			}
			defer C.lance_string_free(cJSON)
			var meta FragmentMetadata
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &meta); err != nil {
				return FragmentMetadata{}, fmt.Errorf("decode fragment metadata: %w", err)
			}
			return meta, nil
		})
}

// fragmentTakeSpecJSON mirrors the spec_json contract of lance_fragment_take.
type fragmentTakeSpecJSON struct {
	Indices []uint32 `json:"indices"`
	Columns []string `json:"columns,omitempty"`
}

// Take returns the rows at the given in-fragment offsets as a single record
// batch, projected onto columns (all columns when none given). The caller must
// Release the result.
func (f *Fragment) Take(ctx context.Context, indices []uint32, columns ...string) (res arrow.RecordBatch, err error) {
	ctx, end := f.obs().start(ctx, "Fragment.Take")
	defer func() { end(&err) }()
	f.mu.Lock()
	defer f.mu.Unlock()
	ptr, err := f.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	if indices == nil {
		indices = []uint32{}
	}
	cSpec, freeSpec, err := marshalOptions(&fragmentTakeSpecJSON{Indices: indices, Columns: columns})
	if err != nil {
		return nil, err
	}
	defer freeSpec()

	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_fragment_take(ptr, cSpec, &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: fragment take: %w", err)
	}
	reader, err := importRecordReader(&stream, "fragment take")
	if err != nil {
		return nil, err
	}
	return singleBatch(reader, "fragment take")
}

// fragmentScanConfig mirrors the scan_json contract of
// lance_fragment_scan_to_stream.
type fragmentScanConfig struct {
	Columns        []string `json:"columns,omitempty"`
	Filter         string   `json:"filter,omitempty"`
	Limit          *int64   `json:"limit,omitempty"`
	Offset         *int64   `json:"offset,omitempty"`
	WithRowID      bool     `json:"with_row_id,omitempty"`
	WithRowAddress bool     `json:"with_row_address,omitempty"`
	BatchSize      uint64   `json:"batch_size,omitempty"`
	ScanInOrder    *bool    `json:"scan_in_order,omitempty"`
}

// FragmentScanner builds a scan restricted to one fragment. It mirrors the
// subset of Scanner knobs that fragment scans support. Configure it with the
// chained methods, then call the terminal Reader.
type FragmentScanner struct {
	f   *Fragment
	cfg fragmentScanConfig
}

// Scan starts building a scan over the fragment.
func (f *Fragment) Scan() *FragmentScanner {
	return &FragmentScanner{f: f}
}

// obs returns the instrumentation handle for this fragment scanner, inherited
// from its fragment (nil-safe).
func (fs *FragmentScanner) obs() *obs { return fs.f.obs() }

// Columns restricts the scan to the given columns (all columns by default).
func (s *FragmentScanner) Columns(columns ...string) *FragmentScanner {
	s.cfg.Columns = columns
	return s
}

// Filter applies an SQL predicate, e.g. "id < 50".
func (s *FragmentScanner) Filter(filter string) *FragmentScanner {
	s.cfg.Filter = filter
	return s
}

// Limit caps the number of rows returned.
func (s *FragmentScanner) Limit(limit int64) *FragmentScanner {
	s.cfg.Limit = &limit
	return s
}

// Offset skips the first offset rows.
func (s *FragmentScanner) Offset(offset int64) *FragmentScanner {
	s.cfg.Offset = &offset
	return s
}

// WithRowID includes the internal _rowid column in the results.
func (s *FragmentScanner) WithRowID() *FragmentScanner {
	s.cfg.WithRowID = true
	return s
}

// WithRowAddress includes the internal _rowaddr column in the results.
func (s *FragmentScanner) WithRowAddress() *FragmentScanner {
	s.cfg.WithRowAddress = true
	return s
}

// BatchSize caps the number of rows per emitted record batch.
func (s *FragmentScanner) BatchSize(n uint64) *FragmentScanner {
	s.cfg.BatchSize = n
	return s
}

// ScanInOrder controls whether batches are returned in stable row order.
func (s *FragmentScanner) ScanInOrder(ordered bool) *FragmentScanner {
	s.cfg.ScanInOrder = &ordered
	return s
}

// Reader executes the fragment scan and returns a reader over the result
// batches. The reader and every record obtained from it must be Released by
// the caller. The reader stays valid after the Fragment and Dataset are
// closed.
func (s *FragmentScanner) Reader(ctx context.Context) (reader array.RecordReader, err error) {
	cfg := observedStream(ctx, s.obs(), "FragmentScanner.Reader")
	defer cfg.endOnError(&err)
	ctx = cfg.ctx
	s.f.mu.Lock()
	defer s.f.mu.Unlock()
	ptr, err := s.f.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	cJSON, freeJSON, err := marshalOptions(&s.cfg)
	if err != nil {
		return nil, err
	}
	defer freeJSON()

	var stream C.struct_ArrowArrayStream
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_fragment_scan_to_stream(ptr, cJSON, &stream)
	}); err != nil {
		return nil, fmt.Errorf("lance: fragment scan: %w", err)
	}
	return importRecordReader(&stream, "fragment scan", cfg)
}
