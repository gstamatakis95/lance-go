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
)

// Transaction is an uncommitted change to a dataset, produced by
// WriteFragments (or read back with Dataset.ReadTransaction). It carries two
// representations:
//
//   - a lossless protobuf encoding (the exact form Lance persists), used to
//     commit the transaction and to round-trip variants this package does not
//     model, the true escape hatch, held opaquely, and
//   - Raw, a JSON summary of the transaction for inspection, plus typed
//     accessors decoded from it.
//
// Lance's Transaction/Operation types are protobuf-backed (not JSON), so the
// protobuf bytes, not the JSON view, are what survive a commit losslessly.
type Transaction struct {
	pb   []byte
	view transactionView

	// Raw is the full JSON summary of the transaction (escape hatch for
	// fields the typed accessors do not surface).
	Raw json.RawMessage
}

type transactionView struct {
	ReadVersion uint64            `json:"read_version"`
	UUID        string            `json:"uuid"`
	Tag         *string           `json:"tag"`
	Operation   OperationView     `json:"operation"`
	Properties  map[string]string `json:"transaction_properties"`
}

// OperationView is a typed summary of a transaction's operation. Type names
// the variant ("Append", "Delete", "UpdateConfig", ...). The remaining fields
// are populated for the variants that carry them. Raw holds the operation's
// full JSON for anything not surfaced here.
type OperationView struct {
	// Type names the operation variant ("Append", "Delete", "UpdateConfig", ...).
	Type string `json:"type"`
	// NumFragments is the number of fragments the operation touched, for
	// variants that carry it.
	NumFragments int `json:"num_fragments"`
	// FragmentIDs lists the fragment ids the operation touched, for variants
	// that carry them.
	FragmentIDs []uint64 `json:"fragment_ids"`
	// Predicate is the SQL predicate, for Delete-like variants.
	Predicate string `json:"predicate"`
	// Version is the target version, for Restore-like variants.
	Version uint64 `json:"version"`
	// NewIndices lists index names created by the operation, if any.
	NewIndices []string `json:"new_indices"`

	Raw json.RawMessage `json:"-"`
}

// UnmarshalJSON captures the operation's raw JSON alongside the typed fields.
func (o *OperationView) UnmarshalJSON(b []byte) error {
	o.Raw = append(json.RawMessage(nil), b...)
	type alias OperationView
	return json.Unmarshal(b, (*alias)(o))
}

// ReadVersion is the dataset version the transaction is based on.
func (t *Transaction) ReadVersion() uint64 { return t.view.ReadVersion }

// UUID is the transaction's unique id.
func (t *Transaction) UUID() string { return t.view.UUID }

// Operation returns a typed summary of the transaction's operation.
func (t *Transaction) Operation() OperationView { return cloneOperationView(t.view.Operation) }

// Properties returns a copy of the transaction's properties, if any.
func (t *Transaction) Properties() map[string]string { return cloneStringMap(t.view.Properties) }

// Bytes returns a copy of the opaque, lossless protobuf encoding of the
// transaction.
func (t *Transaction) Bytes() []byte { return append([]byte(nil), t.pb...) }

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneOperationView(src OperationView) OperationView {
	src.FragmentIDs = append([]uint64(nil), src.FragmentIDs...)
	src.NewIndices = append([]string(nil), src.NewIndices...)
	src.Raw = append(json.RawMessage(nil), src.Raw...)
	return src
}

// newTransaction builds a Transaction from its protobuf bytes and JSON view.
// A JSON view of the literal "null" (no transaction) yields (nil, nil).
func newTransaction(pb []byte, viewJSON []byte) (*Transaction, error) {
	trimmed := string(viewJSON)
	if trimmed == "null" || trimmed == "" {
		return nil, nil
	}
	t := &Transaction{pb: pb, Raw: append(json.RawMessage(nil), viewJSON...)}
	if err := json.Unmarshal(viewJSON, &t.view); err != nil {
		return nil, fmt.Errorf("lance: decode transaction view: %w", err)
	}
	return t, nil
}

// goBytesFree copies a native byte buffer into Go memory and frees the native
// buffer. An empty buffer yields nil; NULL with a non-zero length is rejected.
func goBytesFree(ptr *C.uint8_t, n C.size_t) ([]byte, error) {
	if ptr == nil {
		return copyCBytes(nil, uint64(n))
	}
	defer C.lance_bytes_free(ptr, n)
	if n == 0 {
		return nil, nil
	}
	return copyCBytes(unsafe.Pointer(ptr), uint64(n))
}

// writeFragmentsConfig mirrors the options_json contract of
// lance_write_fragments.
type writeFragmentsConfig struct {
	Mode                  string            `json:"mode,omitempty"`
	MaxRowsPerFile        uint64            `json:"max_rows_per_file,omitempty"`
	MaxRowsPerGroup       uint64            `json:"max_rows_per_group,omitempty"`
	MaxBytesPerFile       uint64            `json:"max_bytes_per_file,omitempty"`
	DataStorageVersion    string            `json:"data_storage_version,omitempty"`
	EnableStableRowIDs    bool              `json:"enable_stable_row_ids,omitempty"`
	EnableV2ManifestPaths bool              `json:"enable_v2_manifest_paths,omitempty"`
	TransactionProperties map[string]string `json:"transaction_properties,omitempty"`

	storage map[string]string
}

// WriteFragmentsOption configures WriteFragments.
type WriteFragmentsOption func(*writeFragmentsConfig)

// WithFragmentsMode selects the write mode: WriteModeCreate (default, writes
// an Overwrite that creates the dataset) or WriteModeAppend (writes an Append
// against the existing dataset, the distributed-append case).
func WithFragmentsMode(mode WriteMode) WriteFragmentsOption {
	return func(c *writeFragmentsConfig) { c.Mode = mode.String() }
}

// WithFragmentsMaxRowsPerFile caps rows per data file.
func WithFragmentsMaxRowsPerFile(n uint64) WriteFragmentsOption {
	return func(c *writeFragmentsConfig) { c.MaxRowsPerFile = n }
}

// WithFragmentsDataStorageVersion selects the Lance file format version.
func WithFragmentsDataStorageVersion(v string) WriteFragmentsOption {
	return func(c *writeFragmentsConfig) { c.DataStorageVersion = v }
}

// WithFragmentsStableRowIDs enables stable row IDs for the written fragments.
func WithFragmentsStableRowIDs(enable bool) WriteFragmentsOption {
	return func(c *writeFragmentsConfig) { c.EnableStableRowIDs = enable }
}

// WithFragmentsStorageOptions passes object-store options for the write.
func WithFragmentsStorageOptions(opts map[string]string) WriteFragmentsOption {
	return func(c *writeFragmentsConfig) { c.storage = opts }
}

// WithFragmentsTransactionProperties attaches key/value properties to the
// resulting Transaction. The properties travel inside the transaction itself
// (they survive the commit and are readable via Dataset.ReadTransaction).
func WithFragmentsTransactionProperties(props map[string]string) WriteFragmentsOption {
	return func(c *writeFragmentsConfig) { c.TransactionProperties = props }
}

// WriteFragments writes rdr's batches as new, uncommitted fragments at uri and
// returns the resulting Transaction. Nothing is committed: ship the returned
// Transaction to a driver and commit it with NewCommit(...).Execute /
// ExecuteBatch. This is the primitive for distributed writes. Run it on N
// workers over disjoint data.
//
// As with Write, rdr's batches are exported across the Arrow C Data Interface
// and must be C-allocated with Allocator.
func WriteFragments(ctx context.Context, uri string, rdr array.RecordReader, opts ...WriteFragmentsOption) (res *Transaction, err error) {
	o := newObs(obsConfig{})
	ctx, end := o.start(ctx, "WriteFragments", datasetURIAttribute(uri))
	defer func() { end(&err) }()

	var cfg writeFragmentsConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	cURI, freeURI := cString(uri)
	defer freeURI()
	kv, freeKV := cStorageKV(cfg.storage)
	defer freeKV()
	cOpts, freeOpts, err := marshalOptions(&cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	var stream C.struct_ArrowArrayStream
	cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(unsafe.Pointer(&stream)))

	var pbPtr *C.uint8_t
	var pbLen C.size_t
	var viewJSON *C.char
	callErr := ffiCall(ctx, func() C.int32_t {
		return C.lance_write_fragments(&stream, cURI, cOpts, kv, &pbPtr, &pbLen, &viewJSON)
	})
	if callErr != nil {
		if pbPtr != nil {
			C.lance_bytes_free(pbPtr, pbLen)
		}
		if viewJSON != nil {
			C.lance_string_free(viewJSON)
		}
		return nil, fmt.Errorf("lance: write fragments %q: %w", uri, callErr)
	}
	defer C.lance_string_free(viewJSON)
	pb, err := goBytesFree(pbPtr, pbLen)
	if err != nil {
		return nil, fmt.Errorf("lance: copy transaction bytes: %w", err)
	}
	return newTransaction(pb, []byte(C.GoString(viewJSON)))
}

// commitConfig mirrors the options_json contract of lance_commit.
type commitConfig struct {
	UseStableRowIDs       *bool             `json:"use_stable_row_ids,omitempty"`
	EnableV2ManifestPaths *bool             `json:"enable_v2_manifest_paths,omitempty"`
	Detached              *bool             `json:"detached,omitempty"`
	MaxRetries            *uint32           `json:"max_retries,omitempty"`
	SkipAutoCleanup       *bool             `json:"skip_auto_cleanup,omitempty"`
	TransactionProperties map[string]string `json:"transaction_properties,omitempty"`
	StorageFormat         string            `json:"storage_format,omitempty"`

	uri     string
	storage map[string]string
}

// CommitBuilder commits one or more Transactions to a dataset. Build it with
// NewCommit and finish with Execute or ExecuteBatch.
type CommitBuilder struct {
	cfg     commitConfig
	withObs *obs
}

// NewCommit begins a commit against the dataset at uri. It uses the OTel global
// providers for instrumentation. Attach explicit providers by passing ObsOptions
// (WithTracerProvider, WithMeterProvider, WithLoggerProvider).
func NewCommit(uri string, opts ...ObsOption) *CommitBuilder {
	return &CommitBuilder{cfg: commitConfig{uri: uri}, withObs: newObsFromOptions(opts)}
}

// obs returns the instrumentation handle for this commit builder (nil-safe).
func (b *CommitBuilder) obs() *obs { return b.withObs }

// UseStableRowIDs enables stable row IDs on the committed version.
func (b *CommitBuilder) UseStableRowIDs() *CommitBuilder {
	v := true
	b.cfg.UseStableRowIDs = &v
	return b
}

// EnableV2ManifestPaths writes v2-style manifest paths.
func (b *CommitBuilder) EnableV2ManifestPaths() *CommitBuilder {
	v := true
	b.cfg.EnableV2ManifestPaths = &v
	return b
}

// Detached makes Execute commit a DETACHED version: one that is not part of
// the dataset's lineage, never appears in Versions/Transactions, and can
// never become the latest version (see Dataset.Restore for the normal,
// attached rollback path). Useful for staging changes or "secondary"
// datasets whose lineage is tracked elsewhere.
//
// The full transaction — operation, UUID, tag, and transaction properties —
// is committed losslessly. Execute's returned Dataset handle is checked out
// at the detached version: its Version has the detached high bit set, its
// ManifestLocation points at the detached manifest, and ReadTransaction
// reads the detached transaction back. Detached commits require V2 manifest
// paths (the default for new datasets) and an existing dataset.
// Dataset.ListDetachedManifests enumerates detached versions later.
func (b *CommitBuilder) Detached() *CommitBuilder {
	v := true
	b.cfg.Detached = &v
	return b
}

// MaxRetries bounds commit conflict retries.
func (b *CommitBuilder) MaxRetries(n uint32) *CommitBuilder {
	b.cfg.MaxRetries = &n
	return b
}

// SkipAutoCleanup disables automatic old-version cleanup on commit.
func (b *CommitBuilder) SkipAutoCleanup() *CommitBuilder {
	v := true
	b.cfg.SkipAutoCleanup = &v
	return b
}

// WithTransactionProperties attaches key/value properties to the commit.
func (b *CommitBuilder) WithTransactionProperties(props map[string]string) *CommitBuilder {
	b.cfg.TransactionProperties = props
	return b
}

// WithStorageFormat sets the storage format (e.g. "2.0", "stable").
func (b *CommitBuilder) WithStorageFormat(format string) *CommitBuilder {
	b.cfg.StorageFormat = format
	return b
}

// WithStorageOptions passes object-store options for the commit.
func (b *CommitBuilder) WithStorageOptions(opts map[string]string) *CommitBuilder {
	b.cfg.storage = opts
	return b
}

// Execute commits a single Transaction and returns a handle to the resulting
// dataset version.
//
// Cancellation is ambiguous: the native commit performs cancellable work
// after the atomic manifest write, so a ctx.Err() return does not guarantee
// the commit was not durably applied — treat it like a network timeout and
// check the dataset's version/state before retrying, or the retry may apply
// the transaction twice.
func (b *CommitBuilder) Execute(ctx context.Context, txn *Transaction) (ds *Dataset, err error) {
	ctx, end := b.obs().start(ctx, "CommitBuilder.Execute")
	defer func() { end(&err) }()

	if txn == nil || len(txn.pb) == 0 {
		return nil, fmt.Errorf("lance: commit: nil transaction: %w", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cURI, freeURI := cString(b.cfg.uri)
	defer freeURI()
	kv, freeKV := cStorageKV(b.cfg.storage)
	defer freeKV()
	cOpts, freeOpts, err := marshalOptions(&b.cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	pbPtr := (*C.uint8_t)(unsafe.Pointer(&txn.pb[0]))
	var ptr *C.LanceDataset
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_commit(cURI, pbPtr, C.size_t(len(txn.pb)), cOpts, kv, &ptr)
	}); err != nil {
		return nil, fmt.Errorf("lance: commit %q: %w", b.cfg.uri, err)
	}
	return newDataset(ptr, b.obs()), nil
}

// ExecuteBatch commits a batch of append Transactions in a single commit.
// Lance merges compatible appends into one manifest, so the result is a
// single new version. The returned slice holds that version.
func (b *CommitBuilder) ExecuteBatch(ctx context.Context, txns []*Transaction) (versions []uint64, err error) {
	ctx, end := b.obs().start(ctx, "CommitBuilder.ExecuteBatch")
	defer func() { end(&err) }()

	if len(txns) == 0 {
		return nil, fmt.Errorf("lance: commit batch: no transactions: %w", ErrInvalidArgument)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	cURI, freeURI := cString(b.cfg.uri)
	defer freeURI()
	kv, freeKV := cStorageKV(b.cfg.storage)
	defer freeKV()
	cOpts, freeOpts, err := marshalOptions(&b.cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	blobs := make([][]byte, len(txns))
	for i, txn := range txns {
		blobs[i] = txn.pb
	}
	ptrs, lens, freeBlobs := cByteArrays(blobs)
	defer freeBlobs()

	var resJSON *C.char
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_commit_batch(cURI, ptrs, lens, C.size_t(len(txns)), cOpts, kv, &resJSON)
	}); err != nil {
		return nil, fmt.Errorf("lance: commit batch %q: %w", b.cfg.uri, err)
	}
	defer C.lance_string_free(resJSON)
	var res struct {
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal([]byte(C.GoString(resJSON)), &res); err != nil {
		return nil, fmt.Errorf("lance: decode commit batch result: %w", err)
	}
	return []uint64{res.Version}, nil
}

// cByteArrays copies each blob into C memory and returns parallel C arrays of
// pointers and lengths. Everything lives in C memory (no Go pointers cross
// into C), satisfying the cgo pointer rules. The caller must invoke the
// returned cleanup.
func cByteArrays(blobs [][]byte) (**C.uint8_t, *C.size_t, func()) {
	n := len(blobs)
	ptrs := (**C.uint8_t)(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*C.uint8_t)(nil)))))
	lens := (*C.size_t)(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof(C.size_t(0)))))
	ptrSlice := unsafe.Slice(ptrs, n)
	lenSlice := unsafe.Slice(lens, n)
	for i, b := range blobs {
		if len(b) == 0 {
			ptrSlice[i] = nil
			lenSlice[i] = 0
			continue
		}
		ptrSlice[i] = (*C.uint8_t)(C.CBytes(b))
		lenSlice[i] = C.size_t(len(b))
	}
	return ptrs, lens, func() {
		for _, p := range ptrSlice {
			if p != nil {
				C.free(unsafe.Pointer(p))
			}
		}
		C.free(unsafe.Pointer(ptrs))
		C.free(unsafe.Pointer(lens))
	}
}

// readTransaction is the shared body of ReadTransaction / TransactionByVersion.
func (d *Dataset) readTransaction(ctx context.Context, fn func(ptr *C.LanceDataset, outPB **C.uint8_t, outLen *C.size_t, outJSON **C.char) C.int32_t) (*Transaction, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ptr, err := d.checkOpen(ctx)
	if err != nil {
		return nil, err
	}
	var pbPtr *C.uint8_t
	var pbLen C.size_t
	var viewJSON *C.char
	callErr := ffiCall(ctx, func() C.int32_t {
		return fn(ptr, &pbPtr, &pbLen, &viewJSON)
	})
	if callErr != nil {
		if pbPtr != nil {
			C.lance_bytes_free(pbPtr, pbLen)
		}
		if viewJSON != nil {
			C.lance_string_free(viewJSON)
		}
		return nil, fmt.Errorf("lance: read transaction: %w", callErr)
	}
	defer C.lance_string_free(viewJSON)
	pb, err := goBytesFree(pbPtr, pbLen)
	if err != nil {
		return nil, fmt.Errorf("lance: copy transaction bytes: %w", err)
	}
	return newTransaction(pb, []byte(C.GoString(viewJSON)))
}

// ReadTransaction returns the transaction that produced the currently
// checked-out version, or (nil, nil) if none is recorded.
func (d *Dataset) ReadTransaction(ctx context.Context) (res *Transaction, err error) {
	ctx, end := d.obs().start(ctx, "Dataset.ReadTransaction")
	defer func() { end(&err) }()
	return d.readTransaction(ctx, func(ptr *C.LanceDataset, outPB **C.uint8_t, outLen *C.size_t, outJSON **C.char) C.int32_t {
		return C.lance_dataset_read_transaction(ptr, outPB, outLen, outJSON)
	})
}

// TransactionByVersion returns the transaction that produced version v, or
// (nil, nil) if none is recorded.
func (d *Dataset) TransactionByVersion(ctx context.Context, v uint64) (res *Transaction, err error) {
	ctx, end := d.obs().start(ctx, "Dataset.TransactionByVersion")
	defer func() { end(&err) }()
	return d.readTransaction(ctx, func(ptr *C.LanceDataset, outPB **C.uint8_t, outLen *C.size_t, outJSON **C.char) C.int32_t {
		return C.lance_dataset_read_transaction_by_version(ptr, C.uint64_t(v), outPB, outLen, outJSON)
	})
}

// Transactions returns up to n recent transaction summaries (most recent
// first). Entries that could not be read appear as nil.
func (d *Dataset) Transactions(ctx context.Context, n int) ([]*TransactionSummary, error) {
	return datasetOp(ctx, d, "Dataset.Transactions", "transactions",
		func(ctx context.Context, ptr *C.LanceDataset) ([]*TransactionSummary, error) {
			var cJSON *C.char
			if err := ffiCall(ctx, func() C.int32_t {
				return C.lance_dataset_get_transactions(ptr, C.size_t(n), &cJSON)
			}); err != nil {
				return nil, err
			}
			defer C.lance_string_free(cJSON)
			var raw []json.RawMessage
			if err := json.Unmarshal([]byte(C.GoString(cJSON)), &raw); err != nil {
				return nil, fmt.Errorf("decode transactions: %w", err)
			}
			out := make([]*TransactionSummary, len(raw))
			for i, r := range raw {
				if string(r) == "null" {
					continue
				}
				var v transactionView
				if err := json.Unmarshal(r, &v); err != nil {
					return nil, fmt.Errorf("decode transaction %d: %w", i, err)
				}
				out[i] = &TransactionSummary{view: v, Raw: r}
			}
			return out, nil
		})
}

// TransactionSummary is a read-only JSON view of a historical transaction
// (Transactions does not return the protobuf bytes. Use ReadTransaction /
// TransactionByVersion when you need to re-commit).
type TransactionSummary struct {
	view transactionView
	Raw  json.RawMessage
}

// ReadVersion is the dataset version the transaction is based on.
func (s *TransactionSummary) ReadVersion() uint64 { return s.view.ReadVersion }

// UUID is the transaction's unique id.
func (s *TransactionSummary) UUID() string { return s.view.UUID }

// Operation returns a typed summary of the transaction's operation.
func (s *TransactionSummary) Operation() OperationView {
	return cloneOperationView(s.view.Operation)
}

// ManifestInfo is a summary of a dataset manifest.
type ManifestInfo struct {
	// Version is the dataset version the manifest backs.
	Version uint64 `json:"version"`
	// Fields lists the manifest's schema fields.
	Fields []ManifestField `json:"fields"`
	// NumFragments is the number of fragments in this version.
	NumFragments int `json:"num_fragments"`
	// Config holds the dataset configuration key-value map.
	Config map[string]string `json:"config"`
	// TableMetadata holds the table metadata key-value map.
	TableMetadata map[string]string `json:"table_metadata"`
	// Tag is the tag name pointing at this version, if any.
	Tag *string `json:"tag"`
	// Branch is the branch this version belongs to, or nil for the main
	// branch.
	Branch *string `json:"branch"`
}

// ManifestField describes one schema field in a manifest.
type ManifestField struct {
	// ID is the field id.
	ID int32 `json:"id"`
	// Name is the field name.
	Name string `json:"name"`
}

// Manifest returns a summary of the current manifest.
func (d *Dataset) Manifest(ctx context.Context) (ManifestInfo, error) {
	return datasetOp(ctx, d, "Dataset.Manifest", "manifest",
		func(ctx context.Context, ptr *C.LanceDataset) (ManifestInfo, error) {
			var info ManifestInfo
			if err := getJSON(ctx, ptr, "manifest", &info, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_manifest(ptr, cJSON)
			}); err != nil {
				return ManifestInfo{}, err
			}
			return info, nil
		})
}

// DetachedManifest describes a detached manifest: a manifest committed
// outside the dataset's normal lineage (see CommitBuilder.Detached). Version
// carries the detached high bit. For the naming scheme and ETag of a freshly
// committed detached version, use Dataset.ManifestLocation on the handle
// returned by Execute.
type DetachedManifest struct {
	// Version is the detached version number (carries the detached high bit).
	Version uint64 `json:"version"`
	// Path is the object-store path of the detached manifest file.
	Path string `json:"path"`
	// Size is the manifest file's size in bytes, if known.
	Size *uint64 `json:"size"`
}

// ListDetachedManifests returns the dataset's detached manifests.
func (d *Dataset) ListDetachedManifests(ctx context.Context) ([]DetachedManifest, error) {
	return datasetOp(ctx, d, "Dataset.ListDetachedManifests", "list detached manifests",
		func(ctx context.Context, ptr *C.LanceDataset) ([]DetachedManifest, error) {
			var out []DetachedManifest
			if err := getJSON(ctx, ptr, "list detached manifests", &out, func(ptr *C.LanceDataset, cJSON **C.char) C.int32_t {
				return C.lance_dataset_list_detached_manifests(ptr, cJSON)
			}); err != nil {
				return nil, err
			}
			return out, nil
		})
}

// Operation is a dataset operation that can be constructed on the Go side and
// committed with CommitOperation. Implementations: UpdateConfigOperation,
// RestoreOperation.
type Operation interface {
	operationJSON() any
}

// UpdateConfigOperation upserts and/or deletes dataset config, table-metadata,
// or schema-metadata entries. A key mapped to nil in a *Deletes map is
// removed. Entries in a *Upserts map are set.
type UpdateConfigOperation struct {
	ConfigUpserts         map[string]string
	ConfigDeletes         []string
	TableMetadataUpserts  map[string]string
	SchemaMetadataUpserts map[string]string
}

func updateMapJSON(upserts map[string]string, deletes []string) any {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	entries := make(map[string]*string, len(upserts)+len(deletes))
	for k, v := range upserts {
		v := v
		entries[k] = &v
	}
	for _, k := range deletes {
		entries[k] = nil
	}
	return map[string]any{"entries": entries}
}

func (o UpdateConfigOperation) operationJSON() any {
	m := map[string]any{"type": "UpdateConfig"}
	if cu := updateMapJSON(o.ConfigUpserts, o.ConfigDeletes); cu != nil {
		m["config_updates"] = cu
	}
	if tu := updateMapJSON(o.TableMetadataUpserts, nil); tu != nil {
		m["table_metadata_updates"] = tu
	}
	if su := updateMapJSON(o.SchemaMetadataUpserts, nil); su != nil {
		m["schema_metadata_updates"] = su
	}
	return m
}

// RestoreOperation restores the dataset to an earlier version.
type RestoreOperation struct {
	Version uint64
}

func (o RestoreOperation) operationJSON() any {
	return map[string]any{"type": "Restore", "version": o.Version}
}

// CommitOperation builds op from JSON, wraps it in a fresh transaction at
// readVersion, commits it against uri, and returns the resulting dataset
// version. Use this for operations that can be constructed directly (config
// updates, restores). Richer operations travel losslessly via WriteFragments.
func CommitOperation(ctx context.Context, uri string, op Operation, readVersion uint64, opts ...func(*CommitBuilder)) (ds *Dataset, err error) {
	b := NewCommit(uri)
	for _, opt := range opts {
		opt(b)
	}
	ctx, end := b.obs().start(ctx, "CommitOperation", datasetURIAttribute(uri))
	defer func() { end(&err) }()

	if err = ctx.Err(); err != nil {
		return nil, err
	}
	opJSON, err := json.Marshal(op.operationJSON())
	if err != nil {
		return nil, fmt.Errorf("lance: marshal operation: %w", err)
	}
	cURI, freeURI := cString(uri)
	defer freeURI()
	cOp, freeOp := cString(string(opJSON))
	defer freeOp()
	kv, freeKV := cStorageKV(b.cfg.storage)
	defer freeKV()
	cOpts, freeOpts, err := marshalOptions(&b.cfg)
	if err != nil {
		return nil, err
	}
	defer freeOpts()

	var ptr *C.LanceDataset
	if err := ffiCall(ctx, func() C.int32_t {
		return C.lance_dataset_commit_operation(cURI, cOp, C.uint64_t(readVersion), cOpts, kv, &ptr)
	}); err != nil {
		return nil, fmt.Errorf("lance: commit operation %q: %w", uri, err)
	}
	return newDataset(ptr, b.obs()), nil
}
