package lance

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCodeToSentinel(t *testing.T) {
	tests := []struct {
		code int32
		want error
	}{
		{0, ErrInternal}, // OK is never passed to codeToSentinel.
		{1, ErrInvalidArgument},
		{2, ErrIO},
		{3, ErrNotFound},
		{4, ErrAlreadyExists},
		{5, ErrIndex},
		{6, ErrInternal},
		{7, ErrNotImplemented},
		{8, ErrConflict},
		{9, ErrTimeout},
		{10, ErrCanceled},
		{999, ErrInternal},
	}
	for _, tt := range tests {
		if got := codeToSentinel(tt.code); !errors.Is(got, tt.want) {
			t.Errorf("codeToSentinel(%d) = %v, want %v", tt.code, got, tt.want)
		}
	}
}

// TestSentinelsCarryNoPackagePrefix pins the single-prefix contract: the
// "lance: " prefix is added exactly once at the wrap layer (ops.go helpers or
// hand-rolled "lance: <verb>: %w" wraps), so the sentinels themselves must
// not carry it — otherwise wrapped errors would read "lance: ...: lance: ...".
func TestSentinelsCarryNoPackagePrefix(t *testing.T) {
	sentinels := []error{
		ErrInvalidArgument, ErrIO, ErrNotFound, ErrAlreadyExists, ErrIndex,
		ErrInternal, ErrNotImplemented, ErrConflict, ErrTimeout,
		ErrReentrantCall, ErrCanceled,
	}
	for _, s := range sentinels {
		if strings.HasPrefix(s.Error(), "lance:") {
			t.Errorf("sentinel %q carries the package prefix; it must be added only at the wrap layer", s)
		}
	}
}

func TestOpaqueAccessorsReturnDefensiveCopies(t *testing.T) {
	txn, err := newTransaction(
		[]byte{1, 2, 3},
		[]byte(`{"read_version":1,"uuid":"u","operation":{"type":"Append","fragment_ids":[7],"new_indices":["idx"]},"transaction_properties":{"key":"value"}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	txnBytes := txn.Bytes()
	txnBytes[0] = 9
	if bytes.Equal(txnBytes, txn.Bytes()) {
		t.Fatal("Transaction.Bytes exposed mutable internal storage")
	}
	props := txn.Properties()
	props["key"] = "changed"
	if got := txn.Properties()["key"]; got != "value" {
		t.Fatalf("Transaction.Properties mutation leaked: %q", got)
	}
	op := txn.Operation()
	op.FragmentIDs[0] = 99
	op.NewIndices[0] = "changed"
	if got := txn.Operation(); got.FragmentIDs[0] != 7 || got.NewIndices[0] != "idx" {
		t.Fatal("Transaction.Operation exposed mutable internal slices")
	}

	index, err := newIndexMetadata(
		[]byte{4, 5, 6},
		[]byte(`{"name":"idx","uuid":"u","fields":[1],"dataset_version":1,"index_version":1,"created_at":"now"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	indexBytes := index.Bytes()
	indexBytes[0] = 9
	if bytes.Equal(indexBytes, index.Bytes()) {
		t.Fatal("IndexMetadata.Bytes exposed mutable internal storage")
	}
	view := index.View()
	view.Fields[0] = 9
	*view.CreatedAt = "changed"
	gotView := index.View()
	if gotView.Fields[0] != 1 || gotView.CreatedAt == nil || *gotView.CreatedAt != "now" {
		t.Fatalf("IndexMetadata.View mutation leaked: %+v", gotView)
	}
}

func TestCopyCBytesRejectsInvalidLengthsAndPointers(t *testing.T) {
	if _, err := copyCBytes(nil, ^uint64(0)); !errors.Is(err, ErrInternal) {
		t.Fatalf("overflow error = %v", err)
	}
	if _, err := copyCBytes(nil, 1); !errors.Is(err, ErrInternal) {
		t.Fatalf("nil pointer error = %v", err)
	}
	if got, err := copyCBytes(nil, 0); err != nil || got != nil {
		t.Fatalf("empty buffer = %v, %v", got, err)
	}
}
