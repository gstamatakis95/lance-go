package lance

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func applyUncommitted(opts ...UncommittedOption) createUncommittedConfig {
	var c createUncommittedConfig
	for _, o := range opts {
		o(&c)
	}
	return c
}

// TestFragmentsOptionSerialization pins the null-vs-[]-vs-absent contract of
// the WithFragments option: not provided omits the key (None = whole
// dataset), an explicit empty slice serializes as [] (Some(vec![]) = zero
// fragments), a non-empty list serializes verbatim, and the ambiguous
// zero-arg form errors.
func TestFragmentsOptionSerialization(t *testing.T) {
	// Not called: the fragments key is omitted entirely.
	c := applyUncommitted()
	if c.optErr != nil {
		t.Fatalf("no WithFragments option should not error: %v", c.optErr)
	}
	if b, _ := json.Marshal(&c); strings.Contains(string(b), "fragments") {
		t.Fatalf("no WithFragments option should omit the key, got %s", b)
	}

	// Explicit empty slice: serialized as [] (restrict to zero fragments).
	empty := []uint32{}
	c = applyUncommitted(WithFragments(empty...))
	if c.optErr != nil {
		t.Fatalf("WithFragments(emptySlice...) should not error: %v", c.optErr)
	}
	if b, _ := json.Marshal(&c); !strings.Contains(string(b), `"fragments":[]`) {
		t.Fatalf("WithFragments(emptySlice...) should serialize [], got %s", b)
	}

	// Non-empty list: serialized verbatim.
	c = applyUncommitted(WithFragments(1, 2))
	if b, _ := json.Marshal(&c); !strings.Contains(string(b), `"fragments":[1,2]`) {
		t.Fatalf("WithFragments(1,2) should serialize [1,2], got %s", b)
	}

	// Zero args: ambiguous, rejected with ErrInvalidArgument.
	c = applyUncommitted(WithFragments())
	if !errors.Is(c.optErr, ErrInvalidArgument) {
		t.Fatalf("WithFragments() should record ErrInvalidArgument, got %v", c.optErr)
	}
}
