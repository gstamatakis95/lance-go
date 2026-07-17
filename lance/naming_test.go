package lance

import (
	"errors"
	"testing"
)

// TestWithFragmentsZeroArgSemantics confirms WithFragments keeps its
// null-vs-[]-vs-error contract: not called omits the key, an explicit empty
// slice is accepted, and a bare zero-arg call is rejected as ambiguous.
func TestWithFragmentsZeroArgSemantics(t *testing.T) {
	// Explicit empty slice is accepted without error.
	empty := []uint32{}
	c := applyUncommitted(WithFragments(empty...))
	if c.optErr != nil {
		t.Fatalf("WithFragments(emptySlice...) should not error: %v", c.optErr)
	}

	// Zero args is ambiguous and must record ErrInvalidArgument.
	c = applyUncommitted(WithFragments())
	if !errors.Is(c.optErr, ErrInvalidArgument) {
		t.Fatalf("WithFragments() should record ErrInvalidArgument, got %v", c.optErr)
	}
}
