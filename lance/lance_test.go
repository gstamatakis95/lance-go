package lance

import (
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	v := Version()
	if v == "" {
		t.Fatal("Version() returned an empty string")
	}
	if !strings.Contains(v, "lance") {
		t.Fatalf("Version() = %q, want it to contain %q", v, "lance")
	}
	t.Logf("native library version: %s", v)
}
