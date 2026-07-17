package lance

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestCleanupPolicyTaggedDefault verifies that a zero-valued CleanupPolicy
// omits error_if_tagged_old_versions, so the lance default (protect tagged
// versions) applies rather than a Go-side false overriding it.
func TestCleanupPolicyTaggedDefault(t *testing.T) {
	// nil pointer: key omitted, lance default protects tagged versions.
	var cfg cleanupPolicyJSON
	if b, _ := json.Marshal(&cfg); strings.Contains(string(b), "error_if_tagged_old_versions") {
		t.Fatalf("zero cleanup policy should omit error_if_tagged_old_versions, got %s", b)
	}

	// Explicit false: key present so the caller can opt out.
	no := false
	cfg.ErrorIfTaggedOldVersions = &no
	if b, _ := json.Marshal(&cfg); !strings.Contains(string(b), `"error_if_tagged_old_versions":false`) {
		t.Fatalf("explicit false should serialize the key, got %s", b)
	}
}
