package lance

import "testing"

func TestSanitizeDatasetURI(t *testing.T) {
	tests := map[string]string{
		"s3://alice:secret@example.com/data?token=private#fragment": "s3://example.com/data",
		"/tmp/local.lance?token=private#fragment":                   "/tmp/local.lance",
		"custom:opaque-secret":                                      "custom:<redacted>",
	}
	for input, want := range tests {
		if got := sanitizeDatasetURI(input); got != want {
			t.Errorf("sanitizeDatasetURI(%q) = %q, want %q", input, got, want)
		}
	}
}
