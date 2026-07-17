package testutil

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"time"
)

// ObjectStoreGateEnv gates the object-store integration tests: they skip
// unless this variable is set (to anything but "0"). The emulators they talk
// to are started with `make object-store-up` (see
// docker-compose.objectstore.yml in this directory).
const ObjectStoreGateEnv = "LANCE_GO_OBJECT_STORE_TESTS"

// Credentials and bucket fixed by docker-compose.objectstore.yml and the
// Makefile object-store-up target.
const (
	// ObjectStoreBucket is the bucket/container created on every emulator.
	ObjectStoreBucket = "lance-test"
	// SeaweedFSAccessKey/SeaweedFSSecretKey match seaweedfs-s3.json.
	SeaweedFSAccessKey = "lance-access-key"
	SeaweedFSSecretKey = "lance-secret-key"
	// AzuriteAccount/AzuriteKey are the well-known Azurite dev-storage
	// credentials.
	AzuriteAccount = "devstoreaccount1"
	AzuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// ObjectStoreEnabled reports whether the object-store integration tests are
// enabled via ObjectStoreGateEnv.
func ObjectStoreEnabled() bool {
	v := os.Getenv(ObjectStoreGateEnv)
	return v != "" && v != "0"
}

// ObjectStoreFixture describes how to reach one emulated object store.
type ObjectStoreFixture struct {
	// Provider is "s3", "azure" or "gcs".
	Provider string
	// URI is the dataset URI (bucket + the path passed to the fixture
	// constructor).
	URI string
	// StorageOptions are the lance storage options (lance.WithStorageOptions
	// / lance.WithWriteStorageOptions) that make the provider's client talk
	// to the emulator.
	StorageOptions map[string]string
	// Endpoint is the emulator's HTTP endpoint, for reachability checks.
	Endpoint string
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// S3Fixture returns a fixture for the SeaweedFS S3 gateway. path is the
// dataset path within the test bucket. The endpoint defaults to
// http://127.0.0.1:8333 and can be overridden with LANCE_GO_S3_ENDPOINT.
//
// Storage-option keys (object_store 0.13 AmazonS3ConfigKey aliases):
// access_key_id, secret_access_key, region, endpoint, allow_http, and
// virtual_hosted_style_request=false to force path-style addressing.
func S3Fixture(path string) ObjectStoreFixture {
	endpoint := envOr("LANCE_GO_S3_ENDPOINT", "http://127.0.0.1:8333")
	return ObjectStoreFixture{
		Provider: "s3",
		URI:      fmt.Sprintf("s3://%s/%s", ObjectStoreBucket, path),
		Endpoint: endpoint,
		StorageOptions: map[string]string{
			"access_key_id":                SeaweedFSAccessKey,
			"secret_access_key":            SeaweedFSSecretKey,
			"region":                       "us-east-1",
			"endpoint":                     endpoint,
			"allow_http":                   "true",
			"virtual_hosted_style_request": "false",
		},
	}
}

// AzureFixture returns a fixture for Azurite. path is the dataset path
// within the test container. The endpoint defaults to http://127.0.0.1:10000
// and can be overridden with LANCE_GO_AZURITE_ENDPOINT.
//
// Storage-option keys (object_store 0.13 AzureConfigKey aliases):
// account_name, account_key and use_emulator=true. Emulator mode makes
// object_store use path-style {endpoint}/{account}/{container} URLs and
// work around Azurite's missing list-with-offset support; it reads the
// endpoint from the AZURITE_BLOB_STORAGE_URL environment variable (not from
// a storage option), which this function sets process-wide.
func AzureFixture(path string) ObjectStoreFixture {
	endpoint := envOr("LANCE_GO_AZURITE_ENDPOINT", "http://127.0.0.1:10000")
	// object_store's emulator mode reads the endpoint from this env var.
	// os.Setenv also updates the C environment (the process is built with
	// cgo), so the Rust side sees it.
	os.Setenv("AZURITE_BLOB_STORAGE_URL", endpoint)
	return ObjectStoreFixture{
		Provider: "azure",
		URI:      fmt.Sprintf("az://%s/%s", ObjectStoreBucket, path),
		Endpoint: endpoint,
		StorageOptions: map[string]string{
			"account_name": AzuriteAccount,
			"account_key":  AzuriteKey,
			"use_emulator": "true",
		},
	}
}

// GCSFixture returns a fixture for fake-gcs-server. path is the dataset path
// within the test bucket. The endpoint defaults to http://localhost:4443 and
// can be overridden with LANCE_GO_GCS_ENDPOINT. The endpoint's host MUST
// match the emulator's -public-host flag (localhost:4443 in the compose
// file): fake-gcs-server routes its XML API — which object_store uses for
// list requests — by Host header and answers 404 on any other host.
//
// Storage-option keys: google_base_url (object_store 0.13 GoogleConfigKey
// alias) points the client at the emulator, google_storage_token (a
// lance-io extension, not an object_store key) supplies a static bearer
// token so the client never contacts the GCE metadata server (fake-gcs
// accepts any token), and allow_http permits the plain-HTTP endpoint.
// google_skip_signature is NOT sufficient here: object_store 0.13.2 still
// fetches credentials for PUT requests even with skip_signature set.
func GCSFixture(path string) ObjectStoreFixture {
	endpoint := envOr("LANCE_GO_GCS_ENDPOINT", "http://localhost:4443")
	return ObjectStoreFixture{
		Provider: "gcs",
		URI:      fmt.Sprintf("gs://%s/%s", ObjectStoreBucket, path),
		Endpoint: endpoint,
		StorageOptions: map[string]string{
			"google_base_url":      endpoint,
			"google_storage_token": "lance-go-test-token",
			"allow_http":           "true",
		},
	}
}

// CheckReachable verifies that the emulator's endpoint accepts TCP
// connections, so gated tests can fail fast with a clear message when the
// emulators are not running.
func (f ObjectStoreFixture) CheckReachable(timeout time.Duration) error {
	u, err := url.Parse(f.Endpoint)
	if err != nil {
		return fmt.Errorf("parse endpoint %q: %w", f.Endpoint, err)
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "80")
	}
	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", host, err)
	}
	return conn.Close()
}
