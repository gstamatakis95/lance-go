# Object stores

lance-go reads and writes datasets on local disk and on S3, Azure Blob
Storage, and Google Cloud Storage (all compiled in by default), selected by
the URI scheme. Provider configuration crosses as string key/value pairs
via `lance.WithStorageOptions` (Open) / `lance.WithWriteStorageOptions`
(Write).

```go
ds, err := lance.Open(ctx, "s3://bucket/path/dataset.lance",
	lance.WithStorageOptions(map[string]string{...}))
```

## URI schemes

| Scheme | Backend |
| --- | --- |
| `/path` or `file:///path` | local filesystem |
| `s3://bucket/path` | Amazon S3 (or any S3-compatible store) |
| `s3+ddb://bucket/path?ddbTableName=<table>` | S3 with DynamoDB commit coordination |
| `az://container/path` | Azure Blob Storage |
| `gs://bucket/path` | Google Cloud Storage |

Option keys are the `object_store` crate's config keys (lance passes them
through), plus a few lance-io extension keys (noted below). `object_store`
accepts each key both bare and provider-prefixed (`region` ==
`aws_region`, `account_name` == `azure_storage_account_name`, ...).
Environment-based credentials (standard AWS/Azure/GCP env vars and
instance metadata) are picked up automatically when no explicit options
are given.

## S3

```go
opts := map[string]string{
	"aws_region":            "eu-west-1",
	"aws_access_key_id":     "...",
	"aws_secret_access_key": "...",
	// "aws_session_token":  "...",       // STS credentials
}
```

Common additional keys:

| Key | Meaning |
| --- | --- |
| `aws_endpoint` | custom endpoint (MinIO, SeaweedFS, localstack, ...) |
| `aws_allow_http` | `"true"` to permit plain-HTTP endpoints (emulators) |
| `aws_virtual_hosted_style_request` | `"true"` for virtual-hosted addressing (`object_store` defaults to `"false"` = path-style) |
| `aws_session_token` | STS credentials |
| `aws_server_side_encryption` | e.g. `"aws:kms"` |
| `aws_sse_kms_key_id` | KMS key for SSE-KMS |

### S3-compatible stores (SeaweedFS, MinIO, ...)

The live-verified option set for an S3-compatible emulator (this is exactly
what the repo's SeaweedFS integration tests pass, see
`internal/testutil/objectstore.go`):

```go
opts := map[string]string{
	"access_key_id":                "...",
	"secret_access_key":            "...",
	"region":                       "us-east-1", // any value; must be present
	"endpoint":                     "http://127.0.0.1:8333",
	"allow_http":                   "true",
	"virtual_hosted_style_request": "false", // path-style addressing
}
```

Lance commits dataset manifests with conditional PUTs (`If-None-Match`),
so the store must support them: SeaweedFS 4.x or newer is required
(older releases lack `If-None-Match` support and commits are unsafe).

### Concurrent writers: `s3+ddb://`

Plain S3 has no atomic rename, so concurrent commits from multiple writers
can race. Lance solves this with a DynamoDB table as the commit lock. The
shim is built with the `dynamodb` feature enabled:

```go
uri := "s3+ddb://my-bucket/datasets/events.lance?ddbTableName=lance-commits"
ds, err := lance.Write(ctx, uri, rdr, lance.WithMode(lance.WriteModeAppend),
	lance.WithWriteStorageOptions(map[string]string{"aws_region": "eu-west-1"}))
```

The DynamoDB table must exist with `base_uri` (partition key, string) and
`version` (sort key, number). Single-writer workloads don't need it.

## Azure Blob Storage

```go
ds, err := lance.Open(ctx, "az://container/path/dataset.lance",
	lance.WithStorageOptions(map[string]string{
		"azure_storage_account_name": "myaccount",
		"azure_storage_account_key":  "...",
	}))
```

| Key | Meaning |
| --- | --- |
| `azure_storage_account_name` | account name (required unless in the URI) |
| `azure_storage_account_key` | shared key auth |
| `azure_storage_sas_key` | SAS-token auth instead of a key |
| `azure_storage_token` | bearer-token auth |
| `azure_storage_use_emulator` | `"true"` targets Azurite (see below) |
| `azure_storage_endpoint` | custom endpoint (ignored in emulator mode) |

### Azurite (emulator)

Emulator mode is **required** against Azurite. It is not just a
convenience. It switches `object_store` to path-style
`{endpoint}/{account}/{container}` URLs and enables lance-io's workaround
for Azurite's missing list-with-offset support. The live-verified options
(see `internal/testutil/objectstore.go`):

```go
opts := map[string]string{
	"account_name": "devstoreaccount1",
	"account_key":  "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
	"use_emulator": "true",
}
```

In emulator mode the endpoint does **not** come from a storage option
(`azure_storage_endpoint` is ignored): `object_store` reads it from the
`AZURITE_BLOB_STORAGE_URL` environment variable, defaulting to
`http://127.0.0.1:10000`. `allow_http` is implied.

## Google Cloud Storage

```go
ds, err := lance.Open(ctx, "gs://bucket/path/dataset.lance",
	lance.WithStorageOptions(map[string]string{
		"google_service_account": "/path/to/service-account.json",
	}))
```

| Key | Meaning |
| --- | --- |
| `google_service_account` | path to a service-account JSON file |
| `google_service_account_key` | the service-account key JSON itself |
| `google_application_credentials` | path to application default credentials |
| `google_base_url` | custom endpoint (emulators) |
| `google_storage_token` | static bearer token (lance-io extension key, not an `object_store` key) |

### fake-gcs-server (emulator)

`object_store` does **not** honor `STORAGE_EMULATOR_HOST`. The emulator
endpoint must be passed as `google_base_url`. The live-verified options
(see `internal/testutil/objectstore.go`):

```go
opts := map[string]string{
	"google_base_url":      "http://localhost:4443",
	"google_storage_token": "any-string", // fake-gcs accepts any token
	"allow_http":           "true",
}
```

`google_storage_token` supplies a static bearer token so the client never
contacts the GCE metadata server (`google_skip_signature` is not enough:
`object_store` 0.13 still fetches credentials for PUTs with it set).

Caveats, all verified by the repo's test harness:

- The stock `fsouza/fake-gcs-server` image does **not** work:
  `object_store` uploads through the GCS *XML* API, which upstream does
  not implement (uploads fail with HTTP 400, fsouza/fake-gcs-server#1164).
  Use the `tustvold/fake-gcs-server` XML-API fork, the same image the
  apache/arrow-rs `object_store` CI runs against.
- The endpoint host in `google_base_url` must equal the server's
  `-public-host` flag (`localhost`, not `127.0.0.1`): fake-gcs-server
  routes the XML API by `Host` header and 404s on any other host.
- The fork ignores `If-None-Match`, so concurrent-commit conflicts are
  not enforced by the emulator (they are on real GCS).

## Testing against emulators

The repo ships a docker-compose harness with one emulator per provider:
SeaweedFS 4.38 (S3), Azurite (Azure), the `tustvold/fake-gcs-server`
XML-API fork (GCS), at
`internal/testutil/docker-compose.objectstore.yml`.

```sh
make object-store-up        # start SeaweedFS + Azurite + fake-gcs
make test-object-store      # run the gated object-store test suite
make object-store-down      # stop and remove the emulators
```

The object-store tests are skipped unless the gate is set:

```sh
LANCE_GO_OBJECT_STORE_TESTS=1 go test ./... -run ObjectStore
```

CI runs the same flow in the dedicated `object-store-tests` job (see
`.github/workflows/ci.yml`).

## Multiple storage bases

A dataset can spread its data files across several storage "bases" (e.g. a
metadata/root base on one store and bulk data on another, or data split across
buckets). Bases are registered in the manifest (on a **new** dataset with
`WithInitialBases`, or on an existing one with `Dataset.AddBases`), and writes
are directed to them by ID, name, or path.

```go
ds, err := lance.Write(ctx, "s3://root-bucket/ds.lance", rdr,
	lance.WithInitialBases(
		lance.BasePath{ID: 1, Path: "s3://root-bucket/ds.lance", IsDatasetRoot: true},
		lance.BasePath{ID: 2, Name: "cold", Path: "s3://data-bucket/ds-data"},
	),
	lance.WithTargetBases(2),                       // route this write's data files to base 2
	lance.WithBaseStoreParams("cold", map[string]string{ // per-base credentials
		"aws_region": "us-west-2",
	}),
)

// Register a base on an existing dataset, then target it by name/path:
err = ds.AddBases(ctx, []lance.BasePath{{ID: 3, Name: "archive", Path: "s3://archive/ds"}}, nil)
_, err = lance.Write(ctx, uri, rdr, lance.WithMode(lance.WriteModeAppend),
	lance.WithTargetBaseNamesOrPaths("archive"))
```

Each base needs a unique non-zero `ID`. `WithBaseStoreParams` sets
credentials/endpoints per base (keyed by name or path). The write-level
storage options remain the fallback. Related write options:
`WithExternalBlobMode`, `WithBlobPackFileSizeThreshold`, and
`WithAllowExternalBlobOutsideBases` for external blob handling.

## Caching object-store reads

To serve immutable file byte-ranges from an external cache (Redis, disk, ...)
over any of these stores, attach an `ObjectStoreCache` with
`OpenWithObjectStoreCache`. Note the `file://` local-reader caveat. See
[caching.md](caching.md).

## Tuning and troubleshooting

- Object-store errors surface as `lance.ErrIO`. The wrapped message
  contains the provider's response (status code, S3 error code, ...).
- Every dataset operation may issue many small requests (manifests,
  fragment footers). Latency to the store dominates. Co-locate compute
  with the bucket region.
- Storage options are per-handle: two `Open`s of the same URI with
  different credentials are independent.
- Timestamps of commits come from the writer's clock. Keep clocks sane
  when using `CleanupOldVersions` across machines.
