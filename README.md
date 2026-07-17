# lance-go

[![CI](https://github.com/gstamatakis95/lance-go/actions/workflows/ci.yml/badge.svg)](https://github.com/gstamatakis95/lance-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/gstamatakis95/lance-go/lance.svg)](https://pkg.go.dev/github.com/gstamatakis95/lance-go/lance)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Production Go bindings for the [Lance](https://github.com/lancedb/lance) columnar
format: datasets, scans, mutations, schema evolution, versioning, and the full
index/search surface (vector + full-text), exposed as an idiomatic Go API.

**Status:** `v0.1.0` (first release), full dataset + index + distributed +
caching/callbacks parity, built against `lance = 8.0.0`.

```
Module:  github.com/gstamatakis95/lance-go
Import:  github.com/gstamatakis95/lance-go/lance
```

lance-go is implemented as cgo bindings over a small Rust FFI shim (`rust/`)
that links the official `lance` crate into a static library. All data moves
between Go and Rust zero-copy via the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
using [arrow-go v18](https://github.com/apache/arrow-go). See
the guides in [docs/](docs/) for the architecture and usage details.

Everything returns wrapped sentinel errors (`lance.ErrNotFound`,
`lance.ErrInvalidArgument`, ...) that work with `errors.Is`. Every method that
takes a `context.Context` cancels its in-flight native call when the context
is done, surfacing `context.Canceled` / `context.DeadlineExceeded` (cancellation
is cooperative — see [Known limitations](#known-limitations)).

## Quickstart

Two paths to a running program. **Prebuilt artifacts** (below) is the fast
path: no Rust toolchain, no `protoc`, no 6-25 minute cold compile. **Build
from source** is the fully supported fallback for platforms outside the
prebuilt matrix or for developing lance-go itself.

### Quickstart: prebuilt artifacts

`scripts/download-artifacts.sh` fetches a checksum-verified prebuilt
`liblance_go.a` for your platform; linux/amd64, linux/arm64, and
darwin/arm64 are covered (see [Platform support](#platform-support)).

This path is for consumers of the published `lance-go` module: prebuilt
artifacts match tagged releases only. From a git checkout of this repo
(developing lance-go itself, or building an unreleased commit), use
`make rust` instead — the artifacts won't include this branch's changes,
and running the download script in-repo overwrites the checked-in
`include/lance_go.h`, which then fails `make header-check`.

1. Create a module and add the dependency:

   ```sh
   mkdir lance-quickstart && cd lance-quickstart
   go mod init lance-quickstart
   go get github.com/gstamatakis95/lance-go/lance
   ```

2. Fetch the prebuilt artifact for your platform (extracts into
   `lib/{os}_{arch}/` + `include/` in the current directory):

   ```sh
   curl -fsSL https://raw.githubusercontent.com/gstamatakis95/lance-go/main/scripts/download-artifacts.sh | bash
   ```

   From a checkout of this repo, `make artifacts` does the same thing (see
   [Prebuilt artifacts](#prebuilt-artifacts) below).

3. Export the CGO flags the script printed:

   ```sh
   export CGO_CFLAGS="-I$PWD/include"
   export CGO_LDFLAGS="-L$PWD/lib/<os>_<arch>"
   ```

   (Or re-print them any time with
   `VERSION=<tag> ./scripts/download-artifacts.sh --print-export`: with a
   pinned `VERSION` that's already installed, this skips the download and
   just prints the exports.)

4. Write `main.go`:

   ```go
   package main

   import (
   	"context"
   	"fmt"
   	"log"

   	"github.com/apache/arrow-go/v18/arrow"
   	"github.com/apache/arrow-go/v18/arrow/array"
   	"github.com/gstamatakis95/lance-go/lance"
   )

   func main() {
   	ctx := context.Background()

   	// IMPORTANT: buffers exported to the native library must live outside
   	// the Go heap. Always build records with a C-malloc-backed allocator.
   	mem := lance.Allocator()

   	schema := arrow.NewSchema([]arrow.Field{
   		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
   	}, nil)

   	b := array.NewRecordBuilder(mem, schema)
   	defer b.Release()
   	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
   	rec := b.NewRecordBatch()
   	defer rec.Release()

   	rdr, err := lance.Records(schema, rec)
   	if err != nil {
   		log.Fatal(err)
   	}
   	defer rdr.Release()

   	// Overwrite makes the demo re-runnable; the default mode fails if the
   	// dataset already exists.
   	ds, err := lance.Write(ctx, "/tmp/quickstart.lance", rdr,
   		lance.WithMode(lance.WriteModeOverwrite))
   	if err != nil {
   		log.Fatal(err)
   	}
   	defer ds.Close()

   	for batch, err := range ds.Scan().All(ctx) {
   		if err != nil {
   			log.Fatal(err)
   		}
   		fmt.Println(batch)
   	}
   }
   ```

5. Run it:

   ```sh
   go run .
   ```

To build your own module against lance-go you need the native library. Two
verified paths:

- **Prebuilt artifacts** (above and [below](#prebuilt-artifacts)): export the
  `CGO_CFLAGS` / `CGO_LDFLAGS` that `scripts/download-artifacts.sh` or
  `make platform-info` prints so the linker finds the library.
- **`replace` directive** at a lance-go checkout with `make rust` already
  run: the `lance` package's `#cgo` directives point at
  `include/` and `rust/target/release/` relative to the package, so plain
  `go build` works with no extra environment.

### Quickstart: build from source

Building `liblance_go.a` from source takes a Rust toolchain, `protoc`, and a
6-25 minute cold compile of the lance + DataFusion tree. Prefer the
[prebuilt-artifacts quickstart](#quickstart-prebuilt-artifacts) above unless
you're developing lance-go itself, targeting a platform outside the prebuilt
matrix (see [Platform support](#platform-support)), or need a version not yet
released.

Prerequisites:

- Go 1.25+
- Rust 1.94+ (CI pins 1.94.0)
- `protoc` on `PATH` (the lance crate generates protobuf code at build time)

Build and test:

```sh
git clone https://github.com/gstamatakis95/lance-go
cd lance-go
make rust           # builds rust/target/release/liblance_go.a (first build: ~6-25 min)
make test           # Rust tests + Go race detector + strict cgo pointer checks
make lint           # cargo fmt, release-profile clippy, and go vet checks
make docs-check     # verify docs/api.md is current (regenerate with `make docs`)
make platform-info  # print the CGO_CFLAGS / CGO_LDFLAGS consumers need
make artifacts      # fetch a prebuilt library instead of building from source
```

The first `make rust` compiles the full lance + DataFusion dependency tree.
Subsequent shim-only rebuilds take seconds.

Notes:

- `rust/Cargo.lock` pins `time = 0.3.47` to work around a rustc-1.94 /
  aws-smithy E0119 conflict. Never run a bare `cargo update`.
- macOS links `-framework Security -framework CoreFoundation
  -framework SystemConfiguration -framework IOKit`. Linux links
  `-lm -ldl -lpthread`. The `lance` Go package carries these in its `#cgo`
  directives. macOS builds consistently target macOS 13 or later; override
  `MACOSX_DEPLOYMENT_TARGET` before invoking Make only if you intentionally
  build for a newer minimum.
- `include/lance_go.h` is generated by cbindgen from `rust/src` during
  `make rust`. Never edit it by hand.

The Rust release profile strips native debug information to reduce the static
archive without imposing whole-program LTO's build cost. For smaller
application binaries, strip Go's symbol and DWARF tables as well:

```sh
go build -trimpath -ldflags="-s -w" ./cmd/your-app
```

### A larger example: index and vector search

Once you have a working build (either path above), see
[examples/vector_search](examples/vector_search) for a runnable program that
builds an IVF_PQ index and runs filtered nearest-neighbor search; the full
example set is indexed in [examples/README.md](examples/README.md).

### Object stores

Point the URI at S3 / Azure / GCS and pass provider options:

```go
ds, err := lance.Open(ctx, "s3://my-bucket/datasets/demo.lance",
	lance.WithStorageOptions(map[string]string{
		"aws_region":            "eu-west-1",
		"aws_access_key_id":     os.Getenv("AWS_ACCESS_KEY_ID"),
		"aws_secret_access_key": os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}))
```

For safe concurrent writers on S3, commit through a DynamoDB table:

```go
ds, err := lance.Write(ctx, "s3+ddb://my-bucket/demo.lance?ddbTableName=lance-commits", rdr,
	lance.WithWriteStorageOptions(map[string]string{"aws_region": "eu-west-1"}))
```

See [docs/storage.md](docs/storage.md) for the full per-provider option
reference and the local emulator test harness.

## Prebuilt artifacts

Tagged releases (`v*`) publish prebuilt static libraries as GitHub Release
assets named `liblance_go-{os}-{arch}.tar.gz` (containing `liblance_go.a`,
`lance_go.h`, and `LICENSE`), plus a `SHA256SUMS` file. Fetch and verify the
right one for your platform with:

```sh
./scripts/download-artifacts.sh                  # latest release
VERSION=v0.1.0 ./scripts/download-artifacts.sh
VERSION=v0.1.0 ./scripts/download-artifacts.sh --print-export    # re-print the CGO_* exports
```

or, from a checkout of this repo:

```sh
make artifacts   # wraps download-artifacts.sh with sensible defaults
```

The script extracts into `lib/{os}_{arch}/` + `include/` and prints the
`CGO_CFLAGS` / `CGO_LDFLAGS` to export so `go build` links the prebuilt
library instead of a locally built one. With `--print-export` and a pinned
`VERSION` that's already installed, it skips the download and verification
and just prints the exports; otherwise (including the default
`VERSION=latest`) it downloads and installs first. Release CI smoke-tests
every archive by building and running a fresh Go module against its
bundled library and header. See
the release workflow (`.github/workflows/release.yml`).

## Platform support

| Platform | Build & test | Prebuilt artifact |
| --- | --- | --- |
| linux/amd64 | CI | yes |
| linux/arm64 | CI | yes |
| darwin/arm64 (Apple Silicon) | CI | yes |
| darwin/amd64 | expected to work, untested | no |
| windows | not supported | no |

## Documentation

- [docs/usage.md](docs/usage.md): dataset lifecycle, point reads, SQL, mutations, CDC, config, branches, schema evolution, versioning
- [docs/indexes.md](docs/indexes.md): every index type, its parameters and defaults, FTS tokenizers, describe/load/prewarm
- [docs/storage.md](docs/storage.md): URI schemes, per-provider storage options, multi-base writes, emulator testing
- [docs/distributed.md](docs/distributed.md): distributed writes and distributed index builds
- [docs/caching.md](docs/caching.md): `Session`, `CacheBackend`, `ObjectStoreCache`, building a Redis-backed cache
- [docs/callbacks.md](docs/callbacks.md): the Go callback/plugin model, write progress, column UDFs, checkpointing
- [docs/memory.md](docs/memory.md): ownership rules, `Release()` obligations, blobs, callbacks, error contract, threading
- [docs/observability.md](docs/observability.md): OpenTelemetry traces, metrics, and logs; enabling a backend
- [docs/troubleshooting.md](docs/troubleshooting.md): build, link, environment, and callback problems with copy-paste fixes
- [docs/api.md](docs/api.md): generated API reference (godoc for the `lance` package); regenerate with `make docs`
- `examples/`: runnable examples (incl. `sql_query`, `take_blobs`, `distributed_index`, `plugincache`, `schema_evolution`, `maintenance`)

## Known limitations

- **Cancellation is cooperative:** context cancellation aborts in-flight
  native calls (the future is dropped), but background work already spawned
  may still run to completion, and a cancelled write can leave orphaned data
  files for `CleanupOldVersions` to reclaim. See the "Context cancellation"
  section of [docs/usage.md](docs/usage.md).
- **Panics become internal errors:** every exported Rust C function contains
  unwinding and records `ErrInternal`; no Rust panic unwinds into Go. Go
  callback shims independently recover callback panics before returning to
  Rust.
- **C allocator required for writes:** Arrow buffers crossing into the native
  library must be allocated with a C-backed allocator such as
  `lance.Allocator()` ([docs/memory.md](docs/memory.md)).

## Contributing

Bug reports, feature requests, and pull requests are welcome. See
[CONTRIBUTING.md](CONTRIBUTING.md) for build/test prerequisites, the hard
rules the codebase enforces, and the pre-PR checklist.

## License

Apache License 2.0. See [LICENSE](LICENSE). lance-go statically links the
[`lance`](https://github.com/lancedb/lance) crate and its dependency tree,
which are likewise Apache-2.0 licensed.
