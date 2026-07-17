# Troubleshooting

Failures that happen before or outside lance-go's error path — build,
link, environment, and callback problems — with copy-paste fixes. For
errors *returned* by the API, classify with `errors.Is` against the
sentinels in `lance/errors.go` (see the error contract in
[memory.md](memory.md#error-contract)).

## Linker: `cannot find -llance_go`

**Symptom** (Linux, or any `go build` of a consumer project):

```
/usr/bin/ld: cannot find -llance_go: No such file or directory
collect2: error: ld returned 1 exit status
```

**Cause.** The `lance` package's `#cgo` directives pass `-llance_go`, but
the linker cannot find `liblance_go.a` on its search path. Either the shim
was never built (`rust/target/release/` is empty) or, for out-of-repo
builds, `CGO_LDFLAGS` does not point at the library.

**Fix.** Build or download the static library, then export the flags the
repo prints for you:

```sh
# In-repo: build from source (needs Rust + protoc)
make rust
make platform-info        # prints the CGO_CFLAGS / CGO_LDFLAGS to export

# Or skip the build: fetch the prebuilt release artifact
./scripts/download-artifacts.sh          # or: make artifacts
eval "$(./scripts/download-artifacts.sh --print-export)"
```

`make platform-info` prints something like:

```
CGO_CFLAGS=-I/path/to/lance-go/include
CGO_LDFLAGS=-L/path/to/lance-go/rust/target/release
```

Export both before `go build`. Only the search paths are needed —
`-llance_go` and the platform libraries are already baked into the
package's `#cgo` directives.

## macOS: undefined symbols / `-framework` link errors

**Symptom:**

```
Undefined symbols for architecture arm64:
  "_SecTrustEvaluateWithError", referenced from: ...
ld: symbol(s) not found for architecture arm64
```

or errors mentioning `Security`, `CoreFoundation`, `SystemConfiguration`,
or `IOKit`.

**Cause.** The link is missing the macOS frameworks the Rust shim depends
on: `-framework Security -framework CoreFoundation -framework
SystemConfiguration -framework IOKit`. These are baked into
`lance/lance.go`'s `#cgo darwin LDFLAGS`, so a normal `go build` that
imports the `lance` package gets them automatically. Seeing this error
means the build is linking the static library without going through the
package's directives (e.g. hand-rolled `CGO_LDFLAGS` that link
`liblance_go.a` directly into another cgo package).

**Fix.** Link through the `lance` package and provide only the search
paths from `make platform-info` / `scripts/download-artifacts.sh`. Do not
replicate `-llance_go` or the framework list by hand. Related but
harmless: the macOS linker may warn about `LC_DYSYMTAB` on the static
library — that warning is benign, ignore it.

## `protoc: command not found` during `make rust`

**Symptom.** A source build fails early with:

```
error: failed to run custom build command for `lance-...`
  ... Could not find `protoc`. If `protoc` is installed, ...
```

or a literal `protoc: command not found`.

**Cause.** The `lance` crate's prost-build generates protobuf code at
compile time and needs the `protoc` binary on PATH.

**Fix.**

```sh
# macOS
brew install protobuf
# Debian/Ubuntu
apt-get install -y protobuf-compiler
# Or point the Makefile at a specific binary
make PROTOC=/path/to/protoc rust
```

Alternatively skip the source build entirely with the prebuilt artifacts
(`./scripts/download-artifacts.sh`, see below).

## Crash: "cgo argument has Go pointer to unpinned Go pointer"

**Symptom.** A hard crash (not a Go error) under
`GOEXPERIMENT=cgocheck2`, typically during `Write`, `MergeInsert`,
`AddColumnsFromReader`, `Merge`, or `NearestArrow`:

```
panic: runtime error: cgo argument has Go pointer to unpinned Go pointer
```

Without cgocheck2 the same code may "work" until the GC moves or collects
a buffer mid-write, corrupting data.

**Cause.** Arrow data exported to the native side was allocated on the Go
heap (`memory.DefaultAllocator`). Exported Arrow buffer trees must live in
C memory.

**Fix.** Build every record you hand to lance-go with the package's
C-backed allocator:

```go
mem := lance.Allocator()
b := array.NewRecordBuilder(mem, schema) // NOT memory.DefaultAllocator
```

In this repo's tests, use `internal/testutil.Allocator()`. Full ownership
rules and the list of affected entry points are in
[memory.md](memory.md#c-allocator-requirement-data-going-into-lance-go).
Do NOT "fix" this by dropping the cgocheck2 test run — CI runs it.

## CI fails: `include/lance_go.h` drifted

**Symptom.** The "Header up to date" CI step fails
(`git diff --exit-code include/lance_go.h` reports changes), or
`make header-check` fails locally.

**Cause.** A Rust change altered the FFI surface. `rust/build.rs`
regenerates `include/lance_go.h` via cbindgen on every build, and the
checked-in copy no longer matches.

**Fix.** Rebuild and commit the regenerated header **together with** the
Rust change:

```sh
make rust
git add include/lance_go.h
```

Never edit the header by hand, and never revert the header while keeping
the Rust change — the header is generated output, the Rust source is the
truth.

## Object store: 403 / AccessDenied despite correct storage options

**Symptom.** `Open`/`Write` against `s3://...` fails with `lance.ErrIO`
and a wrapped 403 (`AccessDenied`, `InvalidAccessKeyId`,
`SignatureDoesNotMatch`) even though the credentials passed via
`WithStorageOptions` / `WithWriteStorageOptions` are correct.

**Cause.** Conflicting `AWS_ACCESS_KEY_ID` (and friends) in the process
environment have been observed to break requests even when correct
credentials are supplied via `WithStorageOptions`/`WithWriteStorageOptions`.
This repository's object-store test fixture reproduces it directly: with
junk `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` set in the environment, a
SeaweedFS request fails with `InvalidAccessKeyId` even though the fixture
passes correct `access_key_id`/`secret_access_key` via storage options;
unsetting the env vars fixes it (`internal/testutil/objectstore.go`). We do
not have a verified, general precedence rule between the two sources (one
would need to read lance-io's credential-resolution source per provider and
version to state that with confidence) — treat "which one wins" as
underdetermined and avoid relying on it either way. This bites in CI
runners, containers, and shells that export AWS credentials for other
tooling.

**Fix.** Make the environment and the options agree — either unset the
environment credentials for the process:

```sh
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN ./your-app
```

or put the intended credentials *in* the environment and drop them from
the options. Verify which identity is actually used by checking the
account/key ID in the wrapped provider message (`err.Error()` contains the
provider's response). Option reference: [storage.md](storage.md).

## `ErrReentrantCall`: "reentrant call from within a callback"

**Symptom.** An operation fails with an error matching
`errors.Is(err, lance.ErrReentrantCall)`.

**Cause.** A synchronous Go callback (an `AddColumnsUDF` mapper, a
`UDFCheckpointStore` method, or a `WriteWithProgress` callback) called back
into a lance-go API while the native operation that invoked it was still
running. The callback runs *inside* the native operation on a shared Tokio
runtime thread; re-entering would drive the runtime from within itself
(which would panic in Tokio), so lance-go rejects the nested call up
front.

**Fix.** Callbacks must not call back into lance-go. Collect whatever you
need inside the callback (copy borrowed inputs) and perform follow-up
lance-go calls after the enclosing operation returns. For the UDF and
checkpoint-store paths the rejected call surfaces as a failure of the
enclosing operation; for the best-effort write-progress callback it is
ignored and the write completes. See
[memory.md](memory.md#callbacks-and-plugins) and the callback guide
([callbacks.md](callbacks.md)).

## First build is very slow

**Symptom.** `make rust` (or the first `go build` after cloning) appears
to hang for many minutes.

**Cause.** Expected: a cold `make rust` compiles the full `lance` +
DataFusion dependency tree — roughly 6–25 minutes depending on the
machine. This is a one-time cost: after that, touching only
`rust/src/*.rs` rebuilds in seconds, and Go-only changes need no Rust
rebuild at all.

**Fix.** Nothing is wrong — wait it out once. To skip the build entirely,
use the prebuilt release artifacts (linux-amd64, linux-arm64,
darwin-arm64):

```sh
./scripts/download-artifacts.sh    # or: make artifacts
eval "$(./scripts/download-artifacts.sh --print-export)"
go build ./...
```

The script downloads the platform tarball from GitHub Releases, verifies
its checksum, extracts `liblance_go.a` into `lib/{os}_{arch}/` and the
header into `include/`, and prints the `CGO_CFLAGS` / `CGO_LDFLAGS` to
export.
