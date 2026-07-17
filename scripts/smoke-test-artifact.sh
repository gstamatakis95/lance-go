#!/usr/bin/env bash
# Build and run a fresh Go module against a packaged liblance_go artifact.
# The copied module intentionally contains no rust/target directory, ensuring
# the linker can only use the archive supplied on the command line.

set -euo pipefail

ARCHIVE="${1:-}"
[ -n "$ARCHIVE" ] || { echo "usage: $0 liblance_go-{os}-{arch}.tar.gz" >&2; exit 2; }
[ -f "$ARCHIVE" ] || { echo "artifact not found: $ARCHIVE" >&2; exit 2; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/artifact" "$TMP/module/include" "$TMP/consumer"
tar -xzf "$ARCHIVE" -C "$TMP/artifact"
[ -f "$TMP/artifact/liblance_go.a" ] || { echo "artifact is missing liblance_go.a" >&2; exit 1; }
[ -f "$TMP/artifact/lance_go.h" ] || { echo "artifact is missing lance_go.h" >&2; exit 1; }

# Copy only the Go module inputs. The header also comes from the artifact, so
# this catches a release archive whose native library and C API disagree.
cp "$ROOT/go.mod" "$ROOT/go.sum" "$TMP/module/"
cp -R "$ROOT/lance" "$TMP/module/"
cp "$TMP/artifact/lance_go.h" "$TMP/module/include/"

cat >"$TMP/consumer/go.mod" <<'EOF'
module example.com/lance-artifact-smoke

go 1.25.0

require github.com/gstamatakis95/lance-go v0.0.0

replace github.com/gstamatakis95/lance-go => ../module
EOF
# A replacement module's go.sum is not inherited by the consuming main
# module. Seed the consumer with the repository's verified dependency sums so
# the smoke build works with the module cache offline as well as in CI.
cp "$ROOT/go.sum" "$TMP/consumer/go.sum"

cat >"$TMP/consumer/main.go" <<'EOF'
package main

import (
	"fmt"

	"github.com/gstamatakis95/lance-go/lance"
)

func main() {
	fmt.Println(lance.Version())
}
EOF

case "$(uname -s)" in
  Darwin)
    export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-13.0}"
    ;;
  Linux) ;;
  *)
    echo "unsupported smoke-test platform: $(uname -s)" >&2
    exit 2
    ;;
esac

export CGO_CFLAGS="-I$TMP/artifact ${CGO_CFLAGS:-}"
export CGO_LDFLAGS="-L$TMP/artifact ${CGO_LDFLAGS:-}"

(
	cd "$TMP/consumer"
	export GOWORK=off
	# The scratch main module intentionally starts minimal. Allow Go to add its
	# pruned indirect requirements while resolving the replaced local module.
	go build -mod=mod -trimpath -ldflags="-s -w" -o smoke .
  version="$(./smoke)"
  case "$version" in
    *lance-go-ffi*) printf 'artifact smoke test passed: %s\n' "$version" ;;
    *) echo "unexpected lance.Version output: $version" >&2; exit 1 ;;
  esac
)
