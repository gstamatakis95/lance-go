#!/usr/bin/env bash
#
# Download a prebuilt liblance_go static library from GitHub Releases,
# verify its checksum, and extract it into lib/{os}_{arch}/ + include/.
#
# Usage:
#   ./scripts/download-artifacts.sh              # latest release
#   VERSION=v0.1.0 ./scripts/download-artifacts.sh
#   ./scripts/download-artifacts.sh --print-export   # only print the CGO_* exports
#
# Environment:
#   VERSION   release tag to download (default: latest release)
#   REPO      GitHub repo (default: gstamatakis95/lance-go)
#   DEST      directory to extract into (default: current directory)
#
# Flags:
#   --print-export   print only the two `export ...` lines (no other
#                     output), ready to paste or eval, e.g.:
#                       eval "$(./scripts/download-artifacts.sh --print-export)"
#                     With a pinned VERSION (not "latest") whose library and
#                     header are already installed under DEST, this skips
#                     the download and verification and just prints the
#                     exports; otherwise it still downloads+installs first.
#   -h, --help        print this usage and exit
#
# On success, prints the CGO_CFLAGS / CGO_LDFLAGS to export so `go build`
# links the prebuilt library (mirrors `make platform-info`).

set -euo pipefail

PRINT_EXPORT=0
for arg in "$@"; do
  case "$arg" in
    --print-export) PRINT_EXPORT=1 ;;
    -h|--help)
      sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "error: unknown argument: $arg" >&2; exit 1 ;;
  esac
done

REPO="${REPO:-gstamatakis95/lance-go}"
VERSION="${VERSION:-latest}"
DEST="${DEST:-$PWD}"

err() { echo "error: $*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || err "curl is required"
command -v tar >/dev/null 2>&1 || err "tar is required"

# --- Detect platform -------------------------------------------------------

case "$(uname -s)" in
  Linux)  OS=linux ;;
  Darwin) OS=darwin ;;
  *) err "unsupported OS: $(uname -s) (prebuilt artifacts cover linux and darwin; build from source with 'make rust')" ;;
esac

case "$(uname -m)" in
  x86_64|amd64)  ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) err "unsupported architecture: $(uname -m) (build from source with 'make rust')" ;;
esac

PLATFORM="${OS}-${ARCH}"
case "$PLATFORM" in
  linux-amd64|linux-arm64|darwin-arm64) ;;
  *) err "no prebuilt artifact for $PLATFORM (build from source with 'make rust')" ;;
esac

ASSET="liblance_go-${PLATFORM}.tar.gz"

LIB_DIR="$DEST/lib/${OS}_${ARCH}"
INCLUDE_DIR="$DEST/include"
VERSION_MARKER="$LIB_DIR/.lance-go-version"

# --- Fast path: already installed for a pinned version ----------------------
#
# --print-export performs a full download+verify+extract on every run unless
# short-circuited here. When VERSION is pinned explicitly (not "latest") and
# that exact version's library+header are already installed, skip the
# network entirely and just print the exports.
if [ "$PRINT_EXPORT" -eq 1 ] && [ "$VERSION" != "latest" ] \
  && [ -f "$LIB_DIR/liblance_go.a" ] && [ -f "$INCLUDE_DIR/lance_go.h" ] \
  && [ -f "$VERSION_MARKER" ] && [ "$(cat "$VERSION_MARKER")" = "$VERSION" ]; then
  echo "export CGO_CFLAGS=\"-I${INCLUDE_DIR}\""
  echo "export CGO_LDFLAGS=\"-L${LIB_DIR}\""
  exit 0
fi

# --- Resolve the release tag ----------------------------------------------

if [ "$VERSION" = "latest" ]; then
  # Follow the /releases/latest redirect to learn the tag without needing jq.
  location="$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
    "https://github.com/${REPO}/releases/latest")"
  VERSION="${location##*/}"
  case "$VERSION" in
    v*) ;;
    *) err "could not resolve the latest release tag (got '$VERSION'); pass VERSION=vX.Y.Z" ;;
  esac
fi

BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
[ "$PRINT_EXPORT" -eq 1 ] || echo "Downloading ${ASSET} (${VERSION}) from ${REPO} ..."

# --- Download and verify ---------------------------------------------------

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

curl -fsSL -o "$TMP/$ASSET" "$BASE_URL/$ASSET" \
  || err "download failed: $BASE_URL/$ASSET"
curl -fsSL -o "$TMP/SHA256SUMS" "$BASE_URL/SHA256SUMS" \
  || err "download failed: $BASE_URL/SHA256SUMS"

expected="$(grep " ${ASSET}\$" "$TMP/SHA256SUMS" | awk '{print $1}')"
[ -n "$expected" ] || err "no checksum for $ASSET in SHA256SUMS"

if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$TMP/$ASSET" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "$TMP/$ASSET" | awk '{print $1}')"
fi
[ "$actual" = "$expected" ] \
  || err "checksum mismatch for $ASSET: expected $expected, got $actual"
[ "$PRINT_EXPORT" -eq 1 ] || echo "Checksum OK: $actual"

# --- Extract ---------------------------------------------------------------

EXTRACT_DIR="$TMP/extract"
mkdir -p "$LIB_DIR" "$INCLUDE_DIR" "$EXTRACT_DIR"

tar -xzf "$TMP/$ASSET" -C "$EXTRACT_DIR"
[ -f "$EXTRACT_DIR/liblance_go.a" ] || err "archive did not contain liblance_go.a"
[ -f "$EXTRACT_DIR/lance_go.h" ] || err "archive did not contain lance_go.h"

mv "$EXTRACT_DIR/liblance_go.a" "$LIB_DIR/liblance_go.a"
mv "$EXTRACT_DIR/lance_go.h" "$INCLUDE_DIR/lance_go.h"
[ -f "$EXTRACT_DIR/LICENSE" ] && mv "$EXTRACT_DIR/LICENSE" "$LIB_DIR/LICENSE"
# Record the installed version so a later --print-export with the same
# pinned VERSION can skip the download (see the fast path above).
echo "$VERSION" > "$VERSION_MARKER"

if [ "$PRINT_EXPORT" -eq 0 ]; then
  echo "Installed:"
  echo "  $LIB_DIR/liblance_go.a"
  echo "  $INCLUDE_DIR/lance_go.h"
fi

# --- Print consumer cgo flags (mirrors `make platform-info`) ---------------

CGO_CFLAGS="-I${INCLUDE_DIR}"
# The lance package's #cgo directives already provide -llance_go and the
# platform libraries. Consumers only need to add the artifact search path.
CGO_LDFLAGS="-L${LIB_DIR}"

if [ "$PRINT_EXPORT" -eq 1 ]; then
  # Machine-readable: only the two export lines, ready to paste or eval.
  echo "export CGO_CFLAGS=\"$CGO_CFLAGS\""
  echo "export CGO_LDFLAGS=\"$CGO_LDFLAGS\""
else
  echo
  echo "Export these before 'go build':"
  echo
  echo "  export CGO_CFLAGS=\"$CGO_CFLAGS\""
  echo "  export CGO_LDFLAGS=\"$CGO_LDFLAGS\""
fi
