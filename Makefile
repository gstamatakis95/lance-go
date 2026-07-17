ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
RUST_DIR := $(ROOT_DIR)rust
INCLUDE_DIR := $(ROOT_DIR)include
RUST_TARGET_DIR := $(RUST_DIR)/target/release

# lance's prost-build requires protoc. Prefer the system binary; override
# with `make PROTOC=/path/to/protoc ...` if needed.
PROTOC ?= $(shell which protoc)
ifneq ($(PROTOC),)
export PROTOC
endif

# cgo flags for consumers building outside this Makefile (see platform-info).
CGO_CFLAGS := -I$(INCLUDE_DIR)
CGO_LDFLAGS := -L$(RUST_TARGET_DIR)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
# Keep Rust and cgo objects on the same supported deployment target. This is
# also the minimum macOS version advertised by release artifacts.
MACOSX_DEPLOYMENT_TARGET ?= 13.0
export MACOSX_DEPLOYMENT_TARGET
else
endif

.PHONY: all rust rust-test header-check go-build go-test go-test-cgocheck test lint fmt clean platform-info artifacts docs docs-check

all: test

## Build the Rust FFI static library (also regenerates include/lance_go.h).
rust:
	cd $(RUST_DIR) && cargo build --release --locked

## Run Rust unit tests against the same optimized profile shipped to users.
rust-test:
	cd $(RUST_DIR) && cargo test --workspace --release --locked

## Verify the checked-in header matches what cbindgen generates.
header-check: rust
	git -C $(ROOT_DIR) diff --exit-code -- include/lance_go.h

## Compile all Go packages (requires `make rust` first).
go-build:
	go build ./...

## Run Go tests with the race detector (requires `make rust` first).
go-test:
	go test ./... -race -count=1

## Run Go tests with strict cgo pointer checking (requires `make rust` first).
go-test-cgocheck:
	GOEXPERIMENT=cgocheck2 go test ./... -count=1

## Full test suite: native build + Rust and Go tests.
test: rust rust-test go-test go-test-cgocheck

## Lint Rust and Go sources.
lint:
	cd $(RUST_DIR) && cargo fmt --all --check
	cd $(RUST_DIR) && cargo clippy --workspace --all-targets --release --locked -- -D warnings
	@files="$$(gofmt -l $$(rg --files -g '*.go'))"; \
	  test -z "$$files" || { echo "Go files need formatting:"; echo "$$files"; exit 1; }
	go vet ./...

## Format Rust and Go sources.
fmt:
	cd $(RUST_DIR) && cargo fmt --all
	gofmt -w $$(rg --files -g '*.go')

## Remove build artifacts.
clean:
	cd $(RUST_DIR) && cargo clean
	go clean ./...

## Print cgo flags for consumers linking against the FFI library.
platform-info:
	@echo "CGO_CFLAGS=$(CGO_CFLAGS)"
	@echo "CGO_LDFLAGS=$(CGO_LDFLAGS)"
	@echo "PROTOC=$(PROTOC)"
	@if [ "$(UNAME_S)" = "Darwin" ]; then echo "MACOSX_DEPLOYMENT_TARGET=$(MACOSX_DEPLOYMENT_TARGET)"; fi

## Fetch the prebuilt liblance_go artifact for the current platform (latest
## release by default; override with VERSION=vX.Y.Z). Wraps
## scripts/download-artifacts.sh, extracting into lib/{os}_{arch}/ +
## include/ and printing the CGO_CFLAGS / CGO_LDFLAGS to export.
artifacts:
	$(ROOT_DIR)scripts/download-artifacts.sh

# Object-store integration tests (SeaweedFS S3 / Azurite / fake-gcs-server).
# See internal/testutil/docker-compose.objectstore.yml.

OBJECT_STORE_COMPOSE := $(ROOT_DIR)internal/testutil/docker-compose.objectstore.yml
OBJECT_STORE_NETWORK := lance-go-objectstore_default
OBJECT_STORE_BUCKET := lance-test
SEAWEEDFS_ACCESS_KEY := lance-access-key
SEAWEEDFS_SECRET_KEY := lance-secret-key
GCS_ENDPOINT ?= http://localhost:4443
AZURITE_ENDPOINT ?= http://127.0.0.1:10000
# Azurite's well-known devstoreaccount1 account key.
AZURITE_ACCOUNT := devstoreaccount1
AZURITE_KEY := Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

.PHONY: object-store-up object-store-down test-object-store

## Start the object-store emulators and create the test buckets.
object-store-up:
	docker compose -f $(OBJECT_STORE_COMPOSE) up -d --wait
	@echo "creating SeaweedFS S3 bucket $(OBJECT_STORE_BUCKET)..."
	@ok=0; for i in $$(seq 1 30); do \
	  if docker run --rm --network $(OBJECT_STORE_NETWORK) \
	      -e AWS_ACCESS_KEY_ID=$(SEAWEEDFS_ACCESS_KEY) \
	      -e AWS_SECRET_ACCESS_KEY=$(SEAWEEDFS_SECRET_KEY) \
	      -e AWS_REGION=us-east-1 \
	      amazon/aws-cli --endpoint-url http://seaweedfs:8333 \
	      s3 mb s3://$(OBJECT_STORE_BUCKET) >/dev/null 2>&1 \
	    || docker run --rm --network $(OBJECT_STORE_NETWORK) \
	      -e AWS_ACCESS_KEY_ID=$(SEAWEEDFS_ACCESS_KEY) \
	      -e AWS_SECRET_ACCESS_KEY=$(SEAWEEDFS_SECRET_KEY) \
	      -e AWS_REGION=us-east-1 \
	      amazon/aws-cli --endpoint-url http://seaweedfs:8333 \
	      s3api head-bucket --bucket $(OBJECT_STORE_BUCKET) >/dev/null 2>&1; then \
	    ok=1; break; \
	  fi; \
	  echo "  waiting for SeaweedFS S3 gateway ($$i)..."; sleep 2; \
	done; [ $$ok -eq 1 ] || { echo "FAILED to create the SeaweedFS bucket"; exit 1; }
	@echo "creating Azurite container $(OBJECT_STORE_BUCKET)..."
	@DATE=$$(date -u '+%a, %d %b %Y %H:%M:%S GMT'); \
	KEY_HEX=$$(printf '%s' '$(AZURITE_KEY)' | openssl base64 -d -A | xxd -p -c 256); \
	SIGN=$$(printf 'PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:%s\nx-ms-version:2021-08-06\n/$(AZURITE_ACCOUNT)/$(AZURITE_ACCOUNT)/$(OBJECT_STORE_BUCKET)\nrestype:container' "$$DATE" \
	  | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$$KEY_HEX" -binary | openssl base64 -A); \
	STATUS=$$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
	  -H "x-ms-date: $$DATE" -H "x-ms-version: 2021-08-06" \
	  -H "Authorization: SharedKey $(AZURITE_ACCOUNT):$$SIGN" \
	  "$(AZURITE_ENDPOINT)/$(AZURITE_ACCOUNT)/$(OBJECT_STORE_BUCKET)?restype=container"); \
	if [ "$$STATUS" = "201" ] || [ "$$STATUS" = "409" ]; then \
	  echo "  container ready (HTTP $$STATUS)"; \
	else \
	  echo "FAILED to create the Azurite container (HTTP $$STATUS)"; exit 1; \
	fi
	@echo "creating fake-gcs bucket $(OBJECT_STORE_BUCKET)..."
	@curl -sf "$(GCS_ENDPOINT)/storage/v1/b/$(OBJECT_STORE_BUCKET)" >/dev/null 2>&1 \
	  || curl -sf -X POST -H "Content-Type: application/json" \
	       -d '{"name":"$(OBJECT_STORE_BUCKET)"}' \
	       "$(GCS_ENDPOINT)/storage/v1/b?project=lance" >/dev/null \
	  || { echo "FAILED to create the fake-gcs bucket"; exit 1; }
	@echo "object stores ready"

## Stop the object-store emulators and discard their state.
object-store-down:
	docker compose -f $(OBJECT_STORE_COMPOSE) down -v --remove-orphans

## Run the object-store integration tests (starts and stops the emulators).
test-object-store: object-store-up
	LANCE_GO_OBJECT_STORE_TESTS=1 go test ./lance -race -count=1 -run 'TestObjectStore' -v; \
	status=$$?; $(MAKE) object-store-down; exit $$status


## Generate the API reference (docs/api.md) from the package godoc.
docs:
	go run github.com/princjef/gomarkdoc/cmd/gomarkdoc@v1.1.0 \
	  --output docs/api.md \
	  --repository.url https://github.com/gstamatakis95/lance-go \
	  --repository.default-branch main \
	  ./lance

## Verify docs/api.md is up to date with the package godoc (CI gate).
docs-check:
	go run github.com/princjef/gomarkdoc/cmd/gomarkdoc@v1.1.0 \
	  --check \
	  --output docs/api.md \
	  --repository.url https://github.com/gstamatakis95/lance-go \
	  --repository.default-branch main \
	  ./lance
