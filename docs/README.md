# lance-go documentation

Narrative guides for the `lance` package. Start with the top-level
[README](../README.md) for installation and a quickstart; the guides below go
deeper on specific areas.

- [usage.md](usage.md): dataset lifecycle, point reads, SQL, mutations, CDC, config, branches, schema evolution, versioning
- [indexes.md](indexes.md): every index type, its parameters and defaults, FTS tokenizers, describe/load/prewarm
- [storage.md](storage.md): URI schemes, per-provider storage options, multi-base writes, emulator testing
- [distributed.md](distributed.md): distributed writes and distributed index builds
- [caching.md](caching.md): `Session`, `CacheBackend`, `ObjectStoreCache`, building a Redis-backed cache
- [callbacks.md](callbacks.md): the Go callback/plugin model, write progress, column UDFs, checkpointing
- [memory.md](memory.md): ownership rules, `Release()` obligations, blobs, callbacks, error contract, threading
- [observability.md](observability.md): OpenTelemetry traces, metrics, and logs; enabling a backend
- [troubleshooting.md](troubleshooting.md): build, link, environment, and callback problems with copy-paste fixes
- [api.md](api.md): generated API reference (godoc for the `lance` package); regenerate with `make docs`, verify with `make docs-check`

See also [../examples/README.md](../examples/README.md) for the runnable
example programs, and [../CONTRIBUTING.md](../CONTRIBUTING.md) for
contributor setup.
