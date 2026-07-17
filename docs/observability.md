# Observability

lance-go is instrumented with [OpenTelemetry](https://opentelemetry.io/):
traces, metrics, and logs for every terminal operation. The library depends
only on the OpenTelemetry API at runtime; the production `lance` package does
not import the SDK. Until your application installs an SDK, all instrumentation
resolves to the OTel globals, which are no-ops: no output and near-zero cost.

The chosen backend (Jaeger, Prometheus, Datadog, …) lives entirely in your
process. The repository's root `go.mod` does list the vendor-neutral OTel SDK
and stdout exporters because package tests and `examples/observability` use
them. They therefore appear in the module graph, but are not imported or linked
by the production `lance` package. No backend-specific SDK is required.

## Enabling it

There are two ways to wire providers in. They compose.

### 1. Global SDK (zero-config)

Install an OTel SDK once at startup with the usual global setters. Every
lance-go handle picks them up automatically. No per-call options needed.

```go
import (
	"go.opentelemetry.io/otel"
	logglobal "go.opentelemetry.io/otel/log/global"
	// ... your SDK providers (tracer, meter, logger) ...
)

otel.SetTracerProvider(tracerProvider)
otel.SetMeterProvider(meterProvider)
logglobal.SetLoggerProvider(loggerProvider)

ds, err := lance.Open(ctx, uri) // instrumented via the globals
```

### 2. Per-constructor injection

Pass providers explicitly at the entry points that resolve them. Every handle
derived from that entry point (scanners, tags, commit builders, …) inherits the
same providers.

```go
ds, err := lance.Open(ctx, uri,
	lance.WithObservability(
		lance.WithTracerProvider(tp),
		lance.WithMeterProvider(mp),
		lance.WithLoggerProvider(lp),
	))

ds, err := lance.Write(ctx, uri, rdr,
	lance.WithWriteObservability(
		lance.WithTracerProvider(tp)))

sess, err := lance.NewSession(cfg,
	lance.WithTracerProvider(tp),
	lance.WithMeterProvider(mp))
```

- `lance.WithObservability(...)`: an `OpenOption` for `lance.Open`.
- `lance.WithWriteObservability(...)`: a `WriteOption` for `lance.Write`.
- `lance.NewSession` / `lance.NewSessionWithCacheBackend` take `...ObsOption`
  directly.

The three `ObsOption`s (`WithTracerProvider`, `WithMeterProvider`,
`WithLoggerProvider`) are independent. Any provider you omit falls back to its
OTel global, so you can inject only the signals you care about. Injected
providers take precedence over the globals.

## Zero-cost default

If you configure nothing, the globals stay no-op: spans are never sampled,
metrics are never recorded, and debug logs are suppressed. Instrumentation adds
a handful of nil-provider calls per operation and nothing else. You pay only
once you install an SDK.

## Traces

Every terminal operation opens exactly one span, named `lance.<Type>.<Method>`.
A failed operation sets the span status to `Error` and records the error as a
span event. Handle-returning constructors (e.g. `Open`, `Checkout`) propagate
the same providers to the child handle rather than opening a nested span.

Representative span names:

| Span | Operation |
| --- | --- |
| `lance.Open` | Open a dataset |
| `lance.Write` | Write a dataset |
| `lance.Dataset.CountRows` | Filtered/total row count |
| `lance.Dataset.Delete` | Delete rows |
| `lance.Scanner.CountRows` | Count over a configured scan |
| `lance.Scanner.Batch` | Materialize a single scan batch |
| `lance.Tags.Create` | Create a tag |
| `lance.CommitBuilder.Execute` | Commit a transaction |

Common span attributes:

| Attribute | Meaning |
| --- | --- |
| `db.system` | Always `lance` |
| `db.operation.name` | The short `<Type>.<Method>` operation name |
| `lance.dataset.uri` | Dataset URI with credentials, query, and fragment removed |
| `lance.filter.length` | Predicate byte length; predicate contents are never recorded |
| `lance.column` | Target column |
| `lance.index_name` | Index name |
| `lance.branch` | Branch name |
| `lance.tag` | Tag name |

Result-bearing operations add a domain-count attribute (e.g.
`lance.rows_deleted` on `Dataset.Delete`).

Dataset URLs and SQL expressions are treated as potentially sensitive. URI
userinfo, query parameters, and fragments are stripped before recording, and
only expression lengths are emitted.

## Metrics

Recorded on the injected (or global) MeterProvider under the instrumentation
scope `github.com/gstamatakis95/lance-go`:

| Metric | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `lance.operation.count` | counter | `{operation}` | `operation`, `status` (`ok`/`error`) |
| `lance.operation.duration` | histogram | `s` | `operation`, `status` |
| `lance.rows.affected` | counter | `{row}` | `operation`, `kind` (`inserted`/`updated`/`deleted`/…) |
| `lance.bytes.written` | counter | `By` | `operation` |

`lance.operation.count` and `lance.operation.duration` are recorded for **every**
instrumented operation. `lance.rows.affected` and `lance.bytes.written` are
recorded only by operations with a meaningful row/byte result.

## Per-scan execution stats (`WithScanStats`)

Independent of OpenTelemetry, the scan builder can report Lance's native
per-scan execution counters. Register a callback with
`Scanner.WithScanStats(fn)`; it receives a `ScanStats` once per report.

In lance 8.0.0, the report is delivered **only** by the streaming terminals
(`Reader`, and therefore the `Scanner.All` iterator) and by `Batch`.
`CountRows`, `Explain`, `AnalyzePlan`, and `AnalyzeCountPlan` accept the
option without error, but upstream does not propagate the callback on those
code paths, so they deliver no report. For `Reader`/`All`, `fn` fires on the
consuming goroutine's thread when the stream is exhausted, while the
reader's internal lock is held: `fn` must not touch that reader (`Next`,
`RecordBatch`, `Release`), or it will deadlock, and an early `Release()`
(before exhaustion) delivers no report. For `Batch`, `fn` fires before the
call returns.

```go
rdr, err := ds.Scan().
	Filter("score > 0.5").
	WithScanStats(func(st lance.ScanStats) {
		log.Printf("scan: iops=%d requests=%d bytes=%d index_comparisons=%d",
			st.IOPS, st.Requests, st.BytesRead, st.IndexComparisons)
	}).
	Reader(ctx)
```

`ScanStats` carries `IOPS`, `Requests`, `BytesRead`, `IndicesLoaded`,
`PartsLoaded`, and `IndexComparisons`, plus `AllCounts`/`AllTimes` maps of
additional, upstream-unstable debugging metrics. The callback is
fire-and-forget: reporting is best-effort, errors are swallowed, and it
**must not re-enter lance-go** (a re-entrant call is rejected with
`ErrReentrantCall` and silently dropped; the scan still completes). To feed
the numbers into your metrics pipeline, record them onto your own
instruments inside the callback.

## Logs

Logs flow through the OTel LoggerProvider via the
[`otelslog`](https://pkg.go.dev/go.opentelemetry.io/contrib/bridges/otelslog)
bridge, so they land in the same pipeline as your traces and metrics and carry
the active span context:

- **ERROR** on operation failure, with the operation name, elapsed time, and
  the error.
- **DEBUG** at operation start and finish. Debug records are emitted only when
  the LoggerProvider has debug enabled, so a production configuration is silent
  by default.

## Backends

Because lance-go speaks only the OTel API, any OTel-compatible backend works
without touching the library. Two Datadog recipes follow. The same shape
applies to Jaeger, Prometheus, Grafana Tempo, and so on.

**Invariant:** no vendor-specific dependency lives in the `lance` package.
Backend/exporter imports for an application belong in *your* `main`; the
stdout exporters in this repository are test/example dependencies only.

### Datadog (recommended): OTLP → Datadog Agent

Export via OTLP to the Datadog Agent's OTLP intake. The only dependency is the
out-of-process Agent. Nothing vendor-specific enters your Go build.

```go
// in your main, at startup:
import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

exp, _ := otlptracegrpc.New(ctx, // → Datadog Agent OTLP intake, e.g. localhost:4317
	otlptracegrpc.WithEndpoint("localhost:4317"), otlptracegrpc.WithInsecure())
tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
otel.SetTracerProvider(tp)
// lance-go now emits to Datadog via the Agent.
```

Enable the OTLP receiver in the Agent (`otlp_config.receiver`) and, for the
full picture, wire a MeterProvider (OTLP metrics) and LoggerProvider the same
way.

### Datadog: existing dd-trace-go/APM users

If you already run [dd-trace-go](https://github.com/DataDog/dd-trace-go), it
exposes an OpenTelemetry `TracerProvider`. Construct it per the dd-trace-go docs
(the import path differs across major versions) and pass it straight into
`WithTracerProvider`. The dd-trace-go import stays in your `main`, never in
lance-go:

```go
import ddotel "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentelemetry"

provider := ddotel.NewTracerProvider() // check the constructor for your dd-trace-go version
defer provider.Shutdown()

ds, _ := lance.Open(ctx, uri,
	lance.WithObservability(lance.WithTracerProvider(provider)))
```

lance-go spans then appear in Datadog APM alongside the rest of your traces.

## Known limitations

Instrumentation covers Go-side terminal operations. Three areas are outside
that scope.

1. **Native scan counters are not emitted.** Operations that return a lazy
   `array.RecordReader` (`Scanner.Reader`, `Dataset.TakeScan`,
   `SQLBuilder.Reader`, the `DeltaBuilder` `InsertedRows`/`UpdatedRows`/
   `UpsertedRows` readers, `FragmentScanner.Reader`, and `ReadIndexPartition`)
   carry a span across iteration. The span ends on EOF, error, context
   cancellation, or `Release`, and records `lance.rows_read`. Fully materialized reads
   (`Scanner.Batch`, `Take`, `Sample`) and `CountRows`/`Explain`/`AnalyzePlan`
   **are** instrumented and their spans cover the whole operation. Lance's
   native per-scan counters (iops, bytes read, index comparisons) are not
   emitted as OTel metrics, but are available programmatically via
   `Scanner.WithScanStats` (see above).

2. **Rust→Go callbacks are not auto-linked to the operation span.** No
   `context.Context` crosses the FFI boundary, so the span/context of an
   operation does not reach a user callback (the `AddColumnsUDF` mapper, write
   progress, cache/checkpoint plugins). The operation itself (e.g.
   `AddColumnsUDF`) is instrumented. The per-callback invocations are not.

3. **Distributed flows are not auto-correlated into one trace.** In a
   distributed write (`WriteFragments` on workers → `CommitBuilder.Execute` on a
   driver) the steps run in different processes. Each gets its own span, but
   correlating them into a single trace requires your application to propagate
   trace context alongside the shipped `Transaction` bytes (as span links).
   There is no in-process parent to inherit. See
   [distributed.md](distributed.md).

## See also

- [`examples/observability`](../examples/observability): a runnable program
  wiring an SDK and emitting spans/metrics/logs.
- [usage.md](usage.md): the operations being instrumented.
