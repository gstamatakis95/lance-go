package lance

// OpenTelemetry instrumentation for lance-go.
//
// This file is the FROZEN CONTRACT that every instrumented method follows.
// It depends only on the OpenTelemetry *API* (never the SDK). The SDK appears
// only in _test.go files. When no OTel SDK is installed by the consuming
// application, the global providers are no-ops, so instrumentation costs
// almost nothing and produces no output.
//
// # How a consumer enables it
//
// Either install a global OTel SDK (otel.SetTracerProvider / SetMeterProvider
// and go.opentelemetry.io/otel/log/global.SetLoggerProvider), picked up
// automatically, or inject providers per call:
//
//	ds, _ := lance.Open(ctx, uri,
//	    lance.WithTracerProvider(tp),
//	    lance.WithMeterProvider(mp),
//	    lance.WithLoggerProvider(lp))
//
// The chosen backend (Jaeger, Prometheus, Datadog, …) lives entirely in the
// consumer's process. This package never imports a vendor SDK.
//
// # The instrumentation pattern
//
// The STANDARD ENTRY for terminal operations is the generic helpers in
// ops.go (datasetOp / datasetDo / fragmentOp). They call obs().start with
// the SAME span name string the method used before ("Dataset.CountRows",
// "Tags.Create", ...) and the same attributes, so span names, attributes and
// metrics are IDENTICAL to the hand-rolled pattern — the contract in this
// file stays frozen; only the plumbing moved:
//
//	func (d *Dataset) CountRows(ctx context.Context, filter string) (uint64, error) {
//	    return datasetOp(ctx, d, "Dataset.CountRows", "count rows",
//	        func(ctx context.Context, ptr *C.LanceDataset) (uint64, error) {
//	            // ffiCall(ctx, ...)-based body; raw (unprefixed) errors.
//	        }, expressionAttribute("filter", filter))
//	}
//
// Ops that cannot use the helpers (streams, result metrics, no-receiver
// constructors — see the ops.go header) keep the original hand-rolled
// pattern, with NAMED RETURNS plus a deferred closer that reads the named
// error:
//
//	func (d *Dataset) TruncateTable(ctx context.Context) (err error) {
//	    ctx, end := d.obs().start(ctx, "Dataset.TruncateTable")
//	    defer func() { end(&err) }()
//	    // ... existing body unchanged. Keep every return EXPLICIT ...
//	}
//
// Shadowing rule (see AGENTS + plan) for the hand-rolled pattern: `end(&err)`
// reads the *named* return `err`. The existing code writes `if err :=
// ffiCall(...); err != nil { return …, fmt.Errorf(...) }`, which works ONLY
// because the explicit return assigns the named return before defers run.
// NEVER introduce a naked return, and NEVER rename `err` in
// `if err := ffiCall(...)`.
//
// Result-bearing ops additionally surface domain counts, as a span attribute
// (via the closer's variadic) and as a metric (via a recordRows/recordBytes
// helper). Because named returns are captured by reference, the result value
// is available at defer time:
//
//	func (d *Dataset) Delete(ctx context.Context, predicate string) (res DeleteResult, err error) {
//	    ctx, end := d.obs().start(ctx, "Dataset.Delete",
//	        expressionAttribute("predicate", predicate))
//	    defer func() {
//	        end(&err, attribute.Int64("lance.rows_deleted", int64(res.NumDeletedRows)))
//	        d.obs().recordRows(ctx, "Dataset.Delete", "deleted", int64(res.NumDeletedRows))
//	    }()
//	    // ... existing body ...
//	}
//
// Handle-returning ops just return newDataset(ptr, <parent>.withObs) so the
// child inherits the same obs config.

import (
	"context"
	"log/slog"
	"net/url"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// scopeName is the OpenTelemetry instrumentation scope for this package.
const scopeName = "github.com/gstamatakis95/lance-go"

// spanPrefix is prepended to the short "<Type>.<Method>" name callers pass to
// start, e.g. "Dataset.CountRows" → span "lance.Dataset.CountRows".
const spanPrefix = "lance."

// datasetURIAttribute records enough location information to distinguish
// datasets without leaking URL credentials, query parameters, or fragments.
func datasetURIAttribute(raw string) attribute.KeyValue {
	return attribute.String("lance.dataset.uri", sanitizeDatasetURI(raw))
}

func sanitizeDatasetURI(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid>"
	}
	if u.Opaque != "" {
		if u.Scheme == "" {
			return "<redacted>"
		}
		return u.Scheme + ":<redacted>"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return u.String()
}

// expressionAttribute avoids putting user data from SQL expressions in
// telemetry while retaining a useful signal for debugging query shape.
func expressionAttribute(kind, expression string) attribute.KeyValue {
	return attribute.Int("lance."+kind+".length", len(expression))
}

// obsConfig collects the OpenTelemetry providers a handle was configured with.
// A nil provider means "use the global default", which is a no-op until the
// application installs an SDK.
type obsConfig struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	loggerProvider otellog.LoggerProvider
}

// obs holds the resolved tracer, logger and metric instruments for a handle.
// It is created once per provider-resolving entry point (Open, Write,
// NewSession, …) via newObs and then shared (by pointer) with every child
// handle. All methods are safe to call on a nil *obs (they become no-ops), so
// handles whose obs was never resolved never panic.
type obs struct {
	tracer trace.Tracer
	logger *slog.Logger

	opCount     metric.Int64Counter
	opDuration  metric.Float64Histogram
	rowsCounter metric.Int64Counter
	byteCounter metric.Int64Counter
}

// newObs resolves providers (falling back to the OTel globals) and builds the
// metric instruments exactly once. It always returns a non-nil *obs. When no
// SDK is installed the underlying tracer/meter/logger are no-ops.
func newObs(cfg obsConfig) *obs {
	tp := cfg.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := cfg.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	logOpts := []otelslog.Option{otelslog.WithSource(false)}
	lp := cfg.loggerProvider
	if lp == nil {
		lp = logglobal.GetLoggerProvider()
	}
	logOpts = append(logOpts, otelslog.WithLoggerProvider(lp))

	o := &obs{
		tracer: tp.Tracer(scopeName),
		logger: otelslog.NewLogger(scopeName, logOpts...),
	}

	meter := mp.Meter(scopeName)
	// The OTel metric API guarantees a usable (no-op on error) instrument even
	// when construction returns an error, and all names/units below are static
	// and valid, so ignoring the errors here cannot leave a nil instrument.
	o.opCount, _ = meter.Int64Counter(
		"lance.operation.count",
		metric.WithDescription("Number of lance operations invoked."),
		metric.WithUnit("{operation}"),
	)
	o.opDuration, _ = meter.Float64Histogram(
		"lance.operation.duration",
		metric.WithDescription("Duration of lance operations."),
		metric.WithUnit("s"),
	)
	o.rowsCounter, _ = meter.Int64Counter(
		"lance.rows.affected",
		metric.WithDescription("Rows affected by lance operations (inserted, updated, deleted, …)."),
		metric.WithUnit("{row}"),
	)
	o.byteCounter, _ = meter.Int64Counter(
		"lance.bytes.written",
		metric.WithDescription("Bytes written to storage by lance operations."),
		metric.WithUnit("By"),
	)
	return o
}

// newObsFromOptions applies ObsOptions to a fresh obsConfig and resolves it.
// Convenience for constructors that take ObsOptions directly (e.g. NewSession).
func newObsFromOptions(opts []ObsOption) *obs {
	var cfg obsConfig
	for _, o := range opts {
		o(&cfg)
	}
	return newObs(cfg)
}

// start opens a span for op (named "lance.<op>"), begins timing, and returns
// the span-bearing context plus a closer. The closer must be deferred. It sets
// span status from *err, records the op-count and duration metrics, appends any
// extra span attributes, and logs (ERROR on failure, DEBUG otherwise). Passing
// a nil *errp is treated as success.
//
// start is safe to call on a nil *obs, in which case it returns ctx unchanged
// and a no-op closer.
func (o *obs) start(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, func(err *error, extra ...attribute.KeyValue)) {
	if o == nil {
		return ctx, func(*error, ...attribute.KeyValue) {}
	}
	begin := time.Now()
	spanAttrs := make([]attribute.KeyValue, 0, len(attrs)+2)
	spanAttrs = append(spanAttrs,
		attribute.String("db.system", "lance"),
		attribute.String("db.operation.name", op),
	)
	spanAttrs = append(spanAttrs, attrs...)
	ctx, span := o.tracer.Start(ctx, spanPrefix+op, trace.WithAttributes(spanAttrs...))

	if o.logger.Enabled(ctx, slog.LevelDebug) {
		o.logger.DebugContext(ctx, "lance operation start", slog.String("operation", op))
	}

	return ctx, func(errp *error, extra ...attribute.KeyValue) {
		elapsed := time.Since(begin)
		var err error
		if errp != nil {
			err = *errp
		}

		status := "ok"
		if err != nil {
			status = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		if len(extra) > 0 {
			span.SetAttributes(extra...)
		}
		span.End()

		mattrs := metric.WithAttributes(
			attribute.String("operation", op),
			attribute.String("status", status),
		)
		o.opCount.Add(ctx, 1, mattrs)
		o.opDuration.Record(ctx, elapsed.Seconds(), mattrs)

		switch {
		case err != nil:
			o.logger.ErrorContext(ctx, "lance operation failed",
				slog.String("operation", op),
				slog.Duration("elapsed", elapsed),
				slog.Any("error", err))
		case o.logger.Enabled(ctx, slog.LevelDebug):
			o.logger.DebugContext(ctx, "lance operation finish",
				slog.String("operation", op),
				slog.Duration("elapsed", elapsed))
		}
	}
}

// recordRows increments the rows-affected counter for op, tagged with kind
// ("inserted", "updated", "deleted", …). Safe on a nil *obs.
func (o *obs) recordRows(ctx context.Context, op, kind string, n int64) {
	if o == nil || n == 0 {
		return
	}
	o.rowsCounter.Add(ctx, n, metric.WithAttributes(
		attribute.String("operation", op),
		attribute.String("kind", kind),
	))
}

// recordBytes increments the bytes-written counter for op. Safe on a nil *obs.
func (o *obs) recordBytes(ctx context.Context, op string, n int64) {
	if o == nil || n == 0 {
		return
	}
	o.byteCounter.Add(ctx, n, metric.WithAttributes(
		attribute.String("operation", op),
	))
}
