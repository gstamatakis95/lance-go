// Command observability demonstrates lance-go's OpenTelemetry instrumentation.
//
// Every lance operation emits a span named lance.<Type>.<Method> (e.g.
// lance.Write, lance.Dataset.CountRows, lance.Scanner.CountRows) plus the
// metrics lance.operation.count, lance.operation.duration, lance.rows.affected
// and lance.bytes.written. You attach your own OpenTelemetry providers at
// construction time. If you attach none, the OTel globals (no-op) are used.
//
// This example wires up the OTel *stdout* exporters so you can see the spans
// and metrics printed to your terminal. To ship this telemetry to Datadog (or
// any OTLP backend), you swap ONLY the exporter: replace the stdouttrace /
// stdoutmetric exporters below with the OTLP exporters
// (go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc and
// .../otlpmetric/otlpmetricgrpc) pointed at your Datadog Agent's OTLP endpoint
// (default localhost:4317). Nothing in lance-go changes, since the library only
// speaks the vendor-neutral OpenTelemetry API.
//
// Usage: go run ./examples/observability
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/gstamatakis95/lance-go/lance"
)

// newRecordReader builds rows records (id int64, name utf8, score float64)
// starting at startID. The buffers are allocated with lance.Allocator, as
// required by lance.Write (their buffers cross the Arrow C Data Interface).
func newRecordReader(startID, rows int64) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(lance.Allocator(), schema)
	defer b.Release()
	for i := int64(0); i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(startID + i)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", startID+i))
		b.Field(2).(*array.Float64Builder).Append(float64(startID+i) / 2.0)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

func main() {
	ctx := context.Background()

	// --- 1. Build a real OpenTelemetry SDK backed by the stdout exporters. ---

	// A resource identifies this service in the emitted telemetry.
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "lance-go-observability-example")),
	)
	if err != nil {
		log.Fatalf("resource.New: %v", err)
	}

	// Tracer provider: stdouttrace prints each finished span as JSON.
	traceExp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatalf("stdouttrace.New: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)

	// Meter provider: stdoutmetric prints the collected metrics as JSON. A
	// PeriodicReader also flushes on Shutdown, so we get one final export.
	metricExp, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		log.Fatalf("stdoutmetric.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			metricExp,
			sdkmetric.WithInterval(30*time.Second),
		)),
		sdkmetric.WithResource(res),
	)

	// Flush and shut both providers down before the process exits, so all
	// buffered spans and metrics are exported to stdout.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Printf("tracer provider shutdown: %v", err)
		}
		if err := mp.Shutdown(shutdownCtx); err != nil {
			log.Printf("meter provider shutdown: %v", err)
		}
	}()

	// --- 2. Write a dataset with those providers attached. ---

	dir, err := os.MkdirTemp("", "lance-observability-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	uri := filepath.Join(dir, "example.lance")

	rdr := newRecordReader(0, 100)
	// WithWriteObservability wires our providers into the returned dataset and
	// every handle derived from it (scanners, etc.). This Write emits a
	// lance.Write span and bumps the operation/rows/bytes metrics.
	ds, err := lance.Write(ctx, uri, rdr,
		lance.WithWriteObservability(
			lance.WithTracerProvider(tp),
			lance.WithMeterProvider(mp),
		),
	)
	rdr.Release()
	if err != nil {
		log.Fatalf("Write: %v", err)
	}
	defer ds.Close()
	fmt.Printf("wrote dataset at %s\n", uri)

	// --- 3. Run a few instrumented operations. Each emits its own span. ---

	// lance.Dataset.CountRows
	count, err := ds.CountRows(ctx, "")
	if err != nil {
		log.Fatalf("CountRows: %v", err)
	}
	fmt.Printf("total rows: %d\n", count)

	// lance.Dataset.Schema
	schema, err := ds.Schema(ctx)
	if err != nil {
		log.Fatalf("Schema: %v", err)
	}
	fmt.Printf("schema has %d fields\n", len(schema.Fields()))

	// lance.Scanner.CountRows (the scanner inherits the dataset's providers).
	filtered, err := ds.Scan().Filter("score >= 45").CountRows(ctx)
	if err != nil {
		log.Fatalf("Scanner.CountRows: %v", err)
	}
	fmt.Printf("rows with score >= 45: %d\n", filtered)

	// lance.Dataset.Delete
	del, err := ds.Delete(ctx, "score < 10")
	if err != nil {
		log.Fatalf("Delete: %v", err)
	}
	fmt.Printf("deleted %d rows\n", del.NumDeletedRows)

	fmt.Println("\n--- OpenTelemetry spans and metrics follow (on shutdown) ---")
	// The deferred tp.Shutdown / mp.Shutdown calls flush the spans and metrics
	// to stdout as the program exits.
}
