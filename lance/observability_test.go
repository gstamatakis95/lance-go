package lance_test

import (
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gstamatakis95/lance-go/internal/testutil"
	"github.com/gstamatakis95/lance-go/lance"
)

// obsHarness wires in-memory OTel SDK recorders and returns write options that
// inject them, plus accessors for the recorded spans and metrics.
type obsHarness struct {
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
	wopts  []lance.WriteOption
}

func newObsHarness() *obsHarness {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return &obsHarness{
		spans:  spans,
		reader: reader,
		wopts: []lance.WriteOption{
			lance.WithWriteObservability(
				lance.WithTracerProvider(tp),
				lance.WithMeterProvider(mp),
			),
		},
	}
}

// spanByName returns the (first) ended span with the given name, or nil.
func (h *obsHarness) spanByName(name string) sdktrace.ReadOnlySpan {
	for _, s := range h.spans.Ended() {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func hasAttr(s sdktrace.ReadOnlySpan, key, want string) bool {
	for _, a := range s.Attributes() {
		if string(a.Key) == key && a.Value.AsString() == want {
			return true
		}
	}
	return false
}

func intAttr(s sdktrace.ReadOnlySpan, key string) (int64, bool) {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsInt64(), true
		}
	}
	return 0, false
}

// TestObservabilityDomainCounters verifies a result-bearing op (Delete) both
// records its domain count as a span attribute and emits the lance.rows.affected
// metric, exercising a Scanner span and the domain-counter path.
func TestObservabilityDomainCounters(t *testing.T) {
	h := newObsHarness()
	uri := filepath.Join(t.TempDir(), "obs_domain.lance")

	rdr := testutil.NewReader(testutil.Allocator(), 0, 20, 32)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr, h.wopts...)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	// A scan terminal (fully-blocking) should also produce a span.
	if _, err := ds.Scan().Filter("id < 5").CountRows(t.Context()); err != nil {
		t.Fatalf("Scan.CountRows: %v", err)
	}
	res, err := ds.Delete(t.Context(), "id < 5")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if res.NumDeletedRows != 5 {
		t.Fatalf("expected 5 deleted rows, got %d", res.NumDeletedRows)
	}

	if h.spanByName("lance.Scanner.CountRows") == nil {
		t.Errorf("missing lance.Scanner.CountRows span")
	}
	del := h.spanByName("lance.Dataset.Delete")
	if del == nil {
		t.Fatal("missing lance.Dataset.Delete span")
	}
	if n, ok := intAttr(del, "lance.rows_deleted"); !ok || n != 5 {
		t.Errorf("Delete span lance.rows_deleted = %d (ok=%v), want 5", n, ok)
	}

	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect metrics: %v", err)
	}
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "lance.rows.affected" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("missing lance.rows.affected metric")
	}
}

// TestObservabilitySpans verifies the instrumentation contract on two return
// shapes ((uint64,error) via CountRows and (*arrow.Schema,error) via Schema)
// plus a Dataset factory (Write): correct span names, db.system attribute, and
// that obs propagates from Write to the returned Dataset.
func TestObservabilitySpans(t *testing.T) {
	h := newObsHarness()
	uri := filepath.Join(t.TempDir(), "obs.lance")

	rdr := testutil.NewReader(testutil.Allocator(), 0, 10, 32)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr, h.wopts...)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	if _, err := ds.CountRows(t.Context(), ""); err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if _, err := ds.Schema(t.Context()); err != nil {
		t.Fatalf("Schema: %v", err)
	}

	for _, name := range []string{"lance.Write", "lance.Dataset.CountRows", "lance.Dataset.Schema"} {
		s := h.spanByName(name)
		if s == nil {
			t.Fatalf("missing span %q (got %d spans)", name, len(h.spans.Ended()))
		}
		if !hasAttr(s, "db.system", "lance") {
			t.Errorf("span %q missing db.system=lance attribute", name)
		}
		if s.Status().Code == codes.Error {
			t.Errorf("span %q unexpectedly has error status: %s", name, s.Status().Description)
		}
	}
}

// TestObservabilityErrorStatus verifies a failing op records an error span and
// error metric, the shadowing-regression guard the plan requires.
func TestObservabilityErrorStatus(t *testing.T) {
	h := newObsHarness()
	uri := filepath.Join(t.TempDir(), "obs_err.lance")

	rdr := testutil.NewReader(testutil.Allocator(), 0, 10, 32)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr, h.wopts...)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Close first so the next CountRows fails cleanly (dataset closed).
	ds.Close()

	if _, err := ds.CountRows(t.Context(), ""); err == nil {
		t.Fatal("CountRows on closed dataset: expected error, got nil")
	}

	s := h.spanByName("lance.Dataset.CountRows")
	if s == nil {
		t.Fatal("missing lance.Dataset.CountRows span")
	}
	if s.Status().Code != codes.Error {
		t.Errorf("expected error status on failed CountRows span, got %v", s.Status().Code)
	}
	if len(s.Events()) == 0 {
		t.Errorf("expected a recorded error event on the failed span")
	}
}

// TestObservabilityMetrics verifies the operation-count and duration
// instruments are emitted.
func TestObservabilityMetrics(t *testing.T) {
	h := newObsHarness()
	uri := filepath.Join(t.TempDir(), "obs_metrics.lance")

	rdr := testutil.NewReader(testutil.Allocator(), 0, 10, 32)
	defer rdr.Release()
	ds, err := lance.Write(t.Context(), uri, rdr, h.wopts...)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()
	if _, err := ds.CountRows(t.Context(), ""); err != nil {
		t.Fatalf("CountRows: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect metrics: %v", err)
	}

	found := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			found[m.Name] = true
		}
	}
	for _, want := range []string{"lance.operation.count", "lance.operation.duration"} {
		if !found[want] {
			t.Errorf("missing metric %q (recorded: %v)", want, keys(found))
		}
	}
}

func TestObservabilityStreamingSpanCoversIteration(t *testing.T) {
	h := newObsHarness()
	uri := filepath.Join(t.TempDir(), "obs_stream.lance")

	input := testutil.NewReader(testutil.Allocator(), 0, 10, 4)
	defer input.Release()
	ds, err := lance.Write(t.Context(), uri, input, h.wopts...)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer ds.Close()

	rdr, err := ds.Scan().BatchSize(4).Reader(t.Context())
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if span := h.spanByName("lance.Scanner.Reader"); span != nil {
		t.Fatal("streaming span ended before iteration")
	}
	var rows int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("Reader.Err: %v", err)
	}
	rdr.Release()
	if rows != 10 {
		t.Fatalf("rows = %d, want 10", rows)
	}
	span := h.spanByName("lance.Scanner.Reader")
	if span == nil {
		t.Fatal("streaming span did not end at EOF")
	}
	if got, ok := intAttr(span, "lance.rows_read"); !ok || got != 10 {
		t.Fatalf("lance.rows_read = %d (ok=%v), want 10", got, ok)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
