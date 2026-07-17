package lance_test

import (
	"testing"

	"github.com/gstamatakis95/lance-go/lance"
)

func TestConfigRoundTrip(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	updated, err := ds.UpdateConfig(ctx, map[string]string{"owner": "alice", "team": "data"})
	if err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	if updated["owner"] != "alice" || updated["team"] != "data" {
		t.Fatalf("update returned %v", updated)
	}

	got, err := ds.Config(ctx)
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got["owner"] != "alice" || got["team"] != "data" {
		t.Fatalf("config = %v, want owner=alice team=data", got)
	}

	if err := ds.DeleteConfigKeys(ctx, "team"); err != nil {
		t.Fatalf("DeleteConfigKeys: %v", err)
	}
	got, err = ds.Config(ctx)
	if err != nil {
		t.Fatalf("Config after delete: %v", err)
	}
	if _, ok := got["team"]; ok {
		t.Fatalf("team key still present after delete: %v", got)
	}
	if got["owner"] != "alice" {
		t.Fatalf("owner lost after delete: %v", got)
	}
}

func TestMetadataRoundTrip(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	if _, err := ds.UpdateMetadata(ctx, map[string]string{"created_by": "test"}); err != nil {
		t.Fatalf("UpdateMetadata: %v", err)
	}
	got, err := ds.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if got["created_by"] != "test" {
		t.Fatalf("metadata = %v, want created_by=test", got)
	}
}

func TestSchemaMetadataRoundTrip(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	if _, err := ds.UpdateSchemaMetadata(ctx, map[string]string{"version": "1"}); err != nil {
		t.Fatalf("UpdateSchemaMetadata: %v", err)
	}
	if err := ds.ReplaceSchemaMetadata(ctx, map[string]string{"version": "2", "extra": "x"}); err != nil {
		t.Fatalf("ReplaceSchemaMetadata: %v", err)
	}
	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	md := schema.Metadata()
	if v, ok := md.GetValue("version"); !ok || v != "2" {
		t.Fatalf("schema metadata version = %q (ok=%v), want 2", v, ok)
	}
	if v, ok := md.GetValue("extra"); !ok || v != "x" {
		t.Fatalf("schema metadata extra = %q (ok=%v), want x", v, ok)
	}
}

func TestFieldMetadataRoundTrip(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)

	if err := ds.UpdateFieldMetadata(ctx, "id", map[string]string{"unit": "count"}); err != nil {
		t.Fatalf("UpdateFieldMetadata: %v", err)
	}
	schema, err := ds.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	idx := schema.FieldIndices("id")
	md := schema.Field(idx[0]).Metadata
	if v, ok := md.GetValue("unit"); !ok || v != "count" {
		t.Fatalf("field metadata unit = %q (ok=%v), want count", v, ok)
	}
}

func TestTruncateTable(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 50)

	if err := ds.TruncateTable(ctx); err != nil {
		t.Fatalf("TruncateTable: %v", err)
	}
	n, err := ds.CountRows(ctx, "")
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if n != 0 {
		t.Fatalf("after truncate rows = %d, want 0", n)
	}
}

func TestValidate(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)
	if err := ds.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestIsStaleAndSuccessor(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 10)

	stale, err := ds.IsStale(ctx)
	if err != nil {
		t.Fatalf("IsStale: %v", err)
	}
	if stale {
		t.Fatal("fresh dataset reported stale")
	}

	// Commit a new version from another handle.
	appendRows(t, uri, 10, 5)

	stale, err = ds.IsStale(ctx)
	if err != nil {
		t.Fatalf("IsStale after append: %v", err)
	}
	if !stale {
		t.Fatal("dataset with newer version not reported stale")
	}
	has, err := ds.HasSuccessorVersion(ctx)
	if err != nil {
		t.Fatalf("HasSuccessorVersion: %v", err)
	}
	if !has {
		t.Fatal("expected a successor version")
	}
}

func TestCountDeletedRows(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 20)

	if _, err := ds.Delete(ctx, "id < 5"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	n, err := ds.CountDeletedRows(ctx)
	if err != nil {
		t.Fatalf("CountDeletedRows: %v", err)
	}
	if n != 5 {
		t.Fatalf("deleted rows = %d, want 5", n)
	}
}

func TestPaths(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)
	paths, err := ds.Paths(ctx)
	if err != nil {
		t.Fatalf("Paths: %v", err)
	}
	if paths.URI == "" {
		t.Fatalf("empty uri in %+v", paths)
	}
	if paths.DataDir == "" || paths.VersionsDir == "" {
		t.Fatalf("empty dirs in %+v", paths)
	}
}

func TestCacheStats(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 10)
	// Touch the dataset so caches have something.
	if _, err := ds.CountRows(ctx, ""); err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if _, err := ds.CacheStats(ctx); err != nil {
		t.Fatalf("CacheStats: %v", err)
	}
}

func TestDataStats(t *testing.T) {
	ctx := t.Context()
	_, ds := writeDataset(t, 100)
	stats, err := ds.DataStats(ctx)
	if err != nil {
		t.Fatalf("DataStats: %v", err)
	}
	if len(stats.Fields) == 0 {
		t.Fatal("no field stats returned")
	}
	// The default test schema (v2 storage) should report non-zero bytes.
	var total uint64
	for _, f := range stats.Fields {
		total += f.BytesOnDisk
	}
	if total == 0 {
		t.Fatal("all field byte counts are zero")
	}
}

func TestNumSmallFiles(t *testing.T) {
	ctx := t.Context()
	// Small dataset -> its single file is "small" relative to a big group.
	_, ds := writeDataset(t, 10)
	n, err := ds.NumSmallFiles(ctx, 1024)
	if err != nil {
		t.Fatalf("NumSmallFiles: %v", err)
	}
	if n == 0 {
		t.Fatal("expected at least one small file")
	}
}

func TestCleanupWithPolicy(t *testing.T) {
	ctx := t.Context()
	uri, ds := writeDataset(t, 10)
	// Create more versions.
	appendRows(t, uri, 10, 5)
	appendRows(t, uri, 15, 5)
	if err := ds.CheckoutLatest(ctx); err != nil {
		t.Fatalf("CheckoutLatest: %v", err)
	}

	latest, err := ds.LatestVersion(ctx)
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	// Remove everything before the latest version.
	stats, err := ds.CleanupWithPolicy(ctx, lance.CleanupPolicy{
		BeforeVersion:            latest,
		DeleteUnverified:         true,
		ErrorIfTaggedOldVersions: ptr(true),
	})
	if err != nil {
		t.Fatalf("CleanupWithPolicy: %v", err)
	}
	if stats.OldVersions == 0 {
		t.Fatalf("expected old versions removed, got %+v", stats)
	}
}
