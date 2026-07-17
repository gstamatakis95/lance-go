package lance

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
)

// Arrow field-metadata keys understood by the Lance encoder. Set them on a
// column's arrow.Field before Write to control on-disk encoding. Values must
// match the exact strings the Lance encoder recognizes (see the Compression*
// and StructuralEncoding* constants). These mirror
// lance-encoding/src/constants.rs and lance-arrow BLOB_META_KEY.
const (
	// CompressionMetaKey selects the column compression codec (one of the
	// Compression* constants).
	CompressionMetaKey = "lance-encoding:compression"
	// CompressionLevelMetaKey sets the codec compression level (integer).
	CompressionLevelMetaKey = "lance-encoding:compression-level"
	// RLEThresholdMetaKey sets the run-length-encoding threshold.
	RLEThresholdMetaKey = "lance-encoding:rle-threshold"
	// StructuralEncodingMetaKey selects the structural encoding (one of the
	// StructuralEncoding* constants).
	StructuralEncodingMetaKey = "lance-encoding:structural-encoding"
	// PackedMetaKey requests packed struct encoding when set to "true".
	PackedMetaKey = "lance-encoding:packed"
	// BSSMetaKey controls byte-stream-split (one of the BSS* constants).
	BSSMetaKey = "lance-encoding:bss"
	// DictDivisorMetaKey, DictSizeRatioMetaKey,
	// DictValuesCompressionMetaKey and DictValuesCompressionLevelMetaKey tune
	// dictionary encoding.
	DictDivisorMetaKey                = "lance-encoding:dict-divisor"
	DictSizeRatioMetaKey              = "lance-encoding:dict-size-ratio"
	DictValuesCompressionMetaKey      = "lance-encoding:dict-values-compression"
	DictValuesCompressionLevelMetaKey = "lance-encoding:dict-values-compression-level"
	// BlobMetaKey marks a large-binary column as a blob column when set to
	// "true" (matches lance-arrow BLOB_META_KEY).
	BlobMetaKey = "lance-encoding:blob"
)

// Column compression codecs (values for CompressionMetaKey).
const (
	// CompressionNone disables compression for the column.
	CompressionNone = "none"
	// CompressionLZ4 selects the LZ4 codec.
	CompressionLZ4 = "lz4"
	// CompressionZstd selects the Zstandard codec.
	CompressionZstd = "zstd"
	// CompressionFsst selects the FSST codec (string-oriented compression).
	CompressionFsst = "fsst"
)

// Structural encodings (values for StructuralEncodingMetaKey).
const (
	// StructuralEncodingMiniblock selects the miniblock structural encoding.
	StructuralEncodingMiniblock = "miniblock"
	// StructuralEncodingFullzip selects the fullzip structural encoding.
	StructuralEncodingFullzip = "fullzip"
)

// Byte-stream-split modes (values for BSSMetaKey).
const (
	// BSSOff disables byte-stream-split.
	BSSOff = "off"
	// BSSOn forces byte-stream-split.
	BSSOn = "on"
	// BSSAuto lets the encoder decide whether to apply byte-stream-split.
	BSSAuto = "auto"
)

// CompressionOption tunes SetFieldCompression.
type CompressionOption func(map[string]string)

// WithCompressionLevel sets the codec compression level.
func WithCompressionLevel(level int) CompressionOption {
	return func(m map[string]string) { m[CompressionLevelMetaKey] = strconv.Itoa(level) }
}

// WithRLEThreshold sets the run-length-encoding threshold.
func WithRLEThreshold(threshold float64) CompressionOption {
	return func(m map[string]string) {
		m[RLEThresholdMetaKey] = strconv.FormatFloat(threshold, 'g', -1, 64)
	}
}

// SetFieldMetadata returns field with kv merged into its Arrow metadata
// (existing keys are overwritten, others preserved).
func SetFieldMetadata(field arrow.Field, kv map[string]string) arrow.Field {
	merged := make(map[string]string, field.Metadata.Len()+len(kv))
	keys := field.Metadata.Keys()
	vals := field.Metadata.Values()
	for i, k := range keys {
		merged[k] = vals[i]
	}
	for k, v := range kv {
		merged[k] = v
	}
	field.Metadata = arrow.MetadataFrom(merged)
	return field
}

// SetFieldCompression returns field with the compression codec (a Compression*
// constant) and any options applied as Arrow metadata. Existing metadata is
// preserved.
func SetFieldCompression(field arrow.Field, codec string, opts ...CompressionOption) arrow.Field {
	m := map[string]string{CompressionMetaKey: codec}
	for _, opt := range opts {
		opt(m)
	}
	return SetFieldMetadata(field, m)
}

// SetFieldStructuralEncoding returns field with the structural encoding (a
// StructuralEncoding* constant) applied.
func SetFieldStructuralEncoding(field arrow.Field, encoding string) arrow.Field {
	return SetFieldMetadata(field, map[string]string{StructuralEncodingMetaKey: encoding})
}

// MarkBlobColumn returns field marked as a Lance blob column. Use on a
// large_binary column to store its values as external blobs.
func MarkBlobColumn(field arrow.Field) arrow.Field {
	return SetFieldMetadata(field, map[string]string{BlobMetaKey: "true"})
}
