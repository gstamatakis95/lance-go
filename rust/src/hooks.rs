//! Batch-UDF `add_columns` bridged to a Go mapper function, with an optional
//! Go-backed [`UDFCheckpointStore`].
//!
//! Per input [`RecordBatch`], the Rust mapper exports the batch across the
//! Arrow C Data Interface, hands the (input, output) struct pointers to the Go
//! plugin, and imports the computed batch back. RecordBatch payloads travel as
//! borrowed C-Data pointers packed into the plugin byte payload, while fragments
//! (for the checkpoint store) travel as JSON.

use std::ffi::c_char;
use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{ArrowError, Schema as ArrowSchema};
use lance::Result as LanceResult;
use lance::dataset::{BatchInfo, BatchUDF, NewColumnTransform, UDFCheckpointStore};
use lance_table::format::Fragment;
use serde::Deserialize;

use crate::callbacks::GoPlugin;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// The BatchUDF mapper closure type expected by `lance::dataset::BatchUDF`.
type MapperFn = Box<dyn Fn(&RecordBatch) -> LanceResult<RecordBatch> + Send + Sync>;

/// Method discriminator for the Go UDF mapper plugin (see `lance/hooks.go`).
/// Payload is 4 little-endian u64 C-Data pointers:
/// `in_array | in_schema | out_array | out_schema`.
const UDF_MAP: i32 = 0;

/// Method discriminators for the Go checkpoint-store plugin.
const CKPT_GET_BATCH: i32 = 0;
const CKPT_INSERT_BATCH: i32 = 1;
const CKPT_GET_FRAGMENT: i32 = 2;
const CKPT_INSERT_FRAGMENT: i32 = 3;

/// Exports a record batch as a C-Data (array, schema) pair.
fn export_batch(batch: &RecordBatch) -> Result<(FFI_ArrowArray, FFI_ArrowSchema), ArrowError> {
    let struct_array: StructArray = batch.clone().into();
    arrow::ffi::to_ffi(&struct_array.into_data())
}

/// Imports a record batch from a C-Data (array, schema) pair, consuming the
/// array.
fn import_batch(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> Result<RecordBatch, ArrowError> {
    // SAFETY: `array`/`schema` are a valid C-Data pair produced by Go.
    let data = unsafe { arrow::ffi::from_ffi(array, schema) }?;
    Ok(RecordBatch::from(StructArray::from(data)))
}

/// Packs four C-Data struct pointers into the 32-byte UDF payload.
fn pack_ptrs(
    in_arr: *mut FFI_ArrowArray,
    in_sch: *mut FFI_ArrowSchema,
    out_arr: *mut FFI_ArrowArray,
    out_sch: *mut FFI_ArrowSchema,
) -> [u8; 32] {
    let mut p = [0u8; 32];
    p[0..8].copy_from_slice(&(in_arr as usize as u64).to_le_bytes());
    p[8..16].copy_from_slice(&(in_sch as usize as u64).to_le_bytes());
    p[16..24].copy_from_slice(&(out_arr as usize as u64).to_le_bytes());
    p[24..32].copy_from_slice(&(out_sch as usize as u64).to_le_bytes());
    p
}

/// Runs the Go mapper on one input batch: export input, call Go, import output.
fn call_go_mapper(plugin: GoPlugin, batch: &RecordBatch) -> LanceResult<RecordBatch> {
    let (mut in_arr, mut in_sch) = export_batch(batch)?;
    let mut out_arr = FFI_ArrowArray::empty();
    let mut out_sch = FFI_ArrowSchema::empty();
    let payload = pack_ptrs(&mut in_arr, &mut in_sch, &mut out_arr, &mut out_sch);
    // Sync mapper: call Go directly (not via the blocking pool). Go consumes
    // the input pair (nulling its release) and populates the output pair.
    plugin
        .call(UDF_MAP, &payload)
        .map_err(|e| ArrowError::ComputeError(format!("udf mapper failed: {}", e.0)))?;
    let out_arr = std::mem::replace(&mut out_arr, FFI_ArrowArray::empty());
    Ok(import_batch(out_arr, &out_sch)?)
}

/// A [`UDFCheckpointStore`] backed by a Go plugin.
#[derive(Debug)]
struct GoCheckpointStore {
    plugin: GoPlugin,
}

/// Packs `(fragment_id, batch_index)` plus two out-pointers or in-pointers.
fn pack_batch_ckpt(
    fragment_id: u32,
    batch_index: usize,
    arr: *mut FFI_ArrowArray,
    sch: *mut FFI_ArrowSchema,
) -> Vec<u8> {
    let mut p = Vec::with_capacity(28);
    p.extend_from_slice(&fragment_id.to_le_bytes());
    p.extend_from_slice(&(batch_index as u64).to_le_bytes());
    p.extend_from_slice(&(arr as usize as u64).to_le_bytes());
    p.extend_from_slice(&(sch as usize as u64).to_le_bytes());
    p
}

impl UDFCheckpointStore for GoCheckpointStore {
    fn get_batch(&self, info: &BatchInfo) -> LanceResult<Option<RecordBatch>> {
        let mut out_arr = FFI_ArrowArray::empty();
        let mut out_sch = FFI_ArrowSchema::empty();
        let payload = pack_batch_ckpt(
            info.fragment_id,
            info.batch_index,
            &mut out_arr,
            &mut out_sch,
        );
        match self.plugin.call(CKPT_GET_BATCH, &payload) {
            Ok(Some(_)) => {
                let out_arr = std::mem::replace(&mut out_arr, FFI_ArrowArray::empty());
                Ok(Some(import_batch(out_arr, &out_sch)?))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                Err(ArrowError::ComputeError(format!("checkpoint get_batch: {}", e.0)).into())
            }
        }
    }

    fn insert_batch(&self, info: BatchInfo, batch: RecordBatch) -> LanceResult<()> {
        let (mut in_arr, mut in_sch) = export_batch(&batch)?;
        let payload = pack_batch_ckpt(info.fragment_id, info.batch_index, &mut in_arr, &mut in_sch);
        self.plugin
            .call(CKPT_INSERT_BATCH, &payload)
            .map_err(|e| ArrowError::ComputeError(format!("checkpoint insert_batch: {}", e.0)))?;
        Ok(())
    }

    fn get_fragment(&self, fragment_id: u32) -> LanceResult<Option<Fragment>> {
        match self
            .plugin
            .call(CKPT_GET_FRAGMENT, &fragment_id.to_le_bytes())
        {
            Ok(Some(bytes)) => {
                let frag: Fragment = serde_json::from_slice(&bytes).map_err(|e| {
                    ArrowError::ComputeError(format!("checkpoint get_fragment decode: {e}"))
                })?;
                Ok(Some(frag))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                Err(ArrowError::ComputeError(format!("checkpoint get_fragment: {}", e.0)).into())
            }
        }
    }

    fn insert_fragment(&self, fragment: Fragment) -> LanceResult<()> {
        let json = serde_json::to_vec(&fragment).map_err(|e| {
            ArrowError::ComputeError(format!("checkpoint insert_fragment encode: {e}"))
        })?;
        self.plugin.call(CKPT_INSERT_FRAGMENT, &json).map_err(|e| {
            ArrowError::ComputeError(format!("checkpoint insert_fragment: {}", e.0))
        })?;
        Ok(())
    }
}

/// Optional `read_columns` JSON: `{"read_columns"?: [string]}`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct UdfOptions {
    read_columns: Option<Vec<String>>,
}

/// Adds columns computed by a Go batch-UDF, committing a new version.
///
/// - `ds`: dataset handle, updated in place to the new version.
/// - `output_schema`: Arrow C schema of the new columns (ownership taken).
/// - `udf_plugin_handle`: Go mapper plugin handle.
/// - `options_json`: `{"read_columns"?: [string]}` or NULL.
/// - `batch_size`: rows per UDF batch (0 = default).
/// - `checkpoint_plugin_handle`: Go checkpoint-store plugin handle (0 = none).
///
/// # Safety
///
/// `ds` must be a valid handle, `output_schema` a valid unmoved Arrow C schema
/// (always consumed), and the plugin handles live (or 0 for the checkpoint).
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_add_columns_udf(
    ds: *mut LanceDataset,
    output_schema: *mut FFI_ArrowSchema,
    udf_plugin_handle: usize,
    options_json: *const c_char,
    batch_size: u32,
    checkpoint_plugin_handle: usize,
) -> i32 {
    // Take ownership of the schema FIRST so it is released on every path.
    let schema = if output_schema.is_null() {
        return set_error(ErrorCode::InvalidArgument, "output_schema must not be NULL");
    } else {
        // SAFETY: replace moves the contents out, leaving an empty struct.
        let ffi = unsafe { std::ptr::replace(output_schema, FFI_ArrowSchema::empty()) };
        match ArrowSchema::try_from(&ffi) {
            Ok(schema) => Arc::new(schema),
            Err(e) => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    format!("failed to import output schema: {e}"),
                );
            }
        }
    };
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let options: UdfOptions = match unsafe { storage::optional_str(options_json, "options_json") } {
        Ok(None) => UdfOptions::default(),
        Ok(Some(s)) if s.trim().is_empty() => UdfOptions::default(),
        Ok(Some(s)) => match serde_json::from_str(s) {
            Ok(opts) => opts,
            Err(e) => {
                return set_error(
                    ErrorCode::InvalidArgument,
                    format!("invalid options JSON: {e}"),
                );
            }
        },
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    let plugin = GoPlugin::new(udf_plugin_handle);
    let mapper: MapperFn = Box::new(move |batch: &RecordBatch| call_go_mapper(plugin, batch));

    let result_checkpoint: Option<Arc<dyn UDFCheckpointStore>> = if checkpoint_plugin_handle == 0 {
        None
    } else {
        Some(Arc::new(GoCheckpointStore {
            plugin: GoPlugin::new(checkpoint_plugin_handle),
        }))
    };

    let transform = NewColumnTransform::BatchUDF(BatchUDF {
        mapper,
        output_schema: schema,
        result_checkpoint,
    });
    let batch_size = if batch_size == 0 {
        None
    } else {
        Some(batch_size)
    };

    let mut guard = unsafe { &*ds }.0.lock().unwrap_or_else(|e| e.into_inner());
    match block_on_cc!(guard.add_columns(transform, options.read_columns, batch_size)) {
        Ok(()) => ok(),
        Err(e) => set_error(map_lance_error(&e), e),
    }
}
