//! Mutations: delete, update and merge-insert.
//!
//! Each mutation commits a new dataset version. The handle passed in is
//! updated in place to track that new version, so subsequent reads through
//! the same handle observe the mutated data without reopening.

use std::collections::BTreeMap;
use std::ffi::{CString, c_char};
use std::sync::Arc;
use std::time::Duration;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{
    MergeInsertBuilder, MergeStats, UpdateBuilder, WhenMatched, WhenNotMatched,
    WhenNotMatchedBySource,
};
use serde::Deserialize;
use serde_json::json;

use crate::arrow_bridge;
use crate::dataset::LanceDataset;
use crate::error::{ErrorCode, map_lance_error, ok, set_error};
use crate::runtime::block_on_cc;
use crate::storage;

/// Replaces the handle's inner dataset with the post-mutation dataset so the
/// handle always tracks the newest version.
fn store_new_dataset(handle: &LanceDataset, new_dataset: Arc<lance::Dataset>) {
    let mut guard = handle.0.lock().unwrap_or_else(|e| e.into_inner());
    *guard = Arc::unwrap_or_clone(new_dataset);
}

/// Serializes `value` into `*out_json` as an owned C string (freed by the
/// caller with `lance_string_free`). A NULL `out_json` discards the result.
///
/// # Safety
///
/// `out_json` must be NULL or valid for writes.
unsafe fn emit_json(value: serde_json::Value, out_json: *mut *mut c_char) -> i32 {
    if out_json.is_null() {
        return ok();
    }
    // serde_json output never contains interior NUL bytes.
    match CString::new(value.to_string()) {
        Ok(cstr) => {
            // SAFETY: `out_json` is non-NULL and valid for writes.
            unsafe { *out_json = cstr.into_raw() };
            ok()
        }
        Err(e) => set_error(ErrorCode::Internal, e),
    }
}

/// Deletes rows matching an SQL predicate and commits a new version.
///
/// - `ds`: dataset handle, updated in place to the new version on success.
/// - `predicate`: SQL filter selecting the rows to delete. Must not be NULL.
/// - `out_json`: if non-NULL, receives `{"num_deleted_rows": uint}` as an
///   owned string (free with `lance_string_free`).
///
/// # Safety
///
/// `ds` must be a valid handle, `predicate` a valid C string, and `out_json`
/// NULL or valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_delete(
    ds: *mut LanceDataset,
    predicate: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let predicate = match unsafe { storage::required_str(predicate, "predicate") } {
        Ok(predicate) => predicate,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let handle = unsafe { &*ds };
    // Work on a clone so the handle's mutex is not held across block_on.
    let mut dataset = handle.dataset();
    match block_on_cc!(dataset.delete(predicate)) {
        Ok(result) => {
            store_new_dataset(handle, result.new_dataset);
            unsafe {
                emit_json(
                    json!({"num_deleted_rows": result.num_deleted_rows}),
                    out_json,
                )
            }
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateOptions {
    /// Column name -> SQL expression producing the new value.
    /// A BTreeMap keeps the application order deterministic.
    set: BTreeMap<String, String>,
    #[serde(rename = "where")]
    filter: Option<String>,
    conflict_retries: Option<u32>,
    retry_timeout_ms: Option<u64>,
}

/// Updates column values on rows matching an optional SQL predicate and
/// commits a new version.
///
/// - `ds`: dataset handle, updated in place to the new version on success.
/// - `update_json`: required JSON object
///   `{"set": {"col": "sql_expr", ...}, "where"?: string,
///   "conflict_retries"?: uint, "retry_timeout_ms"?: uint}`. `set` must be
///   non-empty. Omitting `where` updates every row.
/// - `out_json`: if non-NULL, receives `{"rows_updated": uint}` as an owned
///   string (free with `lance_string_free`).
///
/// # Safety
///
/// `ds` must be a valid handle, `update_json` a valid C string, and
/// `out_json` NULL or valid for writes.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_update(
    ds: *mut LanceDataset,
    update_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let update_json = match unsafe { storage::required_str(update_json, "update_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: UpdateOptions = match serde_json::from_str(update_json) {
        Ok(options) => options,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid update JSON: {e}"),
            );
        }
    };
    if options.set.is_empty() {
        return set_error(
            ErrorCode::InvalidArgument,
            "update requires at least one column in \"set\"",
        );
    }

    let handle = unsafe { &*ds };
    let mut builder = UpdateBuilder::new(Arc::new(handle.dataset()));
    if let Some(filter) = &options.filter {
        builder = match builder.update_where(filter) {
            Ok(builder) => builder,
            Err(e) => return set_error(map_lance_error(&e), e),
        };
    }
    for (column, expr) in &options.set {
        builder = match builder.set(column, expr) {
            Ok(builder) => builder,
            Err(e) => return set_error(map_lance_error(&e), e),
        };
    }
    if let Some(retries) = options.conflict_retries {
        builder = builder.conflict_retries(retries);
    }
    if let Some(ms) = options.retry_timeout_ms {
        builder = builder.retry_timeout(Duration::from_millis(ms));
    }
    let job = match builder.build() {
        Ok(job) => job,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    match block_on_cc!(job.execute()) {
        Ok(result) => {
            store_new_dataset(handle, result.new_dataset);
            unsafe { emit_json(json!({"rows_updated": result.rows_updated}), out_json) }
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// `"update_all" | "do_nothing" | "delete" | "fail" | {"update_if": expr}`.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WhenMatchedOption {
    UpdateAll,
    DoNothing,
    Delete,
    Fail,
    #[serde(untagged)]
    Conditional {
        update_if: String,
    },
}

/// `"insert_all" | "do_nothing"`.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WhenNotMatchedOption {
    InsertAll,
    DoNothing,
}

/// `"keep" | "delete" | {"delete_if": expr}`.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WhenNotMatchedBySourceOption {
    Keep,
    Delete,
    #[serde(untagged)]
    Conditional {
        delete_if: String,
    },
}

/// `"fail" | "first_seen"`. Controls how duplicate source rows that match the
/// same target row are handled. `fail` (Lance's default) errors on duplicates.
/// `first_seen` keeps the first-encountered source row and skips the rest.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SourceDedupeOption {
    Fail,
    FirstSeen,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MergeInsertOptions {
    on: Vec<String>,
    when_matched: Option<WhenMatchedOption>,
    when_not_matched: Option<WhenNotMatchedOption>,
    when_not_matched_by_source: Option<WhenNotMatchedBySourceOption>,
    conflict_retries: Option<u32>,
    retry_timeout_ms: Option<u64>,
    source_dedupe_behavior: Option<SourceDedupeOption>,
    use_index: Option<bool>,
    commit_retries: Option<u32>,
    skip_auto_cleanup: Option<bool>,
}

/// Merges an Arrow stream of source rows into the dataset (upsert /
/// find-or-create / replace-range) and commits a new version.
///
/// - `ds`: dataset handle, updated in place to the new version on success.
/// - `options_json`: required JSON object
///   `{"on": ["col", ...],
///   "when_matched"?: "update_all"|"do_nothing"|"delete"|"fail"|{"update_if": expr},
///   "when_not_matched"?: "insert_all"|"do_nothing",
///   "when_not_matched_by_source"?: "keep"|"delete"|{"delete_if": expr},
///   "conflict_retries"?: uint, "retry_timeout_ms"?: uint,
///   "source_dedupe_behavior"?: "fail"|"first_seen", "use_index"?: bool,
///   "commit_retries"?: uint, "skip_auto_cleanup"?: bool}`.
///   Defaults mirror Lance's find-or-create semantics: matched rows are kept
///   as-is (`do_nothing`), unmatched source rows are inserted (`insert_all`),
///   target rows missing from the source are kept (`keep`), with 10 conflict
///   retries over at most 30 seconds. `update_if` expressions reference the
///   joined row via `target.` / `source.` qualifiers. `delete_if` expressions
///   reference target columns directly. `source_dedupe_behavior` defaults to
///   `fail` (error when the source has duplicate keys that match the same
///   target row). `use_index` defaults to true (use a scalar index on the
///   join key when one exists). `commit_retries` (default 20) is the inner
///   manifest-conflict retry count, distinct from `conflict_retries`.
///   `skip_auto_cleanup` defaults to false.
/// - `stream`: Arrow C stream with the source rows. Ownership is always
///   taken, even on error, so the producer is released exactly once. Must not
///   be NULL.
/// - `out_json`: if non-NULL, receives the merge statistics as an owned
///   string (free with `lance_string_free`):
///   `{"num_inserted_rows": uint, "num_updated_rows": uint,
///   "num_deleted_rows": uint, "num_attempts": uint, "bytes_written": uint,
///   "num_files_written": uint, "num_skipped_duplicates": uint}`.
///
/// # Safety
///
/// All pointer arguments must satisfy the contracts above.
#[lance_go_ffi_macros::ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lance_dataset_merge_insert(
    ds: *mut LanceDataset,
    options_json: *const c_char,
    stream: *mut FFI_ArrowArrayStream,
    out_json: *mut *mut c_char,
) -> i32 {
    // Import the stream FIRST so its producer resources are owned (and thus
    // released) on every subsequent error path.
    let reader = match unsafe { arrow_bridge::import_stream(stream) } {
        Ok(reader) => reader,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };

    if ds.is_null() {
        return set_error(ErrorCode::InvalidArgument, "ds must not be NULL");
    }
    let options_json = match unsafe { storage::required_str(options_json, "options_json") } {
        Ok(json) => json,
        Err(msg) => return set_error(ErrorCode::InvalidArgument, msg),
    };
    let options: MergeInsertOptions = match serde_json::from_str(options_json) {
        Ok(options) => options,
        Err(e) => {
            return set_error(
                ErrorCode::InvalidArgument,
                format!("invalid merge-insert JSON: {e}"),
            );
        }
    };

    let handle = unsafe { &*ds };
    let dataset = Arc::new(handle.dataset());

    let when_matched = match options.when_matched {
        None | Some(WhenMatchedOption::DoNothing) => WhenMatched::DoNothing,
        Some(WhenMatchedOption::UpdateAll) => WhenMatched::UpdateAll,
        Some(WhenMatchedOption::Delete) => WhenMatched::Delete,
        Some(WhenMatchedOption::Fail) => WhenMatched::Fail,
        Some(WhenMatchedOption::Conditional { update_if }) => {
            match WhenMatched::update_if(&dataset, &update_if) {
                Ok(behavior) => behavior,
                Err(e) => return set_error(map_lance_error(&e), e),
            }
        }
    };
    let when_not_matched = match options.when_not_matched {
        None | Some(WhenNotMatchedOption::InsertAll) => WhenNotMatched::InsertAll,
        Some(WhenNotMatchedOption::DoNothing) => WhenNotMatched::DoNothing,
    };
    let when_not_matched_by_source = match options.when_not_matched_by_source {
        None | Some(WhenNotMatchedBySourceOption::Keep) => WhenNotMatchedBySource::Keep,
        Some(WhenNotMatchedBySourceOption::Delete) => WhenNotMatchedBySource::Delete,
        Some(WhenNotMatchedBySourceOption::Conditional { delete_if }) => {
            match WhenNotMatchedBySource::delete_if(&dataset, &delete_if) {
                Ok(behavior) => behavior,
                Err(e) => return set_error(map_lance_error(&e), e),
            }
        }
    };

    let mut builder = match MergeInsertBuilder::try_new(dataset, options.on) {
        Ok(builder) => builder,
        Err(e) => return set_error(map_lance_error(&e), e),
    };
    builder
        .when_matched(when_matched)
        .when_not_matched(when_not_matched)
        .when_not_matched_by_source(when_not_matched_by_source);
    if let Some(retries) = options.conflict_retries {
        builder.conflict_retries(retries);
    }
    if let Some(ms) = options.retry_timeout_ms {
        builder.retry_timeout(Duration::from_millis(ms));
    }
    if let Some(behavior) = options.source_dedupe_behavior {
        let mapped = match behavior {
            SourceDedupeOption::Fail => SourceDedupeBehavior::Fail,
            SourceDedupeOption::FirstSeen => SourceDedupeBehavior::FirstSeen,
        };
        builder.source_dedupe_behavior(mapped);
    }
    if let Some(use_index) = options.use_index {
        builder.use_index(use_index);
    }
    if let Some(retries) = options.commit_retries {
        builder.commit_retries(retries);
    }
    if let Some(skip) = options.skip_auto_cleanup {
        builder.skip_auto_cleanup(skip);
    }
    let job = match builder.try_build() {
        Ok(job) => job,
        Err(e) => return set_error(map_lance_error(&e), e),
    };

    match block_on_cc!(job.execute_reader(reader)) {
        Ok((new_dataset, stats)) => {
            store_new_dataset(handle, new_dataset);
            unsafe { emit_json(merge_stats_json(&stats), out_json) }
        }
        Err(e) => set_error(map_lance_error(&e), e),
    }
}

/// Hand-built JSON for [`MergeStats`], which does not implement `Serialize`.
fn merge_stats_json(stats: &MergeStats) -> serde_json::Value {
    json!({
        "num_inserted_rows": stats.num_inserted_rows,
        "num_updated_rows": stats.num_updated_rows,
        "num_deleted_rows": stats.num_deleted_rows,
        "num_attempts": stats.num_attempts,
        "bytes_written": stats.bytes_written,
        "num_files_written": stats.num_files_written,
        "num_skipped_duplicates": stats.num_skipped_duplicates,
    })
}
