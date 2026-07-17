//! Storage-option plumbing shared by the dataset open and write paths, plus
//! small C-string helpers used by all FFI modules.

use std::collections::HashMap;
use std::ffi::{CStr, c_char};
use std::sync::Arc;

use lance::io::{ObjectStoreParams, StorageOptionsAccessor};

/// Reads a required, NUL-terminated UTF-8 C string.
///
/// # Safety
///
/// `ptr` must be NULL or a valid NUL-terminated string.
pub(crate) unsafe fn required_str<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{what} must not be NULL"));
    }
    // SAFETY: non-NULL per check above. Caller guarantees validity.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| format!("{what} is not valid UTF-8: {e}"))
}

/// Reads an optional, NUL-terminated UTF-8 C string. NULL maps to `None`.
///
/// # Safety
///
/// `ptr` must be NULL or a valid NUL-terminated string.
pub(crate) unsafe fn optional_str<'a>(
    ptr: *const c_char,
    what: &str,
) -> Result<Option<&'a str>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    unsafe { required_str(ptr, what) }.map(Some)
}

/// Parses the FFI storage-options array into a map.
///
/// The wire format is a NULL-terminated array of alternating keys and values:
/// `[k1, v1, k2, v2, NULL]`. A NULL array means "no options".
///
/// # Safety
///
/// `kv` must be NULL or a valid array of valid C strings terminated by a NULL
/// entry.
pub(crate) unsafe fn parse_storage_kv(
    kv: *const *const c_char,
) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    if kv.is_null() {
        return Ok(map);
    }
    let mut i = 0usize;
    loop {
        // SAFETY: caller guarantees the array is NULL-terminated.
        let key_ptr = unsafe { *kv.add(i) };
        if key_ptr.is_null() {
            return Ok(map);
        }
        let value_ptr = unsafe { *kv.add(i + 1) };
        if value_ptr.is_null() {
            return Err(format!(
                "storage options array has a key at index {i} with no value"
            ));
        }
        let key = unsafe { required_str(key_ptr, "storage option key") }?;
        let value = unsafe { required_str(value_ptr, "storage option value") }?;
        map.insert(key.to_owned(), value.to_owned());
        i += 2;
    }
}

/// Wraps parsed storage options into [`ObjectStoreParams`] for the write path
/// (mirrors what `DatasetBuilder::with_storage_options` does on the read
/// path). Returns `None` when there are no options so `WriteParams` keeps its
/// default `store_params`.
pub(crate) fn object_store_params(map: HashMap<String, String>) -> Option<ObjectStoreParams> {
    if map.is_empty() {
        return None;
    }
    Some(ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(map))),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;

    #[test]
    fn parse_null_is_empty() {
        let map = unsafe { parse_storage_kv(ptr::null()) }.unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn parse_pairs() {
        let k = CString::new("aws_region").unwrap();
        let v = CString::new("eu-west-1").unwrap();
        let arr = [k.as_ptr(), v.as_ptr(), ptr::null()];
        let map = unsafe { parse_storage_kv(arr.as_ptr()) }.unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["aws_region"], "eu-west-1");
    }

    #[test]
    fn parse_odd_length_fails() {
        let k = CString::new("key_without_value").unwrap();
        let arr = [k.as_ptr(), ptr::null()];
        assert!(unsafe { parse_storage_kv(arr.as_ptr()) }.is_err());
    }
}
