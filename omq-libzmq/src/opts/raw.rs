//! Read and write raw option values at the C boundary.

use crate::error::fail;
use std::ffi::c_int;

pub(super) fn read_i32(optval: *const libc::c_void, optvallen: usize) -> Option<i32> {
    if optval.is_null() || optvallen < 4 {
        return None;
    }
    // SAFETY: optval is non-null (checked above) and points to at least 4 readable bytes.
    Some(unsafe { std::ptr::read_unaligned(optval.cast::<i32>()) })
}

pub(super) fn read_i64(optval: *const libc::c_void, optvallen: usize) -> Option<i64> {
    if optval.is_null() || optvallen < 8 {
        return None;
    }
    // SAFETY: optval is non-null (checked above) and points to at least 8 readable bytes.
    Some(unsafe { std::ptr::read_unaligned(optval.cast::<i64>()) })
}

pub(super) fn read_string(optval: *const libc::c_void, optvallen: usize) -> Option<String> {
    if optval.is_null() {
        return None;
    }
    if optvallen == 0 {
        return Some(String::new());
    }
    // SAFETY: optval is non-null with optvallen > 0 (checked above).
    let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
    Some(String::from_utf8_lossy(slice).into_owned())
}

pub(super) fn read_bytes(optval: *const libc::c_void, optvallen: usize) -> Option<Vec<u8>> {
    if optvallen == 0 {
        return Some(Vec::new());
    }
    if optval.is_null() {
        return None;
    }
    // SAFETY: optval is non-null with optvallen > 0 (checked above).
    Some(unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) }.to_vec())
}

pub(super) fn none_if_empty(bytes: Vec<u8>) -> Option<Vec<u8>> {
    if bytes.is_empty() { None } else { Some(bytes) }
}

pub(super) fn read_key(optval: *const libc::c_void, optvallen: usize) -> Option<[u8; 32]> {
    if optval.is_null() {
        return None;
    }
    let mut key = [0u8; 32];
    if optvallen == 32 {
        // SAFETY: optval is non-null (checked above) and optvallen == 32.
        let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), 32) };
        key.copy_from_slice(slice);
        return Some(key);
    }
    if optvallen >= 40 {
        // SAFETY: optval is non-null (checked above) and optvallen >= 40.
        let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), 40) };
        let Ok(s) = std::str::from_utf8(slice) else {
            return None;
        };
        if let Ok(decoded) = omq_tokio::proto::z85::decode(s)
            && decoded.len() == 32
        {
            key.copy_from_slice(&decoded);
            return Some(key);
        }
    }
    None
}

pub(super) fn write_i32(optval: *mut libc::c_void, optvallen: *mut usize, val: i32) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above); reading the available size.
    let avail = unsafe { *optvallen };
    if avail < 4 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least 4 bytes available (checked above).
    unsafe {
        std::ptr::write_bytes(optval.cast::<u8>(), 0, avail);
        std::ptr::write_unaligned(optval.cast::<i32>(), val);
        *optvallen = 4;
    }
    0
}

pub(super) fn write_i64(optval: *mut libc::c_void, optvallen: *mut usize, val: i64) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail < 8 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least 8 bytes available (checked above).
    unsafe {
        std::ptr::write_bytes(optval.cast::<u8>(), 0, avail);
        std::ptr::write_unaligned(optval.cast::<i64>(), val);
        *optvallen = 8;
    }
    0
}

pub(super) fn write_bytes(optval: *mut libc::c_void, optvallen: *mut usize, data: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail < data.len() {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least data.len() bytes available.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), optval.cast::<u8>(), data.len());
        *optvallen = data.len();
    }
    0
}

pub(super) fn write_string(optval: *mut libc::c_void, optvallen: *mut usize, data: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    let needed = data.len() + 1;
    if avail < needed {
        return fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least `needed` bytes available.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), optval.cast::<u8>(), data.len());
        *optval.cast::<u8>().add(data.len()) = 0;
        *optvallen = needed;
    }
    0
}

pub(super) fn write_key(optval: *mut libc::c_void, optvallen: *mut usize, key: &[u8; 32]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail >= 41
        && let Ok(z85) = omq_tokio::proto::z85::encode(key)
    {
        // SAFETY: optval has at least 41 bytes available (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(z85.as_ptr(), optval.cast::<u8>(), 40);
            *(optval.cast::<u8>()).add(40) = 0;
            *optvallen = 41;
        }
        return 0;
    }
    if avail >= 32 {
        // SAFETY: optval has at least 32 bytes available (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), optval.cast::<u8>(), 32);
            *optvallen = 32;
        }
        return 0;
    }
    crate::error::fail(libc::EINVAL)
}
