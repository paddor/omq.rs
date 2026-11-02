//! Read and write raw option values at the C boundary.

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

/// Caller storage for a `zmq_getsockopt`-style value: a buffer and its
/// in/out length. Writers zero-fill as libzmq does where noted and set the
/// written length; they fail with `EFAULT` for null pointers and `EINVAL`
/// for a short buffer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OptOut {
    ptr: *mut libc::c_void,
    len: *mut usize,
}

impl OptOut {
    pub(crate) fn new(ptr: *mut libc::c_void, len: *mut usize) -> Self {
        Self { ptr, len }
    }

    /// Available buffer size, after the null checks.
    fn avail(self) -> Result<usize, c_int> {
        if self.ptr.is_null() || self.len.is_null() {
            return Err(libc::EFAULT);
        }
        // SAFETY: len is non-null (checked above); reading the available size.
        Ok(unsafe { *self.len })
    }

    /// Zero the whole buffer, then write a native-endian integer.
    fn scalar<const N: usize>(self, value: [u8; N]) -> Result<(), c_int> {
        let avail = self.avail()?;
        if avail < N {
            return Err(libc::EINVAL);
        }
        // SAFETY: ptr is non-null with at least N of `avail` bytes writable
        // (checked above); len is non-null.
        unsafe {
            std::ptr::write_bytes(self.ptr.cast::<u8>(), 0, avail);
            std::ptr::copy_nonoverlapping(value.as_ptr(), self.ptr.cast::<u8>(), N);
            *self.len = N;
        }
        Ok(())
    }

    pub(crate) fn i32(self, value: i32) -> Result<(), c_int> {
        self.scalar(value.to_ne_bytes())
    }

    pub(crate) fn i64(self, value: i64) -> Result<(), c_int> {
        self.scalar(value.to_ne_bytes())
    }

    pub(crate) fn bytes(self, data: &[u8]) -> Result<(), c_int> {
        let avail = self.avail()?;
        if avail < data.len() {
            return Err(libc::EINVAL);
        }
        // SAFETY: ptr is non-null with at least data.len() bytes available.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.cast::<u8>(), data.len());
            *self.len = data.len();
        }
        Ok(())
    }

    /// Write `data` plus a NUL terminator.
    pub(crate) fn string(self, data: &[u8]) -> Result<(), c_int> {
        let avail = self.avail()?;
        let needed = data.len() + 1;
        if avail < needed {
            return Err(libc::EINVAL);
        }
        // SAFETY: ptr is non-null with at least `needed` bytes available.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.cast::<u8>(), data.len());
            *self.ptr.cast::<u8>().add(data.len()) = 0;
            *self.len = needed;
        }
        Ok(())
    }

    /// [`string`](Self::string), then zero the rest of the buffer.
    pub(crate) fn padded_string(self, data: &[u8]) -> Result<(), c_int> {
        let avail = self.avail()?;
        self.string(data)?;
        let needed = data.len() + 1;
        // SAFETY: the string fit, so ptr is non-null with `avail >= needed`
        // writable bytes.
        unsafe { std::ptr::write_bytes(self.ptr.cast::<u8>().add(needed), 0, avail - needed) };
        Ok(())
    }

    /// A CURVE key as 40 Z85 characters plus NUL when the buffer has room,
    /// else as 32 raw bytes.
    pub(crate) fn key(self, key: &[u8; 32]) -> Result<(), c_int> {
        let avail = self.avail()?;
        if avail >= 41
            && let Ok(z85) = omq_tokio::proto::z85::encode(key)
        {
            // SAFETY: ptr has at least 41 bytes available (checked above).
            unsafe {
                std::ptr::copy_nonoverlapping(z85.as_ptr(), self.ptr.cast::<u8>(), 40);
                *(self.ptr.cast::<u8>()).add(40) = 0;
                *self.len = 41;
            }
            return Ok(());
        }
        if avail >= 32 {
            // SAFETY: ptr has at least 32 bytes available (checked above).
            unsafe {
                std::ptr::copy_nonoverlapping(key.as_ptr(), self.ptr.cast::<u8>(), 32);
                *self.len = 32;
            }
            return Ok(());
        }
        Err(libc::EINVAL)
    }
}
