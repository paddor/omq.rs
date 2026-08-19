//! `zmq_msg_t` implementation.
//!
//! The repr must be exactly 64 bytes to match libzmq's ABI.
//! Public alignment follows `zmq_msg_t`: pointer-sized, not always 8 bytes.
//! kind values: 0=empty, 1=heap-alloc, 2=external-ptr, 3=bytes-arc (from recv).

use std::ffi::{CStr, c_int};

use bytes::Bytes;

// kind discriminants
#[expect(dead_code)]
const KIND_EMPTY: u8 = 0;
const KIND_HEAP: u8 = 1;
const KIND_EXTERNAL: u8 = 2;
const KIND_BYTES: u8 = 3;
const KIND_HEAP_SHARED: u8 = 4;

/// `zmq_msg_t` compatible repr: 64 bytes, C layout.
///
/// libzmq exposes `zmq_msg_t` as an opaque 64-byte blob aligned to pointer
/// size. Keep the Rust type equally opaque: typed `u64` fields would raise
/// alignment to 8 on armv7, which is ABI-incompatible with C callers.
#[repr(C)]
pub struct OmqMsgRepr {
    storage: [usize; MSG_WORDS],
}

// SAFETY: pointer fields are owned by exactly one OmqMsgRepr instance.
// Send is needed for ownership transfer across threads.
// Sync is intentionally omitted: concurrent &-access from multiple
// threads (e.g. via Arc) would be unsound.
unsafe impl Send for OmqMsgRepr {}

impl std::fmt::Debug for OmqMsgRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OmqMsgRepr")
            .field("kind", &self.kind())
            .field("more", &self.more())
            .field("size", &self.size())
            .finish_non_exhaustive()
    }
}

pub(crate) const ZMQ_MSG_T_SIZE: usize = 64;
const PTR_SIZE: usize = std::mem::size_of::<usize>();
const MSG_WORDS: usize = ZMQ_MSG_T_SIZE / PTR_SIZE;
const OFF_KIND: usize = 0;
const OFF_MORE: usize = 1;
const OFF_SIZE: usize = 8;
const OFF_PTR: usize = 16;
const OFF_FREE_FN: usize = OFF_PTR + PTR_SIZE;
const OFF_HINT: usize = OFF_FREE_FN + PTR_SIZE;
const OFF_BOXED: usize = OFF_HINT + PTR_SIZE;
const OFF_RESERVED: usize = OFF_BOXED + PTR_SIZE;
const RESERVED_LEN: usize = 16;
const LARGE_MSG_SHARED_THRESHOLD: usize = 64;

type FreeFn = unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void);

impl OmqMsgRepr {
    #[inline]
    fn clear(&mut self) {
        self.storage = [0; MSG_WORDS];
    }

    #[inline]
    fn bytes(&self) -> &[u8; ZMQ_MSG_T_SIZE] {
        // SAFETY: storage is exactly 64 bytes; `[u8; 64]` has alignment 1.
        unsafe {
            &*(self
                .storage
                .as_ptr()
                .cast::<u8>()
                .cast::<[u8; ZMQ_MSG_T_SIZE]>())
        }
    }

    #[inline]
    fn bytes_mut(&mut self) -> &mut [u8; ZMQ_MSG_T_SIZE] {
        // SAFETY: storage is exactly 64 bytes; `[u8; 64]` has alignment 1.
        unsafe {
            &mut *(self
                .storage
                .as_mut_ptr()
                .cast::<u8>()
                .cast::<[u8; ZMQ_MSG_T_SIZE]>())
        }
    }

    #[inline]
    fn kind(&self) -> u8 {
        self.bytes()[OFF_KIND]
    }

    #[inline]
    fn more(&self) -> u8 {
        self.bytes()[OFF_MORE]
    }

    #[inline]
    fn set_more(&mut self, more: u8) {
        self.bytes_mut()[OFF_MORE] = more;
    }

    #[inline]
    fn size(&self) -> usize {
        usize::try_from(self.read_u64(OFF_SIZE)).unwrap_or(usize::MAX)
    }

    #[inline]
    fn ptr(&self) -> *mut u8 {
        self.read_usize(OFF_PTR) as *mut u8
    }

    #[inline]
    fn boxed(&self) -> *mut libc::c_void {
        self.read_usize(OFF_BOXED) as *mut libc::c_void
    }

    #[inline]
    fn hint(&self) -> *mut libc::c_void {
        self.read_usize(OFF_HINT) as *mut libc::c_void
    }

    #[inline]
    fn free_fn(&self) -> Option<FreeFn> {
        let raw = self.read_usize(OFF_FREE_FN);
        if raw == 0 {
            None
        } else {
            // SAFETY: raw was written from a `FreeFn` in `init_fields`.
            Some(unsafe { std::mem::transmute::<usize, FreeFn>(raw) })
        }
    }

    #[inline]
    fn reserved(&self) -> &[u8] {
        &self.bytes()[OFF_RESERVED..OFF_RESERVED + RESERVED_LEN]
    }

    #[inline]
    fn reserved_mut(&mut self) -> &mut [u8] {
        &mut self.bytes_mut()[OFF_RESERVED..OFF_RESERVED + RESERVED_LEN]
    }

    #[inline]
    fn reserved_array(&self) -> [u8; RESERVED_LEN] {
        let mut reserved = [0; RESERVED_LEN];
        reserved.copy_from_slice(self.reserved());
        reserved
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn init_fields(
        &mut self,
        kind: u8,
        more: u8,
        size: usize,
        ptr: *mut u8,
        free_fn: Option<FreeFn>,
        hint: *mut libc::c_void,
        boxed: *mut libc::c_void,
        reserved: [u8; RESERVED_LEN],
    ) {
        self.clear();
        self.bytes_mut()[OFF_KIND] = kind;
        self.bytes_mut()[OFF_MORE] = more;
        self.write_u64(OFF_SIZE, size as u64);
        self.write_usize(OFF_PTR, ptr as usize);
        self.write_usize(OFF_FREE_FN, free_fn.map_or(0, |f| f as usize));
        self.write_usize(OFF_HINT, hint as usize);
        self.write_usize(OFF_BOXED, boxed as usize);
        self.reserved_mut().copy_from_slice(&reserved);
    }

    #[inline]
    fn read_u64(&self, off: usize) -> u64 {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&self.bytes()[off..off + 8]);
        u64::from_ne_bytes(bytes)
    }

    #[inline]
    fn write_u64(&mut self, off: usize, value: u64) {
        self.bytes_mut()[off..off + 8].copy_from_slice(&value.to_ne_bytes());
    }

    #[inline]
    fn read_usize(&self, off: usize) -> usize {
        let mut bytes = [0; PTR_SIZE];
        bytes.copy_from_slice(&self.bytes()[off..off + PTR_SIZE]);
        usize::from_ne_bytes(bytes)
    }

    #[inline]
    fn write_usize(&mut self, off: usize, value: usize) {
        self.bytes_mut()[off..off + PTR_SIZE].copy_from_slice(&value.to_ne_bytes());
    }
}

const _SIZE_ASSERT: () = assert!(std::mem::size_of::<OmqMsgRepr>() == ZMQ_MSG_T_SIZE);
const _ALIGN_ASSERT: () =
    assert!(std::mem::align_of::<OmqMsgRepr>() == std::mem::align_of::<usize>());
const _RESERVED_ASSERT: () = assert!(OFF_RESERVED + RESERVED_LEN <= ZMQ_MSG_T_SIZE);

#[inline]
/// # Safety
///
/// `msg` must be non-null and point to a valid, initialized `OmqMsgRepr`.
unsafe fn repr<'a>(msg: *mut OmqMsgRepr) -> &'a mut OmqMsgRepr {
    unsafe { &mut *msg }
}

#[inline]
/// # Safety
///
/// `msg` must be non-null and point to a valid, initialized `OmqMsgRepr`.
unsafe fn repr_ref<'a>(msg: *const OmqMsgRepr) -> &'a OmqMsgRepr {
    unsafe { &*msg }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init(msg: *mut OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr(msg).clear() };
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_size(msg: *mut OmqMsgRepr, size: usize) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: libc::malloc is always safe to call; returns null on failure.
    let ptr = unsafe { libc::malloc(size).cast::<u8>() };
    if ptr.is_null() && size > 0 {
        return crate::error::fail(libc::ENOMEM);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    r.init_fields(
        KIND_HEAP,
        0,
        size,
        ptr,
        None,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        [0; RESERVED_LEN],
    );
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_data(
    msg: *mut OmqMsgRepr,
    data: *mut libc::c_void,
    size: usize,
    ffn: Option<unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void)>,
    hint: *mut libc::c_void,
) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if data.is_null() && size > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    r.init_fields(
        KIND_EXTERNAL,
        0,
        size,
        data.cast(),
        ffn,
        hint,
        std::ptr::null_mut(),
        [0; RESERVED_LEN],
    );
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_buffer(
    msg: *mut OmqMsgRepr,
    buf: *const libc::c_void,
    size: usize,
) -> c_int {
    if buf.is_null() && size > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    if zmq_msg_init_size(msg, size) != 0 {
        return -1;
    }
    if size > 0 && !buf.is_null() {
        // SAFETY: msg was just initialized by zmq_msg_init_size; buf is non-null.
        let r = unsafe { repr(msg) };
        unsafe {
            std::ptr::copy_nonoverlapping(buf.cast::<u8>(), r.ptr(), size);
        }
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_data(msg: *mut OmqMsgRepr) -> *mut libc::c_void {
    if msg.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    r.ptr().cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_size(msg: *const OmqMsgRepr) -> usize {
    if msg.is_null() {
        return 0;
    }
    // SAFETY: msg is non-null (checked above).
    // SAFETY: msg is non-null (checked above).
    unsafe { repr_ref(msg).size() }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_more(msg: *const OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return 0;
    }
    // SAFETY: msg is non-null (checked above).
    c_int::from(unsafe { repr_ref(msg).more() })
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_close(msg: *mut OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    match r.kind() {
        KIND_HEAP | KIND_HEAP_SHARED => {
            let ptr = r.ptr();
            if !ptr.is_null() {
                // SAFETY: ptr was allocated by libc::malloc in zmq_msg_init_size.
                unsafe { libc::free(ptr.cast()) };
            }
        }
        KIND_EXTERNAL => {
            let ptr = r.ptr();
            if let Some(ffn) = r.free_fn()
                && !ptr.is_null()
            {
                unsafe { ffn(ptr.cast(), r.hint()) };
            }
        }
        KIND_BYTES => {
            let boxed = r.boxed();
            if !boxed.is_null() {
                // SAFETY: boxed was created by Box::into_raw in zmq_msg_recv.
                drop(unsafe { Box::from_raw(boxed.cast::<Bytes>()) });
            }
        }
        _ => {}
    }
    r.clear();
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_move(dst: *mut OmqMsgRepr, src: *mut OmqMsgRepr) -> c_int {
    if dst.is_null() || src.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if dst == src {
        return 0;
    }
    // Close dst first.
    zmq_msg_close(dst);
    // SAFETY: src and dst are non-null (checked above) and don't overlap
    // (ZMQ API contract). Move the repr then zero src to prevent double-free.
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, 1);
        repr(src).clear();
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_copy(dst: *mut OmqMsgRepr, src: *const OmqMsgRepr) -> c_int {
    if dst.is_null() || src.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if std::ptr::addr_eq(dst.cast_const(), src) {
        return 0;
    }
    zmq_msg_close(dst);
    // SAFETY: src is non-null (checked above).
    let s = unsafe { repr_ref(src) };
    match s.kind() {
        KIND_BYTES => {
            // Clone the Bytes (increments the Arc refcount).
            // SAFETY: boxed was created by Box::into_raw in zmq_msg_recv.
            let boxed = s.boxed();
            if boxed.is_null() {
                return crate::error::fail(libc::EFAULT);
            }
            let original = unsafe { &*(boxed.cast::<Bytes>()) };
            let cloned = Box::new(original.clone());
            // SAFETY: dst was closed above and is ready for reinitialization.
            let d = unsafe { repr(dst) };
            d.init_fields(
                KIND_BYTES,
                s.more(),
                s.size(),
                cloned.as_ptr().cast_mut(),
                None,
                std::ptr::null_mut(),
                Box::into_raw(cloned).cast::<libc::c_void>(),
                s.reserved_array(),
            );
        }
        KIND_HEAP | KIND_EXTERNAL | KIND_HEAP_SHARED => {
            // Deep copy into a new heap allocation.
            let size = s.size();
            let src_ptr = s.ptr();
            if size > 0 && src_ptr.is_null() {
                return crate::error::fail(libc::EFAULT);
            }
            let new_ptr = if size > 0 {
                // SAFETY: libc::malloc is always safe to call.
                let p = unsafe { libc::malloc(size).cast::<u8>() };
                if p.is_null() {
                    return crate::error::fail(libc::ENOMEM);
                }
                // SAFETY: s.ptr is valid for size bytes; p was just allocated.
                unsafe { std::ptr::copy_nonoverlapping(src_ptr, p, size) };
                p
            } else {
                std::ptr::null_mut()
            };
            // SAFETY: dst was closed above and is ready for reinitialization.
            let d = unsafe { repr(dst) };
            let kind = if s.kind() == KIND_EXTERNAL || size >= LARGE_MSG_SHARED_THRESHOLD {
                KIND_HEAP_SHARED
            } else {
                KIND_HEAP
            };
            d.init_fields(
                kind,
                s.more(),
                size,
                new_ptr,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                s.reserved_array(),
            );
        }
        _ => {
            // Empty: just zero dst.
            // SAFETY: dst is non-null (checked above).
            unsafe { repr(dst).clear() };
        }
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_get(msg: *const OmqMsgRepr, property: c_int) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    match property {
        1 => zmq_msg_more(msg),
        3 => {
            // SAFETY: msg is non-null (checked above).
            let r = unsafe { repr_ref(msg) };
            i32::from(matches!(
                r.kind(),
                KIND_EXTERNAL | KIND_BYTES | KIND_HEAP_SHARED
            ))
        }
        5 => zmq_msg_routing_id(msg).cast_signed(),
        _ => {
            crate::error::set_errno(libc::EINVAL);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set(msg: *mut OmqMsgRepr, property: c_int, val: c_int) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    match property {
        1 => {
            r.set_more(u8::from(val != 0));
            0
        }
        5 => {
            r.reserved_mut()[0..4].copy_from_slice(&(val as u32).to_le_bytes());
            0
        }
        _ => crate::error::fail(libc::EINVAL),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_gets(
    msg: *const OmqMsgRepr,
    property: *const libc::c_char,
) -> *const libc::c_char {
    if msg.is_null() || property.is_null() {
        crate::error::set_errno(libc::EINVAL);
        return std::ptr::null();
    }
    // SAFETY: property is non-null (checked above); caller guarantees valid C string.
    let prop = unsafe { std::ffi::CStr::from_ptr(property) };
    match prop.to_bytes() {
        b"Socket-Type" | b"Identity" | b"Routing-Id" | b"Peer-Address" => c"".as_ptr(),
        _ => {
            crate::error::set_errno(libc::EINVAL);
            std::ptr::null()
        }
    }
}

/// Routing ID lives in reserved bytes 0..4 as little-endian u32.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set_routing_id(msg: *mut OmqMsgRepr, routing_id: u32) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    r.reserved_mut()[0..4].copy_from_slice(&routing_id.to_le_bytes());
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_routing_id(msg: *const OmqMsgRepr) -> u32 {
    if msg.is_null() {
        return 0;
    }
    // SAFETY: msg is non-null (checked above).
    let bytes = unsafe { &repr_ref(msg).reserved()[0..4] };
    u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]))
}

/// Group stored as null-terminated string in reserved bytes 4..16 (max 11 + null).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set_group(msg: *mut OmqMsgRepr, group: *const libc::c_char) -> c_int {
    if msg.is_null() || group.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: group is non-null (checked above); caller guarantees valid C string.
    let s = unsafe { CStr::from_ptr(group) }.to_bytes();
    if s.len() > 11 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    let reserved = r.reserved_mut();
    reserved[4..4 + s.len()].copy_from_slice(s);
    reserved[4 + s.len()] = 0;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_group(msg: *const OmqMsgRepr) -> *const libc::c_char {
    if msg.is_null() {
        return c"".as_ptr();
    }
    // SAFETY: msg is non-null (checked above); reserved area is always valid.
    unsafe { repr_ref(msg).reserved()[4..].as_ptr().cast() }
}

/// Send the data held in `msg` on `sock`. The msg is zeroed on success.
///
/// For RADIO sockets: if the msg has a group set via `zmq_msg_set_group`,
/// the group is prepended as the first frame of a 2-part message
/// `[group, body]` which `send_radio` expects.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_send(
    msg: *mut OmqMsgRepr,
    sock: *mut libc::c_void,
    flags: c_int,
) -> c_int {
    use std::sync::Arc;

    if msg.is_null() || sock.is_null() {
        return crate::error::fail(libc::EFAULT);
    }

    // SAFETY: sock is non-null (checked above); caller guarantees a valid socket.
    let sock_arc = unsafe { &*(sock.cast::<Arc<crate::socket::OmqSocket>>()) };

    let group = msg_group_bytes(msg);
    let routing_id = zmq_msg_routing_id(msg);

    // For KIND_BYTES, clone the Bytes (a zero-copy Arc bump) rather than
    // copying the payload. We must NOT mutate `msg` here: libzmq leaves the
    // message intact when a send fails, and the caller may then retry, copy,
    // or close it. Stealing the Box and nulling the fields used to leave the
    // message in a KIND_BYTES-with-null-boxed state that made a subsequent
    // zmq_msg_copy dereference a null pointer. On success the Box is dropped
    // by zmq_msg_close below.
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr_ref(msg) };
    let boxed = r.boxed();
    let bytes = if r.kind() == KIND_BYTES && !boxed.is_null() {
        // SAFETY: boxed was created by Box::into_raw in zmq_msg_recv; non-null.
        unsafe { &*(boxed.cast::<Bytes>()) }.clone()
    } else if r.ptr().is_null() && r.size() > 0 {
        return crate::error::fail(libc::EFAULT);
    } else {
        extract_bytes(msg)
    };

    if !group.is_empty() {
        // SAFETY: libzmq sockets are accessed by at most one application thread.
        let accum = unsafe { sock_arc.send_accum.get() };
        accum.push(group);
    }

    let ret = if sock_arc.socket_type == omq_tokio::SocketType::Server {
        let Ok(ret_len) = c_int::try_from(r.size()) else {
            return crate::error::fail(libc::EMSGSIZE);
        };
        let message = omq_tokio::Message::single(bytes).with_routing_id(routing_id);
        crate::send_recv::send_message(sock_arc, message, ret_len, flags)
    } else {
        crate::send_recv::send_bytes(sock_arc, &bytes, flags)
    };
    if ret >= 0 {
        zmq_msg_close(msg);
    }
    ret
}

/// Deprecated `zmq_sendmsg`: args are (socket, msg, flags), swapped
/// vs `zmq_msg_send` which is (msg, socket, flags).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_sendmsg(
    sock: *mut libc::c_void,
    msg: *mut OmqMsgRepr,
    flags: c_int,
) -> c_int {
    zmq_msg_send(msg, sock, flags)
}

/// Deprecated `zmq_recvmsg`: args are (socket, msg, flags), swapped
/// vs `zmq_msg_recv` which is (msg, socket, flags).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_recvmsg(
    sock: *mut libc::c_void,
    msg: *mut OmqMsgRepr,
    flags: c_int,
) -> c_int {
    zmq_msg_recv(msg, sock, flags)
}

fn msg_group_bytes(msg: *mut OmqMsgRepr) -> bytes::Bytes {
    // SAFETY: caller guarantees msg is non-null.
    let r = unsafe { repr_ref(msg) };
    let group_area = &r.reserved()[4..];
    let nul = group_area
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(group_area.len());
    if nul == 0 {
        return bytes::Bytes::new();
    }
    bytes::Bytes::copy_from_slice(&group_area[..nul])
}

/// Receive into `msg` as a zero-copy `KIND_BYTES` frame.
///
/// The `more` field is set to 1 when additional frames follow in the
/// current multipart message (i.e. `ZMQ_RCVMORE` would be true).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_recv(
    msg: *mut OmqMsgRepr,
    sock_ptr: *mut libc::c_void,
    flags: c_int,
) -> c_int {
    if msg.is_null() || sock_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: sock_ptr is non-null (checked above); caller guarantees a valid socket.
    let sock = unsafe { &*(sock_ptr.cast::<std::sync::Arc<crate::socket::OmqSocket>>()) };
    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return crate::error::fail(crate::error::ETERM);
    }

    let received = if sock.socket_type == omq_tokio::SocketType::Server {
        crate::send_recv::pop_recv_server_message(sock, flags).map(|message| {
            let routing_id = message.routing_id().unwrap_or(0);
            let frame = message.part_bytes(0).unwrap_or_default();
            (frame, false, routing_id)
        })
    } else {
        crate::send_recv::pop_recv_frame(sock, flags).map(|(frame, more)| (frame, more, 0))
    };

    match received {
        Ok((frame, more, routing_id)) => {
            zmq_msg_close(msg);
            let sz = frame.len();
            let mut reserved = [0; RESERVED_LEN];
            reserved[..4].copy_from_slice(&routing_id.to_le_bytes());
            // SAFETY: msg was just closed above and is ready for reinitialization.
            if sz <= 128 {
                // Small frame: malloc + memcpy (1 alloc) instead of
                // Box<Bytes> (2 allocs: Bytes::copy_from_slice + Box::new).
                let ptr = if sz > 0 {
                    // SAFETY: libc::malloc is always safe to call.
                    let p = unsafe { libc::malloc(sz).cast::<u8>() };
                    if p.is_null() {
                        return crate::error::fail(libc::ENOMEM);
                    }
                    // SAFETY: frame is valid for sz bytes; p was just allocated.
                    unsafe {
                        std::ptr::copy_nonoverlapping(frame.as_ptr(), p, sz);
                    }
                    p
                } else {
                    std::ptr::null_mut()
                };
                let r = unsafe { repr(msg) };
                r.init_fields(
                    KIND_HEAP,
                    u8::from(more),
                    sz,
                    ptr,
                    None,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    reserved,
                );
            } else {
                let boxed = Box::new(frame);
                let data_ptr = boxed.as_ptr().cast_mut();
                let r = unsafe { repr(msg) };
                r.init_fields(
                    KIND_BYTES,
                    u8::from(more),
                    sz,
                    data_ptr,
                    None,
                    std::ptr::null_mut(),
                    Box::into_raw(boxed).cast::<libc::c_void>(),
                    reserved,
                );
            }
            match c_int::try_from(sz) {
                Ok(n) => n,
                Err(_) => crate::error::fail(libc::EMSGSIZE),
            }
        }
        Err(e) => crate::error::fail(e),
    }
}

/// Extract a Bytes copy out of a msg without consuming ownership.
fn extract_bytes(msg: *const OmqMsgRepr) -> Bytes {
    // SAFETY: caller guarantees msg is non-null and initialized.
    let r = unsafe { repr_ref(msg) };
    let ptr = r.ptr();
    let size = r.size();
    if ptr.is_null() || size == 0 {
        return Bytes::new();
    }
    // SAFETY: ptr is non-null with size readable bytes (message invariant).
    Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, size) })
}
