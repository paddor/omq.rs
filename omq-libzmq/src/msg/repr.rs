//! `zmq_msg_t` storage layout: a 64-byte, pointer-aligned blob with typed
//! accessors, plus the boxed receive payloads it can own.

use std::ffi::{CStr, CString};
use std::sync::Arc;

use bytes::Bytes;

// kind discriminants
#[expect(dead_code)]
const KIND_EMPTY: u8 = 0;

pub(super) const KIND_HEAP: u8 = 1;
pub(super) const KIND_EXTERNAL: u8 = 2;
pub(super) const KIND_BYTES: u8 = 3;
pub(super) const KIND_HEAP_SHARED: u8 = 4;
pub(super) const KIND_BYTES_METADATA: u8 = 5;

pub(crate) const ZMQ_MSG_T_SIZE: usize = 64;
const PTR_SIZE: usize = std::mem::size_of::<usize>();
const MSG_WORDS: usize = ZMQ_MSG_T_SIZE / PTR_SIZE;
const OFF_KIND: usize = 0;
const OFF_MORE: usize = 1;
/// Set when the group is a heap string. Bytes 2..8 are otherwise padding.
const OFF_GROUP_HEAP: usize = 2;
const OFF_SIZE: usize = 8;
const OFF_PTR: usize = 16;
const OFF_FREE_FN: usize = OFF_PTR + PTR_SIZE;
const OFF_HINT: usize = OFF_FREE_FN + PTR_SIZE;
const OFF_BOXED: usize = OFF_HINT + PTR_SIZE;
const OFF_RESERVED: usize = OFF_BOXED + PTR_SIZE;
const RESERVED_LEN: usize = 16;
/// The routing ID takes reserved bytes 0..4; the group area follows.
const OFF_GROUP: usize = OFF_RESERVED + 4;
/// Longest group that fits in the group area next to its terminator.
const INLINE_GROUP_MAX: usize = RESERVED_LEN - 4 - 1;
/// `ZMQ_GROUP_MAX_LENGTH`.
pub(super) const GROUP_MAX: usize = 255;

#[derive(Clone, Debug)]
pub(super) struct ReceivedFrame {
    pub(super) bytes: Bytes,
    properties: Arc<Vec<(String, Vec<u8>)>>,
}

impl ReceivedFrame {
    pub(super) fn new(bytes: Bytes, peer: &omq_tokio::proto::PeerProperties) -> Self {
        let mut properties = Vec::with_capacity(peer.other.len() + 2);
        if let Some(socket_type) = peer.socket_type {
            properties.push(c_property("Socket-Type", socket_type.as_str().as_bytes()));
        }

        if let Some(identity) = &peer.identity {
            properties.push(c_property("Identity", identity));
        }

        properties.extend(
            peer.other
                .iter()
                .map(|(name, value)| c_property(name, value)),
        );

        Self {
            bytes,
            properties: Arc::new(properties),
        }
    }

    pub(super) fn property(&self, name: &[u8]) -> Option<*const libc::c_char> {
        self.properties
            .iter()
            .find(|(candidate, _)| candidate.as_bytes() == name)
            .map(|(_, value)| value.as_ptr().cast())
    }
}

fn c_property(name: impl Into<String>, value: &[u8]) -> (String, Vec<u8>) {
    let mut c_value = Vec::with_capacity(value.len() + 1);
    c_value.extend_from_slice(value);
    c_value.push(0);

    (name.into(), c_value)
}

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

type FreeFn = unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void);

/// Every field `OmqMsgRepr::init_fields` writes. Optional owners default to null.
#[derive(Clone, Copy)]
pub(super) struct MsgFields {
    kind: u8,
    more: u8,
    size: usize,
    ptr: *mut u8,
    free_fn: Option<FreeFn>,
    hint: *mut libc::c_void,
    boxed: *mut libc::c_void,
    routing_id: u32,
}

impl MsgFields {
    #[inline]
    pub(super) fn new(kind: u8, more: u8, size: usize, ptr: *mut u8) -> Self {
        Self {
            kind,
            more,
            size,
            ptr,
            free_fn: None,
            hint: std::ptr::null_mut(),
            boxed: std::ptr::null_mut(),
            routing_id: 0,
        }
    }

    /// Caller-supplied release callback for `KIND_EXTERNAL` payloads.
    #[inline]
    pub(super) fn free_fn(mut self, free_fn: Option<FreeFn>, hint: *mut libc::c_void) -> Self {
        self.free_fn = free_fn;
        self.hint = hint;
        self
    }

    /// Box that owns the payload behind `ptr` (`Bytes` or `ReceivedFrame`).
    #[inline]
    pub(super) fn boxed(mut self, boxed: *mut libc::c_void) -> Self {
        self.boxed = boxed;
        self
    }

    /// SERVER routing ID carried alongside the payload.
    #[inline]
    pub(super) fn routing_id(mut self, routing_id: u32) -> Self {
        self.routing_id = routing_id;
        self
    }
}

impl OmqMsgRepr {
    #[inline]
    pub(super) fn clear(&mut self) {
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
    pub(super) fn kind(&self) -> u8 {
        self.bytes()[OFF_KIND]
    }

    #[inline]
    pub(super) fn more(&self) -> u8 {
        self.bytes()[OFF_MORE]
    }

    #[inline]
    pub(super) fn set_more(&mut self, more: u8) {
        self.bytes_mut()[OFF_MORE] = more;
    }

    #[inline]
    pub(super) fn size(&self) -> usize {
        usize::try_from(self.read_u64(OFF_SIZE)).unwrap_or(usize::MAX)
    }

    #[inline]
    pub(super) fn ptr(&self) -> *mut u8 {
        self.read_usize(OFF_PTR) as *mut u8
    }

    #[inline]
    fn boxed(&self) -> *mut libc::c_void {
        self.read_usize(OFF_BOXED) as *mut libc::c_void
    }

    #[inline]
    pub(super) fn hint(&self) -> *mut libc::c_void {
        self.read_usize(OFF_HINT) as *mut libc::c_void
    }

    #[inline]
    pub(super) fn free_fn(&self) -> Option<FreeFn> {
        let raw = self.read_usize(OFF_FREE_FN);
        if raw == 0 {
            None
        } else {
            // SAFETY: raw was written from a `FreeFn` in `init_fields`.
            Some(unsafe { std::mem::transmute::<usize, FreeFn>(raw) })
        }
    }

    /// Routing ID: reserved bytes 0..4 as a little-endian u32.
    #[inline]
    pub(super) fn routing_id(&self) -> u32 {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&self.bytes()[OFF_RESERVED..OFF_RESERVED + 4]);
        u32::from_le_bytes(bytes)
    }

    #[inline]
    pub(super) fn set_routing_id(&mut self, routing_id: u32) {
        self.bytes_mut()[OFF_RESERVED..OFF_RESERVED + 4].copy_from_slice(&routing_id.to_le_bytes());
    }

    #[inline]
    pub(super) fn init_fields(&mut self, fields: MsgFields) {
        self.clear();
        self.bytes_mut()[OFF_KIND] = fields.kind;
        self.bytes_mut()[OFF_MORE] = fields.more;
        self.write_u64(OFF_SIZE, fields.size as u64);
        self.write_usize(OFF_PTR, fields.ptr as usize);
        self.write_usize(OFF_FREE_FN, fields.free_fn.map_or(0, |f| f as usize));
        self.write_usize(OFF_HINT, fields.hint as usize);
        self.write_usize(OFF_BOXED, fields.boxed as usize);
        self.set_routing_id(fields.routing_id);
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

/// Payload access and the boxed receive storage behind `KIND_BYTES` and
/// `KIND_BYTES_METADATA` messages.
impl OmqMsgRepr {
    /// An all-zero message, as `zmq_msg_init` leaves it.
    #[cfg(test)]
    pub(super) fn zeroed() -> Self {
        Self {
            storage: [0; MSG_WORDS],
        }
    }

    #[inline]
    pub(super) fn set_ptr(&mut self, ptr: *mut u8) {
        self.write_usize(OFF_PTR, ptr as usize);
    }

    /// The payload bytes; empty when the pointer is null.
    pub(super) fn payload(&self) -> &[u8] {
        let (ptr, size) = (self.ptr(), self.size());
        if ptr.is_null() || size == 0 {
            return &[];
        }

        // SAFETY: a non-null payload pointer is valid for `size` bytes while
        // the message holds it.
        unsafe { std::slice::from_raw_parts(ptr, size) }
    }

    /// The received `Bytes` this message owns, if any.
    #[inline]
    pub(super) fn boxed_bytes(&self) -> Option<&Bytes> {
        let boxed = self.boxed();
        if boxed.is_null() {
            return None;
        }

        match self.kind() {
            // SAFETY: the box of a KIND_BYTES message is a `Box<Bytes>` from
            // `Box::into_raw`, owned by this message.
            KIND_BYTES => Some(unsafe { &*boxed.cast::<Bytes>() }),
            KIND_BYTES_METADATA => self.received_frame().map(|frame| &frame.bytes),
            _ => None,
        }
    }

    /// Mutable [`boxed_bytes`](Self::boxed_bytes).
    pub(super) fn boxed_bytes_mut(&mut self) -> Option<&mut Bytes> {
        let boxed = self.boxed();
        if boxed.is_null() {
            return None;
        }

        match self.kind() {
            // SAFETY: as in `boxed_bytes`; `&mut self` makes the access unique.
            KIND_BYTES => Some(unsafe { &mut *boxed.cast::<Bytes>() }),
            // SAFETY: as in `received_frame`; `&mut self` makes the access unique.
            KIND_BYTES_METADATA => Some(&mut unsafe { &mut *boxed.cast::<ReceivedFrame>() }.bytes),
            _ => None,
        }
    }

    /// The frame and peer properties a `KIND_BYTES_METADATA` message owns.
    pub(super) fn received_frame(&self) -> Option<&ReceivedFrame> {
        let boxed = self.boxed();
        if self.kind() != KIND_BYTES_METADATA || boxed.is_null() {
            return None;
        }

        // SAFETY: the box of a KIND_BYTES_METADATA message is a
        // `Box<ReceivedFrame>` from `Box::into_raw`, owned by this message.
        Some(unsafe { &*boxed.cast::<ReceivedFrame>() })
    }

    /// Clone the receive box for another message (a refcount bump, no copy):
    /// the clone's payload pointer and raw box. `None` without a box.
    pub(super) fn clone_boxed(&self) -> Option<(*mut u8, *mut libc::c_void)> {
        if let Some(frame) = self.received_frame() {
            let cloned = Box::new(frame.clone());
            let ptr = cloned.bytes.as_ptr().cast_mut();
            return Some((ptr, Box::into_raw(cloned).cast()));
        }

        let cloned = Box::new(self.boxed_bytes()?.clone());
        let ptr = cloned.as_ptr().cast_mut();
        Some((ptr, Box::into_raw(cloned).cast()))
    }

    /// Free the receive box, if any, and null its slot.
    pub(super) fn drop_boxed(&mut self) {
        let boxed = self.boxed();
        if boxed.is_null() {
            return;
        }

        match self.kind() {
            // SAFETY: as in `boxed_bytes`; the message gives up the box here.
            KIND_BYTES => drop(unsafe { Box::from_raw(boxed.cast::<Bytes>()) }),
            // SAFETY: as in `received_frame`; the message gives up the box here.
            KIND_BYTES_METADATA => drop(unsafe { Box::from_raw(boxed.cast::<ReceivedFrame>()) }),
            _ => return,
        }

        self.write_usize(OFF_BOXED, 0);
    }
}

/// RADIO/DISH group. Up to 11 bytes sit in the group area as a C string.
/// A longer group is a heap C string whose pointer takes the start of that
/// area, flagged at `OFF_GROUP_HEAP`.
impl OmqMsgRepr {
    #[inline]
    fn group_area(&self) -> &[u8] {
        &self.bytes()[OFF_GROUP..OFF_RESERVED + RESERVED_LEN]
    }

    /// The heap group string; null when the group is inline.
    #[inline]
    fn heap_group(&self) -> *mut libc::c_char {
        if self.bytes()[OFF_GROUP_HEAP] == 0 {
            return std::ptr::null_mut();
        }
        self.read_usize(OFF_GROUP) as *mut libc::c_char
    }

    /// The group as a C string that lives until the message changes.
    pub(super) fn group_ptr(&self) -> *const libc::c_char {
        let heap = self.heap_group();
        if heap.is_null() {
            self.group_area().as_ptr().cast()
        } else {
            heap
        }
    }

    /// The group bytes without their terminator; empty when unset.
    pub(super) fn group(&self) -> &[u8] {
        let heap = self.heap_group();
        if !heap.is_null() {
            // SAFETY: a flagged group pointer is a `CString::into_raw`
            // string owned by this message.
            return unsafe { CStr::from_ptr(heap) }.to_bytes();
        }
        until_nul(self.group_area())
    }

    /// Replace the group with `group`, cut at its first NUL byte.
    pub(super) fn set_group(&mut self, group: &[u8]) {
        self.drop_group();
        let group = until_nul(group);
        if group.len() <= INLINE_GROUP_MAX {
            self.bytes_mut()[OFF_GROUP..OFF_GROUP + group.len()].copy_from_slice(group);
            return;
        }
        // `group` holds no NUL, so the conversion cannot fail.
        let owned = CString::new(group).unwrap_or_default();
        self.write_usize(OFF_GROUP, owned.into_raw() as usize);
        self.bytes_mut()[OFF_GROUP_HEAP] = 1;
    }

    /// Free a heap group, if any, and leave the group empty.
    pub(super) fn drop_group(&mut self) {
        let heap = self.heap_group();
        if !heap.is_null() {
            // SAFETY: as in `group`; the message gives the string up here.
            drop(unsafe { CString::from_raw(heap) });
            self.bytes_mut()[OFF_GROUP_HEAP] = 0;
        }
        self.bytes_mut()[OFF_GROUP..OFF_RESERVED + RESERVED_LEN].fill(0);
    }
}

/// `bytes` up to its first NUL byte.
fn until_nul(bytes: &[u8]) -> &[u8] {
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    &bytes[..end]
}

const _SIZE_ASSERT: () = assert!(std::mem::size_of::<OmqMsgRepr>() == ZMQ_MSG_T_SIZE);

const _ALIGN_ASSERT: () =
    assert!(std::mem::align_of::<OmqMsgRepr>() == std::mem::align_of::<usize>());

const _RESERVED_ASSERT: () = assert!(OFF_RESERVED + RESERVED_LEN <= ZMQ_MSG_T_SIZE);

#[inline]
/// # Safety
///
/// `msg` must be non-null and point to a valid, initialized `OmqMsgRepr`.
pub(super) unsafe fn repr<'a>(msg: *mut OmqMsgRepr) -> &'a mut OmqMsgRepr {
    unsafe { &mut *msg }
}

#[inline]
/// # Safety
///
/// `msg` must be non-null and point to a valid, initialized `OmqMsgRepr`.
pub(super) unsafe fn repr_ref<'a>(msg: *const OmqMsgRepr) -> &'a OmqMsgRepr {
    unsafe { &*msg }
}
