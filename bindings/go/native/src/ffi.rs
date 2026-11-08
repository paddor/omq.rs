//! C ABI value layouts.

use std::ffi::{c_char, c_int, c_void};

#[repr(C)]
pub struct OmqGoStatus {
    pub(crate) code: i32,
    pub(crate) message: *mut c_char,
}

#[repr(C)]
pub struct OmqGoPart {
    pub(crate) data: *mut u8,
    pub(crate) len: usize,
}

#[repr(C)]
pub struct OmqGoMessage {
    pub(crate) parts: *mut OmqGoPart,
    pub(crate) part_count: usize,
    pub(crate) routing_id: u32,
}

#[repr(C)]
pub struct OmqGoWireMessage {
    pub(crate) parts: *const OmqGoPart,
    pub(crate) part_count: usize,
    pub(crate) routing_id: u32,
}

#[repr(C)]
pub struct OmqGoPlainCredential {
    pub(crate) username: *const c_char,
    pub(crate) password: *const c_char,
}

#[repr(C)]
pub struct OmqGoEvent {
    pub(crate) kind: *mut c_char,
    pub(crate) endpoint: *mut c_char,
    pub(crate) peer_ident: *mut c_char,
    pub(crate) peer_address: *mut c_char,
    pub(crate) peer_socket_type: *mut c_char,
    pub(crate) reason: *mut c_char,
    pub(crate) command_name: *mut c_char,
    pub(crate) data: *mut u8,
    pub(crate) data_len: usize,
    pub(crate) peer_identity: *mut u8,
    pub(crate) peer_identity_len: usize,
    pub(crate) connection_id: u64,
    pub(crate) retry_millis: u64,
    pub(crate) attempt: u32,
    pub(crate) zmtp_major: u32,
    pub(crate) zmtp_minor: u32,
    pub(crate) has_peer: c_int,
}

#[repr(C)]
pub struct OmqGoAuthPeer {
    pub(crate) mechanism_data: *const u8,
    pub(crate) mechanism_len: usize,
    pub(crate) public_key_data: *const u8,
    pub(crate) public_key_len: usize,
    pub(crate) identity_data: *const u8,
    pub(crate) identity_len: usize,
    pub(crate) peer_address_data: *const u8,
    pub(crate) peer_address_len: usize,
    pub(crate) username_data: *const u8,
    pub(crate) username_len: usize,
    pub(crate) password_data: *const u8,
    pub(crate) password_len: usize,
}

#[repr(C)]
pub struct OmqGoSendRingMemory {
    pub(crate) control: *mut c_void,
    pub(crate) descriptors: *mut c_void,
    pub(crate) payload: *mut c_void,
    pub(crate) desc_capacity: usize,
    pub(crate) payload_capacity: usize,
}

#[repr(C)]
pub struct OmqGoRecvRingMemory {
    pub(crate) control: *mut c_void,
    pub(crate) descriptors: *mut c_void,
    pub(crate) payload: *mut c_void,
    pub(crate) desc_capacity: usize,
    pub(crate) payload_capacity: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct OmqGoNativeStats {
    pub(crate) contexts_created: u64,
    pub(crate) contexts_freed: u64,
    pub(crate) contexts_live: u64,
    pub(crate) sockets_created: u64,
    pub(crate) sockets_freed: u64,
    pub(crate) sockets_live: u64,
    pub(crate) monitors_created: u64,
    pub(crate) monitors_freed: u64,
    pub(crate) monitors_live: u64,
    pub(crate) send_rings_created: u64,
    pub(crate) send_rings_freed: u64,
    pub(crate) send_rings_live: u64,
    pub(crate) recv_rings_created: u64,
    pub(crate) recv_rings_freed: u64,
    pub(crate) recv_rings_live: u64,
    pub(crate) cancels_created: u64,
    pub(crate) cancels_freed: u64,
    pub(crate) cancels_live: u64,
}
