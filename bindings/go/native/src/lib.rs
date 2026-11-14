#![expect(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "C ABI entrypoints validate raw pointers and report status codes"
)]

#[cfg(not(target_pointer_width = "64"))]
compile_error!("OMQ Go native binding requires a 64-bit target");

mod auth;
mod cancel;
mod context;
mod convert;
mod error;
mod ffi;
mod message;
mod monitor;
mod ring;
mod socket;
mod stats;

pub use auth::{
    omq_go_curve_keypair, omq_go_curve_public, omq_go_socket_set_curve_client,
    omq_go_socket_set_curve_server, omq_go_socket_set_curve_server_callback,
    omq_go_socket_set_plain_client, omq_go_socket_set_plain_server,
    omq_go_socket_set_plain_server_callback,
};
pub use cancel::{
    OmqGoCancel, omq_go_cancel, omq_go_cancel_free, omq_go_cancel_new,
    omq_go_cancel_register_current,
};
pub use context::{
    OmqGoContext, omq_go_context_close, omq_go_context_free, omq_go_context_from_share_key,
    omq_go_context_open, omq_go_context_share_key,
};
pub use convert::omq_go_string_free;
pub use ffi::{
    OmqGoAuthPeer, OmqGoEvent, OmqGoMessage, OmqGoNativeStats, OmqGoPart, OmqGoPlainCredential,
    OmqGoRecvRingMemory, OmqGoSendRingMemory, OmqGoStatus, OmqGoWireMessage,
};
pub use message::omq_go_message_free;
pub use monitor::{
    OmqGoMonitor, omq_go_event_free, omq_go_monitor_close, omq_go_monitor_free,
    omq_go_monitor_recv, omq_go_socket_monitor,
};
pub use ring::recv::{
    OmqGoRecvRing, omq_go_recv_ring_close, omq_go_recv_ring_create, omq_go_recv_ring_fill,
    omq_go_recv_ring_fill_cancelable, omq_go_recv_ring_memory,
};
pub use ring::send::{
    OmqGoSendRing, omq_go_send_ring_close, omq_go_send_ring_create, omq_go_send_ring_error,
    omq_go_send_ring_memory, omq_go_send_ring_wait, omq_go_send_ring_wake,
};
pub use socket::options::{
    omq_go_socket_set_arena_threshold, omq_go_socket_set_compression_auto_train,
    omq_go_socket_set_compression_dict, omq_go_socket_set_compression_dict_capacity,
    omq_go_socket_set_compression_level, omq_go_socket_set_compression_offload_threshold,
    omq_go_socket_set_compression_threshold, omq_go_socket_set_conflate,
    omq_go_socket_set_handshake_timeout, omq_go_socket_set_heartbeat_interval,
    omq_go_socket_set_heartbeat_timeout, omq_go_socket_set_heartbeat_ttl,
    omq_go_socket_set_identity, omq_go_socket_set_large_message_threshold,
    omq_go_socket_set_linger, omq_go_socket_set_max_message_parts,
    omq_go_socket_set_max_message_size, omq_go_socket_set_max_peer_subscriptions,
    omq_go_socket_set_max_pending_handshakes, omq_go_socket_set_max_recv_dict_size,
    omq_go_socket_set_on_mute, omq_go_socket_set_reconnect,
    omq_go_socket_set_reconnect_stop_conn_refused, omq_go_socket_set_recv_buffer_size,
    omq_go_socket_set_recv_hwm, omq_go_socket_set_router_mandatory,
    omq_go_socket_set_send_buffer_size, omq_go_socket_set_send_hwm,
    omq_go_socket_set_tcp_keepalive, omq_go_socket_set_transmit_slot_cap,
    omq_go_socket_set_workload_profile, omq_go_socket_set_xpub_nodrop,
};
pub use socket::recv::{
    omq_go_receive_any, omq_go_socket_recv, omq_go_socket_recv_cancelable,
    omq_go_socket_recv_one_into, omq_go_socket_recv_one_into_cancelable,
};
pub use socket::send::{
    omq_go_socket_send, omq_go_socket_send_cancelable, omq_go_socket_send_one,
    omq_go_socket_try_send_batch,
};
pub use socket::{
    OmqGoSocket, omq_go_socket_bind, omq_go_socket_bind_timeout, omq_go_socket_close,
    omq_go_socket_connect, omq_go_socket_connect_timeout, omq_go_socket_disconnect,
    omq_go_socket_free, omq_go_socket_join, omq_go_socket_leave, omq_go_socket_new,
    omq_go_socket_subscribe, omq_go_socket_unbind, omq_go_socket_unsubscribe,
    omq_go_socket_wait_connected, omq_go_socket_wait_subscribed,
};
pub use stats::omq_go_native_stats;
