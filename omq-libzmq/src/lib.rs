//! omq-libzmq -- libzmq-compatible C interface backed by omq-tokio.
//!
//! # Panic safety
//!
//! Since Rust 1.71, panics in `extern "C"` functions abort rather than
//! UB. No `catch_unwind` wrappers are used: wrapping 50+ FFI functions
//! in `AssertUnwindSafe` would weaken static guarantees for marginal
//! benefit. libzmq itself aborts on internal errors, so abort-on-panic
//! is acceptable behavior for a drop-in replacement.
//!
//! # Send and HWM semantics
//!
//! `ZMQ_SNDHWM` and `ZMQ_RCVHWM` count complete messages, not bytes.
//! `ZMQ_LINGER` defaults to `-1`, matching libzmq. Bound round-robin sockets
//! with no ready peer are muted: blocking `zmq_send()` waits, while
//! `ZMQ_DONTWAIT` or `ZMQ_SNDTIMEO=0` returns `EAGAIN`.

#![deny(unsafe_op_in_unsafe_fn)]
// Crate-level: 53 of 64 extern "C" fns trigger this lint. Per-function
// #[expect] would be pure noise for a C API crate.
#![expect(clippy::not_unsafe_ptr_arg_deref)]

#[cfg(not(target_has_atomic = "64"))]
compile_error!("omq-libzmq requires target_has_atomic = \"64\"");

mod consts;
mod context;
pub mod curve;
mod error;
mod inproc_bypass;
mod local_cell;
mod msg;
mod notify;
mod opts;
pub mod poll;
pub mod proxy;
mod send_recv;
mod socket;
mod util;

pub use context::{
    zmq_ctx_destroy, zmq_ctx_get, zmq_ctx_new, zmq_ctx_set, zmq_ctx_shutdown, zmq_ctx_term,
    zmq_init, zmq_term,
};
pub use curve::{zmq_curve_keypair, zmq_curve_public, zmq_z85_decode, zmq_z85_encode};
pub use error::{zmq_errno, zmq_strerror};
pub use msg::{
    zmq_msg_close, zmq_msg_copy, zmq_msg_data, zmq_msg_get, zmq_msg_gets, zmq_msg_group,
    zmq_msg_init, zmq_msg_init_buffer, zmq_msg_init_data, zmq_msg_init_size, zmq_msg_more,
    zmq_msg_move, zmq_msg_recv, zmq_msg_routing_id, zmq_msg_send, zmq_msg_set, zmq_msg_set_group,
    zmq_msg_set_routing_id, zmq_msg_size, zmq_recvmsg, zmq_sendmsg,
};
pub use poll::zmq_poll;
pub use proxy::{zmq_proxy, zmq_proxy_steerable};
pub use send_recv::{zmq_recv, zmq_send, zmq_send_const};
pub use socket::{
    zmq_bind, zmq_close, zmq_connect, zmq_disconnect, zmq_join, zmq_leave, zmq_socket,
    zmq_socket_monitor, zmq_unbind,
};
pub use util::{
    zmq_atomic_counter_dec, zmq_atomic_counter_destroy, zmq_atomic_counter_inc,
    zmq_atomic_counter_new, zmq_atomic_counter_set, zmq_atomic_counter_value, zmq_has, zmq_sleep,
    zmq_stopwatch_intermediate, zmq_stopwatch_start, zmq_stopwatch_stop, zmq_version,
};

// The opts module exports setsockopt/getsockopt directly as C symbols.
pub use opts::{zmq_getsockopt, zmq_setsockopt};

const _: () = assert!(std::mem::size_of::<msg::OmqMsgRepr>() == 64);
const _: () = assert!(std::mem::align_of::<msg::OmqMsgRepr>() == std::mem::align_of::<usize>());
