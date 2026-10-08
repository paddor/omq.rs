//! Connection driver: tokio glue between a `Transport`'s stream and the
//! sans-I/O ZMTP [`omq_proto::proto::Connection`].
//!
//! The driver owns the stream and the codec and runs a `tokio::select!`
//! loop over socket read/write, separate control and fallback-data inboxes,
//! and cancellation.
//! Socket-owned drivers separate codec control from per-driver fanring data
//! lanes. Standalone drivers retain the caller's combined Tokio event queue.
//!
//! The socket actor composes one of these per peer.

pub(crate) mod actor_output;
pub(crate) mod codec;
pub mod compression_pool;
pub(crate) mod control_inbox;
pub(crate) mod data_inbox;
pub mod driver;
pub(crate) mod framing;
pub(crate) mod peer_completion;
pub(crate) mod peer_events;
pub(crate) mod rate_limit;
pub(crate) mod receive_cell;
mod recv_sink;
pub(crate) use recv_sink::reserve_authenticated;
pub(crate) mod send_pipe;
pub(crate) mod signal;
pub(crate) mod single_inbox;
pub(crate) mod transmit_slot;
pub(crate) mod write_ownership;

pub(crate) use driver::ActorPeerDriverHandle;
pub use driver::{
    AuthenticatedRecvItem, ConnectionDriver, PeerDriverCommand, PeerDriverConfig, PeerDriverData,
    PeerDriverHandle, PeerEvent, RecvSink, RecvSinkConfig, YringSink,
};
pub(crate) use send_pipe::{
    SendPipeConsumer, SendPipeError, SendPipeMode, SendPipeProducer, inproc_send_pipe,
    peer_send_pipe, send_pipe, send_pipe_with_mode,
};
pub use signal::StateSignal;

pub(crate) mod peer_send;
