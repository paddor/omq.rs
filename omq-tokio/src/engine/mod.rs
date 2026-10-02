//! Connection driver: tokio glue between a `Transport`'s stream and the
//! sans-I/O ZMTP [`omq_proto::proto::Connection`].
//!
//! The driver owns the stream and the codec and runs a `tokio::select!`
//! loop over socket read/write, separate control and fallback-data inboxes,
//! and cancellation.
//! Events produced by the codec are forwarded on a `mpsc::Sender<Event>`.
//!
//! The socket actor composes one of these per peer.

pub(crate) mod codec;
pub mod compression_pool;
pub mod driver;
pub(crate) mod framing;
pub(crate) mod peer_completion;
mod peer_events;
pub(crate) mod rate_limit;
mod recv_sink;
pub(crate) use recv_sink::reserve_authenticated;
pub(crate) mod send_pipe;
pub(crate) mod signal;
pub(crate) mod transmit_slot;
pub(crate) mod write_ownership;

pub use driver::{
    AuthenticatedRecvItem, ConnectionDriver, PeerDriverCommand, PeerDriverConfig, PeerDriverData,
    PeerDriverHandle, PeerEvent, RecvSink, RecvSinkConfig, YringSink,
};
pub(crate) use send_pipe::{
    SendPipeConsumer, SendPipeError, SendPipeMode, SendPipeProducer, peer_send_pipe, send_pipe,
    send_pipe_with_mode,
};
pub use signal::StateSignal;

pub(crate) mod peer_send;
