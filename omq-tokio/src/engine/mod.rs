//! Connection driver: tokio glue between a `Transport`'s stream and the
//! sans-I/O ZMTP [`omq_proto::proto::Connection`].
//!
//! The driver owns the stream and the codec and runs a `tokio::select!`
//! loop over (socket read, socket write, command inbox, cancellation).
//! Events produced by the codec are forwarded on a `mpsc::Sender<Event>`.
//!
//! The socket actor composes one of these per peer.

pub mod compression_pool;
pub mod driver;
pub(crate) mod rate_limit;
pub(crate) mod send_pipe;
pub(crate) mod signal;
pub(crate) mod transmit_slot;

pub use crate::socket::recv::RecvItem;
pub use driver::{
    ConnectionDriver, PeerDriverCommand, PeerDriverConfig, PeerDriverHandle, PeerEvent, RecvSink,
    RecvSinkConfig, YringSink,
};
pub(crate) use send_pipe::{
    SendPipeConsumer, SendPipeError, SendPipeMode, SendPipeProducer, send_pipe, send_pipe_with_mode,
};
pub use signal::StateSignal;
