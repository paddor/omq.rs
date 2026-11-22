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
/// Bounded blocking workers for compression transforms.
pub mod compression_pool;
pub(crate) mod control_inbox;
pub(crate) mod control_queue;
pub(crate) mod data_inbox;
pub mod driver;
pub(crate) mod framing;
pub(crate) mod peer_completion;
pub(crate) mod peer_events;
mod producer_ready;
pub(crate) mod rate_limit;
pub(crate) mod receive_cell;
mod recv_sink;
mod ring_drain;

pub(crate) use recv_sink::reserve_authenticated;

pub(crate) mod send_pipe;
pub(crate) mod shm_state;
pub(crate) mod signal;
pub(crate) mod single_inbox;
pub(crate) mod subscription_ledger;
pub(crate) mod transmit_slot;
pub(crate) mod write_ownership;

pub(crate) use driver::ActorPeerDriverHandle;
pub use driver::{
    AuthenticatedRecvItem, ConnectionDriver, PeerDriverCommand, PeerDriverConfig, PeerDriverData,
    PeerDriverHandle, PeerEvent, RecvSink, RecvSinkConfig, YringSink,
};
pub use ring_drain::RingDrain;
pub(crate) use send_pipe::{
    SendPipeConsumer, SendPipeError, SendPipeHome, SendPipeLease, SendPipeMode, SendPipeProducer,
    inproc_send_pipe, peer_send_pipe, send_pipe, send_pipe_with_mode,
};
pub use signal::{StateSignal, block_on};

pub(crate) mod peer_send;

/// Slots a queue ring starts with. It doubles up to its HWM capacity while a
/// backlog builds, so idle and lightly loaded peers stay small.
const QUEUE_RING_INITIAL_SLOTS: usize = 1024;

/// A queue ring bounded at `capacity` that starts with 1024 slots and grows
/// on demand. It keeps its peak size until dropped.
#[doc(hidden)]
#[must_use]
pub fn queue_ring<T>(capacity: usize) -> (yring::Producer<T>, yring::Consumer<T>) {
    yring::spsc_growing(QUEUE_RING_INITIAL_SLOTS, capacity)
}

#[cfg(test)]
mod tests;
