//! Connection driver: tokio glue between a `Transport`'s stream and the
//! sans-I/O ZMTP [`Connection`].
//!
//! The driver owns the stream and the codec and runs a `tokio::select!`
//! loop over (socket read, socket write, command inbox, cancellation).
//! Events produced by the codec are forwarded on a `mpsc::Sender<Event>`.
//!
//! Phase 4 composes one of these per peer behind a `Socket` actor.

pub mod driver;

pub use driver::{ConnectionDriver, DriverCommand, DriverConfig, DriverHandle, PeerOut};
