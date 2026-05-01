//! omq-tokio - tokio-runtime backend for omq.
//!
//! Wire-compatible with libzmq. All 11 standard socket types plus 7
//! draft types, TCP / IPC / inproc / UDP transports, NULL / CURVE /
//! blake3zmq mechanisms, lz4+tcp / zstd+tcp compression transports.
//!
//! The codec, message types, mechanism handshakes, and routing
//! algorithms live in the runtime-agnostic `omq-proto` crate.
//! This crate provides the tokio glue: per-connection drivers,
//! transport implementations, and the public `Socket` actor.

pub mod engine;
pub(crate) mod routing;
pub mod socket;
pub mod transport;

// Re-export the sans-I/O surface so downstream callers don't have
// to depend on omq-proto explicitly. Identical surface to the
// pre-split crate.
#[cfg(feature = "priority")]
pub use omq_proto::ConnectOpts;
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
pub use omq_proto::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "blake3zmq")]
pub use omq_proto::{Blake3ZmqKeypair, Blake3ZmqPublicKey, Blake3ZmqSecretKey};
#[cfg(feature = "curve")]
pub use omq_proto::{CurveKeypair, CurvePublicKey, CurveSecretKey};
pub use omq_proto::{
    Endpoint, EndpointRole, EndpointSpec, Error, Frame, FrameFlags, IpcPath, KeepAlive,
    MechanismConfig, Message, OnMute, Options, Payload, ReconnectPolicy, Result, SocketType,
    is_compatible,
};

// Sub-modules of omq_proto are re-exported under their original
// paths so downstream `use omq_tokio::endpoint::Host` style imports keep
// working.
pub use omq_proto::endpoint;
pub use omq_proto::error;
pub use omq_proto::message;
pub use omq_proto::options;
pub use omq_proto::proto;

pub use socket::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo, Socket,
};
