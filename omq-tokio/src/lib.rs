//! omq-tokio - tokio-runtime backend for omq.
//!
//! Wire-compatible with libzmq. Supports 20 socket types, TCP / IPC /
//! inproc / UDP transports, NULL / PLAIN / CURVE mechanisms, and the
//! lz4+tcp / zstd+tcp compression transports.
//!
//! The codec, message types, mechanism handshakes, and routing
//! algorithms live in the runtime-agnostic `omq-proto` crate.
//! This crate provides the tokio glue: per-connection drivers,
//! transport implementations, and the public `Socket` actor.
//!
//! # Compatibility warnings
//!
//! Native `Options::linger` defaults to zero, unlike libzmq's forever linger
//! default. Native `Options::send_hwm` counts messages, not bytes, and is not
//! an exact total queue cap. Bound no-peer round-robin sends mute like libzmq.
//! Connected no-peer round-robin sends queue in a connect-side pre-ready pipe
//! unless `omq-libzmq` has `ZMQ_IMMEDIATE=1`.
#![forbid(unsafe_code)]

#[cfg(not(target_has_atomic = "64"))]
compile_error!("omq-tokio requires target_has_atomic = \"64\"");

pub mod blocking;
mod buffer_pool;
pub mod context;
pub mod engine;
pub mod exclusive;
pub mod proxy;
pub(crate) mod routing;
pub mod socket;
pub mod transport;

// Re-export the sans-I/O surface so downstream callers don't have
// to depend on omq-proto explicitly. Identical surface to the
// pre-split crate.
pub use buffer_pool::{BufferLengthError, BufferPool, MessageBuffer};
pub use omq_proto::IpcPath;
pub use omq_proto::{AuthenticationResult, AuthenticationStatus, Authenticator, MechanismPeerInfo};
pub use omq_proto::{
    CompressionKind, CompressionOptions, Endpoint, EndpointRole, EndpointSpec, Error, Frame,
    FrameFlags, HandshakeRefusal, KeepAlive, MechanismConfig, MechanismSetup, Message, MessageIter,
    MessagePool, OnMute, Options, PartCountError, ReconnectPolicy, Result, SocketType,
    TrySendError, is_compatible,
};
#[cfg(feature = "curve")]
pub use omq_proto::{CurveKeypair, CurvePublicKey, CurveSecretKey, CurveServerOptions};
#[cfg(feature = "dart")]
pub use omq_proto::{DartCongestion, DartEcn, DartOptions};
#[cfg(feature = "dart")]
pub use transport::dart::{DartCapabilities, DartStats};

// Sub-modules of omq_proto are re-exported under their original
// paths so downstream `use omq_tokio::endpoint::Host` style imports keep
// working.
pub use omq_proto::endpoint;
pub use omq_proto::error;
pub use omq_proto::flow;
pub use omq_proto::message;
pub use omq_proto::options;
pub use omq_proto::proto;

pub use context::{Context, ContextConfig, ContextCore};
pub use proxy::{Proxy, ProxyExit};
pub use socket::{
    ConnectionStatus, DisconnectReason, IdentitySocket, MonitorEvent, MonitorRecvError,
    MonitorStream, MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo, ReceiveReceipt,
    ReceiveSource, Socket, UnshiftError,
};
