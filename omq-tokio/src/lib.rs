//! omq-tokio - tokio-runtime backend for omq.
//!
//! Wire-compatible with libzmq. All 11 standard socket types plus 7
//! draft types, TCP / IPC / inproc / UDP transports, NULL / CURVE /
//! lz4+tcp compression transport.
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
//! an exact total queue cap. Native round-robin no-peer sends queue into
//! fallback up to `send_hwm`; libzmq mutes bound no-peer sockets instead.
#![forbid(unsafe_code)]

#[cfg(not(target_has_atomic = "64"))]
compile_error!("omq-tokio requires target_has_atomic = \"64\"");

pub mod blocking;
pub mod context;
pub mod engine;
pub mod proxy;
pub(crate) mod routing;
pub mod socket;
pub mod transport;

// Re-export the sans-I/O surface so downstream callers don't have
// to depend on omq-proto explicitly. Identical surface to the
// pre-split crate.
pub use omq_proto::IpcPath;
#[cfg(any(feature = "curve", feature = "plain"))]
pub use omq_proto::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "curve")]
pub use omq_proto::{CurveKeypair, CurvePublicKey, CurveSecretKey, CurveServerOptions};
pub use omq_proto::{
    Endpoint, EndpointRole, EndpointSpec, Error, Frame, FrameFlags, KeepAlive, MechanismConfig,
    MechanismSetup, Message, MessageIter, OnMute, Options, PartCountError, ReconnectPolicy, Result,
    SocketType, TrySendError, is_compatible,
};

// Sub-modules of omq_proto are re-exported under their original
// paths so downstream `use omq_tokio::endpoint::Host` style imports keep
// working.
pub use omq_proto::endpoint;
pub use omq_proto::error;
pub use omq_proto::message;
pub use omq_proto::options;
pub use omq_proto::proto;

pub use context::{Context, ContextConfig};
pub use proxy::{Proxy, ProxyExit};
pub use socket::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo, Socket,
};
