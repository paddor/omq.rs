//! omq-compio - compio-runtime backend for omq.
//!
//! Built on compio's thread-per-core executor with io_uring (Linux),
//! IOCP (Windows), and io_uring/poll (macOS) drivers. Each `Socket`
//! is pinned to the executor it was created on; cross-executor sends
//! use a runtime-agnostic mpsc (flume).
//!
//! The codec, message types, mechanism handshakes, and routing
//! algorithms come from the runtime-agnostic `omq-proto` crate.
//! This crate provides only the runtime glue.
//!
//! Status: beachhead. PUSH/PULL on inproc + TCP, send-batching,
//! gather writes via compio's `Vec<Bytes>` IoVectoredBuf path.

pub use omq_proto::{
    Endpoint, EndpointRole, EndpointSpec, Error, Frame, FrameFlags, IpcPath, KeepAlive,
    MechanismConfig, Message, OnMute, Options, Payload, ReconnectPolicy, Result, SocketType,
    is_compatible,
};
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
pub use omq_proto::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "curve")]
pub use omq_proto::{CurveKeypair, CurvePublicKey, CurveSecretKey};
#[cfg(feature = "blake3zmq")]
pub use omq_proto::{Blake3ZmqKeypair, Blake3ZmqPublicKey, Blake3ZmqSecretKey};
#[cfg(feature = "priority")]
pub use omq_proto::ConnectOpts;

pub use omq_proto::endpoint;
pub use omq_proto::error;
pub use omq_proto::message;
pub use omq_proto::options;
pub use omq_proto::proto;

pub mod monitor;
pub mod socket;
pub mod transport;

pub use monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo,
};
pub use socket::Socket;
