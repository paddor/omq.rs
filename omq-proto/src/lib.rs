//! Sans-I/O core for omq.
//!
//! ZMTP codec, message + payload types, frame parsing, mechanism
//! handshakes (NULL / CURVE), compression transforms
//! (lz4), endpoint parsing, options, and the prefix-
//! subscription matcher. None of this depends on a runtime.
#![forbid(unsafe_code)]

pub mod backoff;
pub mod endpoint;
pub mod error;
pub mod fan_out_frame;
pub mod flow;
pub mod frame_buffer;
pub mod handle_frame;
pub mod inproc;
pub mod message;
pub mod monitor;
pub mod options;
pub mod proto;
pub mod routing;
pub mod socket_api;
pub mod socket_ref;
pub mod subscription;
pub mod type_state;

pub use endpoint::IpcPath;
pub use endpoint::{Endpoint, EndpointRole, EndpointSpec};
pub use error::{Error, Result, TrySendError};
pub use message::{Frame, FrameFlags, Message, MessageIter, PartCountError, generated_identity};
pub use monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorTryRecvError,
    PeerCommandKind, PeerIdent, PeerInfo,
};
pub use options::{KeepAlive, MechanismConfig, OnMute, Options, ReconnectPolicy, WorkloadProfile};
pub use proto::mechanism::MechanismSetup;
#[cfg(any(feature = "curve", feature = "plain"))]
pub use proto::mechanism::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "curve")]
pub use proto::mechanism::{CurveKeypair, CurvePublicKey, CurveSecretKey, CurveServerOptions};
pub use proto::{SocketType, is_compatible};
