#[cfg(not(target_pointer_width = "64"))]
compile_error!("OMQ.java native fast path requires a 64-bit target");

mod async_ops;
mod auth;
mod context;
mod convert;
mod error;
mod identity;
mod message;
mod monitor;
mod ring;
mod socket;

pub use async_ops::{
    Java_io_omq_Native_asyncTaskCancel, Java_io_omq_Native_receiveAnyAsync,
    Java_io_omq_Native_receiveAnyAsyncOptional, Java_io_omq_Native_socketRecvAsync,
    Java_io_omq_Native_socketRecvFromAsync, Java_io_omq_Native_socketSendAsync,
};
pub use auth::{
    Java_io_omq_Native_curveKeypair, Java_io_omq_Native_curvePublic,
    Java_io_omq_Native_socketSetCurveClient, Java_io_omq_Native_socketSetCurveServer,
    Java_io_omq_Native_socketSetCurveServerCallback, Java_io_omq_Native_socketSetPlainClient,
    Java_io_omq_Native_socketSetPlainServer, Java_io_omq_Native_socketSetPlainServerCallback,
};
pub use context::{
    Java_io_omq_Native_contextClose, Java_io_omq_Native_contextCreate,
    Java_io_omq_Native_contextFromShareKey, Java_io_omq_Native_contextShareKey,
};
pub use error::{omq_java_last_error_code, omq_java_last_error_message};
pub use identity::{
    Java_io_omq_Native_identityEquals, Java_io_omq_Native_identityHash,
    Java_io_omq_Native_identityPeerInfo, Java_io_omq_Native_identityRelease,
};
pub use monitor::{
    Java_io_omq_Native_monitorClose, Java_io_omq_Native_monitorRecv,
    Java_io_omq_Native_socketMonitor,
};
pub use ring::recv::{
    Java_io_omq_Native_recvRingIdentity, omq_java_recv_ring_close, omq_java_recv_ring_control_addr,
    omq_java_recv_ring_create, omq_java_recv_ring_desc_addr, omq_java_recv_ring_desc_capacity,
    omq_java_recv_ring_error_code, omq_java_recv_ring_error_message, omq_java_recv_ring_fill,
    omq_java_recv_ring_payload_addr, omq_java_recv_ring_payload_capacity,
};
pub use ring::send::{
    omq_java_send_ring_close, omq_java_send_ring_control_addr, omq_java_send_ring_create,
    omq_java_send_ring_desc_addr, omq_java_send_ring_desc_capacity, omq_java_send_ring_error_code,
    omq_java_send_ring_error_message, omq_java_send_ring_payload_addr,
    omq_java_send_ring_payload_capacity, omq_java_send_ring_wait, omq_java_send_ring_wake,
};
pub use socket::options::{
    Java_io_omq_Native_socketSetArenaThreshold, Java_io_omq_Native_socketSetCompressionAutoTrain,
    Java_io_omq_Native_socketSetCompressionDict,
    Java_io_omq_Native_socketSetCompressionDictCapacity,
    Java_io_omq_Native_socketSetCompressionLevel,
    Java_io_omq_Native_socketSetCompressionOffloadThreshold,
    Java_io_omq_Native_socketSetCompressionThreshold, Java_io_omq_Native_socketSetConflate,
    Java_io_omq_Native_socketSetHandshakeTimeout, Java_io_omq_Native_socketSetHeartbeatInterval,
    Java_io_omq_Native_socketSetHeartbeatTimeout, Java_io_omq_Native_socketSetHeartbeatTtl,
    Java_io_omq_Native_socketSetIdentity, Java_io_omq_Native_socketSetLargeMessageThreshold,
    Java_io_omq_Native_socketSetLinger, Java_io_omq_Native_socketSetMaxMessageParts,
    Java_io_omq_Native_socketSetMaxMessageSize, Java_io_omq_Native_socketSetMaxPeerSubscriptions,
    Java_io_omq_Native_socketSetMaxPendingHandshakes,
    Java_io_omq_Native_socketSetMaxReceiveDictSize, Java_io_omq_Native_socketSetOnMute,
    Java_io_omq_Native_socketSetReceiveBufferSize,
    Java_io_omq_Native_socketSetReceiveHighWaterMark, Java_io_omq_Native_socketSetReconnect,
    Java_io_omq_Native_socketSetReconnectStopConnRefused,
    Java_io_omq_Native_socketSetRouterMandatory, Java_io_omq_Native_socketSetSendBufferSize,
    Java_io_omq_Native_socketSetSendHighWaterMark, Java_io_omq_Native_socketSetTcpKeepalive,
    Java_io_omq_Native_socketSetTransmitSlotCap, Java_io_omq_Native_socketSetWorkloadProfile,
    Java_io_omq_Native_socketSetXpubNoDrop,
};
#[cfg(feature = "quic")]
pub use socket::options::{
    Java_io_omq_Native_socketSetQuicServerCertificate, Java_io_omq_Native_socketSetQuicServerName,
    Java_io_omq_Native_socketSetQuicTrustPem, Java_io_omq_Native_socketSetQuicTrustSystem,
};
pub use socket::recv::{
    Java_io_omq_Native_socketRecv, Java_io_omq_Native_socketRecvFrom,
    Java_io_omq_Native_socketRecvInto,
};
pub use socket::send::{
    Java_io_omq_Native_socketSend, Java_io_omq_Native_socketSendMultipart,
    Java_io_omq_Native_socketSendMultipartTimeout, Java_io_omq_Native_socketTrySendMultipart,
};
pub use socket::{
    Java_io_omq_Native_socketBind, Java_io_omq_Native_socketClose,
    Java_io_omq_Native_socketConnect, Java_io_omq_Native_socketCreate,
    Java_io_omq_Native_socketDisconnect, Java_io_omq_Native_socketJoin,
    Java_io_omq_Native_socketLeave, Java_io_omq_Native_socketShutdown,
    Java_io_omq_Native_socketSubscribe, Java_io_omq_Native_socketUnbind,
    Java_io_omq_Native_socketUnsubscribe, Java_io_omq_Native_socketWaitConnected,
    Java_io_omq_Native_socketWaitSubscribed,
};
