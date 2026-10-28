use std::sync::Arc;

use thiserror::Error;

use crate::{ConnectError, SettingsError};

/// Error returned when connecting to a WebTransport endpoint.
#[derive(Error, Debug, Clone)]
pub enum ClientError {
    /// Incoming bytes ended before the handshake exchange completed.
    #[error("unexpected end of stream")]
    UnexpectedEnd,

    /// QUIC connection-level failure.
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    /// Failed to write handshake data.
    #[error("failed to write: {0}")]
    WriteError(#[from] quinn::WriteError),

    /// Failed to read handshake data.
    #[error("failed to read: {0}")]
    ReadError(#[from] quinn::ReadError),

    /// HTTP/3 SETTINGS negotiation failed.
    #[error("failed to exchange h3 settings: {0}")]
    SettingsError(#[from] SettingsError),

    /// HTTP/3 CONNECT negotiation failed.
    #[error("failed to exchange h3 connect: {0}")]
    HttpError(#[from] ConnectError),

    /// Quinn connect attempt failed before a connection was established.
    #[error("quic error: {0}")]
    QuinnError(#[from] quinn::ConnectError),

    /// URL host component could not be converted to a DNS name.
    #[error("invalid DNS name: {0}")]
    InvalidDnsName(String),

    /// URL was invalid for WebTransport usage.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// DNS resolution exceeded the configured timeout.
    #[error("DNS resolution timed out")]
    DnsTimeout,

    /// QUIC and HTTP/3 session establishment exceeded the configured timeout.
    #[error("connection handshake timed out")]
    HandshakeTimeout,

    /// Local UDP endpoint creation failed.
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),

    /// TLS configuration did not provide a QUIC-compatible initial cipher suite.
    #[error("TLS configuration has no QUIC-compatible initial cipher suite")]
    InvalidCryptoConfiguration,
}

/// Errors returned by [`crate::Session`], grouped by QUIC or WebTransport origin.
#[derive(Clone, Error, Debug)]
pub enum SessionError {
    /// Generic QUIC connection failure.
    #[error("connection error: {0}")]
    ConnectionError(quinn::ConnectionError),

    /// WebTransport semantic error mapped from connection context.
    #[error("webtransport error: {0}")]
    WebTransportError(#[from] WebTransportError),

    /// Failed to send a datagram over the active connection.
    #[error("send datagram error: {0}")]
    SendDatagramError(#[from] quinn::SendDatagramError),
}

impl From<quinn::ConnectionError> for SessionError {
    fn from(e: quinn::ConnectionError) -> Self {
        match &e {
            quinn::ConnectionError::ApplicationClosed(close) => {
                match omq_webtrans_proto::error_from_http3(close.error_code.into_inner()) {
                    Some(code) => WebTransportError::Closed(
                        code,
                        String::from_utf8_lossy(&close.reason).into_owned(),
                    )
                    .into(),
                    None => SessionError::ConnectionError(e),
                }
            }
            _ => SessionError::ConnectionError(e),
        }
    }
}

/// Error that can occur when reading or writing the WebTransport stream header.
#[derive(Clone, Error, Debug)]
pub enum WebTransportError {
    /// Session was closed with an application code and reason.
    #[error("closed: code={0} reason={1}")]
    Closed(u32, String),

    /// Stream/session header did not match any known session.
    #[error("unknown session")]
    UnknownSession,

    /// Failed to read stream/session preface data.
    #[error("read error: {0}")]
    ReadError(#[from] quinn::ReadExactError),

    /// Failed to write stream/session preface data.
    #[error("write error: {0}")]
    WriteError(#[from] quinn::WriteError),
}

/// Error when writing to [`crate::SendStream`], similar to [`quinn::WriteError`].
#[derive(Clone, Error, Debug)]
pub enum WriteError {
    /// Peer sent STOP_SENDING with the provided WebTransport code.
    #[error("STOP_SENDING: {0}")]
    Stopped(u32),

    /// STOP_SENDING carried a non-WebTransport error code.
    #[error("invalid STOP_SENDING: {0}")]
    InvalidStopped(quinn::VarInt),

    /// Stream write failed because the parent session failed.
    #[error("session error: {0}")]
    SessionError(#[from] SessionError),

    /// Stream was already closed.
    #[error("stream closed")]
    ClosedStream,
}

impl From<quinn::WriteError> for WriteError {
    fn from(e: quinn::WriteError) -> Self {
        match e {
            quinn::WriteError::Stopped(code) => {
                match omq_webtrans_proto::error_from_http3(code.into_inner()) {
                    Some(code) => WriteError::Stopped(code),
                    None => WriteError::InvalidStopped(code),
                }
            }
            quinn::WriteError::ClosedStream => WriteError::ClosedStream,
            quinn::WriteError::ConnectionLost(e) => WriteError::SessionError(e.into()),
            quinn::WriteError::ZeroRttRejected => unreachable!("0-RTT not supported"),
        }
    }
}

/// Error when reading from [`crate::RecvStream`], similar to [`quinn::ReadError`].
#[derive(Clone, Error, Debug)]
pub enum ReadError {
    /// Stream read failed because the parent session failed.
    #[error("session error: {0}")]
    SessionError(#[from] SessionError),

    /// Peer reset the stream with the provided WebTransport code.
    #[error("RESET_STREAM: {0}")]
    Reset(u32),

    /// RESET_STREAM carried a non-WebTransport error code.
    #[error("invalid RESET_STREAM: {0}")]
    InvalidReset(quinn::VarInt),

    /// Stream was already closed.
    #[error("stream already closed")]
    ClosedStream,

    /// Ordered read API was used on an unordered stream.
    #[error("ordered read on unordered stream")]
    IllegalOrderedRead,
}

impl From<quinn::ReadError> for ReadError {
    fn from(value: quinn::ReadError) -> Self {
        match value {
            quinn::ReadError::Reset(code) => {
                match omq_webtrans_proto::error_from_http3(code.into_inner()) {
                    Some(code) => ReadError::Reset(code),
                    None => ReadError::InvalidReset(code),
                }
            }
            quinn::ReadError::ConnectionLost(e) => ReadError::SessionError(e.into()),
            quinn::ReadError::IllegalOrderedRead => ReadError::IllegalOrderedRead,
            quinn::ReadError::ClosedStream => ReadError::ClosedStream,
            quinn::ReadError::ZeroRttRejected => unreachable!("0-RTT not supported"),
        }
    }
}

/// Error returned by [`crate::RecvStream::read_exact`], similar to [`quinn::ReadExactError`].
#[derive(Clone, Error, Debug)]
pub enum ReadExactError {
    /// Stream ended before the requested number of bytes was read.
    #[error("finished early")]
    FinishedEarly(usize),

    /// Underlying read operation failed.
    #[error("read error: {0}")]
    ReadError(#[from] ReadError),
}

impl From<quinn::ReadExactError> for ReadExactError {
    fn from(e: quinn::ReadExactError) -> Self {
        match e {
            quinn::ReadExactError::FinishedEarly(size) => ReadExactError::FinishedEarly(size),
            quinn::ReadExactError::ReadError(e) => ReadExactError::ReadError(e.into()),
        }
    }
}

/// Error returned by [`crate::RecvStream::read_to_end`], similar to [`quinn::ReadToEndError`].
#[derive(Clone, Error, Debug)]
pub enum ReadToEndError {
    /// Read exceeded the caller-provided limit.
    #[error("too long")]
    TooLong,

    /// Underlying read operation failed.
    #[error("read error: {0}")]
    ReadError(#[from] ReadError),
}

impl From<quinn::ReadToEndError> for ReadToEndError {
    fn from(e: quinn::ReadToEndError) -> Self {
        match e {
            quinn::ReadToEndError::TooLong => ReadToEndError::TooLong,
            quinn::ReadToEndError::Read(e) => ReadToEndError::ReadError(e.into()),
        }
    }
}

/// Error indicating the stream was already closed.
#[derive(Clone, Error, Debug)]
#[error("stream closed")]
pub struct ClosedStream;

impl From<quinn::ClosedStream> for ClosedStream {
    fn from(_: quinn::ClosedStream) -> Self {
        ClosedStream
    }
}

/// Error returned when receiving a new WebTransport session.
#[derive(Error, Debug, Clone)]
pub enum ServerError {
    /// A request no longer owns the handshake state needed to complete it.
    #[error("WebTransport request was already completed")]
    RequestAlreadyCompleted,

    /// Incoming bytes ended before the handshake exchange completed.
    #[error("unexpected end of stream")]
    UnexpectedEnd,

    /// QUIC connection-level failure.
    #[error("connection error")]
    Connection(#[from] quinn::ConnectionError),

    /// QUIC and HTTP/3 session establishment exceeded the configured timeout.
    #[error("connection handshake timed out")]
    HandshakeTimeout,

    /// Failed to write handshake data.
    #[error("failed to write")]
    WriteError(#[from] quinn::WriteError),

    /// Failed to read handshake data.
    #[error("failed to read")]
    ReadError(#[from] quinn::ReadError),

    /// HTTP/3 SETTINGS negotiation failed.
    #[error("failed to exchange h3 settings")]
    SettingsError(#[from] SettingsError),

    /// HTTP/3 CONNECT negotiation failed.
    #[error("failed to exchange h3 connect")]
    ConnectError(#[from] ConnectError),

    /// Generic I/O failure during server setup or handshake.
    #[error("io error: {0}")]
    IoError(Arc<std::io::Error>),

    /// TLS configuration did not provide a QUIC-compatible initial cipher suite.
    #[error("TLS configuration has no QUIC-compatible initial cipher suite")]
    InvalidCryptoConfiguration,
}

// #[derive(Clone, Error, Debug)]
// pub enum SendDatagramError {
//     #[error("peer does not support datagrams")]
//     UnsupportedPeer,
//
//     #[error("peer has disabled datagram support")]
//     DatagramSupportDisabled,
//
//     #[error("datagram too large")]
//     TooLarge,
//
//     #[error("session error: {0}")]
//     SessionError(#[from] SessionError),
// }
//
// impl From<quinn::SendDatagramError> for SendDatagramError {
//     fn from(value: quinn::SendDatagramError) -> Self {
//         match value {
//             quinn::SendDatagramError::UnsupportedByPeer => SendDatagramError::UnsupportedPeer,
//             quinn::SendDatagramError::Disabled => SendDatagramError::DatagramSupportDisabled,
//             quinn::SendDatagramError::TooLarge => SendDatagramError::TooLarge,
//             quinn::SendDatagramError::ConnectionLost(e) => SendDatagramError::SessionError(e.into()),
//         }
//     }
// }
