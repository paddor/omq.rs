//! WebTransport session wrapper that maps Quinn connections to WebTransport semantics.

use std::{
    fmt,
    future::{Future, poll_fn},
    io::Cursor,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};

use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use url::Url;

use crate::{
    ClientError, Connect, RecvStream, SendStream, SessionError, Settings, WebTransportError,
};

use omq_webtrans_proto::{Capsule, CapsuleError, Frame, UniStream, VarInt};

const MAX_CAPSULE_FRAME_SIZE: usize = 2 * 1024;

#[derive(Debug)]
struct CloseCommand {
    code: u32,
    reason: Vec<u8>,
}

fn is_graceful_close(e: &omq_webtrans_proto::CapsuleError) -> bool {
    use std::io::ErrorKind;

    match e {
        omq_webtrans_proto::CapsuleError::Io(ioe) => {
            matches!(
                ioe.kind(),
                ErrorKind::UnexpectedEof
                    | ErrorKind::BrokenPipe
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
            ) || ioe
                .to_string()
                .to_ascii_lowercase()
                .contains("connection lost")
        }
        omq_webtrans_proto::CapsuleError::UnexpectedEnd => true,
        _ => false,
    }
}

/// An established WebTransport session, acting like a full QUIC connection. See [`quinn::Connection`].
///
/// Remember that WebTransport is layered on top of QUIC:
///   1. Each stream begins with bytes identifying the stream type and session ID.
///   2. Error codes are encoded with the session ID, so they are not full QUIC error codes.
///   3. Stream IDs may have gaps introduced by HTTP/3, transparent to the application.
///
/// Deref is used to expose non-overloaded methods on [`quinn::Connection`].
/// These should be safe with WebTransport; please file an issue if you find otherwise.
#[derive(Clone)]
pub struct Session {
    conn: quinn::Connection,

    // The session ID derived from the CONNECT request stream ID.
    session_id: Option<VarInt>,

    // The accept logic is stateful, so share it with Arc<Mutex>.
    accept: Option<Arc<Mutex<SessionAccept>>>,

    // Cached headers that prefix each stream we open.
    header_uni: Vec<u8>,
    header_bi: Vec<u8>,
    header_datagram: Vec<u8>,

    // Keep references to settings and connect streams so they remain open until drop.
    #[allow(dead_code)]
    settings: Option<Arc<Settings>>,

    // The URL used to create the session.
    url: Url,

    // Local close requests are serialized through the CONNECT stream task so
    // the peer receives a CLOSE_WEBTRANSPORT_SESSION capsule before QUIC closes.
    close_tx: Option<tokio::sync::mpsc::Sender<CloseCommand>>,
}

impl Session {
    pub(crate) fn new(conn: quinn::Connection, settings: Settings, connect: Connect) -> Self {
        // The session ID is the stream ID of the CONNECT request.
        let session_id = connect.session_id();

        // Cache the small header that prefixes each stream we open.
        let mut header_uni = Vec::new();
        UniStream::WEBTRANSPORT.encode(&mut header_uni);
        session_id.encode(&mut header_uni);

        let mut header_bi = Vec::new();
        Frame::WEBTRANSPORT.encode(&mut header_bi);
        session_id.encode(&mut header_bi);

        let mut header_datagram = Vec::new();
        session_id.encode(&mut header_datagram);

        // Accept logic is stateful, so use an Arc<Mutex> to share it.
        let accept = SessionAccept::new(conn.clone(), session_id);
        let (close_tx, close_rx) = tokio::sync::mpsc::channel(1);
        let settings = Arc::new(settings);

        let this = Self {
            conn: conn.clone(),
            accept: Some(Arc::new(Mutex::new(accept))),
            session_id: Some(session_id),
            header_uni,
            header_bi,
            header_datagram,
            url: connect.url().clone(),
            settings: Some(settings.clone()),
            close_tx: Some(close_tx),
        };

        // Run a background task to coordinate local and remote CONNECT stream
        // closure without retaining an extra Session/close-sender clone.
        tokio::spawn(async move {
            let result = Self::run_closed(connect, close_rx).await;
            // The HTTP/3 control streams are critical and must remain alive
            // until CONNECT closure processing has completed.
            match result {
                Ok(Some((code, reason))) => {
                    tracing::debug!("WebTransport close received: code={code} reason={reason}");
                    if conn.close_reason().is_none() {
                        Self::close_connection(&conn, code, reason.as_bytes());
                    }
                }
                Ok(None) => {
                    if let Some(reason) = conn.close_reason() {
                        let se: crate::SessionError = reason.into();
                        tracing::debug!("CONNECT stream ended: {se}");
                    } else {
                        tracing::debug!("CONNECT stream ended without CloseWebTransportSession");
                    }
                }
                Err(e) if is_graceful_close(&e) => {
                    if let Some(reason) = conn.close_reason() {
                        let se: crate::SessionError = reason.into();
                        tracing::debug!(
                            "CONNECT stream closed after QUIC close: {se} (capsule={e})"
                        );
                    } else {
                        tracing::debug!("CONNECT stream closed: {e}");
                    }
                }
                Err(e) => {
                    tracing::debug!("CONNECT stream error: {e}");
                    if conn.close_reason().is_none() {
                        Self::close_connection(&conn, 1, b"capsule error");
                    }
                }
            }
            drop(settings);
        });

        this
    }

    // Keep reading from the control stream until it closes.
    async fn run_closed(
        connect: Connect,
        mut close_rx: tokio::sync::mpsc::Receiver<CloseCommand>,
    ) -> Result<Option<(u32, String)>, omq_webtrans_proto::CapsuleError> {
        let (mut send, mut recv) = connect.into_inner();
        let mut pending_capsules = BytesMut::new();

        loop {
            tokio::select! {
                capsule = Self::read_capsule_frame(&mut recv, &mut pending_capsules) => match capsule {
                    Ok(Capsule::CloseWebTransportSession { code, reason }) => {
                        return Ok(Some((code, reason)));
                    }
                    // RFC 9297 requires endpoints to silently ignore unknown
                    // capsule types, including reserved GREASE values.
                    Ok(Capsule::Unknown { .. }) => {}
                    Err(e) if is_graceful_close(&e) => return Ok(None),
                    Err(e) => return Err(e),
                },
                close = close_rx.recv() => {
                    let Some(close) = close else {
                        return Ok(Some((0, "last session handle dropped".to_owned())));
                    };
                    let reason = Self::capsule_reason(&close.reason);
                    let capsule = Capsule::CloseWebTransportSession {
                        code: close.code,
                        reason,
                    };
                    // The deadline includes capsule submission: a peer can
                    // exhaust CONNECT stream credit before reading our close.
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        Self::write_capsule_frame(&mut send, &capsule).await?;
                        let _ = send.finish();
                        let _ = send.stopped().await;
                        Ok::<(), CapsuleError>(())
                    }).await;
                    return Ok(Some((close.code, Self::capsule_reason(&close.reason))));
                }
            }
        }
    }

    async fn read_capsule_frame(
        recv: &mut quinn::RecvStream,
        pending: &mut BytesMut,
    ) -> Result<Capsule, CapsuleError> {
        loop {
            if let Some(capsule) = Self::decode_buffered_capsule(pending)? {
                return Ok(capsule);
            }
            let typ = VarInt::read(recv)
                .await
                .map_err(|_| CapsuleError::UnexpectedEnd)?;
            let length = VarInt::read(recv)
                .await
                .map_err(|_| CapsuleError::UnexpectedEnd)?;
            let length =
                usize::try_from(length.into_inner()).map_err(|_| CapsuleError::MessageTooLong)?;
            if length > MAX_CAPSULE_FRAME_SIZE {
                return Err(CapsuleError::MessageTooLong);
            }

            let mut payload = vec![0; length];
            tokio::io::AsyncReadExt::read_exact(recv, &mut payload).await?;
            let typ = Frame(typ);
            if typ.is_grease() {
                continue;
            }
            if typ != Frame::DATA {
                tracing::warn!("ignoring non-DATA frame on CONNECT stream: {typ:?}");
                continue;
            }
            // DATA frame boundaries are not capsule boundaries. Retain a
            // partial capsule and any following capsules for the next call.
            // A partial capsule is at most 1,044 bytes; one additional DATA
            // frame is bounded by MAX_CAPSULE_FRAME_SIZE above.
            pending.extend_from_slice(&payload);
        }
    }

    fn decode_buffered_capsule(pending: &mut BytesMut) -> Result<Option<Capsule>, CapsuleError> {
        let mut cursor = Cursor::new(pending.as_ref());
        if VarInt::decode(&mut cursor).is_err() {
            return Ok(None);
        }
        let Ok(length) = VarInt::decode(&mut cursor) else {
            return Ok(None);
        };
        let length =
            usize::try_from(length.into_inner()).map_err(|_| CapsuleError::MessageTooLong)?;
        // Match the capsule codec's existing 4-byte code + 1,024-byte payload
        // policy before allocating or waiting for any attacker-chosen length.
        if length > 1028 {
            return Err(CapsuleError::MessageTooLong);
        }
        let total = cursor.position() as usize + length;
        if pending.len() < total {
            return Ok(None);
        }
        let capsule = Capsule::decode(&mut &pending[..total])?;
        pending.advance(total);
        Ok(Some(capsule))
    }

    async fn write_capsule_frame(
        send: &mut quinn::SendStream,
        capsule: &Capsule,
    ) -> Result<(), CapsuleError> {
        let mut payload = Vec::new();
        capsule.encode(&mut payload)?;
        let mut frame = Vec::with_capacity(VarInt::MAX_SIZE * 2 + payload.len());
        Frame::DATA.encode(&mut frame);
        VarInt::try_from(payload.len())
            .map_err(|_| CapsuleError::MessageTooLong)?
            .encode(&mut frame);
        frame.extend_from_slice(&payload);
        tokio::io::AsyncWriteExt::write_all(send, &frame).await?;
        Ok(())
    }

    fn capsule_reason(reason: &[u8]) -> String {
        let mut reason = String::from_utf8_lossy(reason).into_owned();
        while reason.len() > 1024 {
            reason.pop();
        }
        reason
    }

    fn close_connection(conn: &quinn::Connection, code: u32, reason: &[u8]) {
        let mapped = omq_webtrans_proto::error_to_http3(code);
        let code = quinn::VarInt::from_u64(mapped).unwrap_or_else(|_| quinn::VarInt::from_u32(1));
        conn.close(code, reason);
    }

    /// Connect using an established QUIC connection when creating the connection manually.
    /// This only works with a fresh QUIC connection negotiated with the HTTP/3 ALPN.
    pub async fn connect(conn: quinn::Connection, url: Url) -> Result<Session, ClientError> {
        Self::connect_with_origin(conn, url, None).await
    }

    /// Connect with an Origin header for clients exercising browser policy.
    pub async fn connect_with_origin(
        conn: quinn::Connection,
        url: Url,
        origin: Option<String>,
    ) -> Result<Session, ClientError> {
        // Perform the HTTP/3 handshake by sending/receiving SETTINGS frames.
        let settings = Settings::connect(&conn, true).await?;

        // Send the HTTP/3 CONNECT request.
        let connect = Connect::open(&conn, url, origin, settings.version()).await?;

        // Return the session while retaining control/connect streams.
        // If either stream closes, the session ends, so keep references alive.
        let session = Session::new(conn, settings, connect);

        Ok(session)
    }

    /// Accept a new unidirectional stream. See [`quinn::Connection::accept_uni`].
    pub async fn accept_uni(&self) -> Result<RecvStream, SessionError> {
        if let Some(accept) = &self.accept {
            poll_fn(|cx| {
                accept
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .poll_accept_uni(cx)
            })
            .await
        } else {
            self.conn
                .accept_uni()
                .await
                .map(RecvStream::new)
                .map_err(Into::into)
        }
    }

    /// Accept a new bidirectional stream. See [`quinn::Connection::accept_bi`].
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), SessionError> {
        if let Some(accept) = &self.accept {
            poll_fn(|cx| {
                accept
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .poll_accept_bi(cx)
            })
            .await
        } else {
            self.conn
                .accept_bi()
                .await
                .map(|(send, recv)| (SendStream::new(send), RecvStream::new(recv)))
                .map_err(Into::into)
        }
    }

    /// Open a new unidirectional stream. See [`quinn::Connection::open_uni`].
    pub async fn open_uni(&self) -> Result<SendStream, SessionError> {
        let mut send = self.conn.open_uni().await?;

        // Set max priority, then write the stream header.
        // Otherwise application data could be queued ahead of the header.
        // The header is required to determine the session ID without reliable reset.
        send.set_priority(i32::MAX).ok();
        Self::write_full(&mut send, &self.header_uni).await?;

        // Reset stream priority to the default of 0.
        send.set_priority(0).ok();
        Ok(SendStream::new(send))
    }

    /// Open a new bidirectional stream. See [`quinn::Connection::open_bi`].
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), SessionError> {
        let (mut send, recv) = self.conn.open_bi().await?;

        // Set max priority, then write the stream header.
        // Otherwise application data could be queued ahead of the header.
        // The header is required to determine the session ID without reliable reset.
        send.set_priority(i32::MAX).ok();
        Self::write_full(&mut send, &self.header_bi).await?;

        // Reset stream priority to the default of 0.
        send.set_priority(0).ok();
        Ok((SendStream::new(send), RecvStream::new(recv)))
    }

    /// Asynchronously receive an application datagram from the remote peer.
    ///
    /// Waits for a datagram to become available and returns the received bytes.
    pub async fn read_datagram(&self) -> Result<Bytes, SessionError> {
        let mut datagram = self
            .conn
            .read_datagram()
            .await
            .map_err(SessionError::from)?;

        let mut cursor = Cursor::new(&datagram);

        if let Some(session_id) = self.session_id {
            // Validate and strip the session ID from the datagram.
            let actual_id =
                VarInt::decode(&mut cursor).map_err(|_| WebTransportError::UnknownSession)?;
            if actual_id != session_id {
                return Err(WebTransportError::UnknownSession.into());
            }
        }

        // Return the datagram without the session ID.
        let datagram = datagram.split_off(cursor.position() as usize);

        Ok(datagram)
    }

    /// Send an application datagram to the remote peer.
    ///
    /// Datagrams are unreliable and may be dropped or delivered out of order.
    /// The data must be smaller than [`max_datagram_size`](Self::max_datagram_size).
    pub fn send_datagram(&self, data: Bytes) -> Result<(), SessionError> {
        if !self.header_datagram.is_empty() {
            // Quinn requires allocation to prepend the session header.
            // Tracking issue: https://github.com/quinn-rs/quinn/issues/1724
            let mut buf = BytesMut::with_capacity(self.header_datagram.len() + data.len());

            // Prepend the session ID header to the datagram payload.
            buf.extend_from_slice(&self.header_datagram);
            buf.extend_from_slice(&data);

            self.conn.send_datagram(buf.into())?;
        } else {
            self.conn.send_datagram(data)?;
        }

        Ok(())
    }

    /// Compute the maximum size of datagrams that may be passed to
    /// [`send_datagram`](Self::send_datagram).
    pub fn max_datagram_size(&self) -> usize {
        let mtu = self.conn.max_datagram_size().unwrap_or(0);
        mtu.saturating_sub(self.header_datagram.len())
    }

    /// Close the session with an error code and reason.
    ///
    /// WebTransport sessions first send a CLOSE_WEBTRANSPORT_SESSION capsule
    /// from the background CONNECT task. Raw QUIC sessions close immediately.
    /// Close requests use a one-entry queue and a reason capped at 1024 bytes.
    /// Waiting for the peer to acknowledge FIN is bounded to five seconds.
    pub fn close(&self, code: u32, reason: &[u8]) {
        if let Some(close_tx) = &self.close_tx {
            // At most one bounded close request can be queued. Repeated closes
            // must not retain arbitrary caller-controlled reason buffers.
            let reason = Self::capsule_reason(&reason[..reason.len().min(1024)]);
            match close_tx.try_send(CloseCommand {
                code,
                reason: reason.into_bytes(),
            }) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => return,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
            }
        }

        if self.session_id.is_some() {
            Self::close_connection(&self.conn, code, reason);
        } else {
            self.conn.close(quinn::VarInt::from_u32(code), reason);
        }
    }

    /// Wait until the session is closed and return the error. See [`quinn::Connection::closed`].
    pub async fn closed(&self) -> SessionError {
        self.conn.closed().await.into()
    }

    /// Return the close reason, or `None` if the session is still open. See [`quinn::Connection::close_reason`].
    pub fn close_reason(&self) -> Option<SessionError> {
        self.conn.close_reason().map(Into::into)
    }

    async fn write_full(send: &mut quinn::SendStream, buf: &[u8]) -> Result<(), SessionError> {
        match send.write_all(buf).await {
            Ok(_) => Ok(()),
            Err(quinn::WriteError::ConnectionLost(err)) => Err(err.into()),
            Err(err) => Err(WebTransportError::WriteError(err).into()),
        }
    }

    /// Create a new session from a raw QUIC connection and a URL.
    ///
    /// This adapts a QUIC connection to a WebTransport session, which simplifies
    /// supporting WebTransport and raw QUIC side by side.
    pub fn raw(conn: quinn::Connection, url: Url) -> Self {
        Self {
            conn,
            session_id: None,
            header_uni: Default::default(),
            header_bi: Default::default(),
            header_datagram: Default::default(),
            accept: None,
            settings: None,
            url,
            close_tx: None,
        }
    }

    /// Return the URL negotiated for this WebTransport session.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

impl Deref for Session {
    type Target = quinn::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.conn.fmt(f)
    }
}

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        self.conn.stable_id() == other.conn.stable_id()
    }
}

impl Eq for Session {}

// Type aliases to keep clippy from flagging overly complex types.
type AcceptUni = dyn Stream<Item = Result<quinn::RecvStream, quinn::ConnectionError>> + Send;
type AcceptBi = dyn Stream<Item = Result<(quinn::SendStream, quinn::RecvStream), quinn::ConnectionError>>
    + Send;
type PendingUni = dyn Future<Output = Result<(UniStream, quinn::RecvStream), SessionError>> + Send;
type PendingBi = dyn Future<Output = Result<Option<(quinn::SendStream, quinn::RecvStream)>, SessionError>>
    + Send;

// Stream-accept logic, needed because streams include a WebTransport header.
/// State machine that accepts and validates incoming streams for a single session.
pub struct SessionAccept {
    session_id: VarInt,

    // Keep QPACK streams alive if the peer creates them, to prevent premature closure.
    qpack_encoder: Option<quinn::RecvStream>,
    qpack_decoder: Option<quinn::RecvStream>,

    accept_uni: Pin<Box<AcceptUni>>,
    accept_bi: Pin<Box<AcceptBi>>,

    // Track in-flight work to read/write WebTransport stream headers.
    pending_uni: FuturesUnordered<Pin<Box<PendingUni>>>,
    pending_bi: FuturesUnordered<Pin<Box<PendingBi>>>,
}

impl SessionAccept {
    pub(crate) fn new(conn: quinn::Connection, session_id: VarInt) -> Self {
        // Create a stream that yields new incoming streams for polling.
        let accept_uni = Box::pin(futures::stream::unfold(conn.clone(), |conn| async {
            Some((conn.accept_uni().await, conn))
        }));

        let accept_bi = Box::pin(futures::stream::unfold(conn, |conn| async {
            Some((conn.accept_bi().await, conn))
        }));

        Self {
            session_id,

            qpack_decoder: None,
            qpack_encoder: None,

            accept_uni,
            accept_bi,

            pending_uni: FuturesUnordered::new(),
            pending_bi: FuturesUnordered::new(),
        }
    }

    // Poll-based so we can accept and decode streams in parallel.
    // FuturesUnordered keeps the implementation runtime-agnostic.
    /// Poll for the next accepted unidirectional WebTransport stream.
    pub fn poll_accept_uni(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RecvStream, SessionError>> {
        loop {
            // Accept any new streams.
            if let Poll::Ready(Some(res)) = self.accept_uni.poll_next_unpin(cx) {
                // Start decoding the header and track the pending future.
                let recv = res?;
                let pending = Self::decode_uni(recv, self.session_id);
                self.pending_uni.push(Box::pin(pending));

                continue;
            }

            // Poll pending stream decodes.
            let (typ, recv) = match ready!(self.pending_uni.poll_next_unpin(cx)) {
                Some(Ok(res)) => res,
                Some(Err(err)) => {
                    // Ignore errors; the stream may have been reset early.
                    tracing::warn!("failed to decode unidirectional stream: {err:?}");
                    continue;
                }
                None => return Poll::Pending,
            };

            // Decide whether to continue based on the stream type.
            match typ {
                UniStream::WEBTRANSPORT => {
                    let recv = RecvStream::new(recv);
                    return Poll::Ready(Ok(recv));
                }
                UniStream::QPACK_DECODER => {
                    self.qpack_decoder = Some(recv);
                }
                UniStream::QPACK_ENCODER => {
                    self.qpack_encoder = Some(recv);
                }
                _ => {
                    // Ignore unknown streams.
                    tracing::debug!("ignoring unknown unidirectional stream: {typ:?}");
                }
            }
        }
    }

    // Read the stream header and return the stream type.
    async fn decode_uni(
        mut recv: quinn::RecvStream,
        expected_session: VarInt,
    ) -> Result<(UniStream, quinn::RecvStream), SessionError> {
        // Read the VarInt at the start of the stream.
        let typ = VarInt::read(&mut recv)
            .await
            .map_err(|_| WebTransportError::UnknownSession)?;
        let typ = UniStream(typ);

        if typ == UniStream::WEBTRANSPORT {
            // Read and validate the session ID.
            let session_id = VarInt::read(&mut recv)
                .await
                .map_err(|_| WebTransportError::UnknownSession)?;
            if session_id != expected_session {
                return Err(WebTransportError::UnknownSession.into());
            }
        }

        // Return everything so QPACK streams can be retained if the peer created them.
        Ok((typ, recv))
    }

    /// Poll for the next accepted bidirectional WebTransport stream.
    pub fn poll_accept_bi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(SendStream, RecvStream), SessionError>> {
        loop {
            // Accept any new streams.
            if let Poll::Ready(Some(res)) = self.accept_bi.poll_next_unpin(cx) {
                // Start decoding the header and track the pending future.
                let (send, recv) = res?;
                let pending = Self::decode_bi(send, recv, self.session_id);
                self.pending_bi.push(Box::pin(pending));

                continue;
            }

            // Poll pending stream decodes.
            let res = match ready!(self.pending_bi.poll_next_unpin(cx)) {
                Some(Ok(res)) => res,
                Some(Err(err)) => {
                    // Ignore errors; the stream may have been reset early.
                    tracing::warn!("failed to decode bidirectional stream: {err:?}");
                    continue;
                }
                None => return Poll::Pending,
            };

            if let Some((send, recv)) = res {
                // Wrap streams in WebTransport types for correct error handling.
                let send = SendStream::new(send);
                let recv = RecvStream::new(recv);
                return Poll::Ready(Ok((send, recv)));
            }

            // Continue looping when the stream should be ignored.
        }
    }

    // Read the stream header and return `Some` if it is a WebTransport stream.
    async fn decode_bi(
        send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        expected_session: VarInt,
    ) -> Result<Option<(quinn::SendStream, quinn::RecvStream)>, SessionError> {
        let typ = VarInt::read(&mut recv)
            .await
            .map_err(|_| WebTransportError::UnknownSession)?;
        if Frame(typ) != Frame::WEBTRANSPORT {
            tracing::debug!("ignoring unknown bidirectional stream: {typ:?}");
            return Ok(None);
        }

        // Read and validate the session ID.
        let session_id = VarInt::read(&mut recv)
            .await
            .map_err(|_| WebTransportError::UnknownSession)?;
        if session_id != expected_session {
            return Err(WebTransportError::UnknownSession.into());
        }

        Ok(Some((send, recv)))
    }
}
