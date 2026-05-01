//! Per-connection driver: one tokio task per live peer connection.

use std::io;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::transform::MessageTransform;
use omq_proto::proto::{Command, Connection, Event};

const READ_BUF_SIZE: usize = 8 * 1024;

/// Driver-level timing configuration: handshake deadline, heartbeat
/// cadence, idle-close timeout.
#[derive(Debug, Clone, Copy, Default)]
pub struct DriverConfig {
    /// Close the connection if the ZMTP handshake doesn't finish within
    /// this window. `None` = no deadline.
    pub handshake_timeout: Option<Duration>,
    /// PING cadence. `None` disables heartbeat.
    pub heartbeat_interval: Option<Duration>,
    /// Close the connection if nothing has been received for this long.
    /// Defaults to `heartbeat_interval` when unset and heartbeat is on.
    pub heartbeat_timeout: Option<Duration>,
    /// `TTL` field of outgoing PING (peer-hint for when to assume dead).
    pub heartbeat_ttl: Option<Duration>,
}

/// Commands accepted by a running [`ConnectionDriver`].
#[derive(Debug)]
pub enum DriverCommand {
    /// Queue an application message for send.
    SendMessage(Message),
    /// Queue a ZMTP command for send (SUBSCRIBE, CANCEL, JOIN, LEAVE, ...).
    SendCommand(Command),
    /// Initiate clean shutdown.
    Close,
}

/// Handle returned to callers after spawning a driver. `inbox` delivers
/// commands into the driver; `cancel` requests early teardown.
#[derive(Debug, Clone)]
pub struct DriverHandle {
    pub inbox: mpsc::Sender<DriverCommand>,
    pub cancel: CancellationToken,
}

/// What a [`ConnectionDriver`] writes to its shared peer-event
/// channel: either a parsed ZMTP `Event` or a final `Closed` signal
/// emitted just before the driver task exits. Replaces the old
/// per-connection shim task that wrapped Events into the
/// `SocketDriver`'s `InternalEvent::PeerEvent` / `PeerClosed`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PeerOut {
    Event(Event),
    Closed,
}

/// A single-connection driver: reads bytes from the stream, feeds the codec,
/// forwards events out, accepts commands in, writes codec-produced bytes out.
#[derive(Debug)]
pub struct ConnectionDriver<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    stream: T,
    codec: Connection,
    inbox: mpsc::Receiver<DriverCommand>,
    /// Shared multi-producer channel feeding the `SocketDriver`'s
    /// per-peer event loop. Each entry is tagged with the `peer_id`
    /// this driver was assigned; the receiver dispatches on that.
    peer_out: mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
    cancel: CancellationToken,
    config: DriverConfig,
    /// Optional per-connection message transform. Applied to outbound user
    /// messages before [`Connection::send_message`] and to inbound
    /// [`Event::Message`]s before forwarding. Used by `lz4+tcp://` and
    /// `zstd+tcp://`. `None` for plain transports.
    transform: Option<MessageTransform>,
}

impl<T> ConnectionDriver<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    pub fn new(
        stream: T,
        codec: Connection,
        inbox: mpsc::Receiver<DriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerOut)>,
        peer_id: u64,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_config(
            stream,
            codec,
            inbox,
            peer_out,
            peer_id,
            cancel,
            DriverConfig::default(),
        )
    }

    pub fn with_config(
        stream: T,
        codec: Connection,
        inbox: mpsc::Receiver<DriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerOut)>,
        peer_id: u64,
        cancel: CancellationToken,
        config: DriverConfig,
    ) -> Self {
        Self {
            stream,
            codec,
            inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            transform: None,
        }
    }

    /// Install a per-connection message transform. Used by compression
    /// transports.
    #[must_use]
    pub fn with_transform(mut self, transform: MessageTransform) -> Self {
        self.transform = Some(transform);
        self
    }

    /// Run the driver to completion. Returns:
    /// - `Ok(())` on clean shutdown (peer EOF, cancelled, `Close` command,
    ///   inbox dropped).
    /// - `Err(_)` on protocol violations, I/O errors, or codec errors.
    ///
    /// In every exit path (success or error) the driver sends one final
    /// `PeerOut::Closed` on the shared peer-event channel so the
    /// `SocketDriver` can clean up its peer entry. The previous shim task
    /// that did this wrapping is gone - we save one task spawn and one
    /// per-message channel hop on every connection.
    pub async fn run(self) -> Result<()> {
        let peer_out = self.peer_out.clone();
        let peer_id = self.peer_id;
        let result = self.run_inner().await;
        let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
        result
    }

    async fn run_inner(self) -> Result<()> {
        let Self {
            stream,
            mut codec,
            mut inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            mut transform,
        } = self;
        let (mut reader, mut writer) = split(stream);
        let mut read_buf = vec![0u8; READ_BUF_SIZE];
        let mut last_input = Instant::now();
        let mut handshake_deadline: Option<Instant> =
            config.handshake_timeout.map(|d| last_input + d);
        let hb_interval = config.heartbeat_interval;
        let hb_timeout = config
            .heartbeat_timeout
            .or(config.heartbeat_interval)
            .unwrap_or(Duration::MAX);
        let hb_ttl_deciseconds = config
            .heartbeat_ttl
            .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
            .unwrap_or(0);

        loop {
            // Clear the handshake deadline once we're past it.
            if handshake_deadline.is_some() && codec.is_ready() {
                handshake_deadline = None;
            }

            // 1. Drain parsed events. Backpressure via the shared
            //    peer-out channel.
            while let Some(ev) = codec.poll_event() {
                let ev = match (transform.as_mut(), ev) {
                    (Some(t), Event::Message(m)) => match t.decode(m)? {
                        Some(plain) => Event::Message(plain),
                        None => continue, // dict shipment consumed at transport
                    },
                    (_, ev) => ev,
                };
                if peer_out.send((peer_id, PeerOut::Event(ev))).await.is_err() {
                    return Ok(());
                }
            }

            let want_write = codec.has_pending_transmit();
            let hb_enabled = hb_interval.is_some() && codec.is_ready();

            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),

                // Handshake deadline; disabled once handshake completes.
                () = sleep_until_opt(handshake_deadline), if handshake_deadline.is_some() => {
                    return Err(Error::HandshakeFailed("handshake timeout".into()));
                }

                res = reader.read(&mut read_buf) => {
                    last_input = Instant::now();
                    let n = res?;
                    if n == 0 {
                        return Ok(()); // peer EOF
                    }
                    codec.handle_input(&read_buf[..n])?;
                }

                res = flush_once(&mut writer, &mut codec), if want_write => {
                    res?;
                }

                cmd = inbox.recv() => match cmd {
                    Some(DriverCommand::SendMessage(m)) => {
                        if let Some(t) = transform.as_mut() {
                            for wire in t.encode(&m)? {
                                codec.send_message(&wire)?;
                            }
                        } else {
                            codec.send_message(&m)?;
                        }
                    }
                    Some(DriverCommand::SendCommand(c)) => codec.send_command(&c)?,
                    Some(DriverCommand::Close) | None => {
                        // Drain any outbound bytes already queued before returning.
                        drain_writes(&mut writer, &mut codec).await.ok();
                        return Ok(());
                    }
                },

                // Heartbeat tick: enabled only post-handshake when
                // `heartbeat_interval` is set.
                () = tokio::time::sleep(hb_interval.unwrap_or(Duration::MAX)), if hb_enabled => {
                    if last_input.elapsed() > hb_timeout {
                        return Err(Error::Timeout);
                    }
                    let ping = Command::Ping {
                        ttl_deciseconds: hb_ttl_deciseconds,
                        context: Bytes::new(),
                    };
                    // send_command returns Err only if not ready; we just
                    // checked, so unwrap is safe. Still, handle gracefully.
                    let _ = codec.send_command(&ping);
                }
            }
        }
    }
}

/// Sleep until an `Option<Instant>`. Returns immediately if `None`, which
/// paired with a select `if` guard means this branch won't fire.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => std::future::pending::<()>().await,
    }
}

/// One write attempt. Uses `write_vectored` so multi-chunk frame
/// payloads (compression sentinels, CURVE nonces, etc.) hit the kernel
/// as a single gather-write - no userspace memcpy. Partial writes are
/// fine; we loop and try again.
async fn flush_once<W>(writer: &mut W, codec: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let chunks = codec.transmit_chunks();
    if chunks.is_empty() {
        return Ok(());
    }
    let n = writer.write_vectored(&chunks).await?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
    }
    codec.advance_transmit(n);
    Ok(())
}

/// Best-effort flush of remaining outbound bytes on shutdown.
async fn drain_writes<W>(writer: &mut W, codec: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while codec.has_pending_transmit() {
        flush_once(writer, codec).await?;
    }
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use omq_proto::proto::connection::{ConnectionConfig, Role};
    use omq_proto::proto::{Event, SocketType};

    /// Adapter: pull `(u64, PeerOut::Event)` off the shared peer-out
    /// channel and yield bare `Event` values, matching the older
    /// per-side events channel shape the tests were written
    /// against. `PeerOut::Closed` ends the stream (returns None).
    pub(super) struct EventAdapter {
        rx: mpsc::Receiver<(u64, PeerOut)>,
    }

    impl EventAdapter {
        pub(super) async fn recv(&mut self) -> Option<Event> {
            match self.rx.recv().await? {
                (_, PeerOut::Event(e)) => Some(e),
                (_, PeerOut::Closed) => None,
            }
        }
    }

    /// Spin up two drivers connected via an in-memory duplex pair,
    /// return handles + event rxes. The connection driver is generic
    /// over T: AsyncRead+AsyncWrite, so a `tokio::io::duplex` pair
    /// is the simplest way to test it without involving the inproc
    /// transport (which since the inproc fast-path landed bypasses
    /// the codec entirely).
    #[allow(clippy::unused_async)]
    async fn inproc_pair(_name: &str) -> (DriverHandle, EventAdapter, DriverHandle, EventAdapter) {
        let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);

        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_codec = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"c")),
        );

        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let s_cancel = CancellationToken::new();
        let c_cancel = CancellationToken::new();

        let s_driver = ConnectionDriver::new(
            server_stream,
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            s_cancel.clone(),
        );
        let c_driver = ConnectionDriver::new(
            client_stream,
            client_codec,
            c_inbox_rx,
            c_evt_tx,
            0,
            c_cancel.clone(),
        );

        tokio::spawn(async move { s_driver.run().await });
        tokio::spawn(async move { c_driver.run().await });

        (
            DriverHandle {
                inbox: c_inbox_tx,
                cancel: c_cancel,
            },
            EventAdapter { rx: c_evt_rx },
            DriverHandle {
                inbox: s_inbox_tx,
                cancel: s_cancel,
            },
            EventAdapter { rx: s_evt_rx },
        )
    }

    #[tokio::test]
    async fn handshake_completes_over_inproc() {
        let (_client, mut client_events, _server, mut server_events) =
            inproc_pair("drv-handshake").await;

        let c = client_events.recv().await.unwrap();
        let s = server_events.recv().await.unwrap();
        assert!(matches!(c, Event::HandshakeSucceeded { .. }));
        assert!(matches!(s, Event::HandshakeSucceeded { .. }));
    }

    #[tokio::test]
    async fn message_roundtrip_over_inproc() {
        let (client, mut client_events, _server, mut server_events) = inproc_pair("drv-msg").await;
        client_events.recv().await.unwrap();
        server_events.recv().await.unwrap();

        client
            .inbox
            .send(DriverCommand::SendMessage(Message::single("hello")))
            .await
            .unwrap();

        let ev = server_events.recv().await.unwrap();
        match ev {
            Event::Message(m) => {
                assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
            }
            _ => panic!("unexpected {ev:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_stops_driver() {
        let (client, _client_events, _server, _server_events) = inproc_pair("drv-cancel").await;
        client.cancel.cancel();
        // The driver should exit; confirm by closing its inbox and checking
        // a subsequent send fails.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let res = client.inbox.send(DriverCommand::Close).await;
        assert!(res.is_err(), "inbox should be closed after driver exit");
    }

    #[tokio::test]
    async fn handshake_completes_over_tcp() {
        use crate::transport::{Listener as _, TcpTransport, Transport as _};
        use omq_proto::endpoint::{Endpoint, Host};
        use std::net::{IpAddr, Ipv4Addr};

        let bind_ep = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 0,
        };
        let mut listener = TcpTransport::bind(&bind_ep).await.unwrap();
        let local = listener.local_endpoint().clone();
        let Endpoint::Tcp { port, .. } = local else {
            panic!()
        };

        let connect_ep = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port,
        };
        let connect_task = tokio::spawn(async move { TcpTransport::connect(&connect_ep).await });

        let (server_stream, _peer) = listener.accept().await.unwrap();
        let client_stream = connect_task.await.unwrap().unwrap();

        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_codec = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));

        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
        let mut s_evt_rx = EventAdapter { rx: s_evt_rx };

        let s = ConnectionDriver::new(
            server_stream,
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            CancellationToken::new(),
        );
        let c = ConnectionDriver::new(
            client_stream,
            client_codec,
            c_inbox_rx,
            c_evt_tx,
            0,
            CancellationToken::new(),
        );
        tokio::spawn(async move { s.run().await });
        tokio::spawn(async move { c.run().await });

        let _ = c_inbox_tx; // keep inbox open
        let _ = s_inbox_tx;

        match c_evt_rx.recv().await.unwrap() {
            Event::HandshakeSucceeded { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
        match s_evt_rx.recv().await.unwrap() {
            Event::HandshakeSucceeded { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}
