//! Exclusively owned, direct-I/O sockets for latency-sensitive callers.
//!
//! These sockets deliberately require `&mut self`: the caller polls TCP and
//! ZMTP directly, without a connection-driver task or data relay ring.

use std::io;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use omq_proto::endpoint::Host;
use omq_proto::proto::command::Command;
use omq_proto::proto::{Connection, ConnectionConfig, Event as ProtoEvent, Role};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use crate::{Endpoint, Error, Message, ReconnectPolicy, Result, SocketType};

/// Configuration for a caller-driven [`Socket`].
#[derive(Clone, Debug)]
pub struct Options {
    /// ZMTP routing identity presented to the peer.
    pub identity: Bytes,
    /// Maximum time allowed to establish the TCP connection.
    pub connect_timeout: Duration,
    /// Maximum time allowed to complete the ZMTP handshake.
    pub handshake_timeout: Duration,
    /// Optional deadline for each `send`, `recv`, or maintenance write.
    pub io_timeout: Option<Duration>,
    /// Reconnection policy applied by the next operation after a disconnect.
    pub reconnect: ReconnectPolicy,
    /// Interval between outbound ZMTP PING commands.
    pub heartbeat_interval: Option<Duration>,
    /// TTL announced to the peer in outbound PING commands.
    pub heartbeat_ttl: Option<Duration>,
    /// Time allowed for peer activity after an outbound heartbeat PING.
    ///
    /// Defaults to `heartbeat_interval` when heartbeats are enabled.
    pub heartbeat_timeout: Option<Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            identity: Bytes::new(),
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            io_timeout: None,
            reconnect: ReconnectPolicy::default(),
            heartbeat_interval: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
        }
    }
}

impl Options {
    fn validate(&self) -> Result<()> {
        if self.identity.len() > 255 {
            return Err(Error::Config(format!(
                "exclusive socket identity length {} exceeds ZMTP limit of 255",
                self.identity.len()
            )));
        }
        if self.connect_timeout.is_zero() {
            return Err(Error::Config(
                "exclusive socket connect_timeout must be non-zero".into(),
            ));
        }
        if self.handshake_timeout.is_zero() {
            return Err(Error::Config(
                "exclusive socket handshake_timeout must be non-zero".into(),
            ));
        }
        if self.heartbeat_interval.is_some_and(|value| value.is_zero()) {
            return Err(Error::Config(
                "exclusive socket heartbeat_interval must be non-zero".into(),
            ));
        }
        if self.heartbeat_timeout.is_some_and(|value| value.is_zero()) {
            return Err(Error::Config(
                "exclusive socket heartbeat_timeout must be non-zero".into(),
            ));
        }
        if let Some(ttl) = self.heartbeat_ttl
            && ttl > Duration::from_millis(6_553_500)
        {
            return Err(Error::Config(format!(
                "exclusive socket heartbeat_ttl {ttl:?} exceeds ZMTP maximum of 6553.5s"
            )));
        }
        Ok(())
    }
}

/// Lifecycle event for an exclusive socket.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    Connected,
    HandshakeSucceeded,
    Disconnected { reason: String },
    ReconnectDelayed { retry_in: Duration, attempt: u32 },
    Closed,
}

#[derive(Debug)]
struct LiveConnection {
    stream: TcpStream,
    connection: Connection,
    read_buf: BytesMut,
    write_buf: BytesMut,
    last_ping: Instant,
    heartbeat_deadline: Option<Instant>,
    ping_sequence: u64,
}

/// A single-peer socket whose caller owns the TCP data path.
///
/// The initial implementation supports only a connected TCP DEALER using the
/// NULL mechanism. Unsupported socket types and transports return a
/// configuration error.
///
/// A failed `send` is never retried automatically: the peer may have received
/// a partial or complete command. The next operation may restore the
/// connection, but the application decides whether a command is safe to retry.
#[derive(Debug)]
pub struct Socket {
    kind: SocketType,
    endpoint: Endpoint,
    options: Options,
    live: Option<LiveConnection>,
    monitor: broadcast::Sender<Event>,
    reconnect_attempt: u32,
    retry_at: Option<Instant>,
    closed: bool,
}

impl Socket {
    /// Connect a caller-driven socket.
    pub async fn connect(
        socket_type: SocketType,
        endpoint: Endpoint,
        options: Options,
    ) -> Result<Self> {
        validate_mode(socket_type, &endpoint, &options)?;
        let (monitor, _) = broadcast::channel(1024);
        let mut socket = Self {
            kind: socket_type,
            endpoint,
            options,
            live: None,
            monitor,
            reconnect_attempt: 0,
            retry_at: None,
            closed: false,
        };
        socket.establish().await?;
        Ok(socket)
    }

    pub fn monitor(&self) -> broadcast::Receiver<Event> {
        self.monitor.subscribe()
    }

    pub fn is_connected(&self) -> bool {
        self.live.is_some()
    }

    /// Encode and write one message. Failed writes are not replayed.
    pub async fn send(&mut self, message: &Message) -> Result<()> {
        self.ensure_connected().await?;
        let timeout = self.options.io_timeout;
        let result = run_timeout(timeout, async {
            let live = self.live.as_mut().expect("connected");
            live.write_buf.clear();
            live.connection
                .send_message_flat(message, &mut live.write_buf);
            live.stream
                .write_all(&live.write_buf)
                .await
                .map_err(Error::Io)
        })
        .await;
        if let Err(error) = result {
            self.mark_disconnected(&error);
            return Err(error);
        }
        Ok(())
    }

    /// Receive one message. A timeout or transport error disconnects the
    /// current peer; a later operation attempts reconnection.
    pub async fn recv(&mut self) -> Result<Message> {
        self.ensure_connected().await?;
        let io_timeout = self.options.io_timeout;
        let heartbeat_interval = self.options.heartbeat_interval;
        let heartbeat_timeout = effective_heartbeat_timeout(&self.options);
        let heartbeat_ttl = heartbeat_ttl_deciseconds(self.options.heartbeat_ttl);
        let result = run_timeout(
            io_timeout,
            recv_live(
                self.live.as_mut().expect("connected"),
                heartbeat_interval,
                heartbeat_timeout,
                heartbeat_ttl,
            ),
        )
        .await;
        if let Err(error) = &result {
            self.mark_disconnected(error);
        }
        result
    }

    /// Drive heartbeat and reconnect work when no data operation is active.
    ///
    /// `recv` drives heartbeat timers while it waits. An otherwise idle
    /// application must call this method from its own timer because exclusive
    /// sockets never run a background task.
    pub async fn maintain(&mut self) -> Result<()> {
        self.ensure_connected().await?;
        let heartbeat_timeout = effective_heartbeat_timeout(&self.options);
        let input_result =
            drain_ready_input(self.live.as_mut().expect("connected"), heartbeat_timeout);
        if let Err(error) = input_result {
            self.mark_disconnected(&error);
            return Err(error);
        }
        let flush_result = run_timeout(
            self.options.io_timeout,
            flush_live(self.live.as_mut().expect("connected")),
        )
        .await;
        if let Err(error) = flush_result {
            self.mark_disconnected(&error);
            return Err(error);
        }
        let now = Instant::now();
        let live = self.live.as_mut().expect("connected");
        if live
            .heartbeat_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            let error = Error::Timeout;
            self.mark_disconnected(&error);
            return Err(error);
        }
        if let Some(interval) = self.options.heartbeat_interval
            && now.duration_since(live.last_ping) >= interval
        {
            queue_ping(
                live,
                heartbeat_ttl_deciseconds(self.options.heartbeat_ttl),
                heartbeat_timeout.expect("heartbeat interval supplies timeout"),
                now,
            )?;
            let result = run_timeout(self.options.io_timeout, flush_live(live)).await;
            if let Err(error) = result {
                self.mark_disconnected(&error);
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn reconnect_now(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        self.live = None;
        self.retry_at = None;
        self.establish().await
    }

    pub async fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.retry_at = None;
        if let Some(mut live) = self.live.take() {
            live.stream.shutdown().await?;
        }
        let _ = self.monitor.send(Event::Closed);
        Ok(())
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        if self.live.is_some() {
            return Ok(());
        }
        if matches!(self.options.reconnect, ReconnectPolicy::Disabled) {
            return Err(Error::Closed);
        }
        if let Some(retry_at) = self.retry_at {
            tokio::time::sleep_until(retry_at.into()).await;
        }
        self.establish().await
    }

    async fn establish(&mut self) -> Result<()> {
        let result = establish_live(
            self.kind,
            &self.endpoint,
            self.options.identity.clone(),
            self.options.connect_timeout,
            self.options.handshake_timeout,
        )
        .await;
        match result {
            Ok(live) => {
                self.live = Some(live);
                self.reconnect_attempt = 0;
                self.retry_at = None;
                let _ = self.monitor.send(Event::Connected);
                let _ = self.monitor.send(Event::HandshakeSucceeded);
                Ok(())
            }
            Err(error) => {
                self.schedule_reconnect();
                Err(error)
            }
        }
    }

    fn mark_disconnected(&mut self, error: &Error) {
        if self.live.take().is_some() {
            let _ = self.monitor.send(Event::Disconnected {
                reason: error.to_string(),
            });
        }
        self.schedule_reconnect();
    }

    fn schedule_reconnect(&mut self) {
        let Some(delay) = reconnect_delay(self.options.reconnect, self.reconnect_attempt) else {
            self.retry_at = None;
            return;
        };
        self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        self.retry_at = Some(Instant::now() + delay);
        let _ = self.monitor.send(Event::ReconnectDelayed {
            retry_in: delay,
            attempt: self.reconnect_attempt,
        });
    }
}

fn reconnect_delay(policy: ReconnectPolicy, attempt: u32) -> Option<Duration> {
    match policy {
        ReconnectPolicy::Fixed(delay) => Some(delay),
        ReconnectPolicy::Exponential { min, max } => {
            let multiplier = 1_u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
            Some(min.saturating_mul(multiplier).min(max))
        }
        _ => None,
    }
}

fn validate_mode(socket_type: SocketType, endpoint: &Endpoint, options: &Options) -> Result<()> {
    if socket_type != SocketType::Dealer {
        return Err(Error::Config(format!(
            "exclusive sockets currently support only DEALER, not {socket_type:?}"
        )));
    }
    match endpoint {
        Endpoint::Tcp {
            host: Host::Wildcard,
            ..
        } => {
            return Err(Error::Config(
                "exclusive sockets cannot connect to a wildcard TCP host".into(),
            ));
        }
        Endpoint::Tcp { .. } => {}
        _ => {
            return Err(Error::Config(format!(
                "exclusive sockets currently support only tcp:// endpoints, not {endpoint}"
            )));
        }
    }
    options.validate()
}

async fn establish_live(
    socket_type: SocketType,
    endpoint: &Endpoint,
    identity: Bytes,
    connect_timeout: Duration,
    handshake_timeout: Duration,
) -> Result<LiveConnection> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::Config(
            "exclusive sockets currently support only tcp:// endpoints".into(),
        ));
    };
    let host = host.to_string();
    let stream = tokio::time::timeout(connect_timeout, TcpStream::connect((host.as_str(), *port)))
        .await
        .map_err(|_| timed_out("exclusive socket connect timeout"))??;
    stream.set_nodelay(true)?;
    let connection =
        Connection::new(ConnectionConfig::new(Role::Client, socket_type).identity(identity));
    let now = Instant::now();
    let mut live = LiveConnection {
        stream,
        connection,
        read_buf: BytesMut::with_capacity(4 * 1024),
        write_buf: BytesMut::with_capacity(4 * 1024),
        last_ping: now,
        heartbeat_deadline: None,
        ping_sequence: 0,
    };
    tokio::time::timeout(handshake_timeout, finish_handshake(&mut live))
        .await
        .map_err(|_| timed_out("exclusive socket handshake timeout"))??;
    Ok(live)
}

async fn finish_handshake(live: &mut LiveConnection) -> Result<()> {
    while !live.connection.is_ready() {
        flush_live(live).await?;
        if live.connection.is_ready() {
            break;
        }
        read_live_once(live, None).await?;
        while let Some(event) = live.connection.poll_event() {
            if let ProtoEvent::HandshakeSucceeded { .. } = event {
                break;
            }
        }
    }
    flush_live(live).await
}

async fn recv_live(
    live: &mut LiveConnection,
    heartbeat_interval: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
    heartbeat_ttl_deciseconds: u16,
) -> Result<Message> {
    loop {
        if let Some(message) = live.connection.poll_message() {
            return Ok(message);
        }

        let ping_deadline = heartbeat_interval.map(|interval| live.last_ping + interval);
        let heartbeat_deadline = live.heartbeat_deadline;
        tokio::select! {
            biased;
            result = read_live_once(live, heartbeat_timeout) => {
                result?;
                flush_live(live).await?;
            }
            () = sleep_until(heartbeat_deadline), if heartbeat_deadline.is_some() => {
                return Err(Error::Timeout);
            }
            () = sleep_until(ping_deadline), if ping_deadline.is_some() => {
                let now = Instant::now();
                queue_ping(
                    live,
                    heartbeat_ttl_deciseconds,
                    heartbeat_timeout.expect("heartbeat interval supplies timeout"),
                    now,
                )?;
                flush_live(live).await?;
            }
        }
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

fn heartbeat_ttl_deciseconds(ttl: Option<Duration>) -> u16 {
    ttl.and_then(|duration| u16::try_from(duration.as_millis() / 100).ok())
        .unwrap_or(0)
}

fn effective_heartbeat_timeout(options: &Options) -> Option<Duration> {
    options
        .heartbeat_interval
        .map(|interval| options.heartbeat_timeout.unwrap_or(interval))
}

fn queue_ping(
    live: &mut LiveConnection,
    ttl_deciseconds: u16,
    heartbeat_timeout: Duration,
    now: Instant,
) -> Result<()> {
    live.ping_sequence = live.ping_sequence.wrapping_add(1);
    let context = Bytes::copy_from_slice(&live.ping_sequence.to_le_bytes());
    live.connection.send_command(&Command::Ping {
        ttl_deciseconds,
        context,
    })?;
    live.last_ping = now;
    if live.heartbeat_deadline.is_none() {
        live.heartbeat_deadline = now.checked_add(heartbeat_timeout);
    }
    Ok(())
}

fn note_inbound_traffic(live: &mut LiveConnection, heartbeat_timeout: Option<Duration>) {
    if live.heartbeat_deadline.is_some() {
        live.heartbeat_deadline =
            heartbeat_timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    }
}

async fn read_live_once(
    live: &mut LiveConnection,
    heartbeat_timeout: Option<Duration>,
) -> Result<()> {
    let n = live.stream.read_buf(&mut live.read_buf).await?;
    if n == 0 {
        return Err(Error::Closed);
    }
    note_inbound_traffic(live, heartbeat_timeout);
    live.connection.handle_input(live.read_buf.split().freeze())
}

fn drain_ready_input(live: &mut LiveConnection, heartbeat_timeout: Option<Duration>) -> Result<()> {
    loop {
        match live.stream.try_read_buf(&mut live.read_buf) {
            Ok(0) => return Err(Error::Closed),
            Ok(_) => {
                note_inbound_traffic(live, heartbeat_timeout);
                live.connection
                    .handle_input(live.read_buf.split().freeze())?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(Error::Io(error)),
        }
    }
}

async fn flush_live(live: &mut LiveConnection) -> Result<()> {
    while live.connection.has_pending_transmit() {
        let chunks = live.connection.transmit_chunks_capped(64);
        let n = live.stream.write_vectored(&chunks).await?;
        drop(chunks);
        if n == 0 {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "exclusive socket write returned zero",
            )));
        }
        live.connection.advance_transmit(n);
    }
    Ok(())
}

async fn run_timeout<T>(
    timeout: Option<Duration>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| timed_out("exclusive socket I/O timeout"))?,
        None => future.await,
    }
}

fn timed_out(message: &'static str) -> Error {
    Error::Io(io::Error::new(io::ErrorKind::TimedOut, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options as RegularOptions, Socket as RegularSocket};
    use tokio::net::TcpListener;

    async fn router() -> (RegularSocket, Endpoint) {
        let router = RegularSocket::new(SocketType::Router, RegularOptions::default());
        let bound = router
            .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
            .await
            .unwrap();
        (router, bound)
    }

    async fn unresponsive_router() -> (Endpoint, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let now = Instant::now();
            let mut live = LiveConnection {
                stream,
                connection: Connection::new(ConnectionConfig::new(
                    Role::Server,
                    SocketType::Router,
                )),
                read_buf: BytesMut::with_capacity(4 * 1024),
                write_buf: BytesMut::with_capacity(4 * 1024),
                last_ping: now,
                heartbeat_deadline: None,
                ping_sequence: 0,
            };
            finish_handshake(&mut live).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        (endpoint, server)
    }

    #[tokio::test]
    async fn dealer_round_trips_with_standard_router() {
        let (router, endpoint) = router().await;
        let server = tokio::spawn(async move {
            for _ in 0..100 {
                let message = router.recv().await.unwrap();
                router.send(message).await.unwrap();
            }
        });
        let mut socket = Socket::connect(
            SocketType::Dealer,
            endpoint,
            Options {
                identity: Bytes::from_static(b"exclusive-test"),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        for sequence in 0_u64..100 {
            let message = Message::single(Bytes::copy_from_slice(&sequence.to_le_bytes()));
            socket.send(&message).await.unwrap();
            assert_eq!(socket.recv().await.unwrap(), message);
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_preserves_identity_and_reports_events() {
        let (router1, endpoint) = router().await;
        let mut socket = Socket::connect(
            SocketType::Dealer,
            endpoint,
            Options {
                identity: Bytes::from_static(b"stable-id"),
                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(1)),
                io_timeout: Some(Duration::from_millis(100)),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let mut monitor = socket.monitor();
        drop(router1);
        assert!(socket.recv().await.is_err());
        let (router2, rebound) = router().await;
        // A new ephemeral port is expected; exercise explicit reconnect state
        // here. Restart-on-the-same-address is covered by the external Wine
        // reproduction procedure.
        socket.endpoint = rebound;
        socket.reconnect_now().await.unwrap();
        let server = tokio::spawn(async move {
            let message = router2.recv().await.unwrap();
            assert_eq!(message.part_bytes(0).unwrap().as_ref(), b"stable-id");
            router2.send(message).await.unwrap();
        });
        let message = Message::single(Bytes::from_static(b"after-reconnect"));
        socket.send(&message).await.unwrap();
        assert_eq!(socket.recv().await.unwrap(), message);
        server.await.unwrap();
        let events: Vec<_> = std::iter::from_fn(|| monitor.try_recv().ok()).collect();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::Disconnected { .. }))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::HandshakeSucceeded))
        );
    }

    #[tokio::test]
    async fn recv_drives_heartbeat_while_waiting_for_reply() {
        let (router, endpoint) = router().await;
        let server = tokio::spawn(async move {
            let message = router.recv().await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            router.send(message).await.unwrap();
        });
        let mut socket = Socket::connect(
            SocketType::Dealer,
            endpoint,
            Options {
                heartbeat_interval: Some(Duration::from_millis(10)),
                heartbeat_timeout: Some(Duration::from_millis(40)),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let message = Message::single(Bytes::from_static(b"heartbeat"));
        socket.send(&message).await.unwrap();
        assert_eq!(socket.recv().await.unwrap(), message);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn recv_does_not_time_out_before_first_ping() {
        let (endpoint, server) = unresponsive_router().await;
        let mut socket = Socket::connect(
            SocketType::Dealer,
            endpoint,
            Options {
                heartbeat_interval: Some(Duration::from_millis(100)),
                heartbeat_timeout: Some(Duration::from_millis(20)),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), socket.recv())
                .await
                .is_err()
        );
        assert!(socket.is_connected());
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(150), socket.recv()).await,
            Ok(Err(Error::Timeout))
        ));
        assert!(!socket.is_connected());
        server.abort();
    }

    #[tokio::test]
    async fn heartbeat_timeout_defaults_to_interval() {
        let (endpoint, server) = unresponsive_router().await;
        let mut socket = Socket::connect(
            SocketType::Dealer,
            endpoint,
            Options {
                heartbeat_interval: Some(Duration::from_millis(20)),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(200), socket.recv()).await,
            Ok(Err(Error::Timeout))
        ));
        assert!(!socket.is_connected());
        server.abort();
    }

    #[tokio::test]
    async fn unsupported_modes_fail_before_connecting() {
        let endpoint: Endpoint = "tcp://127.0.0.1:1".parse().unwrap();
        let error = Socket::connect(SocketType::Pair, endpoint, Options::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("only DEALER")));

        let endpoint: Endpoint = "inproc://exclusive".parse().unwrap();
        let error = Socket::connect(SocketType::Dealer, endpoint, Options::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("only tcp://")));
    }
}
