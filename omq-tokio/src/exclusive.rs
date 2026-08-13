//! Exclusively owned, direct-I/O sockets for latency-sensitive callers.
//!
//! These sockets deliberately require `&mut self`: the caller polls TCP and
//! ZMTP directly, without a connection-driver task or data relay ring.

use std::io;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use omq_proto::proto::command::Command;
use omq_proto::proto::{Connection, ConnectionConfig, Event, Role};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use crate::{Error, Message, ReconnectPolicy, Result, SocketType};

/// Reliability settings for [`ExclusiveDealer`].
#[derive(Clone, Copy, Debug)]
pub struct ExclusiveDealerOptions {
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    pub io_timeout: Option<Duration>,
    pub reconnect: ReconnectPolicy,
    pub heartbeat_interval: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}

impl Default for ExclusiveDealerOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            io_timeout: None,
            reconnect: ReconnectPolicy::default(),
            heartbeat_interval: None,
            heartbeat_timeout: None,
        }
    }
}

/// Lifecycle event for an exclusive socket.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ExclusiveEvent {
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
    last_input: Instant,
    last_ping: Instant,
    ping_sequence: u64,
}

/// A single-peer DEALER whose caller owns the TCP data path.
///
/// A failed `send` is never retried automatically: the peer may have received
/// a partial or complete command. The next operation may restore the
/// connection, but the application decides whether a command is safe to retry.
#[derive(Debug)]
pub struct ExclusiveDealer {
    address: String,
    identity: Bytes,
    options: ExclusiveDealerOptions,
    live: Option<LiveConnection>,
    monitor: broadcast::Sender<ExclusiveEvent>,
    reconnect_attempt: u32,
    retry_at: Option<Instant>,
    closed: bool,
}

impl ExclusiveDealer {
    pub async fn connect(address: impl Into<String>, identity: Bytes) -> Result<Self> {
        Self::connect_with_options(address, identity, ExclusiveDealerOptions::default()).await
    }

    pub async fn connect_with_options(
        address: impl Into<String>,
        identity: Bytes,
        options: ExclusiveDealerOptions,
    ) -> Result<Self> {
        let (monitor, _) = broadcast::channel(1024);
        let mut dealer = Self {
            address: address.into(),
            identity,
            options,
            live: None,
            monitor,
            reconnect_attempt: 0,
            retry_at: None,
            closed: false,
        };
        dealer.establish().await?;
        Ok(dealer)
    }

    pub fn monitor(&self) -> broadcast::Receiver<ExclusiveEvent> {
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
        let timeout = self.options.io_timeout;
        let result = run_timeout(timeout, recv_live(self.live.as_mut().expect("connected"))).await;
        if let Err(error) = &result {
            self.mark_disconnected(error);
        }
        result
    }

    /// Drive heartbeat and reconnect work. Call this from the bridge timer
    /// when no data operation is active; it never runs a background data task.
    pub async fn maintain(&mut self) -> Result<()> {
        self.ensure_connected().await?;
        let input_result = drain_ready_input(self.live.as_mut().expect("connected"));
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
        if let Some(timeout) = self.options.heartbeat_timeout
            && now.duration_since(live.last_input) >= timeout
        {
            let error = timed_out("exclusive DEALER heartbeat timeout");
            self.mark_disconnected(&error);
            return Err(error);
        }
        if let Some(interval) = self.options.heartbeat_interval
            && now.duration_since(live.last_ping) >= interval
        {
            live.ping_sequence = live.ping_sequence.wrapping_add(1);
            let context = Bytes::copy_from_slice(&live.ping_sequence.to_le_bytes());
            live.connection.send_command(&Command::Ping {
                ttl_deciseconds: 0,
                context,
            })?;
            let result = run_timeout(self.options.io_timeout, flush_live(live)).await;
            if let Err(error) = result {
                self.mark_disconnected(&error);
                return Err(error);
            }
            live.last_ping = now;
        }
        Ok(())
    }

    pub async fn reconnect_now(&mut self) -> Result<()> {
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
        let _ = self.monitor.send(ExclusiveEvent::Closed);
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
            &self.address,
            self.identity.clone(),
            self.options.connect_timeout,
            self.options.handshake_timeout,
        )
        .await;
        match result {
            Ok(live) => {
                self.live = Some(live);
                self.reconnect_attempt = 0;
                self.retry_at = None;
                let _ = self.monitor.send(ExclusiveEvent::Connected);
                let _ = self.monitor.send(ExclusiveEvent::HandshakeSucceeded);
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
            let _ = self.monitor.send(ExclusiveEvent::Disconnected {
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
        let _ = self.monitor.send(ExclusiveEvent::ReconnectDelayed {
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

async fn establish_live(
    address: &str,
    identity: Bytes,
    connect_timeout: Duration,
    handshake_timeout: Duration,
) -> Result<LiveConnection> {
    let stream = tokio::time::timeout(connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| timed_out("exclusive DEALER connect timeout"))??;
    stream.set_nodelay(true)?;
    let connection =
        Connection::new(ConnectionConfig::new(Role::Client, SocketType::Dealer).identity(identity));
    let now = Instant::now();
    let mut live = LiveConnection {
        stream,
        connection,
        read_buf: BytesMut::with_capacity(4 * 1024),
        write_buf: BytesMut::with_capacity(4 * 1024),
        last_input: now,
        last_ping: now,
        ping_sequence: 0,
    };
    tokio::time::timeout(handshake_timeout, finish_handshake(&mut live))
        .await
        .map_err(|_| timed_out("exclusive DEALER handshake timeout"))??;
    Ok(live)
}

async fn finish_handshake(live: &mut LiveConnection) -> Result<()> {
    while !live.connection.is_ready() {
        flush_live(live).await?;
        if live.connection.is_ready() {
            break;
        }
        read_live_once(live).await?;
        while let Some(event) = live.connection.poll_event() {
            if let Event::HandshakeSucceeded { .. } = event {
                break;
            }
        }
    }
    flush_live(live).await
}

async fn recv_live(live: &mut LiveConnection) -> Result<Message> {
    loop {
        if let Some(message) = live.connection.poll_message() {
            return Ok(message);
        }
        read_live_once(live).await?;
        flush_live(live).await?;
    }
}

async fn read_live_once(live: &mut LiveConnection) -> Result<()> {
    let n = live.stream.read_buf(&mut live.read_buf).await?;
    if n == 0 {
        return Err(Error::Closed);
    }
    live.last_input = Instant::now();
    live.connection.handle_input(live.read_buf.split().freeze())
}

fn drain_ready_input(live: &mut LiveConnection) -> Result<()> {
    loop {
        match live.stream.try_read_buf(&mut live.read_buf) {
            Ok(0) => return Err(Error::Closed),
            Ok(_) => {
                live.last_input = Instant::now();
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
                "exclusive DEALER write returned zero",
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
            .map_err(|_| timed_out("exclusive DEALER I/O timeout"))?,
        None => future.await,
    }
}

fn timed_out(message: &'static str) -> Error {
    Error::Io(io::Error::new(io::ErrorKind::TimedOut, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Endpoint, Options, Socket};

    async fn router() -> (Socket, String) {
        let router = Socket::new(SocketType::Router, Options::default());
        let bound = router
            .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
            .await
            .unwrap();
        let Endpoint::Tcp { host, port } = bound else {
            panic!()
        };
        (router, format!("{host}:{port}"))
    }

    #[tokio::test]
    async fn dealer_round_trips_with_standard_router() {
        let (router, address) = router().await;
        let server = tokio::spawn(async move {
            for _ in 0..100 {
                let message = router.recv().await.unwrap();
                router.send(message).await.unwrap();
            }
        });
        let mut dealer = ExclusiveDealer::connect(address, Bytes::from_static(b"exclusive-test"))
            .await
            .unwrap();
        for sequence in 0_u64..100 {
            let message = Message::single(Bytes::copy_from_slice(&sequence.to_le_bytes()));
            dealer.send(&message).await.unwrap();
            assert_eq!(dealer.recv().await.unwrap(), message);
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_preserves_identity_and_reports_events() {
        let (router1, address) = router().await;
        let mut dealer = ExclusiveDealer::connect_with_options(
            address.clone(),
            Bytes::from_static(b"stable-id"),
            ExclusiveDealerOptions {
                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(1)),
                io_timeout: Some(Duration::from_millis(100)),
                ..ExclusiveDealerOptions::default()
            },
        )
        .await
        .unwrap();
        let mut monitor = dealer.monitor();
        drop(router1);
        assert!(dealer.recv().await.is_err());
        let (router2, rebound) = router().await;
        // A new ephemeral port is expected; exercise explicit reconnect state
        // here, while restart-on-the-same-address is covered by the Wine test.
        dealer.address = rebound;
        dealer.reconnect_now().await.unwrap();
        let server = tokio::spawn(async move {
            let message = router2.recv().await.unwrap();
            assert_eq!(message.part_bytes(0).unwrap().as_ref(), b"stable-id");
            router2.send(message).await.unwrap();
        });
        let message = Message::single(Bytes::from_static(b"after-reconnect"));
        dealer.send(&message).await.unwrap();
        assert_eq!(dealer.recv().await.unwrap(), message);
        server.await.unwrap();
        let events: Vec<_> = std::iter::from_fn(|| monitor.try_recv().ok()).collect();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ExclusiveEvent::Disconnected { .. }))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ExclusiveEvent::HandshakeSucceeded))
        );
    }
}
