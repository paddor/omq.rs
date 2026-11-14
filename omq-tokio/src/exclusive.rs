//! Exclusively owned, direct-I/O sockets for latency-sensitive callers.
//!
//! These sockets deliberately require `&mut self`: the caller polls TCP and
//! ZMTP directly, without a connection-driver task or data relay ring.

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::endpoint::Host;
use omq_proto::type_state::TypeState;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use crate::{Endpoint, Error, Message, ReconnectPolicy, Result, SocketType};
use live::{
    HeartbeatPlan, LiveConnection, accept_live, connect_live, drain_ready_input, flush_and_settle,
    flush_live, queue_ping, recv_live, run_timeout,
};

mod live;

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

        if self.identity.first() == Some(&0) {
            return Err(Error::Config(
                "exclusive socket identity must not start with a zero byte".into(),
            ));
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
    /// TCP connection established.
    Connected,
    /// ZMTP greeting and mechanism handshake completed.
    HandshakeSucceeded,
    /// Active peer disconnected or failed.
    Disconnected {
        /// Human-readable disconnect reason.
        reason: String,
    },
    /// Reconnect attempt scheduled after a failure.
    ReconnectDelayed {
        /// Delay before the next reconnect attempt.
        retry_in: Duration,
        /// One-based reconnect attempt number.
        attempt: u32,
    },
    /// Socket closed by the caller.
    Closed,
}

#[derive(Debug)]
enum Mode {
    Connect,
    Bind { listener: TcpListener },
}

/// A single-peer socket whose caller owns the TCP data path.
///
/// Supports connected or bound TCP `PAIR`, `DEALER`, `ROUTER`, `REQ`, `REP`,
/// `CLIENT`, and `SERVER` sockets using the NULL mechanism. Unsupported socket
/// types and transports return a configuration error.
///
/// A failed `send` is never retried automatically: the peer may have received
/// a partial or complete command. The next operation may restore the
/// connection, but the application decides whether a command is safe to retry.
#[derive(Debug)]
pub struct Socket {
    kind: SocketType,
    endpoint: Endpoint,
    options: Options,
    mode: Mode,
    live: Option<LiveConnection>,
    monitor: broadcast::Sender<Event>,
    reconnect_attempt: u32,
    retry_at: Option<Instant>,
    type_state: TypeState,
    heartbeat: HeartbeatPlan,
    rep_request_pending: bool,
    server_routing_id: u32,
    closed: bool,
}

impl Socket {
    fn new(kind: SocketType, endpoint: Endpoint, options: Options, mode: Mode) -> Self {
        let (monitor, _) = broadcast::channel(1024);
        Self {
            kind,
            endpoint,
            heartbeat: HeartbeatPlan::new(&options),
            options,
            mode,
            live: None,
            monitor,
            reconnect_attempt: 0,
            retry_at: None,
            type_state: TypeState::new(),
            rep_request_pending: false,
            server_routing_id: 0,
            closed: false,
        }
    }

    /// Connect a caller-driven TCP socket to a URI string or typed endpoint.
    ///
    /// Currently supports `PAIR`, `DEALER`, `ROUTER`, `REQ`, `REP`,
    /// `CLIENT`, and `SERVER`.
    pub async fn connect(
        socket_type: SocketType,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
        options: Options,
    ) -> Result<Self> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        validate_connect_mode(socket_type, &endpoint, &options)?;
        let mut socket = Self::new(socket_type, endpoint, options, Mode::Connect);
        socket.establish().await?;

        Ok(socket)
    }

    /// Bind a URI string or typed endpoint and return a caller-driven TCP socket.
    ///
    /// The socket accepts one peer at a time. If the peer disconnects, the next
    /// operation waits for a new peer on the same listener.
    ///
    /// Currently supports `PAIR`, `DEALER`, `ROUTER`, `REQ`, `REP`,
    /// `CLIENT`, and `SERVER`.
    pub async fn bind(
        socket_type: SocketType,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
        options: Options,
    ) -> Result<(Self, Endpoint)> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        validate_bind_mode(socket_type, &endpoint, &options)?;
        let (listener, bound) = bind_tcp_listener(&endpoint).await?;
        let socket = Self::new(socket_type, bound.clone(), options, Mode::Bind { listener });

        Ok((socket, bound))
    }

    /// Subscribe to lifecycle events for this socket.
    pub fn monitor(&self) -> broadcast::Receiver<Event> {
        self.monitor.subscribe()
    }

    /// Return true while a live TCP/ZMTP connection is active.
    pub fn is_connected(&self) -> bool {
        self.live.is_some()
    }

    /// Encode and write one message. Failed writes are not replayed.
    pub async fn send(&mut self, message: &Message) -> Result<()> {
        self.ensure_connected().await?;
        let mut wire_message = self.type_state.pre_send(self.kind, message.clone())?;

        let live = connected(&mut self.live)?;

        if self.kind == SocketType::Server
            && wire_message.take_routing_id() != Some(self.server_routing_id)
        {
            return Err(Error::Unroutable);
        }

        if is_identity_routed(self.kind) {
            wire_message =
                strip_single_peer_identity(self.kind, &live.peer_identity, wire_message)?;
        }

        let result = run_timeout(self.options.io_timeout, live.write_message(&wire_message)).await;
        self.disconnect_on_err(result)?;

        if self.kind == SocketType::Rep {
            self.rep_request_pending = false;
        }

        Ok(())
    }

    /// Receive one message. A timeout or transport error disconnects the
    /// current peer; a later operation attempts reconnection.
    pub async fn recv(&mut self) -> Result<Message> {
        loop {
            if self.kind == SocketType::Rep && self.rep_request_pending {
                return Err(Error::Protocol(
                    "REP socket must send a reply before receiving again".into(),
                ));
            }

            self.ensure_connected().await?;
            let live = connected(&mut self.live)?;
            let result =
                run_timeout(self.options.io_timeout, recv_live(live, self.heartbeat)).await;
            let message = self.disconnect_on_err(result)?;

            let message = if self.kind == SocketType::Server {
                message.with_routing_id(self.server_routing_id)
            } else if is_identity_routed(self.kind) {
                let peer_identity = connected(&mut self.live)?.peer_identity.clone();
                Message::with_prefix(peer_identity, message)
            } else {
                message
            };

            if let Some(message) = self.type_state.post_recv(self.kind, message)? {
                if self.kind == SocketType::Rep {
                    self.rep_request_pending = true;
                }

                return Ok(message);
            }
        }
    }

    /// Drive heartbeat and reconnect work when no data operation is active.
    ///
    /// `recv` drives heartbeat timers while it waits. An otherwise idle
    /// application must call this method from its own timer because exclusive
    /// sockets never run a background task.
    pub async fn maintain(&mut self) -> Result<()> {
        self.ensure_connected().await?;
        let input_result = drain_ready_input(connected(&mut self.live)?);
        self.disconnect_on_err(input_result)?;

        let live = connected(&mut self.live)?;
        let flush_result = run_timeout(self.options.io_timeout, flush_and_settle(live)).await;
        self.disconnect_on_err(flush_result)?;

        let now = Instant::now();
        let live = connected(&mut self.live)?;

        if live
            .silence_deadline()
            .is_some_and(|deadline| now >= deadline)
        {
            return self.disconnect_on_err(Err(Error::Timeout));
        }

        if live.connection.peer_minor() >= 1
            && let Some(interval) = self.heartbeat.interval
            && now.duration_since(live.last_ping) >= interval
        {
            queue_ping(live, self.heartbeat, now)?;
            let result = run_timeout(self.options.io_timeout, flush_live(live)).await;
            self.disconnect_on_err(result)?;
        }

        Ok(())
    }

    /// Drop any current connection and reconnect immediately.
    pub async fn reconnect_now(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }

        self.live = None;
        self.retry_at = None;
        self.type_state.on_peer_disconnected();
        self.rep_request_pending = false;

        self.establish().await
    }

    /// Close the socket and shut down the active TCP stream.
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

        if self.live.as_ref().is_some_and(|live| live.write_pending) {
            self.mark_disconnected(&Error::Protocol(
                "exclusive socket write was cancelled".into(),
            ));
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
        let result = match &mut self.mode {
            Mode::Connect => connect_live(self.kind, &self.endpoint, &self.options).await,
            Mode::Bind { listener } => accept_live(self.kind, listener, &self.options).await,
        };

        match result {
            Ok(live) => {
                if self.kind == SocketType::Server {
                    let Some(routing_id) = self.server_routing_id.checked_add(1) else {
                        return Err(Error::Config("SERVER routing IDs exhausted".into()));
                    };

                    self.server_routing_id = routing_id;
                }

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

    /// Pass `result` through; an error first disconnects the peer.
    fn disconnect_on_err<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            self.mark_disconnected(error);
        }

        result
    }

    fn mark_disconnected(&mut self, error: &Error) {
        if self.live.take().is_some() {
            self.type_state.on_peer_disconnected();
            self.rep_request_pending = false;
            let _ = self.monitor.send(Event::Disconnected {
                reason: error.to_string(),
            });
        }

        self.schedule_reconnect();
    }

    fn schedule_reconnect(&mut self) {
        if matches!(self.mode, Mode::Bind { .. }) {
            self.reconnect_attempt = 0;
            self.retry_at = None;
            return;
        }

        let Some(delay) = reconnect_delay(self.options.reconnect, self.reconnect_attempt) else {
            self.retry_at = None;
            return;
        };

        self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        // A delay past the clock range stands for "never": wait a year.
        let now = Instant::now();
        self.retry_at = Some(
            now.checked_add(delay)
                .unwrap_or_else(|| now + Duration::from_hours(365 * 24)),
        );
        let _ = self.monitor.send(Event::ReconnectDelayed {
            retry_in: delay,
            attempt: self.reconnect_attempt,
        });
    }
}

/// Borrow the connection that `ensure_connected` established.
fn connected(live: &mut Option<LiveConnection>) -> Result<&mut LiveConnection> {
    live.as_mut().ok_or(Error::Closed)
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

fn validate_socket_type(socket_type: SocketType) -> Result<()> {
    if !matches!(
        socket_type,
        SocketType::Dealer
            | SocketType::Router
            | SocketType::Req
            | SocketType::Rep
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Server
    ) {
        return Err(Error::Config(format!(
            "exclusive sockets currently support only PAIR, DEALER, ROUTER, REQ, REP, CLIENT, and SERVER, not {socket_type:?}"
        )));
    }

    Ok(())
}

fn validate_connect_mode(
    socket_type: SocketType,
    endpoint: &Endpoint,
    options: &Options,
) -> Result<()> {
    validate_socket_type(socket_type)?;
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

fn validate_bind_mode(
    socket_type: SocketType,
    endpoint: &Endpoint,
    options: &Options,
) -> Result<()> {
    validate_socket_type(socket_type)?;
    match endpoint {
        Endpoint::Tcp { .. } => {}
        _ => {
            return Err(Error::Config(format!(
                "exclusive sockets currently support only tcp:// endpoints, not {endpoint}"
            )));
        }
    }

    options.validate()
}

async fn bind_tcp_listener(endpoint: &Endpoint) -> Result<(TcpListener, Endpoint)> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::Config(
            "exclusive sockets currently support only tcp:// endpoints".into(),
        ));
    };

    let addr = crate::transport::dns::bind_addr(host, *port).await?;
    let listener = crate::transport::tcp::reuse_addr_bind(addr)?;
    let local = listener.local_addr()?;
    let bound = Endpoint::Tcp {
        host: Host::Ip(local.ip()),
        port: local.port(),
    };

    Ok((listener, bound))
}

fn is_identity_routed(socket_type: SocketType) -> bool {
    socket_type == SocketType::Router
}

fn strip_single_peer_identity(
    socket_type: SocketType,
    peer_identity: &Bytes,
    mut message: Message,
) -> Result<Message> {
    let Some(identity) = message.pop_front() else {
        return Err(Error::Protocol(format!(
            "{socket_type:?} socket requires a routing identity frame"
        )));
    };

    if identity != *peer_identity {
        return Err(Error::Unroutable);
    }

    Ok(message)
}

#[cfg(test)]
mod tests;
