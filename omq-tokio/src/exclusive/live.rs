//! TCP and ZMTP I/O of one live exclusive connection.

use std::io;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use omq_proto::message::generated_identity;
use omq_proto::proto::command::Command;
use omq_proto::proto::{Connection, ConnectionConfig, Event as ProtoEvent, Role};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::Options;
use crate::engine::driver::{check_written, heartbeat_timeout, sleep_until_opt, ttl_deciseconds};
use crate::{Endpoint, Error, Message, Result, SocketType};

#[derive(Debug)]
pub(super) struct LiveConnection {
    pub(super) stream: TcpStream,
    pub(super) connection: Connection,
    read_buf: BytesMut,
    write_buf: BytesMut,
    pub(super) last_ping: Instant,
    last_input: Instant,
    heartbeat_deadline: Option<Instant>,
    ping_sequence: u64,
    pub(super) peer_identity: Bytes,
    /// A flat message write started and has not completed. A caller that
    /// drops `send` mid-write leaves a truncated frame on the wire.
    pub(super) write_pending: bool,
}

impl LiveConnection {
    pub(super) fn new(stream: TcpStream, connection: Connection) -> Self {
        let now = Instant::now();
        Self {
            stream,
            connection,
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(4 * 1024),
            last_ping: now,
            last_input: now,
            heartbeat_deadline: None,
            ping_sequence: 0,
            peer_identity: Bytes::new(),
            write_pending: false,
        }
    }

    pub(super) fn silence_deadline(&self) -> Option<Instant> {
        let peer = self
            .connection
            .peer_heartbeat_ttl()
            .and_then(|ttl| self.last_input.checked_add(ttl));
        match (self.heartbeat_deadline, peer) {
            (Some(local), Some(peer)) => Some(local.min(peer)),
            (local, peer) => local.or(peer),
        }
    }

    /// Encode and write one message. `write_pending` stays set if the caller
    /// drops this future mid-write.
    pub(super) async fn write_message(&mut self, message: &Message) -> Result<()> {
        self.write_buf.clear();
        self.connection
            .send_message_flat(message, &mut self.write_buf);
        self.write_pending = true;
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_err(Error::Io)?;
        self.write_pending = false;
        Ok(())
    }
}

/// Heartbeat settings derived once from [`Options`].
#[derive(Debug, Clone, Copy)]
pub(super) struct HeartbeatPlan {
    pub(super) interval: Option<Duration>,
    /// Peer activity allowed after a PING; set whenever `interval` is.
    timeout: Option<Duration>,
    ttl_deciseconds: u16,
}

impl HeartbeatPlan {
    pub(super) fn new(options: &Options) -> Self {
        Self {
            interval: options.heartbeat_interval,
            timeout: heartbeat_timeout(options.heartbeat_interval, options.heartbeat_timeout),
            ttl_deciseconds: ttl_deciseconds(options.heartbeat_ttl),
        }
    }
}

pub(super) async fn connect_live(
    socket_type: SocketType,
    endpoint: &Endpoint,
    options: &Options,
) -> Result<LiveConnection> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::Config(
            "exclusive sockets currently support only tcp:// endpoints".into(),
        ));
    };
    let host = host.to_string();
    let connect = TcpStream::connect((host.as_str(), *port));
    let stream = tokio::time::timeout(options.connect_timeout, connect)
        .await
        .map_err(|_| timed_out("exclusive socket connect timeout"))??;
    handshake_live(stream, Role::Client, socket_type, options).await
}

pub(super) async fn accept_live(
    socket_type: SocketType,
    listener: &TcpListener,
    options: &Options,
) -> Result<LiveConnection> {
    let (stream, _) = tokio::time::timeout(options.connect_timeout, listener.accept())
        .await
        .map_err(|_| timed_out("exclusive socket accept timeout"))??;
    handshake_live(stream, Role::Server, socket_type, options).await
}

async fn handshake_live(
    stream: TcpStream,
    role: Role,
    socket_type: SocketType,
    options: &Options,
) -> Result<LiveConnection> {
    stream.set_nodelay(true)?;
    let config = ConnectionConfig::new(role, socket_type).identity(options.identity.clone());
    let mut live = LiveConnection::new(stream, Connection::new(config));
    live.peer_identity =
        tokio::time::timeout(options.handshake_timeout, finish_handshake(&mut live))
            .await
            .map_err(|_| timed_out("exclusive socket handshake timeout"))??;
    Ok(live)
}

pub(super) async fn finish_handshake(live: &mut LiveConnection) -> Result<Bytes> {
    let mut peer_identity = None;
    while !live.connection.is_ready() {
        flush_live(live).await?;
        if live.connection.is_ready() {
            break;
        }
        read_live_once(live).await?;
        while let Some(event) = live.connection.poll_event() {
            if let ProtoEvent::HandshakeSucceeded {
                peer_properties, ..
            } = event
            {
                peer_identity = Some(
                    peer_properties
                        .identity
                        .clone()
                        .unwrap_or_else(|| generated_identity(0)),
                );
                break;
            }
        }
    }
    flush_and_settle(live).await?;
    Ok(peer_identity.unwrap_or_else(|| generated_identity(0)))
}

pub(super) async fn recv_live(
    live: &mut LiveConnection,
    heartbeat: HeartbeatPlan,
) -> Result<Message> {
    loop {
        if let Some(message) = live.connection.poll_message() {
            return Ok(message);
        }
        if !live.connection.accepts_input() {
            flush_and_settle(live).await?;
            continue;
        }

        let ping_deadline = heartbeat
            .interval
            .filter(|_| live.connection.peer_minor() >= 1)
            // An interval past the clock range never comes due.
            .and_then(|interval| live.last_ping.checked_add(interval));
        let heartbeat_deadline = live.silence_deadline();
        tokio::select! {
            biased;
            result = read_live_once(live) => {
                result?;
                flush_and_settle(live).await?;
            }
            () = sleep_until_opt(heartbeat_deadline), if heartbeat_deadline.is_some() => {
                return Err(Error::Timeout);
            }
            () = sleep_until_opt(ping_deadline), if ping_deadline.is_some() => {
                queue_ping(live, heartbeat, Instant::now())?;
                flush_live(live).await?;
            }
        }
    }
}

pub(super) fn queue_ping(
    live: &mut LiveConnection,
    heartbeat: HeartbeatPlan,
    now: Instant,
) -> Result<()> {
    let timeout = heartbeat
        .timeout
        .expect("heartbeat interval supplies timeout");
    live.ping_sequence = live.ping_sequence.wrapping_add(1);
    let context = Bytes::copy_from_slice(&live.ping_sequence.to_le_bytes());
    live.connection.send_command(&Command::Ping {
        ttl_deciseconds: heartbeat.ttl_deciseconds,
        context,
    })?;
    live.last_ping = now;
    if live.heartbeat_deadline.is_none() {
        live.heartbeat_deadline = now.checked_add(timeout);
    }
    Ok(())
}

fn note_inbound_traffic(live: &mut LiveConnection) {
    live.last_input = Instant::now();
    live.heartbeat_deadline = None;
}

pub(super) async fn read_live_once(live: &mut LiveConnection) -> Result<()> {
    let n = live.stream.read_buf(&mut live.read_buf).await?;
    if n == 0 {
        return Err(Error::Closed);
    }
    note_inbound_traffic(live);
    live.connection.handle_input(live.read_buf.split().freeze())
}

pub(super) fn drain_ready_input(live: &mut LiveConnection) -> Result<()> {
    // Paused input resumes after the caller writes the reply backlog.
    while live.connection.accepts_input() {
        match live.stream.try_read_buf(&mut live.read_buf) {
            Ok(0) => return Err(Error::Closed),
            Ok(_) => {
                note_inbound_traffic(live);
                live.connection
                    .handle_input(live.read_buf.split().freeze())?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(Error::Io(error)),
        }
    }
    Ok(())
}

pub(super) async fn flush_live(live: &mut LiveConnection) -> Result<()> {
    // Unwritten bytes stay queued in the codec, so cancellation is safe here.
    while live.connection.has_pending_transmit() {
        let chunks = live.connection.transmit_chunks_capped(64);
        let n = check_written(live.stream.write_vectored(&chunks).await?)?;
        drop(chunks);
        live.connection.advance_transmit(n);
    }
    Ok(())
}

/// Write queued output and resume input the core paused behind automatic
/// replies. Returns once the connection accepts more wire input.
pub(super) async fn flush_and_settle(live: &mut LiveConnection) -> Result<()> {
    loop {
        flush_live(live).await?;
        if live.connection.accepts_input() {
            return Ok(());
        }
        live.connection.resume_input()?;
    }
}

pub(super) async fn run_timeout<T>(
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
