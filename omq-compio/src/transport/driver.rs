//! Shared connection driver for stream transports (compio).
//!
//! Each connection spawns two tasks:
//!  * a **read task** that always polls the buffer-ownership read
//!    to completion (mid-flight cancellation of completion-based
//!    reads is fragile; doing it from a separate task sidesteps
//!    the issue) and forwards bytes through a flume channel;
//!  * a **driver task** that owns the codec, drains events, flushes
//!    pending writes via gather I/O (`write_vectored`), and selects
//!    between the read channel, the user inbox, the pre-handshake
//!    deadline, and the post-handshake heartbeat tick.
//!
//! Generic over any `Splittable` stream whose halves implement
//! `AsyncRead` + `AsyncWrite`. TCP and IPC each provide bind/connect
//! glue and call `run_connection`.
//!
//! Note: compio's `Vec<bytes::Bytes>` directly implements
//! `IoVectoredBuf` (via the `bytes` feature on compio-buf), so the
//! codec's owned chunks go straight into `writer.write_vectored` -
//! no unsafe iovec adapter, no manual `libc::iovec` construction.

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::io::AsyncWrite;
use compio::BufResult;
use flume::{Receiver, Sender};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::MessageTransform;
use omq_proto::proto::{Command, Event, SocketType};
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{
    MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo,
};
use crate::transport::inproc::{InprocFrame, InprocPeerSnapshot};

/// Per-flush byte cap. Once a single drain has buffered this many
/// bytes we stop pulling more from the inbox and let writev flush.
/// 1 MiB folds large messages into bigger writev calls without
/// outgrowing typical kernel TCP send buffers. Smaller caps (e.g.
/// 256 KiB) under-utilise writev for 32 KiB+ messages and let the
/// per-syscall overhead dominate; larger caps add latency without
/// extra throughput once the kernel send buffer is the bottleneck.
/// Override at runtime via `OMQ_BATCH_BYTES`.
fn max_batch_bytes() -> usize {
    use std::sync::OnceLock;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("OMQ_BATCH_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024 * 1024)
    })
}

/// Sleep until `deadline`, or hang forever when `None`. Lets the
/// driver loop unconditionally include the timeout / heartbeat
/// branches in its `select_biased!` and disable them by clearing
/// the deadline (rather than restructuring the select).
async fn maybe_sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(t) => compio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

#[derive(Debug)]
pub enum DriverCommand {
    SendMessage(Message),
    SendCommand(Command),
    Close,
}

/// Per-connection context: monitor publisher + per-peer subscription
/// set. Carried by the driver so it can publish HandshakeSucceeded /
/// PeerCommand events with the correct peer/endpoint/connection_id,
/// drive PUB-side fan-out filtering off the peer's
/// SUBSCRIBE / CANCEL stream, and publish Disconnected on exit.
#[derive(Clone, Debug)]
pub(crate) struct MonitorCtx {
    pub monitor: MonitorPublisher,
    pub endpoint: Endpoint,
    pub connection_id: u64,
    pub peer_info: Arc<RwLock<Option<PeerInfo>>>,
    pub peer_address: Option<std::net::SocketAddr>,
    /// PUB-side fan-out filter for this peer. The driver applies
    /// SUBSCRIBE / CANCEL to it as they arrive over the wire so the
    /// socket layer's send-time filter has up-to-date state. `None`
    /// for non-pub-side socket types.
    pub peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
}

/// Drive one connection through the ZMTP codec. Generic over the
/// concrete owned read/write halves so TCP, IPC, and any future
/// stream transport reuse this body.
///
/// `shared_msg_rx` is the per-socket round-robin queue (PUSH /
/// DEALER / REQ / PAIR / REP). When provided, the driver races
/// `recv_async` on it alongside the per-peer inbox - every driver
/// for the socket is racing the same queue, so whichever flushes
/// fastest absorbs more work (work-stealing). `None` for
/// per-peer-routing socket types (PUB / XPUB / RADIO / ROUTER /
/// XSUB).
pub(crate) async fn run_connection<R, W>(
    mut reader: R,
    mut writer: W,
    role: Role,
    socket_type: SocketType,
    options: Options,
    inbox: Receiver<DriverCommand>,
    shared_msg_rx: Option<Receiver<Message>>,
    peer_in_tx: Sender<InprocFrame>,
    peer_snapshot_tx: Sender<InprocPeerSnapshot>,
    monitor_ctx: Option<MonitorCtx>,
    mut transform: Option<MessageTransform>,
) -> Result<()>
where
    R: compio::io::AsyncRead + 'static,
    W: AsyncWrite + 'static,
{
    let identity = options.identity.clone();
    let handshake_timeout = options.handshake_timeout;
    let hb_interval = options.heartbeat_interval;
    let hb_timeout = options
        .heartbeat_timeout
        .or(options.heartbeat_interval)
        .unwrap_or(Duration::MAX);
    let hb_ttl_deciseconds = options
        .heartbeat_ttl
        .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
        .unwrap_or(0);

    let mut cfg = ConnectionConfig::new(role, socket_type)
        .identity(identity)
        .mechanism(options.mechanism.to_setup());
    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    let mut codec = Connection::new(cfg);

    // Dedicated read task: continuously polls `read` and ferries
    // chunks via flume so the driver loop never has to cancel an
    // in-flight completion-based read.
    //
    // Sized at 64 KiB so a 32 KiB ZMTP message lands in a single read
    // (kernel TCP loopback delivers up to ~64 KiB per read on a fresh
    // buffer); historical 8 KiB caused 4 syscalls per 32 KiB message.
    // We move the filled `Vec<u8>` into the channel rather than cloning
    // it - the previous `buf.clone()` was an alloc + memcpy of the
    // entire read on the hot path.
    const READ_BUF_BYTES: usize = 64 * 1024;
    let (bytes_tx, bytes_rx) = flume::bounded::<Vec<u8>>(4);
    compio::runtime::spawn(async move {
        let mut buf = Vec::with_capacity(READ_BUF_BYTES);
        loop {
            // compio's read returns BufResult<usize, T>. We give it the
            // owned buffer; on success we ship the filled buffer down
            // the channel (move, not clone) and allocate a fresh one
            // for the next iteration. The system allocator caches the
            // freed buffer so subsequent allocs typically reuse pages.
            let BufResult(res, filled) = reader.read(buf).await;
            match res {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let mut chunk = filled;
                    chunk.truncate(n);
                    if bytes_tx.send_async(chunk).await.is_err() {
                        break;
                    }
                    buf = Vec::with_capacity(READ_BUF_BYTES);
                }
            }
        }
    })
    .detach();

    let mut handshake_done = false;
    let mut pending_cmds: VecDeque<DriverCommand> = VecDeque::new();
    let mut deadline: Option<Instant> = handshake_timeout.map(|t| Instant::now() + t);
    let mut last_input = Instant::now();
    let mut hb_next: Option<Instant> = None;
    // Set when we receive `DriverCommand::Close`. We don't bail
    // immediately; we let the codec drain pending_cmds (post-
    // handshake), flush every transmit chunk to the wire, and then
    // exit. Socket::close caps the wall-clock budget, so a stuck
    // peer gets force-cancelled there.
    let mut closing = false;
    // Set once `shared_msg_rx` has returned None - the socket's
    // shared send queue closed (the socket is on its way down).
    // We stop selecting on it but keep running until pending writes
    // flush; the per-peer inbox still carries the eventual Close.
    let mut shared_closed = false;
    // The peer's identity (their READY property), captured at
    // handshake. Empty until then. Tag each inbound Message with it
    // so identity-routing sockets (ROUTER) can recover the source.
    let mut peer_identity = bytes::Bytes::new();

    loop {
        // Close path: once the user has asked to close AND the
        // handshake completed AND every pending command has been
        // encoded AND the codec has nothing left to write, we exit
        // cleanly. Pre-handshake closes wait here for the handshake
        // (or its own timeout); a stuck peer is bounded by
        // Socket::close's wall-clock budget.
        if closing
            && handshake_done
            && pending_cmds.is_empty()
            && !codec.has_pending_transmit()
        {
            return Ok(());
        }

        // 1) Drain parsed events.
        while let Some(ev) = codec.poll_event() {
            match ev {
                Event::HandshakeSucceeded { peer_minor, peer_properties } => {
                    if !handshake_done {
                        handshake_done = true;
                        deadline = None;
                        if let Some(iv) = hb_interval {
                            hb_next = Some(Instant::now() + iv);
                        }
                        peer_identity =
                            peer_properties.identity.clone().unwrap_or_default();
                        let snap = InprocPeerSnapshot {
                            socket_type: peer_properties
                                .socket_type
                                .unwrap_or(SocketType::Pair),
                            identity: peer_identity.clone(),
                        };
                        let _ = peer_snapshot_tx.send(snap);
                        if let Some(ctx) = &monitor_ctx {
                            let info = PeerInfo {
                                connection_id: ctx.connection_id,
                                peer_address: ctx.peer_address,
                                peer_identity: peer_properties.identity.clone(),
                                peer_properties: peer_properties.clone(),
                                zmtp_version: (3, peer_minor),
                            };
                            *ctx.peer_info.write().expect("peer_info lock") =
                                Some(info.clone());
                            ctx.monitor.publish(MonitorEvent::HandshakeSucceeded {
                                endpoint: ctx.endpoint.clone(),
                                peer: info,
                            });
                        }
                        while let Some(cmd) = pending_cmds.pop_front() {
                            match cmd {
                                DriverCommand::SendMessage(m) => {
                                    if let Some(t) = transform.as_mut() {
                                        for wire in t.encode(&m)? {
                                            codec.send_message(&wire)?;
                                        }
                                    } else {
                                        codec.send_message(&m)?;
                                    }
                                }
                                DriverCommand::SendCommand(c) => codec.send_command(&c)?,
                                // Defer; exit happens at the top of
                                // the loop once pending writes flush.
                                DriverCommand::Close => closing = true,
                            }
                        }
                    }
                }
                Event::Message(m) => {
                    // Compression transport: decode the wire payload
                    // back to plaintext before delivery. `None` means
                    // the wire message was a dict shipment consumed
                    // at the transport layer - don't surface it.
                    let m = if let Some(t) = transform.as_mut() {
                        match t.decode(m)? {
                            Some(plain) => plain,
                            None => continue,
                        }
                    } else {
                        m
                    };
                    let frame = InprocFrame::message_from(peer_identity.clone(), m);
                    if peer_in_tx.send_async(frame).await.is_err() {
                        return Ok(());
                    }
                }
                Event::Command(c) => {
                    match c {
                        Command::Subscribe(prefix) => {
                            if let Some(ctx) = &monitor_ctx {
                                if let Some(set) = &ctx.peer_sub {
                                    set.write()
                                        .expect("peer_sub lock")
                                        .add(prefix.clone());
                                }
                            }
                            // XPUB surfaces the SUBSCRIBE as a recv-side
                            // message (`\x01<topic>`). Forward via the
                            // shared in_tx and let the socket layer's
                            // recv() do the encoding so the per-socket
                            // logic sits in one place.
                            if matches!(socket_type, SocketType::XPub) {
                                if peer_in_tx
                                    .send_async(InprocFrame::Command(
                                        Command::Subscribe(prefix),
                                    ))
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                        }
                        Command::Cancel(prefix) => {
                            if let Some(ctx) = &monitor_ctx {
                                if let Some(set) = &ctx.peer_sub {
                                    set.write()
                                        .expect("peer_sub lock")
                                        .remove(&prefix);
                                }
                            }
                            if matches!(socket_type, SocketType::XPub) {
                                if peer_in_tx
                                    .send_async(InprocFrame::Command(
                                        Command::Cancel(prefix),
                                    ))
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                        }
                        Command::Error { reason } => {
                            if let Some(ctx) = &monitor_ctx {
                                if let Some(info) = ctx
                                    .peer_info
                                    .read()
                                    .expect("peer_info lock")
                                    .clone()
                                {
                                    ctx.monitor.publish(MonitorEvent::PeerCommand {
                                        endpoint: ctx.endpoint.clone(),
                                        peer: info,
                                        command: PeerCommandKind::Error { reason },
                                    });
                                }
                            }
                        }
                        Command::Unknown { name, body } => {
                            if let Some(ctx) = &monitor_ctx {
                                if let Some(info) = ctx
                                    .peer_info
                                    .read()
                                    .expect("peer_info lock")
                                    .clone()
                                {
                                    ctx.monitor.publish(MonitorEvent::PeerCommand {
                                        endpoint: ctx.endpoint.clone(),
                                        peer: info,
                                        command: PeerCommandKind::Unknown { name, body },
                                    });
                                }
                            }
                        }
                        other => {
                            if peer_in_tx
                                .send_async(InprocFrame::Command(other))
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        // 2) Flush pending outbound via gather I/O. compio's
        //    `IoVectoredBuf` impl on `Vec<bytes::Bytes>` lets us hand
        //    the codec's owned chunks to `write_vectored` directly -
        //    no unsafe iovec adapter.
        if codec.has_pending_transmit() {
            let mut chunks = codec.clone_transmit_chunks();
            if chunks.len() > 1024 {
                chunks.truncate(1024);
            }
            if !chunks.is_empty() {
                let BufResult(res, _bufs) = writer.write_vectored(chunks).await;
                let written = res.map_err(Error::Io)?;
                if written == 0 {
                    return Ok(());
                }
                codec.advance_transmit(written);
                continue;
            }
        }

        // 3) Race a read against an inbox command, plus the
        //    pre-handshake deadline and the post-handshake heartbeat
        //    tick. When the socket has a shared round-robin queue,
        //    also race `shared_msg_rx`: every peer driver receives
        //    on it, so whichever flushes its codec fastest grabs
        //    the next message - work-stealing without an
        //    intermediate pump task.
        use futures::FutureExt;
        let bytes_fut = bytes_rx.recv_async();
        let cmd_fut = inbox.recv_async();
        let timeout_fut = maybe_sleep_until(deadline);
        let hb_fut = maybe_sleep_until(hb_next);
        let shared_active = shared_msg_rx.as_ref().filter(|_| !shared_closed);
        let shared_fut = async {
            match shared_active {
                Some(rx) => rx.recv_async().await.ok(),
                None => std::future::pending::<Option<Message>>().await,
            }
        };
        futures::pin_mut!(bytes_fut);
        futures::pin_mut!(cmd_fut);
        futures::pin_mut!(timeout_fut);
        futures::pin_mut!(hb_fut);
        futures::pin_mut!(shared_fut);
        let cap = max_batch_bytes();
        futures::select_biased! {
            () = timeout_fut.fuse() => {
                return Err(Error::HandshakeFailed("handshake timeout".into()));
            }
            () = hb_fut.fuse() => {
                if last_input.elapsed() > hb_timeout {
                    return Err(Error::Timeout);
                }
                let ping = Command::Ping {
                    ttl_deciseconds: hb_ttl_deciseconds,
                    context: Bytes::new(),
                };
                let _ = codec.send_command(&ping);
                if let Some(iv) = hb_interval {
                    hb_next = Some(Instant::now() + iv);
                }
            }
            bytes = bytes_fut.fuse() => {
                let bytes = match bytes {
                    Ok(b) => b,
                    Err(_) => return Ok(()),
                };
                last_input = Instant::now();
                codec.handle_input(&bytes)?;
            }
            cmd = cmd_fut.fuse() => {
                let mut next = match cmd {
                    Ok(c) => Some(c),
                    Err(_) => return Ok(()),
                };
                while let Some(cmd) = next.take() {
                    if !handshake_done {
                        pending_cmds.push_back(cmd);
                    } else {
                        match cmd {
                            DriverCommand::SendMessage(m) => {
                                if let Some(t) = transform.as_mut() {
                                    for wire in t.encode(&m)? {
                                        codec.send_message(&wire)?;
                                    }
                                } else {
                                    codec.send_message(&m)?;
                                }
                            }
                            DriverCommand::SendCommand(c) => codec.send_command(&c)?,
                            DriverCommand::Close => closing = true,
                        }
                    }
                    if codec.pending_transmit_size() >= cap { break; }
                    if let Ok(c) = inbox.try_recv() {
                        next = Some(c);
                    }
                }
            }
            msg = shared_fut.fuse() => {
                // Pre-handshake msgs queue up in pending_cmds (drained
                // when handshake completes). Post-handshake we drain
                // the shared queue greedily until codec hits the batch
                // cap - fewer wakes, more amortization in writev.
                let mut next = match msg {
                    Some(m) => Some(m),
                    None => {
                        // Queue closed (socket closing) - stop
                        // selecting on it but keep running until
                        // pending writes flush + Close lands.
                        shared_closed = true;
                        continue;
                    }
                };
                let shared = shared_msg_rx
                    .as_ref()
                    .expect("shared_fut only ready when rx is Some");
                while let Some(m) = next.take() {
                    if !handshake_done {
                        pending_cmds.push_back(DriverCommand::SendMessage(m));
                    } else if let Some(t) = transform.as_mut() {
                        for wire in t.encode(&m)? {
                            codec.send_message(&wire)?;
                        }
                    } else {
                        codec.send_message(&m)?;
                    }
                    if codec.pending_transmit_size() >= cap { break; }
                    if let Ok(m) = shared.try_recv() {
                        next = Some(m);
                    }
                }
            }
        }
    }
}
