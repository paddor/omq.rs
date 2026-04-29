//! Shared connection driver for stream transports (compio).
//!
//! One driver task per connection. Co-owns the codec, transform,
//! writer and reader through [`PeerIo`] behind an async [`Mutex`].
//! The driver `select_biased!`s between `PollFd::read_ready` (kernel
//! readability), the per-peer inbox, the shared work-stealing queue
//! (round-robin types), the pre-handshake deadline, the heartbeat
//! tick, and the recv-direct claim/release signals.
//!
//! Lock discipline: the [`PeerIo`] mutex is per-op only — never held
//! across an await — so the direct send/recv fast paths can grab it
//! between driver iterations.
//!
//! Generic over any `Splittable` stream whose halves implement
//! `AsyncRead` + `AsyncWrite`. TCP and IPC each provide bind/connect
//! glue and call `run_connection`.
//!
//! [`Mutex`]: async_lock::Mutex

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::{Receiver, Sender};
use smallvec::SmallVec;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::MessageTransform;
use omq_proto::proto::{Command, Event, SocketType};
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{
    MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo,
};
use crate::socket::DirectIoState;
use crate::transport::inproc::{InprocFrame, InprocPeerSnapshot};
use crate::transport::peer_io::{PeerIo, SharedPeerIo, WireReader, WireWriter};

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
    /// RADIO-side per-peer joined-group set. Updated as JOIN / LEAVE
    /// commands arrive over the wire from the connected DISH so
    /// `send_radio` can filter per peer. `None` for non-radio types.
    pub peer_groups: Option<Arc<RwLock<std::collections::HashSet<bytes::Bytes>>>>,
}

/// Events drained from the codec under the [`PeerIo`] lock that need
/// post-processing OUTSIDE the lock (because the post-processing
/// awaits on the per-socket `peer_in_tx` flume channel, which we
/// must not hold across).
enum Drained {
    Handshake {
        peer_minor: u8,
        peer_properties: Arc<PeerProperties>,
    },
    Msg(Message),
    Cmd(Command),
}

/// Build a fresh [`Connection`] for this driver from the negotiated
/// options + role. Factored out only so the codec construction is in
/// one place.
fn make_codec(role: Role, socket_type: SocketType, options: &Options) -> Connection {
    let mut cfg = ConnectionConfig::new(role, socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.to_setup());
    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    Connection::new(cfg)
}

/// Apply a SUBSCRIBE/CANCEL coming from a peer: update the per-peer
/// subscription set and, on XPUB, surface the command to the user
/// recv stream as a `\x01<prefix>` / `\x00<prefix>` message.
async fn handle_sub_cmd(
    socket_type: SocketType,
    monitor_ctx: Option<&MonitorCtx>,
    peer_in_tx: &flume::Sender<InprocFrame>,
    cmd: Command,
) -> std::io::Result<()> {
    let prefix = match &cmd {
        Command::Subscribe(p) | Command::Cancel(p) => p.clone(),
        _ => return Ok(()),
    };
    if let Some(ctx) = monitor_ctx {
        if let Some(set) = &ctx.peer_sub {
            let mut s = set.write().expect("peer_sub lock");
            match cmd {
                Command::Subscribe(_) => s.add(prefix.clone()),
                Command::Cancel(_) => s.remove(&prefix),
                _ => {}
            }
        }
    }
    if matches!(socket_type, SocketType::XPub) {
        // Surface to the XPUB user as a 0x01/0x00-prefixed message.
        // libzmq does the same — XPUB readers consume these to know
        // who subscribed.
        let _ = peer_in_tx.send_async(InprocFrame::Command(cmd)).await;
    }
    Ok(())
}

/// Build the [`SharedPeerIo`] handed to the driver and to the direct
/// send/recv fast paths. Constructs the codec; reader/writer halves
/// arrive wrapped in concrete [`WireReader`] / [`WireWriter`] enums so
/// per-call dispatch is a static `match` and never heap-allocates a
/// future.
pub(crate) fn build_peer_io(
    role: Role,
    socket_type: SocketType,
    options: &Options,
    reader: WireReader,
    writer: WireWriter,
    transform: Option<MessageTransform>,
) -> SharedPeerIo {
    let codec = make_codec(role, socket_type, options);
    Arc::new(async_lock::Mutex::new(PeerIo {
        codec,
        transform,
        writer,
        reader,
        handshake_done: false,
    }))
}

/// Drive one connection through the ZMTP codec. The reader, writer,
/// codec, and transform all live inside [`SharedPeerIo`] so
/// `Socket::send`'s direct-write fast path and `Socket::recv`'s
/// direct-read fast path can drive them too.
///
/// `shared_msg_rx` is the per-socket round-robin queue (PUSH /
/// DEALER / REQ / PAIR / REP). When provided, the driver races
/// `recv_async` on it alongside the per-peer inbox - every driver
/// for the socket is racing the same queue, so whichever flushes
/// fastest absorbs more work (work-stealing). `None` for
/// per-peer-routing socket types (PUB / XPUB / RADIO / ROUTER /
/// XSUB).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_connection(
    state: Arc<DirectIoState>,
    socket_type: SocketType,
    options: Options,
    inbox: Receiver<DriverCommand>,
    shared_msg_rx: Option<Receiver<Message>>,
    peer_in_tx: Sender<InprocFrame>,
    peer_snapshot_tx: Sender<InprocPeerSnapshot>,
    monitor_ctx: Option<MonitorCtx>,
) -> Result<()> {
    let peer_io: SharedPeerIo = state.peer_io.clone();
    let poll_fd = state.poll_fd.clone();
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

    // Read buffer reused across iterations. Sized at 64 KiB so a
    // 32 KiB ZMTP message lands in a single read (kernel loopback
    // delivers up to ~64 KiB per read on a fresh buffer); historical
    // 8 KiB caused 4 syscalls per 32 KiB message. The buffer is
    // handed to compio's read by move and returned via `BufResult`,
    // so no copy on the hot path.
    const READ_BUF_BYTES: usize = 64 * 1024;
    let mut read_buf: Vec<u8> = Vec::with_capacity(READ_BUF_BYTES);

    let mut pending_cmds: VecDeque<DriverCommand> = VecDeque::new();
    let mut deadline: Option<Instant> = handshake_timeout.map(|t| Instant::now() + t);
    state
        .last_input_nanos
        .store(state.hb_epoch.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        if closing {
            let io = peer_io.lock().await;
            if io.handshake_done
                && pending_cmds.is_empty()
                && !io.codec.has_pending_transmit()
            {
                return Ok(());
            }
        }

        // 1) Drain parsed events. Lock briefly, capture everything
        //    we'll dispatch outside; drain pending_cmds into the
        //    codec the moment the handshake completes (so writes
        //    can land in step 3 of the same iteration).
        let drained: SmallVec<[Drained; 8]> = {
            let mut io = peer_io.lock().await;
            let mut out: SmallVec<[Drained; 8]> = SmallVec::new();
            while let Some(ev) = io.codec.poll_event() {
                match ev {
                    Event::HandshakeSucceeded { peer_minor, peer_properties } => {
                        if !io.handshake_done {
                            io.handshake_done = true;
                            deadline = None;
                            if let Some(iv) = hb_interval {
                                hb_next = Some(Instant::now() + iv);
                            }
                            peer_identity = peer_properties
                                .identity
                                .clone()
                                .unwrap_or_default();
                            // Drain pre-handshake commands into the
                            // codec now that we're allowed to send.
                            while let Some(cmd) = pending_cmds.pop_front() {
                                match cmd {
                                    DriverCommand::SendMessage(m) => {
                                        if let Some(t) = io.transform.as_mut() {
                                            for wire in t.encode(&m)? {
                                                io.codec.send_message(&wire)?;
                                            }
                                        } else {
                                            io.codec.send_message(&m)?;
                                        }
                                    }
                                    DriverCommand::SendCommand(c) => {
                                        io.codec.send_command(&c)?;
                                    }
                                    DriverCommand::Close => closing = true,
                                }
                            }
                            out.push(Drained::Handshake {
                                peer_minor,
                                peer_properties,
                            });
                        }
                    }
                    Event::Message(m) => {
                        // Compression transport: decode the wire
                        // payload back to plaintext before delivery.
                        // `None` means the wire message was a dict
                        // shipment consumed at the transport layer -
                        // don't surface it.
                        let m = if let Some(t) = io.transform.as_mut() {
                            match t.decode(m)? {
                                Some(plain) => plain,
                                None => continue,
                            }
                        } else {
                            m
                        };
                        out.push(Drained::Msg(m));
                    }
                    Event::Command(c) => out.push(Drained::Cmd(c)),
                }
            }
            out
        };

        // 2) Dispatch drained events outside the lock.
        for de in drained {
            match de {
                Drained::Handshake { peer_minor, peer_properties } => {
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
                }
                Drained::Msg(m) => {
                    // PUB/XPUB also accept the legacy ZMTP 3.0 form of
                    // SUBSCRIBE/CANCEL: a single-frame message whose
                    // body starts with 0x01 (subscribe) or 0x00
                    // (cancel). pyzmq XSUB and libzmq's older paths
                    // emit these instead of the 3.1 wire commands.
                    if matches!(socket_type, SocketType::Pub | SocketType::XPub)
                        && m.parts().len() == 1
                    {
                        let body = m.parts()[0].coalesce();
                        if let Some((tag, prefix)) = body.split_first() {
                            let cmd = match tag {
                                0x01 => Some(Command::Subscribe(
                                    bytes::Bytes::copy_from_slice(prefix),
                                )),
                                0x00 => Some(Command::Cancel(
                                    bytes::Bytes::copy_from_slice(prefix),
                                )),
                                _ => None,
                            };
                            if let Some(c) = cmd {
                                handle_sub_cmd(
                                    socket_type,
                                    monitor_ctx.as_ref(),
                                    &peer_in_tx,
                                    c,
                                )
                                .await?;
                                continue;
                            }
                        }
                    }
                    let frame = InprocFrame::message_from(peer_identity.clone(), m);
                    if peer_in_tx.send_async(frame).await.is_err() {
                        return Ok(());
                    }
                }
                Drained::Cmd(c) => match c {
                    Command::Subscribe(_) | Command::Cancel(_) => {
                        handle_sub_cmd(
                            socket_type,
                            monitor_ctx.as_ref(),
                            &peer_in_tx,
                            c,
                        )
                        .await?;
                    }
                    Command::Join(group) => {
                        if let Some(ctx) = &monitor_ctx {
                            if let Some(set) = &ctx.peer_groups {
                                set.write()
                                    .expect("peer_groups lock")
                                    .insert(group);
                            }
                        }
                    }
                    Command::Leave(group) => {
                        if let Some(ctx) = &monitor_ctx {
                            if let Some(set) = &ctx.peer_groups {
                                set.write()
                                    .expect("peer_groups lock")
                                    .remove(&group);
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
                },
            }
        }

        // 3) Flush pending outbound via gather I/O. Hold the lock
        //    across the write so a concurrent Socket::send fast
        //    path can't take the same chunks before we
        //    `advance_transmit`. compio's `IoVectoredBuf` impl on
        //    `Vec<bytes::Bytes>` lets the codec's owned chunks go
        //    directly to `write_vectored` - no unsafe iovec
        //    adapter.
        let wrote_something = {
            let mut io = peer_io.lock().await;
            if io.codec.has_pending_transmit() {
                let mut chunks = io.codec.clone_transmit_chunks();
                if chunks.len() > 1024 {
                    chunks.truncate(1024);
                }
                if !chunks.is_empty() {
                    let written = io
                        .writer
                        .write_vectored(chunks)
                        .await
                        .map_err(Error::Io)?;
                    if written == 0 {
                        return Ok(());
                    }
                    io.codec.advance_transmit(written);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if wrote_something {
            continue;
        }

        // 4) Race readability on the wire against an inbox command,
        //    plus the pre-handshake deadline and post-handshake
        //    heartbeat tick. When the socket has a shared round-robin
        //    queue, also race `shared_msg_rx`: every peer driver
        //    receives on it, so whichever flushes its codec fastest
        //    grabs the next message - work-stealing without an
        //    intermediate pump task.
        //
        //    The `peer_io` lock is NEVER held across this select - the
        //    fast-path send caller is free to grab the lock between
        //    iterations.
        //
        //    `PollFd::read_ready` is cancellation-safe (the underlying
        //    io_uring `PollOnce` SQE can be cancelled cleanly), so we
        //    can drop it when another arm wins the race. Once it
        //    fires, we do an inline `reader.read(buf).await` - kernel
        //    data is already queued, the SQE completes immediately,
        //    and we never abandon a buffer-owning read mid-flight.
        use futures::FutureExt;
        // Recv-direct gate: when a `recv()` caller has claimed the
        // read path (`recv_claim == 1`), the driver must NOT race the
        // FD readiness or it would steal bytes out from under the
        // claim. Park on `recv_state_changed` instead - the claim
        // is released via a `notify(usize::MAX)` on Drop, which
        // wakes us so we re-evaluate.
        //
        // EOF / fatal-read signal: when the recv direct path
        // observes EOF or a fatal read error, it notifies
        // `eof_signal` so we exit instead of looping.
        let recv_active = state.recv_claim.load(Ordering::Acquire) == 1;
        // Avoid per-iteration `event_listener::Listener` allocations
        // when the recv claim isn't held. Most loop iterations on a
        // throughput-bound PULL run with `recv_active == false` (the
        // recv direct path peeks `in_rx` and falls back when the
        // driver has buffered messages); creating + dropping two
        // unused Listeners per iter would dominate the small-message
        // hot path.
        let read_ready_fut = async {
            if recv_active {
                state.recv_state_changed.listen().await;
                Err(())
            } else {
                poll_fd.read_ready().await.map_err(|_| ())
            }
        };
        let eof_fut = async {
            if recv_active {
                state.eof_signal.listen().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
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
        futures::pin_mut!(read_ready_fut);
        futures::pin_mut!(eof_fut);
        futures::pin_mut!(cmd_fut);
        futures::pin_mut!(timeout_fut);
        futures::pin_mut!(hb_fut);
        futures::pin_mut!(shared_fut);
        let cap = max_batch_bytes();
        futures::select_biased! {
            () = eof_fut.fuse() => {
                // Recv direct path observed EOF / read error.
                return Ok(());
            }
            () = timeout_fut.fuse() => {
                return Err(Error::HandshakeFailed("handshake timeout".into()));
            }
            () = hb_fut.fuse() => {
                let now_nanos = state.hb_epoch.elapsed().as_nanos() as u64;
                let last_nanos = state
                    .last_input_nanos
                    .load(Ordering::Relaxed);
                let elapsed = Duration::from_nanos(now_nanos.saturating_sub(last_nanos));
                if elapsed > hb_timeout {
                    return Err(Error::Timeout);
                }
                let ping = Command::Ping {
                    ttl_deciseconds: hb_ttl_deciseconds,
                    context: Bytes::new(),
                };
                {
                    let mut io = peer_io.lock().await;
                    let _ = io.codec.send_command(&ping);
                }
                if let Some(iv) = hb_interval {
                    hb_next = Some(Instant::now() + iv);
                }
            }
            ready = read_ready_fut.fuse() => {
                if ready.is_err() {
                    // Either the read_ready FD probe failed (peer
                    // closed) or the recv claim flipped state. In
                    // the FD-error case, exit. In the
                    // claim-changed case, just loop and re-evaluate
                    // (recv may have released; we'll grab reads
                    // again).
                    if recv_active {
                        // Claim state flipped - reloop.
                    } else {
                        return Ok(());
                    }
                } else {
                    let buf = std::mem::replace(
                        &mut read_buf,
                        Vec::with_capacity(READ_BUF_BYTES),
                    );
                    let mut io = peer_io.lock().await;
                    // Re-check claim under lock. Subtle race: the
                    // recv direct caller may have flipped
                    // `recv_claim` to 1 between our `recv_active`
                    // snapshot and now. If so, we must NOT read -
                    // the recv side's own `read_ready` SQE has
                    // fired too, and reading here would steal the
                    // kernel data, hanging the recv future on its
                    // subsequent read SQE.
                    if state.recv_claim.load(Ordering::Acquire) == 1 {
                        drop(io);
                        read_buf = buf;
                    } else {
                        let (res, filled) = io.reader.read(buf).await;
                        let n = match res {
                            Ok(0) | Err(_) => return Ok(()),
                            Ok(n) => n,
                        };
                        state.last_input_nanos.store(
                            state.hb_epoch.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        io.codec.handle_input(&filled[..n])?;
                        drop(io);
                        read_buf = filled;
                        read_buf.clear();
                    }
                }
            }
            cmd = cmd_fut.fuse() => {
                let mut next = match cmd {
                    Ok(c) => Some(c),
                    Err(_) => return Ok(()),
                };
                while let Some(cmd) = next.take() {
                    let cap_reached = {
                        let mut io = peer_io.lock().await;
                        if !io.handshake_done {
                            pending_cmds.push_back(cmd);
                        } else {
                            match cmd {
                                DriverCommand::SendMessage(m) => {
                                    if let Some(t) = io.transform.as_mut() {
                                        for wire in t.encode(&m)? {
                                            io.codec.send_message(&wire)?;
                                        }
                                    } else {
                                        io.codec.send_message(&m)?;
                                    }
                                }
                                DriverCommand::SendCommand(c) => {
                                    io.codec.send_command(&c)?;
                                }
                                DriverCommand::Close => closing = true,
                            }
                        }
                        io.codec.pending_transmit_size() >= cap
                    };
                    if cap_reached { break; }
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
                    let cap_reached = {
                        let mut io = peer_io.lock().await;
                        if !io.handshake_done {
                            pending_cmds.push_back(DriverCommand::SendMessage(m));
                        } else if let Some(t) = io.transform.as_mut() {
                            for wire in t.encode(&m)? {
                                io.codec.send_message(&wire)?;
                            }
                        } else {
                            io.codec.send_message(&m)?;
                        }
                        io.codec.pending_transmit_size() >= cap
                    };
                    if cap_reached { break; }
                    if let Ok(m) = shared.try_recv() {
                        next = Some(m);
                    }
                }
            }
        }
    }
}
