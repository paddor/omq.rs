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
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use smallvec::SmallVec;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::MessageDecoder;
use omq_proto::proto::{Command, Event, SocketType};

use crate::socket::DirectIoState;
use crate::transport::dispatch::{Drained, MonitorCtx, SnapshotSink, dispatch_drained_events};
use crate::transport::inproc::InboundFrame;
use crate::transport::peer_io::{PeerIo, SharedPeerIo, WireReader};
use crate::transport::recv_stream::{StreamArmOutcome, pull_stream};

/// Per-flush byte cap. Once a single drain has buffered this many
/// bytes we stop pulling more from the inbox and let writev flush.
/// 1 MiB folds large messages into bigger writev calls without
/// outgrowing typical kernel TCP send buffers. Smaller caps (e.g.
/// 256 KiB) under-utilize writev for 32 KiB+ messages and let the
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

#[allow(clippy::struct_excessive_bools)]
struct DriverLoopState {
    closing: bool,
    deadline: Option<Instant>,
    hb_next: Option<Instant>,
    pending_cmds: VecDeque<DriverCommand>,
    codec_maybe_dirty: bool,
    codec_has_input: bool,
    shared_closed: bool,
    peer_identity: Bytes,
    drain_buf: Vec<Bytes>,
}

impl DriverLoopState {
    fn new(handshake_timeout: Option<Duration>) -> Self {
        Self {
            closing: false,
            deadline: handshake_timeout.map(|t| Instant::now() + t),
            hb_next: None,
            pending_cmds: VecDeque::new(),
            codec_maybe_dirty: true,
            codec_has_input: true,
            shared_closed: false,
            peer_identity: Bytes::new(),
            drain_buf: Vec::with_capacity(64),
        }
    }
}

#[derive(Debug)]
pub enum DriverCommand {
    SendMessage(Message),
    SendCommand(Command),
    Close,
}

fn generated_identity(connection_id: u64) -> bytes::Bytes {
    let mut buf = Vec::with_capacity(9);
    buf.push(0); // libzmq-style leading null marks "auto-generated"
    buf.extend_from_slice(&connection_id.to_be_bytes());
    bytes::Bytes::from(buf)
}

/// Build the [`SharedPeerIo`] handed to the driver and to the direct
/// send/recv fast paths. Constructs the codec; the reader half arrives
/// wrapped in a concrete [`WireReader`] enum so per-call dispatch is a
/// static `match`. The writer half is stored separately in
/// [`DirectIoState::writer`] so the codec lock can be released before
/// `write_vectored`, letting the fast-path sender encode while I/O is
/// in flight.
///
/// The encoder is stored separately in [`DirectIoState::encoder`]; only
/// the decoder lives here alongside the codec + reader.
fn make_codec(
    role: Role,
    socket_type: SocketType,
    options: &Options,
    #[cfg(feature = "ws")] ws_role: Option<omq_proto::proto::connection::WsRole>,
) -> Connection {
    let mut cfg = ConnectionConfig::new(role, socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.to_setup());
    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    #[cfg(feature = "ws")]
    if let Some(wr) = ws_role {
        cfg = cfg.ws_role(wr);
    }
    Connection::new(cfg)
}

pub(crate) fn build_peer_io(
    role: Role,
    socket_type: SocketType,
    options: &Options,
    reader: WireReader,
    decoder: Option<MessageDecoder>,
    #[cfg(feature = "ws")] ws_role: Option<omq_proto::proto::connection::WsRole>,
    #[cfg(feature = "ws")] leftover: Option<bytes::Bytes>,
) -> std::io::Result<(
    SharedPeerIo,
    Option<crate::transport::peer_io::CancellableRecvStream>,
)> {
    let recv_stream = if reader.supports_multishot() {
        Some(reader.build_recv_stream()?)
    } else {
        None
    };
    #[cfg_attr(not(feature = "ws"), allow(unused_mut))]
    let mut codec = make_codec(
        role,
        socket_type,
        options,
        #[cfg(feature = "ws")]
        ws_role,
    );
    #[cfg(feature = "ws")]
    if let Some(ref wr) = ws_role
        && let Some(leftover) = leftover
        && !leftover.is_empty()
    {
        let _ = codec.handle_input(leftover);
        let _ = wr; // suppress unused
    }
    #[allow(clippy::arc_with_non_send_sync)]
    let peer_io = Arc::new(std::sync::Mutex::new(PeerIo {
        codec,
        decoder,
        reader,
        handshake_done: false,
    }));
    Ok((peer_io, recv_stream))
}

/// Encode a user message through the appropriate path (transform /
/// crypto / plain) and return whether the batch cap was reached.
impl DriverLoopState {
    async fn encode_outbound_message(
        &mut self,
        state: &DirectIoState,
        m: &Message,
        cap: usize,
    ) -> Result<bool> {
        if state.has_transform {
            let mut enc = state.encoder.lock().await;
            let wires = enc
                .as_mut()
                .expect("has_transform but no encoder")
                .encode(m)?;
            drop(enc);
            let mut eq = state.encoded_queue.lock().expect("encoded_queue");
            let cr = eq.total_bytes() >= cap;
            for wire in &wires {
                if wire.byte_len() < crate::socket::FLAT_THRESHOLD {
                    eq.encode_and_push_flat(wire);
                } else {
                    eq.encode_and_push(wire);
                }
            }
            Ok(cr)
        } else if state.uses_crypto {
            let mut io = state.lock_io();
            io.codec.send_message(m)?;
            let cr = io.codec.pending_transmit_size() >= cap;
            drop(io);
            self.codec_maybe_dirty = true;
            Ok(cr)
        } else {
            let mut eq = state.encoded_queue.lock().expect("encoded_queue");
            let cr = eq.total_bytes() >= cap;
            #[cfg(feature = "ws")]
            if state.is_ws {
                eq.encode_and_push_flat_ws(m, state.ws_masked);
                return Ok(cr);
            }
            if m.byte_len() < crate::socket::FLAT_THRESHOLD {
                eq.encode_and_push_flat(m);
            } else {
                eq.encode_and_push(m);
            }
            Ok(cr)
        }
    }

    fn drain_pending_commands(&mut self, state: &DirectIoState, io: &mut PeerIo) -> Result<()> {
        while let Some(cmd) = self.pending_cmds.pop_front() {
            match cmd {
                DriverCommand::SendMessage(m) => {
                    if state.has_transform {
                        let mut enc = state
                            .encoder
                            .try_lock()
                            .expect("encoder uncontended during handshake drain");
                        let wires = enc
                            .as_mut()
                            .expect("has_transform but no encoder")
                            .encode(&m)?;
                        drop(enc);
                        let mut eq = state.encoded_queue.lock().expect("encoded_queue");
                        for wire in &wires {
                            if wire.byte_len() < crate::socket::FLAT_THRESHOLD {
                                eq.encode_and_push_flat(wire);
                            } else {
                                eq.encode_and_push(wire);
                            }
                        }
                    } else if state.uses_crypto {
                        io.codec.send_message(&m)?;
                    } else {
                        let mut eq = state.encoded_queue.lock().expect("encoded_queue");
                        #[cfg(feature = "ws")]
                        if state.is_ws {
                            eq.encode_and_push_flat_ws(&m, state.ws_masked);
                            continue;
                        }
                        if m.byte_len() < crate::socket::FLAT_THRESHOLD {
                            eq.encode_and_push_flat(&m);
                        } else {
                            eq.encode_and_push(&m);
                        }
                    }
                }
                DriverCommand::SendCommand(c) => {
                    io.codec.send_command(&c)?;
                }
                DriverCommand::Close => self.closing = true,
            }
        }
        Ok(())
    }

    fn process_handshake_succeeded(
        &mut self,
        state: &DirectIoState,
        io: &mut PeerIo,
        peer_properties: &Arc<PeerProperties>,
        monitor_ctx: Option<&MonitorCtx>,
        hb_interval: Option<Duration>,
    ) -> Result<()> {
        io.handshake_done = true;
        state.handshake_done.store(true, Ordering::Relaxed);
        self.deadline = None;
        if let Some(iv) = hb_interval {
            self.hb_next = Some(Instant::now() + iv);
        }
        self.peer_identity = peer_properties.identity.clone().unwrap_or_else(|| {
            monitor_ctx.map_or_else(Bytes::new, |ctx| generated_identity(ctx.connection_id))
        });
        self.drain_pending_commands(state, io)
    }
}

impl DriverLoopState {
    async fn drain_inbox(
        &mut self,
        first: DriverCommand,
        inbox: &Receiver<DriverCommand>,
        state: &DirectIoState,
        cap: usize,
    ) -> Result<()> {
        let mut next = Some(first);
        while let Some(cmd) = next.take() {
            let cap_reached = if state.handshake_done.load(Ordering::Relaxed) {
                match cmd {
                    DriverCommand::SendMessage(m) => {
                        self.encode_outbound_message(state, &m, cap).await?
                    }
                    DriverCommand::SendCommand(c) => {
                        let mut io = state.lock_io();
                        io.codec.send_command(&c)?;
                        let cr = io.codec.pending_transmit_size() >= cap;
                        drop(io);
                        self.codec_maybe_dirty = true;
                        cr
                    }
                    DriverCommand::Close => {
                        self.closing = true;
                        false
                    }
                }
            } else {
                self.pending_cmds.push_back(cmd);
                false
            };
            if cap_reached {
                break;
            }
            next = inbox.try_recv().ok();
        }
        Ok(())
    }

    #[cfg(not(feature = "priority"))]
    async fn drain_shared(
        &mut self,
        first: Message,
        shared: &crate::socket::shared_queue::SharedQueueReceiver,
        state: &DirectIoState,
        cap: usize,
    ) -> Result<()> {
        let mut next = Some(first);
        while let Some(m) = next.take() {
            let cap_reached = if state.handshake_done.load(Ordering::Relaxed) {
                self.encode_outbound_message(state, &m, cap).await?
            } else {
                self.pending_cmds.push_back(DriverCommand::SendMessage(m));
                false
            };
            if cap_reached {
                break;
            }
            next = shared.try_recv();
        }
        Ok(())
    }

    #[cfg(feature = "priority")]
    async fn drain_shared(
        &mut self,
        first: Message,
        shared: &Receiver<Message>,
        state: &DirectIoState,
        cap: usize,
    ) -> Result<()> {
        let mut next = Some(first);
        while let Some(m) = next.take() {
            let cap_reached = if state.handshake_done.load(Ordering::Relaxed) {
                self.encode_outbound_message(state, &m, cap).await?
            } else {
                self.pending_cmds.push_back(DriverCommand::SendMessage(m));
                false
            };
            if cap_reached {
                break;
            }
            next = shared.try_recv().ok();
        }
        Ok(())
    }
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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn run_connection(
    state: Arc<DirectIoState>,
    socket_type: SocketType,
    options: Options,
    inbox: Receiver<DriverCommand>,
    #[cfg(not(feature = "priority"))] shared_msg_rx: Option<
        crate::socket::shared_queue::SharedQueueReceiver,
    >,
    #[cfg(feature = "priority")] shared_msg_rx: Option<Receiver<Message>>,
    peer_in_tx: blume::Sender<InboundFrame>,
    snapshot_sink: Box<dyn SnapshotSink>,
    monitor_ctx: Option<MonitorCtx>,
) -> Result<()> {
    let peer_io: SharedPeerIo = state.peer_io.clone();
    let hb_interval = options.heartbeat_interval;
    let hb_timeout = options
        .heartbeat_timeout
        .or(options.heartbeat_interval)
        .unwrap_or(Duration::MAX);
    let hb_ttl_deciseconds = options
        .heartbeat_ttl
        .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
        .unwrap_or(0);
    let mut ls = DriverLoopState::new(options.handshake_timeout);
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    loop {
        use futures::FutureExt;
        // Clear the driver_in_select flag at the top of every iteration.
        // The flag is set again just before we park in select_biased!
        // so the fast-path sender can tell whether a transmit_ready
        // notification is worth issuing.
        state.driver_in_select.store(false, Ordering::Relaxed);

        // Close path: once the user has asked to close AND the
        // handshake completed AND every pending command has been
        // encoded AND the codec + encoded_queue have nothing left to
        // write, we exit cleanly. Pre-handshake closes wait here for
        // the handshake (or its own timeout); a stuck peer is bounded
        // by Socket::close's wall-clock budget.
        if ls.closing {
            let io = state.lock_io();
            let eq = state.encoded_queue.lock().expect("encoded_queue");
            if io.handshake_done
                && ls.pending_cmds.is_empty()
                && !io.codec.has_pending_transmit()
                && eq.is_empty()
            {
                return Ok(());
            }
        }

        // 1) Drain parsed events. Skipped post-handshake when no new
        //    bytes arrived (codec_has_input is false) or when the recv
        //    direct path holds the claim (recv_claimed). Under the
        //    claim, try_direct_recv is consuming events from the codec
        //    inline; draining here would surface events out of FIFO
        //    order.
        let drained = ls.drain_codec_events(&state, monitor_ctx.as_ref(), hb_interval)?;

        // 2) Dispatch drained events outside the lock.
        if dispatch_drained_events(
            drained,
            socket_type,
            &peer_in_tx,
            &*snapshot_sink,
            monitor_ctx.as_ref(),
            &ls.peer_identity,
        )
        .await?
        {
            return Ok(());
        }

        // 3a) Flush codec buffer.
        if !state.handshake_done.load(Ordering::Relaxed) || ls.codec_maybe_dirty {
            let flushed = ls.flush_codec_to_wire(&state).await?;
            if flushed {
                continue;
            }
        }

        // 3b) Flush EncodedQueue.
        if ls.flush_encoded_queue(&state).await? {
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
        //    io_uring `PollOnce` SQE can be canceled cleanly), so we
        //    can drop it when another arm wins the race. Once it
        //    fires, we do an inline `reader.read(buf).await` - kernel
        //    data is already queued, the SQE completes immediately,
        //    and we never abandon a buffer-owning read mid-flight.
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
        let accumulating = state.large_recv_pending.load(Ordering::Acquire) != 0;

        // Signal that we are about to park. The fast-path sender reads this
        // to decide whether to issue a transmit_ready notification. Set before
        // creating the listener so no sender notification is missed: in the
        // cooperative single-threaded runtime the sender cannot run between
        // the store and the actual yield inside select_biased!. After setting
        // the flag, check encoded_queue one last time to close the race where
        // the sender encoded but saw driver_in_select=false and skipped notify.
        state.driver_in_select.store(true, Ordering::Relaxed);
        if !state
            .encoded_queue
            .lock()
            .expect("encoded_queue")
            .is_empty()
        {
            continue;
        }

        let stream_arm = pull_stream(&state, &peer_io, recv_active, accumulating);
        let eof_fut = async {
            if recv_active {
                state.eof_signal.listen().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let cmd_fut = inbox.recv_async();
        let timeout_fut = maybe_sleep_until(ls.deadline);
        let hb_fut = maybe_sleep_until(ls.hb_next);
        let shared_active = shared_msg_rx.as_ref().filter(|_| !ls.shared_closed);
        let shared_fut = async {
            match shared_active {
                Some(rx) => rx.recv_async().await.ok(),
                None => std::future::pending::<Option<Message>>().await,
            }
        };
        // Woken by the fast-path sender when it encodes directly into
        // the codec buffer while we are parked here. The listener is
        // created after the previous `wrote_something == false` check,
        // with no `.await` in between, so no sender task can run in
        // that window (cooperative runtime). Any `notify` from a
        // sender that runs inside the select is captured.
        let transmit_ready_fut = state.transmit_ready.listen();
        futures::pin_mut!(stream_arm);
        futures::pin_mut!(eof_fut);
        futures::pin_mut!(cmd_fut);
        futures::pin_mut!(timeout_fut);
        futures::pin_mut!(hb_fut);
        futures::pin_mut!(shared_fut);
        futures::pin_mut!(transmit_ready_fut);
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
                ls.handle_heartbeat(
                    &state, hb_interval, hb_ttl_deciseconds, hb_timeout,
                )?;
            }
            outcome = stream_arm.fuse() => {
                if ls.handle_stream_outcome(
                    outcome, accumulating, &state, &peer_io,
                ).await? {
                    return Ok(());
                }
            }
            cmd = cmd_fut.fuse() => {
                let Ok(cmd) = cmd else { return Ok(()) };
                ls.drain_inbox(cmd, &inbox, &state, cap)
                    .await?;
            }
            msg = shared_fut.fuse() => {
                let Some(m) = msg else {
                    ls.shared_closed = true;
                    continue;
                };
                let shared = shared_msg_rx
                    .as_ref()
                    .expect("shared_fut only ready when rx is Some");
                ls.drain_shared(m, shared, &state, cap)
                    .await?;
            }
            () = transmit_ready_fut.fuse() => {
                ls.codec_maybe_dirty = true;
            }
        }
    }
}

impl DriverLoopState {
    fn drain_codec_events(
        &mut self,
        state: &DirectIoState,
        monitor_ctx: Option<&MonitorCtx>,
        hb_interval: Option<Duration>,
    ) -> Result<SmallVec<[Drained; 8]>> {
        let post_handshake = state.handshake_done.load(Ordering::Relaxed);
        let recv_claimed = state.recv_claim.load(Ordering::Acquire) == 1;
        if post_handshake && (!self.codec_has_input || recv_claimed) {
            return Ok(SmallVec::new());
        }
        self.codec_has_input = false;
        let mut io = state.lock_io();
        let mut out: SmallVec<[Drained; 8]> = SmallVec::new();
        while let Some(ev) = io.codec.poll_event() {
            match ev {
                Event::HandshakeSucceeded {
                    peer_minor,
                    peer_properties,
                } => {
                    if !io.handshake_done {
                        self.process_handshake_succeeded(
                            state,
                            &mut io,
                            &peer_properties,
                            monitor_ctx,
                            hb_interval,
                        )?;
                        self.codec_maybe_dirty = true;
                        out.push(Drained::Handshake {
                            peer_minor,
                            peer_properties,
                        });
                    }
                }
                Event::Message(_) => unreachable!("messages use poll_message"),
                Event::Command(c) => out.push(Drained::Cmd(c)),
            }
        }
        while let Some(m) = io.codec.poll_message() {
            let m = if let Some(dec) = io.decoder.as_mut() {
                match dec.decode(m)? {
                    Some(plain) => plain,
                    None => continue,
                }
            } else {
                m
            };
            out.push(Drained::Msg(m));
        }
        Ok(out)
    }

    /// Handle the outcome of the stream/read arm. Returns `Ok(true)` to
    /// exit `run_connection`, `Ok(false)` to continue the loop.
    async fn handle_stream_outcome(
        &mut self,
        outcome: StreamArmOutcome,
        accumulating: bool,
        state: &DirectIoState,
        peer_io: &SharedPeerIo,
    ) -> Result<bool> {
        match outcome {
            StreamArmOutcome::ClaimFlipped => {
                self.codec_maybe_dirty = true;
            }
            StreamArmOutcome::Eof => return Ok(true),
            StreamArmOutcome::StreamEnded => {
                if cfg!(target_os = "linux") {
                    return Ok(true);
                }
                let mut sguard = state.recv_stream.0.lock().await;
                *sguard = Some(crate::socket::RecvStreamState::OneShot);
                self.codec_maybe_dirty = true;
            }
            StreamArmOutcome::ProtoErr(e) => return Err(e),
            StreamArmOutcome::Err(e) => {
                if e.raw_os_error() != Some(libc::ENOBUFS) {
                    return Ok(true);
                }
                if accumulating {
                    let mut sguard = state.recv_stream.0.lock().await;
                    *sguard = Some(crate::socket::RecvStreamState::OneShot);
                } else if state.recv_stream.rearm(peer_io).await.is_err() {
                    return Ok(true);
                } else {
                    state.multishot_rearms.fetch_add(1, Ordering::Relaxed);
                }
                self.codec_maybe_dirty = true;
            }
            StreamArmOutcome::Fed => {
                self.codec_has_input = true;
                if state.recv_claim.load(Ordering::Acquire) == 1 {
                    state.recv_codec_ready.notify(usize::MAX);
                }
                self.codec_maybe_dirty = true;
            }
            StreamArmOutcome::AccData(bytes) => {
                let payload_len = state.large_recv_pending.load(Ordering::Acquire);
                let mut acc_guard = state.pending_acc.lock().expect("pending_acc");
                let acc = acc_guard.as_mut().expect("AccData without buffer");
                let needed = payload_len - acc.len();
                let extra = if bytes.len() <= needed {
                    acc.extend_from_slice(&bytes);
                    None
                } else {
                    acc.extend_from_slice(&bytes[..needed]);
                    Some(bytes.slice(needed..))
                };
                if acc.len() >= payload_len {
                    let payload = acc_guard.take().unwrap().freeze();
                    drop(acc_guard);
                    state.large_recv_pending.store(0, Ordering::Release);
                    let mut io = state.lock_io();
                    io.codec.supply_payload(payload)?;
                    if let Some(extra) = extra {
                        io.codec.handle_input(extra)?;
                    }
                    self.codec_has_input = true;
                    self.codec_maybe_dirty = true;
                }
            }
        }
        Ok(false)
    }

    fn handle_heartbeat(
        &mut self,
        state: &DirectIoState,
        hb_interval: Option<Duration>,
        hb_ttl_deciseconds: u16,
        hb_timeout: Duration,
    ) -> Result<()> {
        let now_nanos = state.hb_epoch.elapsed().as_nanos() as u64;
        let last_nanos = state.last_input_nanos.load(Ordering::Relaxed);
        let elapsed = Duration::from_nanos(now_nanos.saturating_sub(last_nanos));
        if elapsed > hb_timeout {
            return Err(Error::Timeout);
        }
        let ping = Command::Ping {
            ttl_deciseconds: hb_ttl_deciseconds,
            context: Bytes::new(),
        };
        {
            let mut io = state.lock_io();
            let _ = io.codec.send_command(&ping);
            self.codec_maybe_dirty = true;
        }
        if let Some(iv) = hb_interval {
            self.hb_next = Some(Instant::now() + iv);
        }
        Ok(())
    }

    async fn flush_codec_to_wire(&mut self, state: &DirectIoState) -> Result<bool> {
        let mut writer = state.writer.lock().await;
        let (chunks, total) = {
            let mut io = state.lock_io();
            if io.codec.has_pending_transmit() {
                let mut c = io.codec.clone_transmit_chunks();
                if c.len() > 1024 {
                    c.truncate(1024);
                }
                let t: usize = c.iter().map(Bytes::len).sum();
                io.codec.advance_transmit(t);
                (c, t)
            } else {
                self.codec_maybe_dirty = false;
                return Ok(false);
            }
        };
        if chunks.is_empty() {
            return Ok(false);
        }
        let (res, returned) = writer.write_vectored(chunks).await;
        let written = res.map_err(Error::Io)?;
        if written == 0 {
            state
                .encoded_queue
                .lock()
                .expect("encoded_queue")
                .put_back_unwritten(returned, 0);
            return Ok(false);
        }
        if written < total {
            state
                .encoded_queue
                .lock()
                .expect("encoded_queue")
                .put_back_unwritten(returned, written);
        }
        Ok(true)
    }

    async fn flush_encoded_queue(&mut self, state: &DirectIoState) -> Result<bool> {
        self.drain_buf.clear();
        {
            let mut eq = state.encoded_queue.lock().expect("encoded_queue");
            eq.drain_into_vec(&mut self.drain_buf, 1024);
        }
        if self.drain_buf.is_empty() {
            return Ok(false);
        }
        let tmp = std::mem::take(&mut self.drain_buf);
        let (res, returned) = state.writer.lock().await.write_vectored(tmp).await;
        let written = res.map_err(Error::Io)?;
        if written == 0 {
            state
                .encoded_queue
                .lock()
                .expect("encoded_queue")
                .put_back_unwritten(returned, 0);
            return Ok(false);
        }
        let total_drained: usize = returned.iter().map(Bytes::len).sum();
        if written < total_drained {
            state
                .encoded_queue
                .lock()
                .expect("encoded_queue")
                .put_back_unwritten(returned, written);
        } else {
            self.drain_buf = returned;
        }
        Ok(true)
    }
}
