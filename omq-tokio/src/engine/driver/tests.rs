mod control;
mod delivery;
mod heartbeat;
mod lifecycle;
mod read;
mod write;

use super::super::signal::StateSignal;
use super::control::HeartbeatProbe;
use super::read::*;
use super::receive::*;
use super::write::*;
use super::*;
use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
#[cfg(feature = "ws")]
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::io::{DuplexStream, ReadBuf};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::proto::connection::{ConnectionConfig, Role};
use omq_proto::proto::{Event, SocketType};
use omq_proto::{
    error::{Error, Result},
    frame_buffer::FrameBuffer,
};

impl DriverStream for DuplexStream {
    type Reader = tokio::io::ReadHalf<Self>;
    type Writer = tokio::io::WriteHalf<Self>;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        tokio::io::split(self)
    }
}

#[derive(Debug)]
struct StalledShutdownStream {
    stream: DuplexStream,
    shutdown_seen: Arc<std::sync::atomic::AtomicBool>,
}

impl DriverStream for StalledShutdownStream {
    type Reader = tokio::io::ReadHalf<DuplexStream>;
    type Writer = StalledShutdownWriter;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = tokio::io::split(self.stream);
        (
            reader,
            StalledShutdownWriter {
                writer,
                shutdown_seen: self.shutdown_seen,
            },
        )
    }
}

#[derive(Debug)]
struct StalledShutdownWriter {
    writer: tokio::io::WriteHalf<DuplexStream>,
    shutdown_seen: Arc<std::sync::atomic::AtomicBool>,
}

impl AsyncWrite for StalledShutdownWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shutdown_seen.store(true, Ordering::Release);
        Poll::Pending
    }
}

#[derive(Debug)]
struct ChoppyDuplex {
    inner: DuplexStream,
    read_cap: usize,
    write_cap: usize,
}

#[cfg(feature = "plain")]
#[derive(Debug)]
struct GreetingOnlyStream {
    stream: DuplexStream,
    stalled: Arc<StateSignal>,
}

#[cfg(feature = "plain")]
impl DriverStream for GreetingOnlyStream {
    type Reader = tokio::io::ReadHalf<DuplexStream>;
    type Writer = GreetingOnlyWriter;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = tokio::io::split(self.stream);
        (
            reader,
            GreetingOnlyWriter {
                writer,
                remaining: 64,
                stalled: self.stalled,
            },
        )
    }
}

#[cfg(feature = "plain")]
#[derive(Debug)]
struct GreetingOnlyWriter {
    writer: tokio::io::WriteHalf<DuplexStream>,
    remaining: usize,
    stalled: Arc<StateSignal>,
}

#[cfg(feature = "plain")]
impl AsyncWrite for GreetingOnlyWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.remaining == 0 {
            self.stalled.notify_changed();
            return Poll::Pending;
        }
        let length = self.remaining.min(buf.len());
        match Pin::new(&mut self.writer).poll_write(cx, &buf[..length]) {
            Poll::Ready(Ok(written)) => {
                self.remaining -= written;
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl ChoppyDuplex {
    fn new(inner: DuplexStream, read_cap: usize, write_cap: usize) -> Self {
        Self {
            inner,
            read_cap: read_cap.max(1),
            write_cap: write_cap.max(1),
        }
    }
}

impl DriverStream for ChoppyDuplex {
    type Reader = ChoppyReader<tokio::io::ReadHalf<DuplexStream>>;
    type Writer = ChoppyWriter<tokio::io::WriteHalf<DuplexStream>>;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = tokio::io::split(self.inner);
        (
            ChoppyReader {
                inner: reader,
                cap: self.read_cap,
            },
            ChoppyWriter {
                inner: writer,
                cap: self.write_cap,
            },
        )
    }
}

#[derive(Debug)]
struct ChoppyReader<R> {
    inner: R,
    cap: usize,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ChoppyReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut scratch = vec![0u8; self.cap.min(buf.remaining())];
        let mut limited = ReadBuf::new(&mut scratch);
        match Pin::new(&mut self.inner).poll_read(cx, &mut limited) {
            Poll::Ready(Ok(())) => {
                buf.put_slice(limited.filled());
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl DriverWrite for StalledShutdownWriter {}
#[cfg(feature = "plain")]
impl DriverWrite for GreetingOnlyWriter {}
impl<W: AsyncWrite + Send + Unpin + 'static> DriverWrite for ChoppyWriter<W> {}

#[derive(Debug)]
struct ChoppyWriter<W> {
    inner: W,
    cap: usize,
}

impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for ChoppyWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = self.cap.min(buf.len());
        Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut remaining = self.cap;
        let mut limited: SmallVec<[io::IoSlice<'_>; 64]> = SmallVec::new();
        for buf in bufs {
            if remaining == 0 {
                break;
            }
            let n = remaining.min(buf.len());
            if n > 0 {
                limited.push(io::IoSlice::new(&buf[..n]));
                remaining -= n;
            }
        }
        Pin::new(&mut self.inner).poll_write_vectored(cx, &limited)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
struct PartialVectoredWriter {
    out: Vec<u8>,
    first_cap: usize,
    next_cap: usize,
    writes: usize,
}

impl PartialVectoredWriter {
    fn new(first_cap: usize, next_cap: usize) -> Self {
        Self {
            out: Vec::new(),
            first_cap,
            next_cap,
            writes: 0,
        }
    }
}

impl tokio::io::AsyncWrite for PartialVectoredWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let cap = if self.writes == 0 {
            self.first_cap
        } else {
            self.next_cap
        };
        let n = cap.min(buf.len());
        if n > 0 {
            self.out.extend_from_slice(&buf[..n]);
        }
        self.writes += 1;
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        let cap = if self.writes == 0 {
            self.first_cap
        } else {
            self.next_cap
        };
        let mut remaining = cap.min(total);
        for buf in bufs {
            if remaining == 0 {
                break;
            }
            let n = remaining.min(buf.len());
            self.out.extend_from_slice(&buf[..n]);
            remaining -= n;
        }
        self.writes += 1;
        Poll::Ready(Ok(cap.min(total)))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct ScriptedVectoredWriter {
    out: Vec<u8>,
    caps: VecDeque<usize>,
    fallback_cap: usize,
}

impl ScriptedVectoredWriter {
    fn new(caps: impl IntoIterator<Item = usize>, fallback_cap: usize) -> Self {
        Self {
            out: Vec::new(),
            caps: caps.into_iter().collect(),
            fallback_cap,
        }
    }
}

impl tokio::io::AsyncWrite for ScriptedVectoredWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let cap = self.caps.pop_front().unwrap_or(self.fallback_cap).max(1);
        let n = cap.min(buf.len());
        self.out.extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        let cap = self.caps.pop_front().unwrap_or(self.fallback_cap).max(1);
        let mut remaining = cap.min(total);
        for buf in bufs {
            if remaining == 0 {
                break;
            }
            let n = remaining.min(buf.len());
            self.out.extend_from_slice(&buf[..n]);
            remaining -= n;
        }
        Poll::Ready(Ok(cap.min(total)))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct ScriptedReader {
    data: Bytes,
    pos: usize,
    chunks: VecDeque<usize>,
}

impl ScriptedReader {
    fn new(data: Bytes, chunks: impl IntoIterator<Item = usize>) -> Self {
        Self {
            data,
            pos: 0,
            chunks: chunks.into_iter().collect(),
        }
    }
}

impl tokio::io::AsyncRead for ScriptedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pos >= self.data.len() {
            return Poll::Ready(Ok(()));
        }
        let chunk_cap = self.chunks.pop_front().unwrap_or(usize::MAX);
        let n = chunk_cap
            .min(buf.remaining())
            .min(self.data.len() - self.pos);
        let end = self.pos + n;
        buf.put_slice(&self.data[self.pos..end]);
        self.pos = end;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct UninitProbeReader {
    inner: ScriptedReader,
}

impl tokio::io::AsyncRead for UninitProbeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        assert!(
            buf.initialized().is_empty(),
            "large recv path must fill uninitialized spare capacity"
        );
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

/// Adapter: pull `(u64, PeerEvent::Event)` off the shared peer-out
/// channel and yield bare `Event` values, matching the older
/// per-side events channel shape the tests were written
/// against. `PeerEvent::Closed` ends the stream (returns None).
pub(super) struct EventAdapter {
    rx: mpsc::Receiver<(u64, PeerEvent)>,
}

impl EventAdapter {
    pub(super) async fn recv(&mut self) -> Option<Event> {
        match self.rx.recv().await? {
            (_, PeerEvent::Event(e)) => Some(e),
            (_, PeerEvent::Closed { .. }) => None,
        }
    }
}

/// Spin up two drivers connected via an in-memory duplex pair,
/// return handles + event rxes. The connection driver is generic
/// over T: AsyncRead+AsyncWrite, so a `tokio::io::duplex` pair
/// is the simplest way to test it without involving the inproc
/// transport (which since the inproc fast-path landed bypasses
/// the connection entirely).
#[expect(clippy::unused_async)]
async fn inproc_pair(
    _name: &str,
) -> (
    PeerDriverHandle,
    EventAdapter,
    PeerDriverHandle,
    EventAdapter,
) {
    let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);

    let server_connection = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let client_connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).identity(Bytes::from_static(b"c")),
    );

    let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
    let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
    let (s_data_tx, s_data_rx) = mpsc::channel(16);
    let (c_data_tx, c_data_rx) = mpsc::channel(16);
    let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
    let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
    let s_cancel = CancellationToken::new();
    let c_cancel = CancellationToken::new();

    let s_driver = ConnectionDriver::new(
        server_stream,
        server_connection,
        s_inbox_rx,
        s_evt_tx,
        0,
        s_cancel.clone(),
    )
    .with_data_inbox(s_data_rx);
    let c_driver = ConnectionDriver::new(
        client_stream,
        client_connection,
        c_inbox_rx,
        c_evt_tx,
        0,
        c_cancel.clone(),
    )
    .with_data_inbox(c_data_rx);

    tokio::spawn(Box::pin(s_driver.run()));
    tokio::spawn(Box::pin(c_driver.run()));

    (
        PeerDriverHandle {
            inbox: c_inbox_tx,
            data_inbox: c_data_tx,
            cancel: c_cancel,
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: None,
            inproc: None,
        },
        EventAdapter { rx: c_evt_rx },
        PeerDriverHandle {
            inbox: s_inbox_tx,
            data_inbox: s_data_tx,
            cancel: s_cancel,
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: None,
            inproc: None,
        },
        EventAdapter { rx: s_evt_rx },
    )
}

fn drain_transmit(connection: &mut Connection) -> Vec<u8> {
    let mut out = Vec::new();
    while connection.has_pending_transmit() {
        let len_before = out.len();
        for chunk in connection.transmit_chunks_capped(128) {
            out.extend_from_slice(&chunk);
        }
        connection.advance_transmit(out.len() - len_before);
    }
    out
}

fn ready_push_pull_connections() -> (Connection, Connection) {
    let mut push = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).identity(Bytes::from_static(b"c")),
    );
    let mut pull = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    for _ in 0..10 {
        let push_out = drain_transmit(&mut push);
        let pull_out = drain_transmit(&mut pull);
        if push_out.is_empty() && pull_out.is_empty() {
            break;
        }
        if !push_out.is_empty() {
            pull.handle_input(Bytes::from(push_out)).unwrap();
        }
        if !pull_out.is_empty() {
            push.handle_input(Bytes::from(pull_out)).unwrap();
        }
    }
    assert!(push.is_ready());
    assert!(pull.is_ready());
    (push, pull)
}

fn feed_fragmented_input(connection: &mut Connection, wire: &Bytes, end: usize) {
    let mut start = 0;
    for boundary in [5, 17, 4093, 65_541, end] {
        let boundary = boundary.min(end);
        if boundary > start {
            connection
                .handle_input(wire.slice(start..boundary))
                .unwrap();
            start = boundary;
        }
    }
    if start < end {
        connection.handle_input(wire.slice(start..end)).unwrap();
    }
}

async fn handle_large_messages_test<R: AsyncRead + Unpin>(
    connection: &mut Connection,
    reader: &mut R,
    config: &PeerDriverConfig,
    last_input: &mut Instant,
) -> Result<()> {
    let recv_pool = RecvBufPool::new();
    while let Some(mut large) = PendingLargeRead::begin(connection, config, &recv_pool)? {
        while !large.complete() {
            large.read(reader).await?;
            *last_input = Instant::now();
        }
        large.finish(connection, &recv_pool)?;
    }
    Ok(())
}

fn feed_input_in_chunks(
    connection: &mut Connection,
    wire: &Bytes,
    end: usize,
    chunks: impl IntoIterator<Item = usize>,
) {
    let chunks = chunks.into_iter().collect::<Vec<_>>();
    let mut start = 0;
    let mut index = 0;
    while start < end {
        let size = chunks[index % chunks.len()];
        index += 1;
        let next = start.saturating_add(size).min(end);
        connection.handle_input(wire.slice(start..next)).unwrap();
        start = next;
    }
}

fn patterned_payload(len: usize, seq: u64) -> Vec<u8> {
    let mut payload = vec![0u8; len];
    payload[..8].copy_from_slice(&0xDEAD_BEEF_CAFE_F00Du64.to_le_bytes());
    payload[8..16].copy_from_slice(&seq.to_le_bytes());
    let mask = (seq as u8).wrapping_mul(7) ^ ((seq >> 8) as u8).wrapping_mul(3);
    for (i, byte) in payload.iter_mut().enumerate().skip(16) {
        let mut expected = (i as u8).wrapping_mul(31);
        expected ^= ((i >> 8) as u8).wrapping_mul(17);
        expected ^= ((i >> 16) as u8).wrapping_mul(13);
        *byte = expected ^ mask;
    }
    payload
}

fn push_expected_single_frame(out: &mut Vec<u8>, payload: &[u8]) {
    if payload.len() > 255 {
        out.push(0x02);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    } else {
        out.push(0);
        out.push(payload.len() as u8);
    }
    out.extend_from_slice(payload);
}

fn next_random(seed: &mut u64) -> usize {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed as usize
}

async fn drain_large_messages_until(
    consumer: &mut yring::Consumer<Message>,
    space: &StateSignal,
    msg_size: usize,
    next_recv: &mut usize,
    target: usize,
    wait: bool,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while *next_recv < target {
        if consumer.prefetch() == 0 && consumer.is_empty() {
            if !wait || Instant::now() >= deadline {
                break;
            }
            tokio::task::yield_now().await;
            continue;
        }
        let mut released = false;
        while let Some(item) = consumer.pop() {
            released = true;
            let data = item.part_bytes(0).unwrap();
            assert_eq!(
                data.as_ref(),
                patterned_payload(msg_size, *next_recv as u64)
            );
            *next_recv += 1;
        }
        if released {
            consumer.release();
            space.notify_changed();
        }
    }
    if wait {
        assert!(
            *next_recv >= target,
            "received {} large messages, expected {target}",
            *next_recv,
        );
    }
}

/// Owned-chunk writer with Quinn's `write_chunks` semantics: it empties
/// accepted chunks and trims a partly accepted one. `caps` scripts each
/// call: `Some(n)` accepts up to `n` bytes, `None` stays pending without
/// a wake until the test removes it, like a flow-control stall.
#[derive(Debug, Default)]
struct OwnedChunkWriter {
    caps: VecDeque<Option<usize>>,
    default_cap: usize,
    out: Vec<u8>,
    chunk_starts: Vec<usize>,
}

impl OwnedChunkWriter {
    fn new(caps: impl IntoIterator<Item = Option<usize>>, default_cap: usize) -> Self {
        Self {
            caps: caps.into_iter().collect(),
            default_cap,
            ..Self::default()
        }
    }
}

impl ChunkWrite for OwnedChunkWriter {
    fn poll_write_chunks(
        &mut self,
        _cx: &mut Context<'_>,
        bufs: &mut [Bytes],
    ) -> Poll<io::Result<usize>> {
        if self.caps.front() == Some(&None) {
            return Poll::Pending;
        }
        let mut left = self.caps.pop_front().flatten().unwrap_or(self.default_cap);
        let mut written = 0;
        for buf in bufs.iter_mut() {
            if left == 0 {
                break;
            }
            let take = buf.len().min(left);
            let chunk = if take == buf.len() {
                std::mem::take(buf)
            } else {
                buf.split_to(take)
            };
            self.chunk_starts.push(chunk.as_ptr() as usize);
            self.out.extend_from_slice(&chunk);
            written += take;
            left -= take;
        }
        Poll::Ready(Ok(written))
    }
}

impl DriverWrite for OwnedChunkWriter {
    fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
        Some(self)
    }
}

impl AsyncWrite for OwnedChunkWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.out.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Small (arena) and large (external) frames in one buffer, plus the
/// expected wire bytes.
fn mixed_frames(eq: &mut FrameBuffer) -> (Vec<u8>, Vec<usize>) {
    let mut expected = Vec::new();
    let mut large_starts = Vec::new();
    for (seq, len) in [16, 300, 70_000, 64, 1_000, 200_000, 32]
        .into_iter()
        .enumerate()
    {
        let payload = Bytes::from(patterned_payload(len, seq as u64));
        push_expected_single_frame(&mut expected, &payload);
        if len >= omq_proto::frame_buffer::ARENA_THRESHOLD {
            large_starts.push(payload.as_ptr() as usize);
        }
        eq.frame(&Message::single(payload));
    }
    (expected, large_starts)
}

async fn write_owned_until_idle(
    writer: &mut OwnedChunkWriter,
    eq: &mut FrameBuffer,
    pending: &mut PendingWrite,
    connection: &mut Connection,
) {
    let mut heartbeat = HeartbeatProbe::default();
    while !pending.is_empty() || !eq.is_empty() {
        write_driver_progress(writer, eq, pending, connection, &mut heartbeat)
            .await
            .unwrap();
    }
}
