//! Shared carrier ring with Unix setup, descriptor transfer and EOF detection.
//! The driver retains its handshake, reconnect, control and receive admission.

use std::fs::File;
use std::io::{self, Read};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UnixStream;

pub(crate) type ShmState = Arc<Mutex<omq_shm::Connection>>;

// AsyncFd retains the File; callers never replace it while registered.
#[allow(deprecated, reason = "AsyncFd::register requires unsafe code")]
fn register_signal(signal: Arc<File>) -> io::Result<AsyncFd<Arc<File>>> {
    AsyncFd::new(signal)
}

#[derive(Debug)]
pub(crate) struct ShmStream {
    stream: UnixStream,
    pub(crate) state: ShmState,
    send: [u8; omq_shm::PREFACE_SIZE],
    sent: usize,
    receive: [u8; omq_shm::PREFACE_SIZE],
    received: usize,
    writer_signal: AsyncFd<Arc<File>>,
    reader_signal: Option<AsyncFd<Arc<File>>>,
}

impl ShmStream {
    pub(crate) fn new(
        stream: UnixStream,
        options: &omq_proto::Options,
        pool: omq_shm::SendPool,
    ) -> io::Result<Self> {
        let connection = omq_shm::Connection::with_send_pool(
            &stream,
            options.send_hwm.max(1) as usize,
            options.max_message_size,
            pool,
        )?;
        let send = connection.preface();
        let writer_signal = register_signal(connection.carrier_writer_signal())?;
        Ok(Self {
            stream,
            state: Arc::new(Mutex::new(connection)),
            send,
            sent: 0,
            receive: [0; omq_shm::PREFACE_SIZE],
            received: 0,
            writer_signal,
            reader_signal: None,
        })
    }

    pub(crate) fn migrate(mut self) -> io::Result<Self> {
        self.stream = UnixStream::from_std(self.stream.into_std()?)?;
        self.writer_signal = register_signal(self.writer_signal.into_inner())?;
        self.reader_signal = self
            .reader_signal
            .map(|signal| register_signal(signal.into_inner()))
            .transpose()?;
        Ok(self)
    }

    pub(crate) fn apply_options(&self, options: &omq_proto::Options) -> io::Result<()> {
        options.apply_socket_buffers(&self.stream)
    }

    fn poll_preface(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Fixed 24-byte setup work; partial writes preserve their cursor.
        while self.sent < self.send.len() {
            let written = std::task::ready!(poll_send(
                &self.stream,
                &self.state,
                cx,
                &[io::IoSlice::new(&self.send[self.sent..])]
            ))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.sent += written;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_carrier_send(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        // Retry only a bounded number of stale notifier permits in one poll.
        for _ in 0..SIGNAL_POLL_LIMIT {
            if self
                .state
                .lock()
                .expect("SHM connection")
                .has_pending_descriptors()
            {
                std::task::ready!(poll_send(
                    &self.stream,
                    &self.state,
                    cx,
                    &[io::IoSlice::new(&[0])]
                ))?;
            }
            match self
                .state
                .lock()
                .expect("SHM connection")
                .try_write_carrier(bytes)
            {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
            self.state
                .lock()
                .expect("SHM connection")
                .arm_carrier_write();
            // The arm/recheck pair closes the space and registration wake race.
            match self
                .state
                .lock()
                .expect("SHM connection")
                .try_write_carrier(bytes)
            {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
            std::task::ready!(poll_signal(&self.writer_signal, cx))?;
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn poll_carrier_receive(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        for _ in 0..SIGNAL_POLL_LIMIT {
            match self
                .state
                .lock()
                .expect("SHM connection")
                .try_read_carrier(bytes)
            {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
            // The stream carries only FD registration markers after its preface.
            // Keep its readiness registered for both registration and peer EOF.
            match self.poll_registration(cx) {
                Poll::Ready(Ok(0)) => return Poll::Ready(self.read_before_eof(bytes)),
                Poll::Ready(Ok(_)) => continue,
                Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
            if self
                .state
                .lock()
                .expect("SHM connection")
                .arm_carrier_read()?
            {
                continue;
            }
            std::task::ready!(poll_signal(
                self.reader_signal.as_ref().expect("carrier reader signal"),
                cx
            ))?;
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn poll_registration(&self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        std::task::ready!(self.stream.poll_read_ready(cx))?;
        let mut control = [0; CONTROL_READ_BYTES];
        let received = self.stream.try_io(Interest::READABLE, || {
            self.state
                .lock()
                .expect("SHM connection")
                .try_read(&self.stream, &mut control)
        })?;
        if control[..received].iter().any(|&byte| byte != 0) {
            return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
        }
        Poll::Ready(Ok(received))
    }

    fn read_before_eof(&self, bytes: &mut [u8]) -> io::Result<usize> {
        // Publication can race the empty read above. Drain the final published
        // bytes before reporting orderly EOF.
        match self
            .state
            .lock()
            .expect("SHM connection")
            .try_read_carrier(bytes)
        {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(0),
            result => result,
        }
    }
}

const SIGNAL_POLL_LIMIT: usize = 4;
const CONTROL_READ_BYTES: usize = 64;

fn poll_signal(signal: &AsyncFd<Arc<File>>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let mut ready = std::task::ready!(signal.poll_read_ready(cx))?;
    let mut counter = [0; std::mem::size_of::<u64>()];
    let result = ready.try_io(|signal| {
        let mut file = signal.get_ref().as_ref();
        file.read_exact(&mut counter)
    });
    if let Ok(result) = result {
        ready.clear_ready();
        Poll::Ready(result)
    } else {
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn poll_send(
    stream: &UnixStream,
    state: &ShmState,
    cx: &mut Context<'_>,
    bytes: &[io::IoSlice<'_>],
) -> Poll<io::Result<usize>> {
    loop {
        std::task::ready!(stream.poll_write_ready(cx))?;
        match stream.try_io(Interest::WRITABLE, || {
            state
                .lock()
                .expect("SHM connection")
                .try_write(stream, bytes)
        }) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            result => return Poll::Ready(result),
        }
    }
}

fn poll_receive(
    stream: &UnixStream,
    state: &ShmState,
    cx: &mut Context<'_>,
    bytes: &mut [u8],
) -> Poll<io::Result<usize>> {
    loop {
        std::task::ready!(stream.poll_read_ready(cx))?;
        match stream.try_io(Interest::READABLE, || {
            state
                .lock()
                .expect("SHM connection")
                .try_read(stream, bytes)
        }) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            result => return Poll::Ready(result),
        }
    }
}

impl AsyncRead for ShmStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.received < this.receive.len() {
            let received = std::task::ready!(poll_receive(
                &this.stream,
                &this.state,
                cx,
                &mut this.receive[this.received..]
            ))?;
            if received == 0 {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
            }
            this.received += received;
            if this.received < this.receive.len() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            this.state
                .lock()
                .expect("SHM connection")
                .accept_preface(&this.receive)?;
            let signal = this
                .state
                .lock()
                .expect("SHM connection")
                .carrier_reader_signal()
                .ok_or(io::ErrorKind::InvalidData)?;
            this.reader_signal = Some(register_signal(signal)?);
        }
        let received =
            std::task::ready!(this.poll_carrier_receive(cx, output.initialize_unfilled()))?;
        output.advance(received);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ShmStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        std::task::ready!(this.poll_preface(cx))?;
        this.poll_carrier_send(cx, &[io::IoSlice::new(bytes)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bytes.iter().all(|slice| slice.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        std::task::ready!(this.poll_preface(cx))?;
        this.poll_carrier_send(cx, bytes)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_preface(cx))?;
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
