//! Carrier registration and per-recipient lease state.
#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, IoSlice};
use std::os::fd::AsFd;
use std::sync::Arc;
use std::sync::atomic::Ordering;

mod codec;
mod state;

use omq_proto::{Message, SocketType};

use crate::lease::{CreditWait, Table};
use crate::mapping::{LEASES, Mapping, invalid};
use crate::process::Process;
use crate::wire::{self, READER_CLOSED};
use crate::{MAX_LEASES, PREFACE_SIZE, SendPool, socket};
use state::{InboundRegions, OutboundLeases, Registrations};

#[cfg(all(test, not(miri)))]
use crate::pool::find_slot;
#[cfg(all(test, not(miri)))]
use crate::wire::{BODY, COPIED};
#[cfg(all(test, not(miri)))]
use crate::{BATCH_MAX_BYTES, BatchKind, COPY_THRESHOLD, body};
#[cfg(all(test, not(miri)))]
use omq_proto::Payload;

/// Per-connection SHM encoding and lease state. Used by IO drivers and lanes;
/// caller threads continue to enqueue ordinary Message values.
#[derive(Debug)]
pub struct Connection {
    peer: Arc<Process>,
    outgoing: Arc<Table>,
    incoming: Option<Arc<Table>>,
    leases: OutboundLeases,
    send_pool: SendPool,
    inbound: InboundRegions,
    registrations: Registrations,
    blocked_sequence: u32,
    max_message_size: Option<usize>,
    received: Option<crate::ReceivedBatch>,
    views: crate::views::Views,
    received_descriptors: u64,
    receive_polling: bool,
}

/// A decoded single message or a validated batch awaiting application receive.
#[derive(Debug)]
pub enum Received {
    /// An ordinary, independently owned message.
    Message(Message),
    /// Bodies retained together until the application receives them.
    Batch(crate::ReceivedBatch),
}

/// Whether a socket type supports SHM: every type except STREAM. Envelope
/// frames remain ordinary codec frames; only the final frame uses shared storage.
pub const fn supports(socket_type: SocketType) -> bool {
    !matches!(socket_type, SocketType::Stream)
}

impl Connection {
    /// Authenticate the Unix peer credentials and preallocate bounded leases.
    /// Creates automatic outbound storage with the default 64 MiB cap.
    /// Use [`Self::with_send_pool`] to share storage between socket peers.
    ///
    /// # Errors
    /// Returns an error if credentials, pidfds, or completion-table setup fail.
    pub fn new(socket: impl AsFd, hwm: usize, max_message_size: Option<usize>) -> io::Result<Self> {
        Self::with_send_pool(
            socket,
            hwm,
            max_message_size,
            SendPool::new(hwm, Some(omq_proto::options::DEFAULT_SHM_MAX_MEMORY)),
        )
    }

    /// Create a connection using the socket's shared automatic outbound storage.
    ///
    /// All peers of one socket should receive clones of the same [`SendPool`].
    /// `hwm` bounds logical leased messages; `max_message_size` bounds decoded
    /// incoming messages including envelopes and payload-slot overhead.
    ///
    /// # Errors
    /// Returns an error if peer credentials/pidfds or completion-table setup fail.
    pub fn with_send_pool(
        socket: impl AsFd,
        hwm: usize,
        max_message_size: Option<usize>,
        send_pool: SendPool,
    ) -> io::Result<Self> {
        let peer = Arc::new(Process::peer(socket)?);
        let count = hwm.clamp(1, MAX_LEASES);
        let outgoing = Arc::new(Table::new(
            Mapping::create(crate::carrier::Carrier::length(count), LEASES)?,
            count,
            crate::carrier::Carrier::new(count, true)?,
        ));
        outgoing
            .mapping
            .atomic64(crate::carrier::COUNT_OFFSET)
            .store(count as u64, Ordering::Relaxed);

        let descriptors = VecDeque::from([
            outgoing.mapping.descriptor()?,
            outgoing
                .carrier
                .reader_signal()
                .expect("reader signal")
                .try_clone()?,
            outgoing
                .carrier
                .writer_signal()
                .expect("writer signal")
                .try_clone()?,
        ]);

        Ok(Self {
            peer,
            outgoing,
            incoming: None,
            leases: OutboundLeases::new(count),
            send_pool,
            inbound: InboundRegions::default(),
            registrations: Registrations::new(descriptors),
            blocked_sequence: 0,
            max_message_size,
            received: None,
            views: crate::views::Views::new(count.max(crate::BATCH_MAX_MESSAGES)),
            received_descriptors: 0,
            receive_polling: false,
        })
    }

    /// Exchange before ZMTP so even unread references can be retired on close.
    pub fn preface(&self) -> [u8; PREFACE_SIZE] {
        wire::Preface {
            id: self.outgoing.mapping.id,
            count: self.outgoing.count,
            token: self.outgoing.mapping.token,
        }
        .encode()
    }

    /// Verify the peer's completion table received through `SCM_RIGHTS`.
    ///
    /// # Errors
    /// Returns an error for a missing descriptor or incompatible/invalid preface.
    pub fn accept_preface(&mut self, bytes: &[u8; PREFACE_SIZE]) -> io::Result<()> {
        let preface = wire::Preface::decode(bytes)?;
        let table = self.incoming("missing SHM lease descriptor")?;

        if table.mapping.id != preface.id
            || table.mapping.token != preface.token
            || table.count != preface.count
        {
            return Err(invalid("invalid SHM lease table size"));
        }

        Ok(())
    }

    /// Write Unix setup/control bytes, attaching any newly registered regions.
    /// Positive partial writes transfer descriptors exactly once. `WouldBlock`
    /// retains them for the next writable reactor notification.
    ///
    /// # Errors
    /// Returns carrier IO errors, including `WouldBlock` when the socket is full.
    pub fn try_write(&mut self, socket: impl AsFd, bytes: &[IoSlice<'_>]) -> io::Result<usize> {
        if bytes.iter().all(|bytes| bytes.is_empty()) {
            return Ok(0);
        }

        let written = socket::send(socket, bytes, &self.registrations.descriptors)?;
        if written != 0 && self.registrations.mark_sent() {
            // Registration capacity also uses the persistent credit wait.
            self.outgoing.notify();
        }

        Ok(written)
    }

    /// Receive Unix setup/control bytes and import attached regions.
    /// Every received FD is CLOEXEC and owned on all success/error paths.
    ///
    /// # Errors
    /// Returns carrier IO errors or errors validating/mapping received regions.
    pub fn try_read(&mut self, socket: impl AsFd, bytes: &mut [u8]) -> io::Result<usize> {
        let (received, descriptors) = socket::receive(socket, bytes)?;
        if descriptors.len() > socket::SEND_LIMIT {
            return Err(invalid("too many SHM descriptors in one transfer"));
        }

        let count = descriptors.len();
        for file in descriptors {
            self.accept_descriptor(file)?;
        }

        if count != 0 {
            self.received_descriptors = self.received_descriptors.wrapping_add(count as u64);
            let incoming = self.incoming("missing SHM lease descriptor")?;
            incoming
                .carrier
                .acknowledge_imports(&incoming.mapping, self.received_descriptors)?;
        }

        Ok(received)
    }

    /// Whether descriptor registration must progress before publishing carrier bytes.
    pub fn has_pending_descriptors(&self) -> bool {
        !self.registrations.descriptors.is_empty()
    }

    /// Publish a bounded prefix of already framed carrier bytes into shared memory.
    /// Descriptors must be sent through [`Self::try_write`] and acknowledged first.
    ///
    /// # Errors
    /// Returns `WouldBlock` for registration/space pressure, or invalid shared state.
    pub fn try_write_carrier(&mut self, bytes: &[IoSlice<'_>]) -> io::Result<usize> {
        if self.has_pending_descriptors()
            || self.outgoing.carrier.imported(&self.outgoing.mapping) != self.registrations.sent
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        if self
            .outgoing
            .mapping
            .atomic64(READER_CLOSED)
            .load(Ordering::Acquire)
            != 0
        {
            return Err(io::ErrorKind::BrokenPipe.into());
        }

        self.outgoing.carrier.write(&self.outgoing.mapping, bytes)
    }

    /// Copy a bounded prefix of published carrier bytes into the codec's input buffer.
    ///
    /// # Errors
    /// Returns `WouldBlock` when empty, or an error for invalid shared cursors.
    pub fn try_read_carrier(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let table = self.incoming("missing SHM carrier")?;
        table.carrier.read(&table.mapping, bytes)
    }

    /// Register a producer wait before rechecking registration and ring space.
    pub fn arm_carrier_write(&self) {
        self.outgoing.carrier.park_writer(&self.outgoing.mapping);
    }

    /// Register a consumer wait and recheck publication before parking.
    ///
    /// # Errors
    /// Returns an error for a missing carrier or invalid shared cursors.
    pub fn arm_carrier_read(&self) -> io::Result<bool> {
        let table = self.incoming("missing SHM carrier")?;
        table
            .carrier
            .arm_reader(&table.mapping, self.receive_polling)
    }

    /// Keep the receiver's reactor polling, suppressing cross-process wake syscalls.
    /// The driver must poll carrier reads while this is enabled.
    pub fn carrier_receive_polling(&mut self, polling: bool) {
        self.receive_polling = polling;
    }

    /// Shared eventfd for incoming carrier publication, available after the preface.
    pub fn carrier_reader_signal(&self) -> Option<Arc<File>> {
        self.incoming.as_ref()?.carrier.reader_signal()
    }

    /// Shared eventfd for outbound registration acknowledgements and ring space.
    pub fn carrier_writer_signal(&self) -> Arc<File> {
        self.outgoing
            .carrier
            .writer_signal()
            .expect("outgoing carrier signal")
    }

    fn accept_descriptor(&mut self, file: File) -> io::Result<()> {
        if let Some(table) = &self.incoming
            && (table.carrier.reader_signal().is_none() || table.carrier.writer_signal().is_none())
        {
            table.carrier.accept_signal(file)?;
            return Ok(());
        }

        let mapping = Mapping::import(file)?;
        if mapping.kind == LEASES {
            let count = usize::try_from(
                mapping
                    .atomic64(crate::carrier::COUNT_OFFSET)
                    .load(Ordering::Acquire),
            )
            .map_err(|_| invalid("invalid SHM lease count"))?;

            if self.incoming.is_some()
                || !(1..=MAX_LEASES).contains(&count)
                || mapping.len() != crate::carrier::Carrier::length(count)
            {
                return Err(invalid("invalid SHM lease descriptor"));
            }

            self.incoming = Some(Arc::new(Table::new(
                mapping,
                count,
                crate::carrier::Carrier::new(count, false)?,
            )));

            return Ok(());
        }

        let Some(incoming) = &self.incoming else {
            return Err(invalid("missing SHM lease descriptor"));
        };

        self.inbound.insert(mapping, incoming)
    }

    /// The peer's lease table, or `missing` as an invalid-data error.
    fn incoming(&self, missing: &str) -> io::Result<&Arc<Table>> {
        self.incoming.as_ref().ok_or_else(|| invalid(missing))
    }

    fn reclaim(&mut self, needed: usize) {
        self.leases.reclaim(&self.outgoing, &self.peer, needed);
    }

    /// Prepare a persistent blocking wait after encode returned None.
    pub fn credit_wait(&self) -> CreditWait {
        CreditWait {
            table: self.outgoing.clone(),
            sequence: self.blocked_sequence,
        }
    }

    /// Duplicate the peer pidfd for reactor registration on the IO runtime.
    /// `None` means the process had already exited during connection setup.
    ///
    /// # Errors
    /// Returns an error if duplicating the pidfd fails.
    pub fn peer_descriptor(&self) -> io::Result<Option<std::fs::File>> {
        self.peer.descriptor()
    }

    /// Verify kernel process-exit proof after a pidfd reactor wake.
    pub fn peer_exited(&self) {
        let _ = self.peer.observe_exit();
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.peer.disconnect();
        if let Some(table) = &self.incoming {
            table.close_reader();
        }

        self.outgoing.stop();
        let _ = self.outgoing.carrier.notify_writer(&self.outgoing.mapping);
        // EOF alone never clears borrowed outgoing leases. Application pools
        // retain those leases; pidfd death or final receiver Drop returns them.
    }
}

#[cfg(all(test, not(miri)))]
#[path = "tests.rs"]
mod tests;
