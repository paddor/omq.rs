//! Carrier registration, body encoding, and per-recipient lease state.
#![forbid(unsafe_code)]

use std::collections::{BTreeSet, VecDeque};
use std::fs::File;
use std::io::{self, IoSlice};
use std::os::fd::AsFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use omq_proto::{Message, Payload, Result, SocketType};

use crate::lease::{Borrowed, CreditWait, Lease, PENDING, Table, lease_completed};
use crate::mapping::{LEASES, Mapping, invalid};
use crate::pool::{Slot, find_slot};
use crate::process::Process;
use crate::wire::{
    self, BODY, COPIED, COPIED_BATCH, GENERATION_STEP, READER_CLOSED, REFERENCE_BATCH, STATE_BITS,
};
use crate::{
    BATCH_MAX_BYTES, BatchKind, COPY_THRESHOLD, MAX_LEASES, PREFACE_SIZE, SendPool, batch, body,
    check_body_size, copied_record, replace_body, socket,
};

/// Per-connection SHM encoding and lease state. Used by IO drivers and lanes;
/// caller threads continue to enqueue ordinary Message values.
#[derive(Debug)]
pub struct Connection {
    peer: Arc<Process>,
    outgoing: Arc<Table>,
    incoming: Option<Arc<Table>>,
    free: VecDeque<usize>,
    active: VecDeque<ActiveLease>,
    leased_messages: usize,
    generation: u64,
    send_pool: SendPool,
    regions: VecDeque<(u32, u64, Arc<Mapping>)>,
    retired_regions: VecDeque<(u32, u64, Arc<Mapping>)>,
    registered: VecDeque<(u32, u64)>,
    descriptors: VecDeque<File>,
    blocked_sequence: u32,
    hwm: usize,
    max_message_size: Option<usize>,
    received: VecDeque<Message>,
    views: crate::views::Views,
    sent_descriptors: u64,
    received_descriptors: u64,
    receive_polling: bool,
}

#[derive(Debug)]
struct ActiveLease {
    index: usize,
    generation: u64,
    messages: usize,
}

/// Whether a socket type supports SHM: CHANNEL, CLIENT/SERVER, SCATTER/GATHER,
/// PEER, and RADIO/DISH. Group and routing envelopes remain ordinary codec
/// frames; only the final application body uses shared storage.
pub const fn supports(socket_type: SocketType) -> bool {
    matches!(
        socket_type,
        SocketType::Channel
            | SocketType::Client
            | SocketType::Server
            | SocketType::Scatter
            | SocketType::Gather
            | SocketType::Peer
            | SocketType::Radio
            | SocketType::Dish
    )
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
        let outgoing = Arc::new(Table {
            mapping: Mapping::create(crate::carrier::Carrier::length(count), LEASES)?,
            count,
            stopped: AtomicBool::new(false),
            carrier: crate::carrier::Carrier::new(count, true)?,
        });
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
            free: (0..count).collect(),
            active: VecDeque::with_capacity(count),
            leased_messages: 0,
            generation: 0,
            send_pool,
            regions: VecDeque::new(),
            retired_regions: VecDeque::new(),
            registered: VecDeque::new(),
            descriptors,
            blocked_sequence: 0,
            hwm: count,
            max_message_size,
            received: VecDeque::with_capacity(crate::BATCH_MAX_MESSAGES),
            views: crate::views::Views::new(count.max(crate::BATCH_MAX_MESSAGES)),
            sent_descriptors: 0,
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
        let table = self
            .incoming
            .as_ref()
            .ok_or_else(|| invalid("missing SHM lease descriptor"))?;
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
        let written = socket::send(socket, bytes, &self.descriptors)?;
        if written != 0 && !self.descriptors.is_empty() {
            self.sent_descriptors = self
                .sent_descriptors
                .wrapping_add(self.descriptors.len() as u64);
            self.descriptors.clear();
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
            let incoming = self
                .incoming
                .as_ref()
                .ok_or_else(|| invalid("missing SHM lease descriptor"))?;
            incoming
                .carrier
                .acknowledge_imports(&incoming.mapping, self.received_descriptors)?;
        }
        Ok(received)
    }

    /// Whether descriptor registration must progress before publishing carrier bytes.
    pub fn has_pending_descriptors(&self) -> bool {
        !self.descriptors.is_empty()
    }

    /// Publish a bounded prefix of already framed carrier bytes into shared memory.
    /// Descriptors must be sent through [`Self::try_write`] and acknowledged first.
    ///
    /// # Errors
    /// Returns `WouldBlock` for registration/space pressure, or invalid shared state.
    pub fn try_write_carrier(&mut self, bytes: &[IoSlice<'_>]) -> io::Result<usize> {
        if self.has_pending_descriptors()
            || self.outgoing.carrier.imported(&self.outgoing.mapping) != self.sent_descriptors
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
        let table = self
            .incoming
            .as_ref()
            .ok_or_else(|| invalid("missing SHM carrier"))?;
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
        let table = self
            .incoming
            .as_ref()
            .ok_or_else(|| invalid("missing SHM carrier"))?;
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
            self.incoming = Some(Arc::new(Table {
                mapping,
                count,
                stopped: AtomicBool::new(false),
                carrier: crate::carrier::Carrier::new(count, false)?,
            }));
            return Ok(());
        }
        if self.incoming.is_none() {
            return Err(invalid("missing SHM lease descriptor"));
        }
        let identity = (mapping.id, mapping.token);
        self.regions
            .retain(|(id, token, _)| (*id, *token) != identity);
        self.retired_regions
            .retain(|(id, token, _)| (*id, *token) != identity);
        self.regions
            .push_back((mapping.id, mapping.token, Arc::new(mapping)));
        if self.regions.len() > socket::SEND_LIMIT {
            // Both sides retain the last 64 registrations in wire order.
            // Older regions remain discoverable while queued references need
            // them; borrowed owners independently retain their mappings.
            let pending = self.pending_regions();
            self.retired_regions
                .retain(|(id, token, _)| pending.contains(&(*id, *token)));
            let region = self.regions.pop_front().expect("registered region");
            if pending.contains(&(region.0, region.1)) {
                self.retired_regions.push_back(region);
            }
        }
        Ok(())
    }

    fn pending_regions(&self) -> BTreeSet<(u32, u64)> {
        let mut regions = BTreeSet::new();
        if let Some(table) = &self.incoming {
            for index in 0..table.count {
                if table.cell(index).load(Ordering::Acquire) & STATE_BITS == PENDING {
                    let id = table
                        .coordinates(index, wire::REGION_ID)
                        .load(Ordering::Relaxed) as u32;
                    let token = table
                        .coordinates(index, wire::REGION_TOKEN)
                        .load(Ordering::Relaxed);
                    regions.insert((id, token));
                    if table
                        .coordinates(index, wire::RECORD_KIND)
                        .load(Ordering::Relaxed)
                        == u64::from(REFERENCE_BATCH)
                        && let Some(mapping) = self.find_region(id, token)
                    {
                        let offset = table
                            .coordinates(index, wire::BODY_OFFSET)
                            .load(Ordering::Relaxed);
                        let length = table
                            .coordinates(index, wire::BODY_LENGTH)
                            .load(Ordering::Relaxed);
                        if let (Ok(offset), Ok(length)) =
                            (usize::try_from(offset), usize::try_from(length))
                            && mapping.contains(offset, length)
                            && let Ok(references) = batch::references(mapping.bytes(offset, length))
                        {
                            regions.extend(
                                references.map(|reference| (reference.id, reference.token)),
                            );
                        }
                    }
                }
            }
        }
        regions
    }

    fn find_region(&self, id: u32, token: u64) -> Option<&Arc<Mapping>> {
        self.regions
            .iter()
            .chain(self.retired_regions.iter())
            .find(|(registered, nonce, _)| *registered == id && *nonce == token)
            .map(|(_, _, mapping)| mapping)
    }

    fn reclaim(&mut self, needed: usize) {
        if self.hwm - self.leased_messages >= needed {
            return;
        }
        // Scan the complete bounded window before parking. A batch can need
        // more credit even while a few unused positions remain in the table.
        let mut budget = crate::LEASE_RECLAIM_BUDGET;
        for _ in 0..self.active.len() {
            if budget.exhausted() {
                break;
            }
            let lease = self.active.pop_front().expect("active lease");
            let _ = budget.account(std::mem::size_of::<ActiveLease>());
            if lease_completed(&self.outgoing, &self.peer, lease.index, lease.generation) {
                self.free.push_back(lease.index);
                self.leased_messages -= lease.messages;
            } else {
                self.active.push_back(lease);
            }
        }
    }

    fn register_region(&mut self, mapping: &Mapping) -> io::Result<bool> {
        let identity = (mapping.id, mapping.token);
        if self.registered.contains(&identity) {
            return Ok(true);
        }
        if self.descriptors.len() == socket::SEND_LIMIT {
            return Ok(false);
        }
        self.descriptors.push_back(mapping.descriptor()?);
        if self.registered.len() == socket::SEND_LIMIT {
            self.registered.pop_front();
        }
        self.registered.push_back(identity);
        Ok(true)
    }

    /// Convert the last application body to copied bytes or a region reference.
    /// `None` means pool/lease backpressure: retain the message and await credit.
    /// Automatic storage exhaustion uses copied carrier bytes. Existing pooled
    /// bodies keep their mapped storage and reserve one recipient lease.
    ///
    /// # Errors
    /// Returns an error for a missing body, closed reader, or failed OS operation.
    pub fn encode(&mut self, message: &Message) -> Result<Option<Message>> {
        self.encode_record(message, 1, BODY)
    }

    fn encode_record(
        &mut self,
        message: &Message,
        count: usize,
        kind: u8,
    ) -> Result<Option<Message>> {
        self.prepare_encode()?;
        let original = body(message)?;
        if kind == BODY && original.len() <= COPY_THRESHOLD {
            return Ok(Some(copied_record(message)?));
        }
        self.reclaim(count);
        if self.hwm - self.leased_messages < count {
            return Ok(None);
        }
        let prepared;
        let (source, slot) = if let Some(slot) = find_slot(original) {
            (original, slot)
        } else {
            prepared = match self.send_pool.prepare(message)? {
                Some(prepared) => prepared,
                None if kind == BODY => return Ok(Some(copied_record(message)?)),
                None => return Ok(None),
            };
            let source = body(&prepared)?;
            (source, find_slot(source).expect("prepared SHM body"))
        };
        if !self.register_sources(&slot, kind)? {
            return Ok(None);
        }
        let offset = source.as_ptr() as usize - slot.mapping.address();
        let reference = self.publish_lease(&slot, count, kind, offset, source.len())?;
        Ok(Some(replace_body(
            message,
            Message::from_slice(&reference.encode()),
        )))
    }

    fn register_sources(&mut self, slot: &Slot, kind: u8) -> Result<bool> {
        if kind == REFERENCE_BATCH {
            let state = slot.state.lock().expect("SHM batch sources");
            if state.published_batch.is_none() {
                return Err(invalid("SHM reference batch was not prepared").into());
            }
            for mapping in &state.regions {
                if !self.register_region(mapping)? {
                    return Ok(false);
                }
            }
        }
        Ok(self.register_region(&slot.mapping)?)
    }

    fn publish_lease(
        &mut self,
        slot: &Slot,
        count: usize,
        kind: u8,
        offset: usize,
        length: usize,
    ) -> Result<wire::Reference> {
        let generation = self
            .generation
            .checked_add(GENERATION_STEP)
            .ok_or_else(|| omq_proto::Error::Protocol("SHM lease generation exhausted".into()))?;
        let reference = wire::Reference {
            kind,
            id: slot.mapping.id,
            token: slot.mapping.token,
            offset,
            length,
            lease: self.free.pop_front().expect("reserved lease credit"),
            generation,
        };
        self.generation = generation;
        self.leased_messages += count;
        // Publication acquires on borrow; paired fences prevent a racing
        // reader close and publication from both missing each other.
        if self.outgoing.publish(reference) {
            self.outgoing.notify();
        }
        self.active.push_back(ActiveLease {
            index: reference.lease,
            generation,
            messages: count,
        });
        slot.track_lease(
            Lease {
                table: self.outgoing.clone(),
                peer: self.peer.clone(),
                index: reference.lease,
                generation,
            },
            kind,
        );
        Ok(reference)
    }

    /// Frame copied bodies directly; false retains the message for credit.
    ///
    /// # Errors
    /// Returns the same encoding and connection errors as [`Self::encode`].
    pub fn encode_and_frame(
        &mut self,
        message: &Message,
        frames: &mut omq_proto::frame_buffer::FrameBuffer,
    ) -> Result<bool> {
        let index = message
            .len()
            .checked_sub(1)
            .ok_or_else(|| omq_proto::Error::Protocol("missing SHM message body".into()))?;
        if message.part_slice(index).expect("message body").len() <= COPY_THRESHOLD {
            self.prepare_encode()?;
            crate::frame_copied(message, frames);
            return Ok(true);
        }
        let Some(wire) = self.encode(message)? else {
            return Ok(false);
        };
        frames.frame(&wire);
        Ok(true)
    }

    /// Publish a prepared batch with one lease and logical HWM credit.
    /// `false` means retain this batch and wait for connection credit.
    ///
    /// # Errors
    /// Returns an error for invalid batches, a closed reader, or OS failures.
    pub fn encode_batch_and_frame(
        &mut self,
        message: &Message,
        kind: BatchKind,
        frames: &mut omq_proto::frame_buffer::FrameBuffer,
    ) -> Result<bool> {
        let body = message
            .part_slice(message.len().saturating_sub(1))
            .ok_or_else(|| invalid("missing SHM batch"))?;
        let count = batch::count(body)?;
        if count > self.hwm {
            return Err(invalid("SHM batch exceeds send HWM").into());
        }
        let Some(wire) = self.encode_record(message, count, kind.record())? else {
            return Ok(false);
        };
        frames.frame(&wire);
        Ok(true)
    }

    fn prepare_encode(&mut self) -> Result<()> {
        self.blocked_sequence = self.outgoing.sequence();
        // Reader closure must remain observable without free lease credit.
        if self
            .outgoing
            .mapping
            .atomic64(READER_CLOSED)
            .load(Ordering::Acquire)
            != 0
        {
            return Err(omq_proto::Error::Closed);
        }
        Ok(())
    }

    /// Decode a single body. References borrow mapped storage; carrier records
    /// create an ordinary owned payload. Clones and byte views retain a lease.
    ///
    /// # Errors
    /// Returns an error for stale leases, invalid coordinates, or oversized bodies.
    pub fn decode(&mut self, message: Message) -> Result<Message> {
        self.decode_record(message, BODY)
    }

    fn decode_record(&mut self, mut message: Message, kind: u8) -> Result<Message> {
        let bytes = body(&message)?;
        if bytes.first() == Some(&COPIED) {
            self.check_size(&message, bytes.len() - 1)?;
            let payload = Payload::from_slice(&bytes[1..]);
            message.replace_last_payload(payload);
            return Ok(message);
        }
        let reference = wire::Reference::decode(bytes, kind)?;
        if kind == BODY {
            self.check_size(&message, reference.length)?;
        } else if reference.length > BATCH_MAX_BYTES {
            return Err(invalid("SHM batch is too large").into());
        }
        message.replace_last_payload(self.borrow(reference, kind == BODY)?);
        Ok(message)
    }

    fn borrow(&self, reference: wire::Reference, cached: bool) -> Result<Payload> {
        let table = self
            .incoming
            .as_ref()
            .ok_or_else(|| invalid("missing SHM preface"))?
            .clone();
        let mapping = self
            .find_region(reference.id, reference.token)
            .cloned()
            .ok_or_else(|| invalid("SHM region descriptor was not registered"))?;
        if !mapping.contains(reference.offset, reference.length) {
            return Err(invalid("SHM reference is outside its region").into());
        }
        table.borrow(reference)?;
        let borrowed = Borrowed {
            mapping,
            offset: reference.offset,
            length: reference.length,
            table,
            index: reference.lease,
            generation: reference.generation,
        };
        Ok(if cached {
            self.views.borrowed(borrowed)
        } else {
            // Batch children release this shared owner independently. Keep
            // its non-final Arc decrements outside the view recycler gate.
            Payload::from_shared_owner(Arc::new(borrowed))
        })
    }

    /// Decode a record and retain a bounded batch's independently owned bodies.
    /// Drain [`Self::poll_received`] before providing the next carrier record.
    ///
    /// # Errors
    /// Returns an error for undrained batches or invalid/oversized records.
    pub fn decode_received(&mut self, message: Message) -> Result<Message> {
        if !self.received.is_empty() {
            return Err(invalid("SHM batch must be drained before the next record").into());
        }
        let kind = message
            .part_slice(message.len().saturating_sub(1))
            .and_then(|body| body.first().copied());
        let Some(kind @ (COPIED_BATCH | REFERENCE_BATCH)) = kind else {
            return self.decode(message);
        };
        let decoded = self.decode_record(message, kind)?;
        let mut received = std::mem::take(&mut self.received);
        let result = if kind == COPIED_BATCH {
            batch::decode(&decoded, self.max_message_size, &self.views, &mut received)
        } else {
            batch::decode_references(
                &decoded,
                self.max_message_size,
                &self.views,
                |id, token| self.find_region(id, token).cloned(),
                &mut received,
            )
        };
        if result.is_err() {
            received.clear();
        }
        self.received = received;
        result?;
        Ok(self.received.pop_front().expect("nonempty SHM batch"))
    }

    /// Continue a decoded batch before polling the next carrier record.
    pub fn poll_received(&mut self) -> Option<Message> {
        self.received.pop_front()
    }

    fn check_size(&self, message: &Message, length: usize) -> Result<()> {
        check_body_size(message, length, self.max_message_size)
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
        self.outgoing.stopped.store(true, Ordering::Release);
        self.outgoing.notify();
        let _ = self.outgoing.carrier.notify_writer(&self.outgoing.mapping);
        // EOF alone never clears borrowed outgoing leases. Application pools
        // retain those leases; pidfd death or final receiver Drop returns them.
    }
}

#[cfg(all(test, not(miri)))]
#[path = "tests.rs"]
mod tests;
