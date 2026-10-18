//! Bounded SPSC carrier bytes alongside the generation-tagged lease cells.
#![forbid(unsafe_code)]

use std::fs::File;
use std::io::{self, IoSlice, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, OnceLock};

use crate::coordination::CarrierState;
use crate::mapping::{HEADER, Mapping, invalid};
use crate::wire::CELL_BYTES;

pub(super) const COUNT_OFFSET: usize = HEADER - std::mem::size_of::<u64>();
const CACHE_LINE: usize = 64;
const WRITE: usize = 0;
const READ: usize = CACHE_LINE;
const IMPORTED: usize = 2 * CACHE_LINE;
const READER_PARKED: usize = IMPORTED + std::mem::size_of::<u64>();
const WRITER_PARKED: usize = READER_PARKED + std::mem::size_of::<u32>();
const CONTROL_BYTES: usize = 3 * CACHE_LINE;
pub(super) const CAPACITY: usize = 64 * 1024;
pub(super) const MAX_SLICES: usize = 128;

#[derive(Debug)]
pub(super) struct Carrier {
    offset: usize,
    reader: OnceLock<Arc<File>>,
    writer: OnceLock<Arc<File>>,
}

impl Carrier {
    fn state<'a>(&self, mapping: &'a Mapping) -> CarrierState<'a> {
        CarrierState {
            write: mapping.atomic64(self.offset + WRITE),
            read: mapping.atomic64(self.offset + READ),
            imported: mapping.atomic64(self.offset + IMPORTED),
            reader_parked: mapping.atomic32(self.offset + READER_PARKED),
            writer_parked: mapping.atomic32(self.offset + WRITER_PARKED),
            capacity: CAPACITY,
        }
    }
    pub(super) fn length(count: usize) -> usize {
        Self::offset(count) + CONTROL_BYTES + CAPACITY
    }

    fn offset(count: usize) -> usize {
        (HEADER + count * CELL_BYTES).next_multiple_of(CACHE_LINE)
    }

    pub(super) fn new(count: usize, outgoing: bool) -> io::Result<Self> {
        let carrier = Self {
            offset: Self::offset(count),
            reader: OnceLock::new(),
            writer: OnceLock::new(),
        };
        if outgoing {
            carrier
                .reader
                .set(Arc::new(eventfd()?))
                .expect("new reader signal");
            carrier
                .writer
                .set(Arc::new(eventfd()?))
                .expect("new writer signal");
        }
        Ok(carrier)
    }

    pub(super) fn accept_signal(&self, file: File) -> io::Result<bool> {
        let target = if self.reader.get().is_none() {
            &self.reader
        } else if self.writer.get().is_none() {
            &self.writer
        } else {
            return Ok(false);
        };
        if !is_eventfd(&file)? {
            return Err(invalid("invalid SHM carrier notifier"));
        }
        target
            .set(Arc::new(file))
            .expect("unregistered carrier signal");
        Ok(true)
    }

    pub(super) fn reader_signal(&self) -> Option<Arc<File>> {
        self.reader.get().cloned()
    }

    pub(super) fn writer_signal(&self) -> Option<Arc<File>> {
        self.writer.get().cloned()
    }

    pub(super) fn imported(&self, mapping: &Mapping) -> u64 {
        self.state(mapping).imported()
    }

    pub(super) fn acknowledge_imports(&self, mapping: &Mapping, count: u64) -> io::Result<()> {
        if self.state(mapping).acknowledge_imports(count) {
            self.signal_writer()?;
        }
        Ok(())
    }

    pub(super) fn park_writer(&self, mapping: &Mapping) {
        self.state(mapping).park_writer();
    }

    pub(super) fn arm_reader(&self, mapping: &Mapping, polling: bool) -> io::Result<bool> {
        self.state(mapping)
            .arm_reader(polling)
            .ok_or_else(|| invalid("invalid SHM carrier cursor"))
    }

    pub(super) fn write(&self, mapping: &Mapping, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        let mut slices = slices
            .iter()
            .filter(|slice| !slice.is_empty())
            .take(MAX_SLICES)
            .peekable();
        if slices.peek().is_none() {
            return Ok(0);
        }
        let state = self.state(mapping);
        let window = state
            .write_window()
            .ok_or_else(|| invalid("invalid SHM carrier cursor"))?;
        let write = window.cursor;
        let mut remaining = window.length;
        let mut position = write as usize % CAPACITY;
        let mut copied = 0;
        for slice in slices {
            let mut bytes = &slice[..slice.len().min(remaining)];
            while !bytes.is_empty() {
                let length = bytes.len().min(CAPACITY - position);
                mapping.write_carrier(self.offset + CONTROL_BYTES + position, &bytes[..length]);
                bytes = &bytes[length..];
                position = (position + length) % CAPACITY;
                copied += length;
                remaining -= length;
            }
            if remaining == 0 {
                break;
            }
        }
        if copied == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if state.publish(write.wrapping_add(copied as u64)) {
            signal(self.reader.get().expect("carrier reader signal"))?;
        }
        Ok(copied)
    }

    pub(super) fn read(&self, mapping: &Mapping, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let state = self.state(mapping);
        let window = state
            .read_window()
            .ok_or_else(|| invalid("invalid SHM carrier cursor"))?;
        let length = window.length.min(output.len());
        if length == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let read = window.cursor;
        let position = read as usize % CAPACITY;
        let first = length.min(CAPACITY - position);
        let base = self.offset + CONTROL_BYTES;
        mapping.read_carrier(base + position, &mut output[..first]);
        mapping.read_carrier(base, &mut output[first..length]);
        if state.return_bytes(read.wrapping_add(length as u64)) {
            self.signal_writer()?;
        }
        Ok(length)
    }

    pub(super) fn notify_writer(&self, mapping: &Mapping) -> io::Result<()> {
        if self.state(mapping).notify_writer() {
            self.signal_writer()?;
        }
        Ok(())
    }

    fn signal_writer(&self) -> io::Result<()> {
        signal(
            self.writer
                .get()
                .ok_or_else(|| invalid("missing carrier writer signal"))?,
        )
    }

    #[cfg(all(test, miri))]
    pub(super) fn testing(count: usize) -> Self {
        Self {
            offset: Self::offset(count),
            reader: OnceLock::new(),
            writer: OnceLock::new(),
        }
    }
}

fn eventfd() -> io::Result<File> {
    Ok(File::from(rustix::event::eventfd(
        0,
        rustix::event::EventfdFlags::CLOEXEC | rustix::event::EventfdFlags::NONBLOCK,
    )?))
}

/// An eventfd shares the anonymous inode with a local eventfd, which excludes
/// files, devices, sockets, pidfds, and secure anonymous inodes. Of the other
/// anonymous descriptors, only an eventfd accepts an 8-byte write; adding zero
/// changes nothing.
fn is_eventfd(file: &File) -> io::Result<bool> {
    let metadata = file.metadata()?;
    let local = eventfd()?.metadata()?;
    let flags = rustix::fs::fcntl_getfl(file)?;
    Ok(metadata.dev() == local.dev()
        && metadata.ino() == local.ino()
        && flags.contains(rustix::fs::OFlags::RDWR | rustix::fs::OFlags::NONBLOCK)
        && rustix::io::write(file, &0_u64.to_ne_bytes()) == Ok(8))
}

fn signal(file: &File) -> io::Result<()> {
    // write_all retries interruptions; a saturated eventfd is already readable.
    match (&*file).write_all(&1_u64.to_ne_bytes()) {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
        result => result,
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::mapping::LEASES;
    use std::io::Read;
    use std::sync::atomic::Ordering;

    fn pair() -> (Carrier, Mapping, Carrier, Mapping) {
        let sender = Carrier::new(2, true).unwrap();
        let receiver = Carrier::new(2, false).unwrap();
        for signal in [&sender.reader, &sender.writer] {
            receiver
                .accept_signal(signal.get().unwrap().try_clone().unwrap())
                .unwrap();
        }
        let outgoing = Mapping::create(Carrier::length(2), LEASES).unwrap();
        let incoming = Mapping::import(outgoing.descriptor().unwrap()).unwrap();
        (sender, outgoing, receiver, incoming)
    }

    fn counter(file: &File) -> io::Result<u64> {
        let mut bytes = [0; std::mem::size_of::<u64>()];
        (&*file).read_exact(&mut bytes)?;
        Ok(u64::from_ne_bytes(bytes))
    }

    #[test]
    fn notifiers_must_be_nonblocking_eventfds() {
        use rustix::fs::OFlags;
        let carrier = Carrier::new(2, false).unwrap();
        let epoll = File::from(
            rustix::event::epoll::create(rustix::event::epoll::CreateFlags::CLOEXEC).unwrap(),
        );
        rustix::fs::fcntl_setfl(&epoll, OFlags::NONBLOCK).unwrap();
        let blocking =
            File::from(rustix::event::eventfd(0, rustix::event::EventfdFlags::CLOEXEC).unwrap());
        let (pipe, _writer) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::NONBLOCK).unwrap();
        let memfd = File::from(
            rustix::fs::memfd_create("omq-notifier", rustix::fs::MemfdFlags::CLOEXEC).unwrap(),
        );
        for file in [epoll, blocking, File::from(pipe), memfd] {
            assert_eq!(
                carrier.accept_signal(file).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        let notifier = eventfd().unwrap();
        assert!(
            carrier
                .accept_signal(notifier.try_clone().unwrap())
                .unwrap()
        );
        assert_eq!(
            counter(&notifier).unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "the probe must not signal"
        );
    }

    #[test]
    fn wraps_and_reuses_only_returned_bytes() {
        let (sender, outgoing, receiver, incoming) = pair();
        let first = vec![1; CAPACITY - 7];
        assert_eq!(
            sender.write(&outgoing, &[IoSlice::new(&first)]).unwrap(),
            first.len()
        );
        let mut output = vec![0; CAPACITY - 11];
        assert_eq!(receiver.read(&incoming, &mut output).unwrap(), output.len());
        assert!(output.iter().all(|&byte| byte == 1));
        let second = vec![2; CAPACITY];
        assert_eq!(
            sender.write(&outgoing, &[IoSlice::new(&second)]).unwrap(),
            CAPACITY - 4
        );
        assert_eq!(
            sender
                .write(&outgoing, &[IoSlice::new(b"full")])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        output.resize(CAPACITY, 0);
        assert_eq!(receiver.read(&incoming, &mut output).unwrap(), CAPACITY);
        assert_eq!(&output[..4], &[1; 4]);
        assert!(output[4..].iter().all(|&byte| byte == 2));
        assert_eq!(
            receiver.read(&incoming, &mut output).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn notifies_only_armed_consumers_and_producers() {
        let (sender, outgoing, receiver, incoming) = pair();
        assert!(!receiver.arm_reader(&incoming, false).unwrap());
        sender.write(&outgoing, &[IoSlice::new(b"one")]).unwrap();
        sender.write(&outgoing, &[IoSlice::new(b"two")]).unwrap();
        assert_eq!(counter(receiver.reader.get().unwrap()).unwrap(), 1);
        sender.park_writer(&outgoing);
        let mut output = [0; 6];
        assert_eq!(receiver.read(&incoming, &mut output).unwrap(), 6);
        assert_eq!(&output, b"onetwo");
        assert_eq!(counter(sender.writer.get().unwrap()).unwrap(), 1);
        assert!(!receiver.arm_reader(&incoming, true).unwrap());
        sender
            .write(&outgoing, &[IoSlice::new(b"polling")])
            .unwrap();
        assert_eq!(
            counter(receiver.reader.get().unwrap()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        // Arm after publication must see data without relying on a wake.
        assert!(receiver.arm_reader(&incoming, false).unwrap());
    }

    #[test]
    fn registration_acknowledgment_closes_both_arm_races() {
        let (sender, outgoing, receiver, incoming) = pair();
        receiver.acknowledge_imports(&incoming, 3).unwrap();
        sender.park_writer(&outgoing);
        assert_eq!(sender.imported(&outgoing), 3);
        receiver.acknowledge_imports(&incoming, 4).unwrap();
        assert_eq!(sender.imported(&outgoing), 4);
        assert_eq!(counter(sender.writer.get().unwrap()).unwrap(), 1);
    }

    #[test]
    fn rejects_corrupt_cursors_and_handles_empty_buffers() {
        let (sender, outgoing, receiver, incoming) = pair();
        assert_eq!(sender.write(&outgoing, &[]).unwrap(), 0);
        assert_eq!(receiver.read(&incoming, &mut []).unwrap(), 0);
        outgoing
            .atomic64(sender.offset + WRITE)
            .store(CAPACITY as u64 + 1, Ordering::Release);
        assert_eq!(
            receiver.read(&incoming, &mut [0]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            sender
                .write(&outgoing, &[IoSlice::new(b"bad")])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn leading_empty_slices_do_not_report_a_zero_write() {
        let (sender, outgoing, receiver, incoming) = pair();
        let mut slices = vec![IoSlice::new(&[]); MAX_SLICES + 1];
        slices.push(IoSlice::new(b"data"));
        assert_eq!(sender.write(&outgoing, &slices).unwrap(), 4);
        let mut output = [0; 4];
        assert_eq!(receiver.read(&incoming, &mut output).unwrap(), 4);
        assert_eq!(&output, b"data");
    }

    #[test]
    fn cursor_overflow_preserves_order() {
        let (sender, outgoing, receiver, incoming) = pair();
        let cursor = u64::MAX - 3;
        outgoing
            .atomic64(sender.offset + WRITE)
            .store(cursor, Ordering::Relaxed);
        outgoing
            .atomic64(sender.offset + READ)
            .store(cursor, Ordering::Relaxed);
        sender
            .write(&outgoing, &[IoSlice::new(b"overflow")])
            .unwrap();
        let mut output = [0; 8];
        assert_eq!(receiver.read(&incoming, &mut output).unwrap(), output.len());
        assert_eq!(&output, b"overflow");
    }

    #[test]
    fn concurrent_partial_transfers_preserve_publication_order() {
        let (sender, outgoing, receiver, incoming) = pair();
        let bytes: Vec<_> = (0..2 * CAPACITY + 937)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut offset = 0;
                while offset < bytes.len() {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "carrier writer stalled"
                    );
                    sender.park_writer(&outgoing);
                    let end = (offset + 97).min(bytes.len());
                    match sender.write(&outgoing, &[IoSlice::new(&bytes[offset..end])]) {
                        Ok(length) => offset += length,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("carrier write: {error}"),
                    }
                }
            });
            let mut offset = 0;
            let mut output = [0; 257];
            while offset < bytes.len() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "carrier reader stalled"
                );
                receiver.arm_reader(&incoming, false).unwrap();
                match receiver.read(&incoming, &mut output) {
                    Ok(length) => {
                        assert_eq!(&output[..length], &bytes[offset..offset + length]);
                        offset += length;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(error) => panic!("carrier read: {error}"),
                }
            }
        });
    }
}
