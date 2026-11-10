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
mod tests;
