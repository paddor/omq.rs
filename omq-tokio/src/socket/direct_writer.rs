//! Caller-side TCP writes for the latency profile.

use std::io;
use std::io::IoSlice;
use std::io::Write;
use std::sync::{Mutex, MutexGuard};

/// Caller-side TCP writer for the latency profile. It uses a duplicated
/// nonblocking descriptor, so sends do not enter the connection driver's
/// reactor loop.
#[derive(Debug)]
pub(crate) struct DirectTcpWriter {
    state: Mutex<DirectWriteState>,
}

#[derive(Debug)]
pub(crate) struct DirectWriteState<W = std::net::TcpStream> {
    stream: W,
    ownership: crate::engine::write_ownership::WriteOwnership,
}

impl DirectTcpWriter {
    pub(crate) fn new(stream: std::net::TcpStream) -> Self {
        Self {
            state: Mutex::new(DirectWriteState {
                stream,
                ownership: crate::engine::write_ownership::WriteOwnership::new(),
            }),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, DirectWriteState> {
        self.state.lock().expect("direct writer state")
    }

    pub(crate) fn claim_driver(&self) -> bool {
        self.lock().ownership.claim_driver()
    }

    /// Queue observations must happen under the same lock as send admission.
    pub(crate) fn publish_idle(&self, empty: impl FnOnce() -> bool) {
        self.lock().ownership.publish_idle(empty);
    }
}

impl<W: Write> DirectWriteState<W> {
    pub(crate) fn is_closed(&self) -> bool {
        self.ownership.is_closed()
    }

    pub(crate) fn close(&mut self) {
        self.ownership.close();
    }

    pub(crate) fn queued(&mut self) {
        self.ownership.claim_driver();
    }

    /// Called with admission locked. Ineligible means enqueue the owned message.
    pub(crate) fn try_send(
        &mut self,
        slot: &crate::engine::transmit_slot::PeerTransmitSlot,
        message: &omq_proto::Message,
    ) -> crate::engine::transmit_slot::TryFrameResult {
        use crate::engine::transmit_slot::TryFrameResult;
        if self.is_closed() {
            return TryFrameResult::Dead;
        }

        if !self.ownership.is_idle() {
            return TryFrameResult::Ineligible;
        }

        let result = slot.try_encode_without_signal(message);
        if result != TryFrameResult::Ok {
            return result;
        }

        // Admission succeeded. A subsequent peer failure must not become a
        // socket-closed error, nor cause the caller to retry accepted bytes.
        match slot.try_direct_write(|slices| self.try_write(slices)) {
            Ok(()) if slot.is_empty() => {}
            Ok(()) => {
                self.queued();
                slot.data_signal.mark();
            }
            Err(_) => {
                self.close();
                slot.data_signal.mark();
            }
        }

        TryFrameResult::Ok
    }

    fn try_write(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        let written = match slices {
            [one] => self.stream.write(one),
            _ => self.stream.write_vectored(slices),
        };

        match written {
            Ok(0) => Err(io::Error::new(io::ErrorKind::WriteZero, "tcp write")),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests;
