//! WebSocket-specific bounded assembly state.
//!
//! Multipart parts and fragments each cap at 65,536, counting empty items.
//! Interleaved control frames do not reset assembly counts. The tokio backend
//! enables parser service budgets; standalone codecs opt in with
//! `ConnectionConfig::ws_input_budget`. A turn stops between frames at 256
//! frames or 256 KiB, checking its 1 ms target every 64 frames. One large frame
//! operation can exceed that time target; these limits are not byte admission.
//! Unmasked complete bodies may share input storage; masked/spanning bodies
//! require assembly. CLOSE queues once and discards the unstarted pending PONG.
//! PONG coalescing bounds backlog, but ordered data can still delay control.

use bytes::BytesMut;
use std::time::{Duration, Instant};

pub(super) const MAX_PARTS: usize = 65_536;
pub(super) const MAX_FRAGMENTS: usize = 65_536;

pub(super) const SERVICE_FRAMES: usize = 256;
pub(super) const SERVICE_BYTES: usize = 256 * 1024;
const SERVICE_TIME: Duration = Duration::from_millis(1);

pub(super) struct Service {
    frames: usize,
    bytes: usize,
    started: Instant,
}

impl Service {
    pub(super) fn new() -> Self {
        Self {
            frames: 0,
            bytes: 0,
            started: Instant::now(),
        }
    }

    pub(super) fn account(&mut self, bytes: usize) -> bool {
        self.frames += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        self.frames < SERVICE_FRAMES
            && self.bytes < SERVICE_BYTES
            && (!self.frames.is_multiple_of(64) || self.started.elapsed() < SERVICE_TIME)
    }
}

#[derive(Debug)]
pub(super) struct Fragment {
    pub(super) bytes: BytesMut,
    pub(super) count: usize,
}

/// One staged PONG is immutable until acknowledged by the writer. Further
/// PINGs replace only the bounded pending payload, never an active wire frame.
#[derive(Debug, Default)]
pub(super) struct Control {
    pub(super) pong_remaining: Option<usize>,
    pub(super) pending_pong: Option<Pong>,
    /// Data arriving after local CLOSE is discarded incrementally while
    /// waiting for the peer's CLOSE; it must not allocate an assembly buffer.
    pub(super) skip_payload: usize,
}

#[derive(Debug)]
pub(super) struct Pong {
    data: [u8; 125],
    len: u8,
}

impl Pong {
    pub(super) fn new(payload: &[u8]) -> Self {
        debug_assert!(payload.len() <= 125);
        let mut data = [0; 125];
        data[..payload.len()].copy_from_slice(payload);
        Self {
            data,
            len: payload.len() as u8,
        }
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        &self.data[..usize::from(self.len)]
    }
}

#[cfg(test)]
mod tests;
