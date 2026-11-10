use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use omq_proto::proto::transform::MessageEncoder;

/// Runtime-global pool of reusable compression encoders, shared across
/// all connections on a socket. Sized to `available_parallelism()`.
///
/// Encoders keep their warm `out_buf` and configured compression
/// context across borrows, avoiding per-message allocation.
pub(crate) struct CompressionPool {
    encoders: Mutex<Vec<MessageEncoder>>,
    in_flight: AtomicUsize,
    cap: usize,
}

impl std::fmt::Debug for CompressionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionPool")
            .field("cap", &self.cap)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl CompressionPool {
    pub(crate) fn new() -> Self {
        let cap = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        Self {
            encoders: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            cap,
        }
    }

    /// Borrow a pool encoder matching the primary's variant, syncing
    /// its full configuration. Returns `None` when all `cap` encoders are
    /// in flight. New encoders are created on demand up to `cap`.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn try_take(self: &Arc<Self>, primary: &MessageEncoder) -> Option<EncoderLoan> {
        {
            let mut pool = self.encoders.lock().unwrap();
            // Prefer an encoder whose context already has the primary's level.
            let pos = pool
                .iter()
                .position(|e| e.variant_matches(primary) && e.level() == primary.level())
                .or_else(|| pool.iter().position(|e| e.variant_matches(primary)));

            if let Some(pos) = pos {
                let mut encoder = pool.swap_remove(pos);
                drop(pool);
                self.in_flight.fetch_add(1, Ordering::Relaxed);
                encoder.sync_offload_config(primary);

                return Some(EncoderLoan {
                    encoder,
                    slot: InFlightSlot(self.clone()),
                });
            }
        }

        let prev = self.in_flight.fetch_add(1, Ordering::Relaxed);
        let slot = InFlightSlot(self.clone());

        if prev >= self.cap {
            return None;
        }

        Some(EncoderLoan {
            encoder: MessageEncoder::new_offload(primary),
            slot,
        })
    }

    pub(crate) fn clear(&self) {
        self.encoders.lock().unwrap().clear();
    }
}

/// A pool encoder on loan. `restore` keeps the warm encoder for reuse.
/// Dropping the loan instead, as when a closing connection drops an
/// unfinished offload or the encode panics, discards the encoder but still
/// frees its in-flight slot.
#[derive(Debug)]
pub(crate) struct EncoderLoan {
    pub(crate) encoder: MessageEncoder,
    slot: InFlightSlot,
}

impl EncoderLoan {
    pub(crate) fn restore(self) {
        let Self { encoder, slot } = self;
        slot.0.encoders.lock().unwrap().push(encoder);
    }
}

#[derive(Debug)]
struct InFlightSlot(Arc<CompressionPool>);

impl Drop for InFlightSlot {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod tests;
