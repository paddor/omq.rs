use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    /// its dict state. Returns `None` when all `cap` encoders are
    /// in flight. New encoders are created on demand up to `cap`.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn try_take(&self, primary: &MessageEncoder) -> Option<MessageEncoder> {
        {
            let mut pool = self.encoders.lock().unwrap();
            if let Some(pos) = pool.iter().position(|e| e.variant_matches(primary)) {
                let mut enc = pool.swap_remove(pos);
                drop(pool);
                self.in_flight.fetch_add(1, Ordering::Relaxed);
                enc.sync_dict(primary);
                return Some(enc);
            }
        }
        let prev = self.in_flight.fetch_add(1, Ordering::Relaxed);
        if prev >= self.cap {
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(MessageEncoder::new_offload(primary))
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn put(&self, enc: MessageEncoder) {
        self.encoders.lock().unwrap().push(enc);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn clear(&self) {
        self.encoders.lock().unwrap().clear();
    }
}
