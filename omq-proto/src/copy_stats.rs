//! Message-byte copy accounting for the copy-budget tests.
//!
//! Every steady-state data-path copy of message bytes records its site. With
//! the `copy-stats` feature disabled, recording compiles to nothing. Copies
//! bounded by the inline limits (55 B per message, 62 B per part) and
//! transform output (compression, encryption) are not recorded.

#[cfg(feature = "copy-stats")]
use std::sync::atomic::{AtomicU64, Ordering};

/// Where message bytes were copied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Site {
    /// Message framed contiguously into a send arena.
    FrameInline,
    /// Pre-framed fan-out bytes appended to a peer arena.
    PreFramed,
    /// Send arena copied out for a write batch.
    ArenaDrain,
    /// Fan-out frame copied or flattened into a per-peer chunk.
    FanOutChunk,
    /// Fan-out frame retained for peers that were full.
    FanOutPrepared,
    /// Small send chunks merged for an owned-chunk writer.
    Coalesce,
    /// Received frame assembled from several read chunks.
    RecvAssemble,
    /// Large-frame payload prefix moved out of the read buffer.
    RecvLargePrefix,
    /// Received payload copied to bound its retained storage.
    BoundStorage,
    /// WebSocket framing or unmasking copy.
    WebSocket,
}

impl Site {
    pub const ALL: [Self; 10] = [
        Self::FrameInline,
        Self::PreFramed,
        Self::ArenaDrain,
        Self::FanOutChunk,
        Self::FanOutPrepared,
        Self::Coalesce,
        Self::RecvAssemble,
        Self::RecvLargePrefix,
        Self::BoundStorage,
        Self::WebSocket,
    ];

    /// Sites on the receive path. All others copy on the send path.
    pub fn is_recv(self) -> bool {
        matches!(
            self,
            Self::RecvAssemble | Self::RecvLargePrefix | Self::BoundStorage
        )
    }
}

#[cfg(feature = "copy-stats")]
static COPIED: [AtomicU64; Site::ALL.len()] = [const { AtomicU64::new(0) }; Site::ALL.len()];

/// Record `bytes` copied at `site`.
#[inline]
pub fn record(site: Site, bytes: usize) {
    #[cfg(feature = "copy-stats")]
    COPIED[site as usize].fetch_add(bytes as u64, Ordering::Relaxed);
    #[cfg(not(feature = "copy-stats"))]
    let _ = (site, bytes);
}

/// Copied bytes per site since the last [`reset`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CopyCounts([u64; Site::ALL.len()]);

impl CopyCounts {
    pub fn get(&self, site: Site) -> u64 {
        self.0[site as usize]
    }

    pub fn send(&self) -> u64 {
        self.sum(|site| !site.is_recv())
    }

    pub fn recv(&self) -> u64 {
        self.sum(Site::is_recv)
    }

    fn sum(&self, keep: impl Fn(Site) -> bool) -> u64 {
        Site::ALL
            .into_iter()
            .filter(|site| keep(*site))
            .map(|site| self.get(site))
            .sum()
    }
}

impl std::fmt::Display for CopyCounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for site in Site::ALL {
            let bytes = self.get(site);
            if bytes == 0 {
                continue;
            }
            if !first {
                f.write_str(", ")?;
            }
            first = false;
            write!(f, "{site:?}={bytes}")?;
        }
        if first {
            f.write_str("none")?;
        }
        Ok(())
    }
}

/// Current counters. Always zero without the `copy-stats` feature.
pub fn snapshot() -> CopyCounts {
    #[cfg(feature = "copy-stats")]
    {
        CopyCounts(std::array::from_fn(|i| COPIED[i].load(Ordering::Relaxed)))
    }
    #[cfg(not(feature = "copy-stats"))]
    CopyCounts::default()
}

/// Zero all counters.
pub fn reset() {
    #[cfg(feature = "copy-stats")]
    for counter in &COPIED {
        counter.store(0, Ordering::Relaxed);
    }
}

/// Whether recording is compiled in.
pub const ENABLED: bool = cfg!(feature = "copy-stats");
