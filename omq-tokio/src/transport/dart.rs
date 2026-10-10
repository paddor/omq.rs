//! Reliable UDP messages with bounded retention and reusable body storage.

mod io;
mod pool;
pub(crate) mod worker;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use omq_proto::DartOptions;

use crate::PayloadPool;
pub use io::{DartIo, ReceiveBatch, ReceivedDatagram};

/// Shared by the socket handle and its endpoint workers. TCP-only sockets
/// receive storage is configured explicitly before endpoint setup.
#[derive(Debug)]
pub(crate) struct SocketState {
    options: DartOptions,
    receive_pool: OnceLock<Option<PayloadPool>>,
    pub(crate) counters: Counters,
    pub(crate) peers: std::sync::Arc<tokio::sync::Semaphore>,
    identity: OnceLock<bytes::Bytes>,
    carriers: Mutex<Vec<Weak<DartIo>>>,
}

impl SocketState {
    pub(crate) fn new(options: DartOptions, socket_type: omq_proto::SocketType) -> Self {
        Self {
            options,
            receive_pool: OnceLock::new(),
            counters: Counters::default(),
            peers: std::sync::Arc::new(tokio::sync::Semaphore::new(
                if socket_type == omq_proto::SocketType::Channel {
                    1
                } else {
                    options.max_ready_peers
                },
            )),
            identity: OnceLock::new(),
            carriers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn configure_receive_pool(&self, pool: Option<PayloadPool>) {
        self.receive_pool.get_or_init(|| pool);
    }

    pub(crate) fn receive_pool(&self) -> Option<&PayloadPool> {
        self.receive_pool.get().and_then(Option::as_ref)
    }

    pub(crate) fn register_carrier(&self, carrier: &Arc<DartIo>) {
        let mut carriers = self.carriers.lock().expect("DART carriers poisoned");
        carriers.retain(|carrier| carrier.strong_count() != 0);
        carriers.push(Arc::downgrade(carrier));
    }

    pub(crate) fn capabilities(&self) -> Option<DartCapabilities> {
        self.carriers
            .lock()
            .expect("DART carriers poisoned")
            .iter()
            .filter_map(Weak::upgrade)
            .map(|carrier| carrier.capabilities())
            .reduce(DartCapabilities::merge)
    }

    pub(crate) fn identity(&self, configured: &bytes::Bytes) -> bytes::Bytes {
        if !configured.is_empty() {
            return configured.clone();
        }
        self.identity
            .get_or_init(|| {
                let mut value = [0; 17];
                value[1..].copy_from_slice(&rand::random::<[u8; 16]>());
                bytes::Bytes::copy_from_slice(&value)
            })
            .clone()
    }
}

/// Conservative capabilities across live DART endpoints. Segment limits
/// reflect runtime GSO downshifts. ECN `None` means availability is unconfirmed
/// on at least one endpoint. Endpoints for an unused IP family may report None.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DartCapabilities {
    /// Maximum datagrams per UDP segmentation offload submission.
    pub max_gso_segments: usize,
    /// Maximum datagrams per UDP receive offload aggregate.
    pub max_gro_segments: usize,
    /// IPv4 ECN metadata support; `None` means unconfirmed.
    pub ecn_ipv4: Option<bool>,
    /// IPv6 ECN metadata support; `None` means unconfirmed.
    pub ecn_ipv6: Option<bool>,
    /// Whether the UDP carrier may permit IP fragmentation.
    pub may_fragment: bool,
}

impl DartCapabilities {
    fn merge(self, other: Self) -> Self {
        fn ecn(first: Option<bool>, second: Option<bool>) -> Option<bool> {
            match (first, second) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            }
        }
        Self {
            max_gso_segments: self.max_gso_segments.min(other.max_gso_segments),
            max_gro_segments: self.max_gro_segments.min(other.max_gro_segments),
            ecn_ipv4: ecn(self.ecn_ipv4, other.ecn_ipv4),
            ecn_ipv6: ecn(self.ecn_ipv6, other.ecn_ipv6),
            may_fragment: self.may_fragment || other.may_fragment,
        }
    }
}

macro_rules! statistics {
    ($($(#[$meta:meta])* $field:ident),* $(,)?) => {
        /// Approximate socket-wide DART counters. A snapshot is not atomic
        /// across fields. ECN absence is counted separately from Not-ECT.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct DartStats { $($(#[$meta])* pub $field: u64,)* }

        #[derive(Debug, Default)]
        pub(crate) struct Counters { $($field: AtomicU64,)* }

        impl Counters {
            pub(crate) fn add(&self, changes: DartStats) {
                $(if changes.$field != 0 {
                    self.$field.fetch_add(changes.$field, Ordering::Relaxed);
                })*
            }

            pub(crate) fn snapshot(&self) -> DartStats {
                DartStats { $($field: self.$field.load(Ordering::Relaxed),)* }
            }
        }
    };
}

statistics!(
    /// Received UDP datagrams, including control and invalid packets.
    received_datagrams,
    /// Complete application messages delivered to the receive queue.
    received_messages,
    /// Application messages whose initial transmission completed.
    sent_messages,
    /// Malformed datagrams and rejected receive aggregates.
    invalid_datagrams,
    /// Receive payload pool misses requiring fallback allocation.
    pool_exhausted,
    /// Reserved receive-capacity overflow counter; currently always zero.
    receive_overflow,
    /// UDP receive errors.
    receive_failures,
    /// UDP send errors.
    send_failures,
    /// Accepted sequence units marked ECT(0).
    ect0,
    /// Accepted sequence units marked ECT(1).
    ect1,
    /// Accepted sequence units marked Congestion Experienced.
    ce,
    /// Accepted sequence units confirmed as Not-ECT.
    not_ect,
    /// Accepted sequence units without confirmed ECN metadata.
    ecn_unavailable,
    /// Application messages retired after remote acknowledgment.
    acknowledged,
    /// Retransmitted sequence units, including fragments.
    retransmitted,
    /// Duplicate sequence units received.
    duplicates,
    /// Out-of-order sequence units received.
    reordered,
    /// Transmission attempts blocked by receive credit.
    credit_stalls,
    /// Transmission attempts blocked by congestion control or pacing.
    congestion_stalls,
    /// ECN feedback validation failures.
    ecn_failures,
);
