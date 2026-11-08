//! Native wrapper counters.

use crate::ffi::OmqGoNativeStats;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static CONTEXTS_CREATED: AtomicU64 = AtomicU64::new(0);

pub(crate) static CONTEXTS_FREED: AtomicU64 = AtomicU64::new(0);

pub(crate) static SOCKETS_CREATED: AtomicU64 = AtomicU64::new(0);

pub(crate) static SOCKETS_FREED: AtomicU64 = AtomicU64::new(0);

pub(crate) static MONITORS_CREATED: AtomicU64 = AtomicU64::new(0);

pub(crate) static MONITORS_FREED: AtomicU64 = AtomicU64::new(0);

pub(crate) static SEND_RINGS_CREATED: AtomicU64 = AtomicU64::new(0);

pub(crate) static SEND_RINGS_FREED: AtomicU64 = AtomicU64::new(0);

pub(crate) static RECV_RINGS_CREATED: AtomicU64 = AtomicU64::new(0);

pub(crate) static RECV_RINGS_FREED: AtomicU64 = AtomicU64::new(0);

pub(crate) static CANCELS_CREATED: AtomicU64 = AtomicU64::new(0);

pub(crate) static CANCELS_FREED: AtomicU64 = AtomicU64::new(0);

pub(crate) fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn snapshot_counter(created: &AtomicU64, freed: &AtomicU64) -> (u64, u64, u64) {
    let created = created.load(Ordering::Relaxed);
    let freed = freed.load(Ordering::Relaxed);
    (created, freed, created.saturating_sub(freed))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_native_stats(out: *mut OmqGoNativeStats) {
    if out.is_null() {
        return;
    }

    let (contexts_created, contexts_freed, contexts_live) =
        snapshot_counter(&CONTEXTS_CREATED, &CONTEXTS_FREED);
    let (sockets_created, sockets_freed, sockets_live) =
        snapshot_counter(&SOCKETS_CREATED, &SOCKETS_FREED);
    let (monitors_created, monitors_freed, monitors_live) =
        snapshot_counter(&MONITORS_CREATED, &MONITORS_FREED);
    let (send_rings_created, send_rings_freed, send_rings_live) =
        snapshot_counter(&SEND_RINGS_CREATED, &SEND_RINGS_FREED);
    let (recv_rings_created, recv_rings_freed, recv_rings_live) =
        snapshot_counter(&RECV_RINGS_CREATED, &RECV_RINGS_FREED);
    let (cancels_created, cancels_freed, cancels_live) =
        snapshot_counter(&CANCELS_CREATED, &CANCELS_FREED);

    unsafe {
        *out = OmqGoNativeStats {
            contexts_created,
            contexts_freed,
            contexts_live,
            sockets_created,
            sockets_freed,
            sockets_live,
            monitors_created,
            monitors_freed,
            monitors_live,
            send_rings_created,
            send_rings_freed,
            send_rings_live,
            recv_rings_created,
            recv_rings_freed,
            recv_rings_live,
            cancels_created,
            cancels_freed,
            cancels_live,
        };
    }
}
