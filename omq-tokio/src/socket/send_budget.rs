//! Bound ready async sends on each executor thread without shared atomics.

use std::cell::Cell;

const SEND_YIELD_INTERVAL: u16 = 4096;

thread_local! {
    static REMAINING: Cell<u16> = const { Cell::new(SEND_YIELD_INTERVAL) };
}

/// All sockets on a thread share its work budget. A task cannot evade a yield
/// by changing sockets or clones. Migrating tasks use the new thread's budget.
#[inline]
pub(super) async fn cooperate() {
    let exhausted = REMAINING.with(|remaining| {
        let next = remaining.get() - 1;
        remaining.set(if next == 0 { SEND_YIELD_INTERVAL } else { next });
        next == 0
    });
    if exhausted {
        // Also wakes and returns Pending when polled outside a Tokio runtime.
        tokio::task::yield_now().await;
    }
}
