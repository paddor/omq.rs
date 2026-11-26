//! Bound ready async sends on each executor thread without shared atomics.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Budget a thread spends between yields. Queued bytes reach the wire only
/// once a driver runs, and a driver woken on the sender's worker runs only
/// after the sending task yields.
const SEND_YIELD_BUDGET: usize = 2 * 1024 * 1024;

/// Budget each send costs besides its bytes, so small sends still yield
/// every 4096 sends.
const SEND_COST: usize = SEND_YIELD_BUDGET / 4096;

thread_local! {
    static SPENT: Cell<usize> = const { Cell::new(0) };
}

/// All sockets on a thread share its work budget. A task cannot evade a yield
/// by changing sockets or clones. Migrating tasks use the new thread's budget.
/// A send of `bytes` that spends the budget yields before it is queued, so
/// canceling the yield loses nothing.
#[inline]
pub(super) async fn cooperate(bytes: usize) {
    if spend(bytes) {
        // Also wakes and returns Pending when polled outside a Tokio runtime.
        tokio::task::yield_now().await;
    }
}

/// Charge one send of `bytes`. Returns whether the budget is spent, and then
/// refills it.
#[inline]
fn spend(bytes: usize) -> bool {
    let spent = SPENT.get().saturating_add(bytes).saturating_add(SEND_COST);
    let exhausted = spent >= SEND_YIELD_BUDGET;
    SPENT.set(if exhausted { 0 } else { spent });

    exhausted
}

/// Live routes whose driver writes queued bytes: every peer except inproc,
/// plus UDP dialers. Sends spend their bytes from the budget only while one
/// exists. Inproc sends reach the receiver without a driver.
#[derive(Debug, Default)]
pub(crate) struct WireRoutes(AtomicUsize);

impl WireRoutes {
    /// Whether any wire route is live.
    pub(crate) fn any(&self) -> bool {
        // Relaxed: the count only selects how sends charge the budget.
        self.0.load(Ordering::Relaxed) != 0
    }

    /// Count one wire route until the returned guard drops.
    pub(crate) fn hold(self: &Arc<Self>) -> WireRoute {
        // Relaxed: see `any`.
        self.0.fetch_add(1, Ordering::Relaxed);
        WireRoute(self.clone())
    }
}

/// One counted wire route, released on drop.
#[derive(Debug)]
pub(crate) struct WireRoute(Arc<WireRoutes>);

impl Drop for WireRoute {
    fn drop(&mut self) {
        // Relaxed: see `WireRoutes::any`.
        self.0.0.fetch_sub(1, Ordering::Relaxed);
    }
}
