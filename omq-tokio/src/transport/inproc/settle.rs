//! Waits until an inproc peer has applied this side's commands.
//!
//! Each direction of a connection has one queue. The side that sends
//! commands (SUBSCRIBE, CANCEL, JOIN, LEAVE) registers its callers and
//! sends a settle mark behind the commands. The other side completes them
//! once its socket handled everything before the mark. Callers also go
//! free while that side cannot take commands (an XPUB whose application
//! stopped reading) and once the connection ends, so a caller never waits
//! on another socket's application.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// One caller's wait. The receiver from [`SettleWaiter::new`] completes
/// when the last clone drops.
#[derive(Debug, Clone)]
pub(crate) struct SettleWaiter {
    _done: Arc<oneshot::Sender<()>>,
}

impl SettleWaiter {
    pub(crate) fn new() -> (Self, oneshot::Receiver<()>) {
        let (done, settled) = oneshot::channel();
        (
            Self {
                _done: Arc::new(done),
            },
            settled,
        )
    }
}

/// Callers waiting for one direction of a connection to settle.
#[derive(Debug, Default)]
pub(crate) struct SettleQueue {
    state: Mutex<SettleState>,
}

#[derive(Debug, Default)]
struct SettleState {
    /// Waiters with the mark that covers them, oldest first.
    waiters: VecDeque<(u64, SettleWaiter)>,
    /// The mark the next [`SettleQueue::mark`] returns.
    next_mark: u64,
    /// The receiving side takes no commands until its application reads.
    stalled: bool,
    closed: bool,
}

impl SettleQueue {
    /// Hold `waiter` until the receiving side handled every command sent
    /// so far and up to the next mark. False if it went free at once, so
    /// no mark is needed.
    pub(crate) fn register(&self, waiter: SettleWaiter) -> bool {
        let mut state = self.state.lock();
        let held = !state.stalled && !state.closed;
        if held {
            let mark = state.next_mark;
            state.waiters.push_back((mark, waiter));
        }

        held
    }

    /// Whether a waiter needs a mark sent.
    pub(crate) fn has_waiters(&self) -> bool {
        !self.state.lock().waiters.is_empty()
    }

    /// A mark to send behind the commands sent so far. It covers every
    /// waiter registered before this call.
    pub(crate) fn mark(&self) -> u64 {
        let mut state = self.state.lock();
        let mark = state.next_mark;
        state.next_mark = mark.wrapping_add(1);
        mark
    }

    /// The receiving side handled everything before `mark`.
    pub(crate) fn settled(&self, mark: u64) {
        let mut state = self.state.lock();
        let covered = state
            .waiters
            .iter()
            .take_while(|(waiter_mark, _)| *waiter_mark <= mark)
            .count();
        let done: Vec<_> = state.waiters.drain(..covered).collect();
        drop(state);
        drop(done);
    }

    /// While stalled, waiters complete at once instead of waiting for the
    /// receiving side's application.
    pub(crate) fn set_stalled(&self, stalled: bool) {
        let mut state = self.state.lock();
        state.stalled = stalled;
        let done = stalled.then(|| std::mem::take(&mut state.waiters));
        drop(state);
        drop(done);
    }

    /// The connection ended: every waiter completes, now and later.
    pub(crate) fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        let done = std::mem::take(&mut state.waiters);
        drop(state);
        drop(done);
    }
}

/// The settle queues of one connection, seen from one side.
#[derive(Debug, Clone)]
pub(crate) struct InprocSettle {
    /// Callers here, waiting for the peer to apply this side's commands.
    pub(crate) outgoing: Arc<SettleQueue>,
    /// The peer's callers, completed as this side applies its commands.
    pub(crate) incoming: Arc<SettleQueue>,
}

impl InprocSettle {
    /// The two sides of a new connection.
    pub(crate) fn pair() -> (Self, Self) {
        let forward = Arc::new(SettleQueue::default());
        let backward = Arc::new(SettleQueue::default());

        (
            Self {
                outgoing: forward.clone(),
                incoming: backward.clone(),
            },
            Self {
                outgoing: backward,
                incoming: forward,
            },
        )
    }

    pub(crate) fn close(&self) {
        self.outgoing.close();
        self.incoming.close();
    }
}

/// Where a `connect` waits for the connection's first commands: on the
/// side that sends them.
#[derive(Debug, Default)]
pub(crate) enum ConnectSettle {
    #[default]
    None,
    /// The connecting socket subscribes or joins.
    Connector(SettleWaiter),
    /// The connecting socket publishes; the bound one subscribes.
    Listener(SettleWaiter),
}

#[cfg(test)]
mod tests;
