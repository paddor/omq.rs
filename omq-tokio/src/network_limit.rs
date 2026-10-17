//! Context bandwidth allocation. Only the allocator mutates bucket state;
//! each owned runtime publishes batched accounting through its own mailbox.

mod io;
mod ledger;
#[cfg(test)]
mod tests;
pub(crate) use io::{PacedRead, PacedWrite};

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::engine::signal::DataSignal;
use ledger::{Ledger, Reports};

/// A byte token bucket. Both fields must be nonzero.
///
/// Buckets start full and refill at `bytes_per_second`, up to `burst_bytes`.
/// Credit already granted to an IO lane still occupies burst capacity, so
/// adding IO threads never multiplies the allowance. Larger bursts permit
/// larger immediate transfers and can increase queueing latency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRateLimit {
    /// Sustained refill rate in bytes per second.
    pub bytes_per_second: u64,
    /// Maximum accumulated credit, shared by all connections and IO threads.
    pub burst_bytes: u64,
}

impl ByteRateLimit {
    /// Construct a limit, validated when installed on a context.
    #[must_use]
    pub const fn new(bytes_per_second: u64, burst_bytes: u64) -> Self {
        Self {
            bytes_per_second,
            burst_bytes,
        }
    }
}

/// Independent aggregate network limits for an OMQ-owned context.
///
/// `None` disables that direction. TCP setup is exempt. Byte accounting is
/// at the transport interface, not at the network interface's packet boundary.
///
/// TCP (including STREAM), WS/WSS, and QUIC count accepted stream bytes after
/// compression/framing; TLS, IP and carrier recovery overhead is excluded.
/// UDP counts complete datagrams. DART counts data datagrams and retransmissions,
/// while ACK, STATUS, READY, NAK and PROBE traffic stays unpaced. Each fan-out
/// copy consumes its own egress allowance. IPC, inproc and SHM are excluded.
///
/// Ingress paces stream reads, UDP receives, and DART receive-credit grants.
/// Already admitted/buffered traffic can still arrive, and UDP can drop in the
/// kernel while reads wait. DART precharges a maximum datagram per sequence,
/// reconciles actual data bytes once on arrival, and repays duplicate/repair
/// debt before issuing further credit. Datagram endpoints require a burst
/// large enough for their maximum datagram (65,507 for UDP, 1,452 for DART).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetworkRateLimits {
    /// Incoming transport bytes per second and accumulated burst allowance.
    pub ingress: Option<ByteRateLimit>,
    /// Outgoing transport bytes per second and accumulated burst allowance.
    pub egress: Option<ByteRateLimit>,
}

impl NetworkRateLimits {
    pub(crate) fn enabled(self) -> bool {
        self.ingress.is_some() || self.egress.is_some()
    }

    pub(crate) fn validate(self) -> crate::Result<()> {
        for limit in [self.ingress, self.egress].into_iter().flatten() {
            if limit.bytes_per_second == 0 || limit.burst_bytes == 0 {
                return Err(crate::Error::Config(
                    "network rate and burst must be nonzero".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Mailbox {
    reports: CachePadded<Reports>,
    granted: CachePadded<AtomicU64>,
    allocator: Arc<DataSignal>,
    lane: Arc<DataSignal>,
    quantum: u64,
    burst: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Lane {
    ingress: Option<Arc<Mailbox>>,
    egress: Option<Arc<Mailbox>>,
}

impl Lane {
    pub(crate) fn ingress(&self) -> Option<Gate> {
        self.ingress
            .as_ref()
            .map(|mailbox| Gate::new(mailbox.clone()))
    }

    pub(crate) fn egress(&self) -> Option<Gate> {
        self.egress
            .as_ref()
            .map(|mailbox| Gate::new(mailbox.clone()))
    }

    pub(crate) fn has_egress(&self) -> bool {
        self.egress.is_some()
    }

    pub(crate) fn validate_datagrams(&self, maximum: usize) -> crate::Result<()> {
        if [&self.ingress, &self.egress]
            .into_iter()
            .flatten()
            .any(|m| m.burst < maximum as u64)
        {
            return Err(crate::Error::Config(format!(
                "network burst must accommodate a {maximum}-byte datagram"
            )));
        }
        Ok(())
    }

    pub(crate) async fn run(self, cancel: CancellationToken) {
        let Some(signal) = self
            .ingress
            .as_ref()
            .or(self.egress.as_ref())
            .map(|m| m.lane.clone())
        else {
            return;
        };
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                () = signal.ready() => {}
            }
            signal.begin_drain();
            for mailbox in [&self.ingress, &self.egress].into_iter().flatten() {
                LOCAL.with(|locals| {
                    if let Some(local) = locals.borrow_mut().get_mut(&key(mailbox)) {
                        local.collect();
                        if local.waiters.is_empty() && local.active == 0 {
                            local.return_idle();
                        } else {
                            local.publish();
                            local.wake_front();
                        }
                    }
                });
            }
            signal.clear_after(true);
        }
        // Owned runtime threads destroy LOCAL on exit, after dropping their
        // drivers. Removing it here lets a driver still being shut down
        // recreate zeroed counters and collect an old cumulative grant again.
    }
}

#[derive(Debug)]
pub(crate) struct Allocator {
    lanes: Vec<Lane>,
    ingress: Option<Bucket>,
    egress: Option<Bucket>,
    signal: Arc<DataSignal>,
}

impl Allocator {
    pub(crate) fn new(config: NetworkRateLimits, count: usize) -> Self {
        let signal = Arc::new(DataSignal::new());
        let lanes = (0..count)
            .map(|_| {
                let lane = Arc::new(DataSignal::new());
                let mailbox = |limit: ByteRateLimit| {
                    Arc::new(Mailbox {
                        reports: CachePadded::new(Reports::default()),
                        granted: CachePadded::new(AtomicU64::new(0)),
                        allocator: signal.clone(),
                        lane: lane.clone(),
                        quantum: (limit.burst_bytes / (2 * count as u64)).clamp(1, 64 * 1024),
                        burst: limit.burst_bytes,
                    })
                };
                Lane {
                    ingress: config.ingress.map(mailbox),
                    egress: config.egress.map(mailbox),
                }
            })
            .collect::<Vec<_>>();
        let now = Instant::now();
        Self {
            ingress: config.ingress.map(|limit| {
                Bucket::new(
                    limit,
                    lanes.iter().filter_map(|l| l.ingress.clone()).collect(),
                    now,
                )
            }),
            egress: config.egress.map(|limit| {
                Bucket::new(
                    limit,
                    lanes.iter().filter_map(|l| l.egress.clone()).collect(),
                    now,
                )
            }),
            lanes,
            signal,
        }
    }

    pub(crate) fn lanes(&self) -> Vec<Lane> {
        self.lanes.clone()
    }

    pub(crate) async fn run(mut self, cancel: CancellationToken) {
        loop {
            self.signal.begin_drain();
            let now = Instant::now();
            let deadline = [&mut self.ingress, &mut self.egress]
                .into_iter()
                .flatten()
                .filter_map(|bucket| bucket.service(now))
                .min();
            self.signal.clear_after(true);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = self.signal.ready() => {},
                () = async { tokio::time::sleep_until(deadline.unwrap()).await; }, if deadline.is_some() => {}
            }
        }
    }
}

#[derive(Debug)]
struct Account {
    mailbox: Arc<Mailbox>,
    totals: Ledger,
}

#[derive(Debug)]
struct Bucket {
    limit: ByteRateLimit,
    available: u64,
    held: u64,
    fraction: u128,
    updated: Instant,
    accounts: Vec<Account>,
    cursor: usize,
    debt: u128,
}

impl Bucket {
    fn new(limit: ByteRateLimit, mailboxes: Vec<Arc<Mailbox>>, now: Instant) -> Self {
        Self {
            limit,
            available: limit.burst_bytes,
            held: 0,
            fraction: 0,
            updated: now,
            cursor: 0,
            debt: 0,
            accounts: mailboxes
                .into_iter()
                .map(|mailbox| Account {
                    mailbox,
                    totals: Ledger::default(),
                })
                .collect(),
        }
    }

    fn refill(&mut self, now: Instant) {
        let room = self.limit.burst_bytes - self.held - self.available;
        let generated = now
            .saturating_duration_since(self.updated)
            .as_nanos()
            .saturating_mul(u128::from(self.limit.bytes_per_second))
            .saturating_add(self.fraction);
        let whole = generated / 1_000_000_000;
        let repaid = whole.min(self.debt);
        self.debt -= repaid;
        let whole = whole - repaid;
        let added = whole.min(u128::from(room)) as u64;
        self.available += added;
        self.fraction = if self.debt == 0 && whole >= u128::from(room) {
            0
        } else {
            generated % 1_000_000_000
        };
        self.updated = now;
    }

    fn service(&mut self, now: Instant) -> Option<Instant> {
        // Reports release capacity only from now onward. Backdating this
        // refill after a delayed spend report would manufacture burst credit.
        self.refill(now);
        for account in &mut self.accounts {
            let updates = account.totals.collect(&account.mailbox.reports);
            self.held -= updates.spent + updates.returned;
            self.available += updates.returned;
            #[cfg(feature = "dart")]
            {
                self.debt += u128::from(updates.debt);
                let refund = u128::from(updates.refunded);
                let repaid = refund.min(self.debt);
                self.debt -= repaid;
                self.available = (u128::from(self.available) + refund - repaid)
                    .min(u128::from(self.limit.burst_bytes - self.held))
                    as u64;
            }
        }
        let repaid = self.debt.min(u128::from(self.available));
        self.debt -= repaid;
        self.available -= repaid as u64;
        let count = self.accounts.len();
        let mut waiting = false;
        let mut next_bytes = u64::MAX;
        let start = self.cursor;
        // One bounded grant per runtime per turn. A follow-up turn rotates
        // past the last recipient, including when only one byte is available.
        for offset in 0..count {
            let index = (start + offset) % count;
            let account = &mut self.accounts[index];
            let requested = account
                .mailbox
                .reports
                .demand
                .load(Ordering::Acquire)
                .min(self.limit.burst_bytes);
            let demand = if requested == 0 {
                0
            } else {
                requested
                    .max(2 * account.mailbox.quantum)
                    .min(self.limit.burst_bytes)
            };
            let needed = demand.saturating_sub(account.totals.held);
            if needed == 0 {
                continue;
            }
            let minimum = requested.saturating_sub(account.totals.held).min(needed);
            if self.available < minimum {
                // Preserve this lane's turn while an indivisible packet
                // accumulates credit. Serving smaller requests behind it
                // could otherwise keep it below its minimum forever.
                self.cursor = index;
                waiting = true;
                next_bytes = minimum;
                break;
            }
            // Never spread an indivisible datagram's credit over several
            // lanes: all lanes could otherwise hold an unusable partial grant.
            let grant = needed
                .min(account.mailbox.quantum.max(minimum))
                .min(self.available);
            if grant != 0 {
                self.available -= grant;
                self.held += grant;
                account.totals.grant(grant, &account.mailbox.granted);
                account.mailbox.lane.mark();
                self.cursor = (index + 1) % count;
            }
            if grant < needed {
                waiting = true;
                next_bytes =
                    next_bytes.min((needed - grant).min(account.mailbox.quantum.max(minimum)));
            }
        }
        if !waiting {
            return None;
        }
        if self.available >= next_bytes {
            return Some(now);
        }
        if next_bytes > self.limit.burst_bytes - self.held {
            return None;
        }
        let bytes = next_bytes
            .min(self.limit.burst_bytes - self.held)
            .saturating_sub(self.available);
        let nanos = ((u128::from(bytes) + self.debt).saturating_mul(1_000_000_000))
            .saturating_sub(self.fraction)
            .div_ceil(u128::from(self.limit.bytes_per_second))
            .max(1);
        Some(now + Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64))
    }
}

thread_local! {
    static LOCAL: RefCell<HashMap<usize, Local>> = RefCell::new(HashMap::new());
}
static NEXT_GATE: AtomicU64 = AtomicU64::new(1);

fn key(mailbox: &Arc<Mailbox>) -> usize {
    Arc::as_ptr(mailbox) as usize
}

struct Waiter {
    id: u64,
    minimum: u64,
    waker: Waker,
}

struct Local {
    mailbox: Arc<Mailbox>,
    seen: u64,
    credit: u64,
    spent: u64,
    returned: u64,
    #[cfg(feature = "dart")]
    refunded: u64,
    #[cfg(feature = "dart")]
    debt: u64,
    waiters: VecDeque<Waiter>,
    published: [u64; 5],
    active: usize,
}

impl Local {
    fn new(mailbox: Arc<Mailbox>) -> Self {
        Self {
            mailbox,
            seen: 0,
            credit: 0,
            spent: 0,
            returned: 0,
            waiters: VecDeque::new(),
            published: [0; 5],
            active: 0,
            #[cfg(feature = "dart")]
            refunded: 0,
            #[cfg(feature = "dart")]
            debt: 0,
        }
    }

    fn collect(&mut self) {
        let granted = self.mailbox.granted.load(Ordering::Acquire);
        self.credit += granted.wrapping_sub(self.seen);
        self.seen = granted;
    }

    fn publish(&mut self) {
        // A pending batch cannot use a partial balance. Return it before
        // advertising demand so fragments on other lanes cannot prevent an
        // indivisible datagram at the allocator's cursor from ever fitting.
        if self
            .waiters
            .front()
            .is_some_and(|w| self.credit < w.minimum)
        {
            self.return_credit();
        }
        // A fully used IO allowance predicts another operation. Refill at
        // half a quantum; canceled/idle operations withdraw this demand.
        let prefetch = u64::from(self.active != 0 && self.credit <= self.mailbox.quantum / 2);
        let demand = self.waiters.front().map_or(prefetch, |w| w.minimum);
        #[cfg(feature = "dart")]
        let adjustment = [self.refunded, self.debt];
        #[cfg(not(feature = "dart"))]
        let adjustment = [0, 0];
        let snapshot = [
            self.spent,
            self.returned,
            demand,
            adjustment[0],
            adjustment[1],
        ];
        if snapshot == self.published {
            return;
        }
        self.published = snapshot;
        self.mailbox.reports.publish(snapshot);
        self.mailbox.allocator.mark();
    }

    fn return_credit(&mut self) {
        self.returned = self.returned.wrapping_add(self.credit);
        self.credit = 0;
    }

    fn return_idle(&mut self) {
        self.return_credit();
        self.publish();
    }

    fn wake_front(&self) {
        if let Some(front) = self.waiters.front()
            && self.credit >= front.minimum
        {
            front.waker.wake_by_ref();
        }
    }
}

/// A connection's place in its runtime-local fair queue. Permits last only
/// for one nonblocking IO poll; no permit is held across Pending.
#[derive(Debug)]
pub(crate) struct Gate {
    mailbox: Arc<Mailbox>,
    id: u64,
    blocked: bool,
    active: bool,
}

impl Gate {
    fn new(mailbox: Arc<Mailbox>) -> Self {
        Self {
            mailbox,
            id: NEXT_GATE.fetch_add(1, Ordering::Relaxed),
            blocked: false,
            active: false,
        }
    }

    pub(crate) fn blocked(&self) -> bool {
        self.blocked
    }

    pub(crate) fn cancel_wait(&mut self) {
        if !self.blocked && !self.active {
            return;
        }
        self.blocked = false;
        LOCAL.with(|locals| {
            if let Some(local) = locals.borrow_mut().get_mut(&key(&self.mailbox)) {
                local.active -= usize::from(self.active);
                local.waiters.retain(|w| w.id != self.id);
                local.wake_front();
                self.mailbox.lane.mark();
            }
        });
        self.active = false;
    }

    pub(crate) fn poll(&mut self, cx: &Context<'_>, wanted: usize, minimum: usize) -> Poll<usize> {
        if wanted == 0 {
            self.cancel_wait();
            return Poll::Ready(0);
        }
        // Queue a useful batch, not just the transport's smallest operation.
        // Otherwise a stream can receive only a few bytes per turn while a
        // competing datagram consumes its entire packet on every turn.
        let minimum = wanted.min(self.mailbox.quantum.max(minimum as u64) as usize);
        let granted = LOCAL.with(|locals| {
            let mut locals = locals.borrow_mut();
            let local = locals
                .entry(key(&self.mailbox))
                .or_insert_with(|| Local::new(self.mailbox.clone()));
            if let Some(waiter) = local.waiters.iter_mut().find(|w| w.id == self.id) {
                waiter.minimum = minimum as u64;
                waiter.waker.clone_from(cx.waker());
            } else {
                local.waiters.push_back(Waiter {
                    id: self.id,
                    minimum: minimum as u64,
                    waker: cx.waker().clone(),
                });
            }
            if local.credit < minimum as u64 {
                local.collect();
            }
            if local.waiters.front().is_some_and(|w| w.id == self.id)
                && local.credit >= minimum as u64
            {
                let count = (wanted as u64)
                    .min(local.credit)
                    .min(self.mailbox.quantum.max(minimum as u64));
                local.credit -= count;
                local.waiters.pop_front();
                Some(count as usize)
            } else {
                local.publish();
                local.wake_front();
                None
            }
        });
        self.blocked = granted.is_none();
        granted.map_or(Poll::Pending, Poll::Ready)
    }

    pub(crate) fn finish(&mut self, reserved: usize, spent: usize) {
        debug_assert!(spent <= reserved);
        if reserved == 0 {
            return;
        }
        LOCAL.with(|locals| {
            let mut locals = locals.borrow_mut();
            let local = locals
                .get_mut(&key(&self.mailbox))
                .expect("reserved lane credit");
            local.credit += (reserved - spent) as u64;
            local.spent = local.spent.wrapping_add(spent as u64);
            let active = spent == reserved;
            local.active = local.active + usize::from(active) - usize::from(self.active);
            self.active = active;
            local.wake_front();
            // One lane service wake per IO turn, including a final short
            // batch. It returns idle credit and publishes batched reports.
            self.mailbox.lane.mark();
        });
    }

    #[cfg(test)]
    pub(crate) async fn acquire(&mut self, bytes: usize) -> usize {
        std::future::poll_fn(|cx| self.poll(cx, bytes, bytes)).await
    }

    /// DART credit is charged before arrival. Reconcile each received data
    /// datagram once, refunding unused reservation or charging repair debt.
    #[cfg(feature = "dart")]
    pub(crate) fn reconcile(&mut self, prepaid: u64, actual: u64) {
        LOCAL.with(|locals| {
            let mut locals = locals.borrow_mut();
            let local = locals
                .entry(key(&self.mailbox))
                .or_insert_with(|| Local::new(self.mailbox.clone()));
            local.refunded = local.refunded.wrapping_add(prepaid.saturating_sub(actual));
            local.debt = local.debt.wrapping_add(actual.saturating_sub(prepaid));
            self.mailbox.lane.mark();
        });
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.cancel_wait();
    }
}
