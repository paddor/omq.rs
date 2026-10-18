//! Admission owned continuously from transport setup through authentication.
//!
//! Every attempt carries one absolute deadline through DNS, dialing, carrier
//! setup, actor handoff, and ZMTP READY. A timeout or cancellation drops both
//! the transport and its reservation before waiting for the reconnect delay.
//! Waiting for initial admission happens before starting that attempt's clock.

use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use omq_proto::{Error, Result};

use tokio_util::sync::CancellationToken;

use crate::engine::signal::StateSignal;

/// One remote host may hold at most this share of a socket's pending
/// handshakes, so a single source cannot fill the pool.
const SOURCE_SHARE: usize = 8;

#[derive(Clone, Debug)]
pub(crate) struct Admission {
    used: Arc<AtomicUsize>,
    changed: Arc<StateSignal>,
    limit: usize,
    sources: SourceCounts,
}

/// Pending handshakes per remote host.
type SourceCounts = Arc<Mutex<FxHashMap<SourceKey, usize>>>;

/// A remote host: an IPv4 address or an IPv6 /64. One host can pick any
/// address in its /64, so per-host limits must not key on the full address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceKey {
    V4([u8; 4]),
    V6([u8; 8]),
}

impl SourceKey {
    pub(crate) fn host(address: IpAddr) -> Self {
        match address {
            IpAddr::V4(v4) => Self::V4(v4.octets()),
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => Self::V4(v4.octets()),
                None => Self::V6(v6.octets()[..8].try_into().expect("prefix")),
            },
        }
    }

    /// Loopback peers are local and exempt.
    fn of(address: IpAddr) -> Option<Self> {
        let key = Self::host(address);
        (!address.is_loopback() && !matches!(key, Self::V4([127, ..]))).then_some(key)
    }
}

impl Admission {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            used: Arc::new(AtomicUsize::new(0)),
            changed: Arc::new(StateSignal::new()),
            limit,
            sources: Arc::new(Mutex::new(FxHashMap::default())),
        }
    }

    /// Slots currently reserved.
    #[cfg(feature = "quic")]
    pub(crate) fn in_use(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    /// Reserve a slot for a peer at `source`, within the socket limit and
    /// its per-source share. Returns `None` without waiting.
    pub(crate) fn try_acquire_from(&self, source: Option<IpAddr>) -> Option<Permit> {
        let Some(key) = source.and_then(SourceKey::of) else {
            return self.try_acquire();
        };
        let mut sources = self.sources.lock().expect("admission sources poisoned");
        let held = sources.entry(key).or_default();
        if *held >= (self.limit / SOURCE_SHARE).max(1) {
            return None;
        }
        let mut permit = self.try_acquire()?;
        *held += 1;
        permit.source = Some(Box::new((key, self.sources.clone())));
        Some(permit)
    }

    // Atomic::try_update is unstable on MSRV 1.93.
    #[allow(deprecated)]
    pub(crate) fn try_acquire(&self) -> Option<Permit> {
        self.used
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |used| {
                (used < self.limit).then(|| used + 1)
            })
            .ok()?;
        Some(Permit {
            used: self.used.clone(),
            changed: self.changed.clone(),
            source: None,
        })
    }

    pub(crate) async fn acquire(&self) -> Permit {
        loop {
            let seen = self.changed.generation();
            if let Some(permit) = self.try_acquire() {
                return permit;
            }
            self.changed.changed_after(seen).await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct Permit {
    used: Arc<AtomicUsize>,
    changed: Arc<StateSignal>,
    // Boxed: inbound transports carry permits through setup by value.
    source: Option<Box<(SourceKey, SourceCounts)>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some((key, sources)) = self.source.take().map(|source| *source) {
            let mut sources = sources.lock().expect("admission sources poisoned");
            if let Some(held) = sources.get_mut(&key) {
                *held -= 1;
                if *held == 0 {
                    sources.remove(&key);
                }
            }
        }
        self.used.fetch_sub(1, Ordering::Release);
        self.changed.notify_changed();
    }
}

#[derive(Debug)]
pub(crate) struct PendingHandshake {
    _socket: Permit,
    _listener: Option<Permit>,
}

impl PendingHandshake {
    pub(crate) fn from_socket(socket: Permit) -> Self {
        Self {
            _socket: socket,
            _listener: None,
        }
    }

    pub(crate) fn acquire(
        socket: &Admission,
        listener: Option<&Admission>,
        source: Option<IpAddr>,
    ) -> Option<Self> {
        // Neither reservation waits. Failure drops the first before returning.
        let socket = socket.try_acquire_from(source)?;
        let listener = match listener {
            Some(limit) => Some(limit.try_acquire()?),
            None => None,
        };
        Some(Self {
            _socket: socket,
            _listener: listener,
        })
    }
}

/// Listener-side setup policy shared by encrypted carriers.
#[cfg(any(feature = "ws", feature = "quic"))]
pub(crate) struct AcceptSetup {
    pub(crate) admission: Admission,
    pub(crate) timeout: Duration,
    pub(crate) cancel: CancellationToken,
    pub(crate) monitor: crate::socket::monitor::MonitorPublisher,
    /// Data IO runtimes for carriers that place setup work per peer.
    #[cfg_attr(not(feature = "quic"), expect(dead_code))]
    pub(crate) io_pool: crate::context::IoPoolHandle,
}

#[derive(Debug)]
pub(crate) struct SetupState {
    pub(crate) deadline: Option<Instant>,
    pub(crate) cancel: CancellationToken,
    pub(crate) admission: PendingHandshake,
}

/// One reservation and one absolute deadline for each reconnect attempt.
pub(crate) struct DialSetup {
    pub(crate) admission: Option<Admission>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) cancel: CancellationToken,
}

impl DialSetup {
    /// The attempt's absolute deadline: a supplied one, or now plus timeout.
    pub(crate) fn deadline(&self, first_deadline: Option<Instant>) -> Result<Option<Instant>> {
        match first_deadline {
            Some(deadline) => Ok(Some(deadline)),
            None => self
                .timeout
                .map(|timeout| {
                    Instant::now().checked_add(timeout).ok_or_else(|| {
                        Error::HandshakeFailed("transport setup timeout exceeds clock range".into())
                    })
                })
                .transpose(),
        }
    }

    pub(crate) async fn run_until<T>(
        &self,
        first_deadline: Option<Instant>,
        first_admission: Option<PendingHandshake>,
        dial: impl Future<Output = Result<T>>,
    ) -> Result<(T, Option<SetupState>)> {
        let deadline = self.deadline(first_deadline)?;
        let admission = match first_admission {
            Some(admission) => Some(admission),
            None => self
                .admission
                .as_ref()
                .map(|limit| {
                    PendingHandshake::acquire(limit, None, None).ok_or_else(|| {
                        Error::HandshakeFailed("socket pending-handshake limit reached".into())
                    })
                })
                .transpose()?,
        };
        let result = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline.into(), dial)
                .await
                .map_err(|_| Error::HandshakeFailed("transport setup timeout".into()))??
        } else {
            dial.await?
        };
        Ok((
            result,
            admission.map(|admission| SetupState {
                deadline,
                cancel: self.cancel.clone(),
                admission,
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_listener_reservation_releases_socket_credit() {
        let socket = Admission::new(2);
        let listener = Admission::new(1);
        let first = PendingHandshake::acquire(&socket, Some(&listener), None).unwrap();
        assert!(PendingHandshake::acquire(&socket, Some(&listener), None).is_none());
        let second = PendingHandshake::acquire(&socket, None, None).unwrap();
        assert!(PendingHandshake::acquire(&socket, None, None).is_none());
        drop(first);
        let third = PendingHandshake::acquire(&socket, Some(&listener), None).unwrap();
        drop((second, third));
        assert_eq!(socket.used.load(Ordering::Relaxed), 0);
        assert_eq!(listener.used.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn waiting_for_admission_wakes_and_carries_one_reservation() {
        let admission = Admission::new(1);
        let first = admission.try_acquire().unwrap();
        let waiting = tokio::spawn({
            let admission = admission.clone();
            async move { admission.acquire().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        let permit = waiting.await.unwrap();
        assert!(admission.try_acquire().is_none());
        let setup = DialSetup {
            admission: Some(admission.clone()),
            timeout: None,
            cancel: CancellationToken::new(),
        };
        let ((), state) = setup
            .run_until(None, Some(PendingHandshake::from_socket(permit)), async {
                Ok(())
            })
            .await
            .unwrap();
        assert!(admission.try_acquire().is_none());
        drop(state);
        assert!(admission.try_acquire().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn supplied_deadline_does_not_restart_after_dns() {
        let admission = Admission::new(1);
        let setup = DialSetup {
            admission: Some(admission.clone()),
            timeout: Some(Duration::from_secs(1)),
            cancel: CancellationToken::new(),
        };
        let deadline = Instant::now() + Duration::from_millis(100);
        let result = setup
            .run_until(Some(deadline), None, async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(())
            })
            .await;
        assert!(matches!(result, Err(Error::HandshakeFailed(_))));
        assert!(admission.try_acquire().is_some());
    }

    #[tokio::test]
    async fn deadline_and_cancellation_release_but_success_transfers_admission() {
        let admission = Admission::new(1);
        let setup = DialSetup {
            admission: Some(admission.clone()),
            timeout: Some(Duration::from_millis(10)),
            cancel: CancellationToken::new(),
        };
        assert!(
            setup
                .run_until(None, None, std::future::pending::<Result<()>>())
                .await
                .is_err()
        );
        assert!(admission.try_acquire().is_some());

        let mut pending =
            Box::pin(setup.run_until(None, None, std::future::pending::<Result<()>>()));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(admission.try_acquire().is_none());
        drop(pending);
        assert!(admission.try_acquire().is_some());

        let ((), state) = setup.run_until(None, None, async { Ok(()) }).await.unwrap();
        assert!(admission.try_acquire().is_none());
        drop(state);
        assert!(admission.try_acquire().is_some());
    }
    #[test]
    fn one_remote_host_holds_at_most_its_share_of_inbound_slots() {
        let admission = Admission::new(16);
        let host: IpAddr = "192.0.2.1".parse().unwrap();
        let same_prefix: IpAddr = "2001:db8::1".parse().unwrap();
        let first = admission.try_acquire_from(Some(host)).unwrap();
        let _second = admission.try_acquire_from(Some(host)).unwrap();
        assert!(admission.try_acquire_from(Some(host)).is_none());
        // Another host still gets slots; one IPv6 /64 counts as one host.
        let other = admission.try_acquire_from(Some("192.0.2.2".parse().unwrap()));
        assert!(other.is_some());
        let _v6 = admission.try_acquire_from(Some(same_prefix)).unwrap();
        let _v6b = admission
            .try_acquire_from(Some("2001:db8::2".parse().unwrap()))
            .unwrap();
        assert!(
            admission
                .try_acquire_from(Some("2001:db8::3".parse().unwrap()))
                .is_none()
        );
        // Releasing a slot returns it to that host.
        drop(first);
        assert!(admission.try_acquire_from(Some(host)).is_some());
        // Loopback peers are local and use only the socket limit.
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let held: Vec<_> = (0..8)
            .map(|_| admission.try_acquire_from(Some(loopback)).unwrap())
            .collect();
        assert_eq!(held.len(), 8);
    }
}
