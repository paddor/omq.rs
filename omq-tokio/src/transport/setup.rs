//! Admission owned continuously from transport setup through authentication.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use omq_proto::{Error, Result};

use tokio_util::sync::CancellationToken;

use crate::engine::signal::StateSignal;

#[derive(Clone, Debug)]
pub(crate) struct Admission {
    used: Arc<AtomicUsize>,
    changed: Arc<StateSignal>,
    limit: usize,
}

impl Admission {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            used: Arc::new(AtomicUsize::new(0)),
            changed: Arc::new(StateSignal::new()),
            limit,
        }
    }

    pub(crate) fn try_acquire(&self) -> Option<Permit> {
        self.used
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |used| {
                (used < self.limit).then(|| used + 1)
            })
            .ok()?;
        Some(Permit {
            used: self.used.clone(),
            changed: self.changed.clone(),
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
}

impl Drop for Permit {
    fn drop(&mut self) {
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

    pub(crate) fn acquire(socket: &Admission, listener: Option<&Admission>) -> Option<Self> {
        // Neither reservation waits. Failure drops the first before returning.
        let socket = socket.try_acquire()?;
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
    pub(crate) async fn run_until<T>(
        &self,
        first_deadline: Option<Instant>,
        first_admission: Option<PendingHandshake>,
        dial: impl Future<Output = Result<T>>,
    ) -> Result<(T, Option<SetupState>)> {
        let deadline = match first_deadline {
            Some(deadline) => Some(deadline),
            None => self
                .timeout
                .map(|timeout| {
                    Instant::now().checked_add(timeout).ok_or_else(|| {
                        Error::HandshakeFailed("transport setup timeout exceeds clock range".into())
                    })
                })
                .transpose()?,
        };
        let admission = match first_admission {
            Some(admission) => Some(admission),
            None => self
                .admission
                .as_ref()
                .map(|limit| {
                    PendingHandshake::acquire(limit, None).ok_or_else(|| {
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
        let first = PendingHandshake::acquire(&socket, Some(&listener)).unwrap();
        assert!(PendingHandshake::acquire(&socket, Some(&listener)).is_none());
        let second = PendingHandshake::acquire(&socket, None).unwrap();
        assert!(PendingHandshake::acquire(&socket, None).is_none());
        drop(first);
        let third = PendingHandshake::acquire(&socket, Some(&listener)).unwrap();
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
}
