//! Reconnect attempts with bounded setup and cancelable actor handoff.

use omq_proto::Options;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{AnyConn, Endpoint, Error, InternalEvent, MonitorEvent, Result};
use crate::socket::dispatch::{InprocSetup, connect_any};
use crate::socket::monitor::MonitorPublisher;
use crate::transport::Canceled;
use crate::transport::backoff::{DialRetry, dial_with_backoff_from};
use crate::transport::setup::{DialSetup, PendingHandshake};

/// Setup budget carried from DNS resolution into a dialer's first attempt.
#[derive(Default)]
pub(super) struct FirstAttempt {
    pub(super) deadline: Option<std::time::Instant>,
    pub(super) admission: Option<PendingHandshake>,
}

pub(super) struct DialTask {
    pub(super) endpoint: Endpoint,
    pub(super) options: Arc<Options>,
    pub(super) route_id: u64,
    pub(super) cancel: CancellationToken,
    pub(super) tx: mpsc::Sender<InternalEvent>,
    pub(super) monitor: MonitorPublisher,
    pub(super) inproc: InprocSetup,
    pub(super) setup: DialSetup,
    pub(super) first: FirstAttempt,
    pub(super) failed_attempts: Arc<AtomicU32>,
    #[cfg_attr(not(feature = "quic"), expect(dead_code))]
    pub(super) io_pool: crate::context::IoPoolHandle,
}

impl DialTask {
    pub(super) async fn run(mut self) {
        let mut first = std::mem::take(&mut self.first);
        // A redial carries its failed attempts so a peer lost before READY
        // keeps growing its backoff instead of retrying at once.
        let retry = DialRetry {
            policy: self.options.reconnect,
            stop_conn_refused: self.options.reconnect_stop_conn_refused,
            attempt: self.failed_attempts.load(Ordering::Relaxed),
        };

        let result = dial_with_backoff_from(
            || self.attempt(std::mem::take(&mut first)),
            retry,
            &self.cancel,
            |retry_in, attempt| {
                self.failed_attempts.store(attempt, Ordering::Relaxed);
                self.monitor.publish(MonitorEvent::ConnectDelayed {
                    endpoint: self.endpoint.clone(),
                    retry_in,
                    attempt,
                });
            },
        )
        .await;

        match result {
            Ok(()) | Err(Canceled::Token) => {}
            Err(Canceled::PolicyDisabled | Canceled::StoppedConnRefused) => {
                tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => {}
                    _ = self.tx.send(InternalEvent::ConnectGaveUp {
                        route_id: self.route_id,
                    }) => {}
                }
            }
        }
    }

    async fn attempt(&self, first: FirstAttempt) -> Result<()> {
        // Fix the absolute deadline once so carriers can enforce it after
        // their transport handshake completes.
        let deadline = self.setup.deadline(first.deadline)?;
        let (mut conn, state) = match self
            .setup
            .run_until(
                deadline,
                first.admission,
                connect_any(
                    &self.endpoint,
                    &self.inproc,
                    #[cfg(any(feature = "ws", feature = "quic"))]
                    crate::socket::dispatch::CarrierConnect {
                        options: &self.options,
                        deadline,
                        #[cfg(feature = "quic")]
                        io_pool: &self.io_pool,
                    },
                ),
            )
            .await
        {
            Ok(connected) => connected,
            Err(error) => {
                self.report_setup_failure(&error);
                return Err(error);
            }
        };

        if let AnyConn::ByteStream { setup, .. } = &mut conn {
            *setup = state;
        }

        self.deliver(conn).await
    }

    /// TLS, certificate, and carrier setup failures stay internal to retry,
    /// but remain visible to monitors. Plain connect errors keep only
    /// `ConnectDelayed`.
    fn report_setup_failure(&self, error: &Error) {
        if let Error::HandshakeFailed(reason) = error {
            self.monitor.publish(MonitorEvent::HandshakeFailed {
                endpoint: self.endpoint.clone(),
                peer_ident: crate::socket::dispatch::peer_ident_for_endpoint(&self.endpoint),
                reason: reason.clone(),
            });
        }
    }

    async fn deliver(&self, conn: AnyConn) -> Result<()> {
        let deadline = match &conn {
            AnyConn::ByteStream { setup, .. } => setup.as_ref().and_then(|s| s.deadline),
            AnyConn::Inproc { .. } => None,
        };

        let delivery = self.tx.send(InternalEvent::Connected {
            conn,
            endpoint: self.endpoint.clone(),
            route_id: self.route_id,
        });

        let delivered = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline.into(), delivery)
                .await
                .map_err(|_| Error::HandshakeFailed("setup handoff timeout".into()))?
        } else {
            delivery.await
        };

        delivered.map_err(|_| Error::Closed)
    }
}
