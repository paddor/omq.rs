//! Reconnect attempts with bounded setup and cancelable actor handoff.

use omq_proto::Options;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{AnyConn, Endpoint, Error, InprocPeerSnapshot, InternalEvent, MonitorEvent, Result};
use crate::socket::dispatch::connect_any;
use crate::socket::monitor::MonitorPublisher;
use crate::transport::inproc::{InprocRegistry, RecvConfig};
use crate::transport::setup::DialSetup;
use crate::transport::{Canceled, dial_with_backoff};

pub(super) struct DialTask {
    pub(super) endpoint: Endpoint,
    pub(super) options: Arc<Options>,
    pub(super) route_id: u64,
    pub(super) cancel: CancellationToken,
    pub(super) tx: mpsc::Sender<InternalEvent>,
    pub(super) monitor: MonitorPublisher,
    pub(super) snapshot: InprocPeerSnapshot,
    pub(super) recv: RecvConfig,
    pub(super) registry: Arc<InprocRegistry>,
    pub(super) setup: DialSetup,
    pub(super) first_deadline: Option<std::time::Instant>,
    pub(super) first_admission: Option<crate::transport::setup::PendingHandshake>,
}

impl DialTask {
    pub(super) async fn run(mut self) {
        let mut first_deadline = self.first_deadline;
        let mut first_admission = self.first_admission.take();
        let result = dial_with_backoff(
            || self.attempt(first_deadline.take(), first_admission.take()),
            self.options.reconnect,
            self.options.reconnect_stop_conn_refused,
            &self.cancel,
            |retry_in, attempt| {
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
                        endpoint: self.endpoint,
                        route_id: self.route_id,
                    }) => {}
                }
            }
        }
    }

    async fn attempt(
        &self,
        first_deadline: Option<std::time::Instant>,
        first_admission: Option<crate::transport::setup::PendingHandshake>,
    ) -> Result<()> {
        let (mut conn, state) = self
            .setup
            .run_until(
                first_deadline,
                first_admission,
                connect_any(
                    &self.registry,
                    &self.endpoint,
                    &self.snapshot,
                    &self.recv,
                    #[cfg(feature = "ws")]
                    crate::socket::dispatch::WsConnectOptions {
                        wss_tls: &self.options.wss_tls,
                        mechanism: &self.options.mechanism,
                    },
                ),
            )
            .await?;
        if let AnyConn::ByteStream { setup, .. } = &mut conn {
            *setup = state;
        }
        self.deliver(conn).await
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
