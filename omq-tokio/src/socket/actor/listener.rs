//! Bounded delivery of accepted transports to the socket actor.

use omq_proto::Options;
use std::sync::Arc;

use std::time::Duration;

use omq_proto::flow::DrainBudget;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{AnyConn, Endpoint, InternalEvent, MonitorEvent};
use crate::socket::dispatch::AnyListener;
use crate::socket::monitor::MonitorPublisher;
use crate::transport::setup::{Admission, PendingHandshake, SetupState};

pub(super) struct ListenerTask {
    pub(super) endpoint: Endpoint,
    pub(super) options: Arc<Options>,
    pub(super) cancel: CancellationToken,
    pub(super) tx: mpsc::Sender<InternalEvent>,
    pub(super) admission: Option<Admission>,
    pub(super) monitor: MonitorPublisher,
}

impl ListenerTask {
    pub(super) async fn run(self, mut listener: AnyListener) {
        let mut budget = DrainBudget::new(32, 128 * 1024);
        loop {
            if !budget.account(4096) {
                tokio::task::yield_now().await;
                budget.reset();
            }
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => return,
                result = listener.accept() => match result {
                    Ok(mut conn) => {
                        if self.admit(&mut conn) && !self.deliver(conn) {
                            return;
                        }
                    }
                    Err(_) => {
                        // Raw accept errors (EMFILE etc.) must not spin.
                        tokio::select! {
                            biased;
                            () = self.cancel.cancelled() => return,
                            () = tokio::time::sleep(Duration::from_millis(50)) => {}
                        }
                    }
                }
            }
        }
    }

    fn admit(&self, conn: &mut AnyConn) -> bool {
        if let AnyConn::ByteStream {
            setup, peer_ident, ..
        } = conn
            && setup.is_none()
            && let Some(limit) = &self.admission
        {
            let Some(admission) = PendingHandshake::acquire(limit, None) else {
                self.reject(peer_ident.clone(), "socket pending-handshake limit reached");
                return false;
            };
            let deadline = match self.options.handshake_timeout {
                Some(timeout) => {
                    let Some(deadline) = std::time::Instant::now().checked_add(timeout) else {
                        self.reject(peer_ident.clone(), "setup timeout exceeds clock range");
                        return false;
                    };
                    Some(deadline)
                }
                None => None,
            };
            *setup = Some(SetupState {
                deadline,
                cancel: self.cancel.clone(),
                admission,
            });
        }
        true
    }

    fn deliver(&self, conn: AnyConn) -> bool {
        match self.tx.try_send(InternalEvent::Accepted {
            conn,
            endpoint: self.endpoint.clone(),
            options: self.options.clone(),
        }) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
            Err(mpsc::error::TrySendError::Full(InternalEvent::Accepted { conn, .. })) => {
                // Keep polling other setup deadlines when the actor is busy.
                // Dropping this transport releases admission; peers reconnect.
                self.reject(conn.peer_ident().clone(), "socket setup mailbox full");
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => unreachable!(),
        }
    }

    fn reject(&self, peer_ident: omq_proto::monitor::PeerIdent, reason: &str) {
        self.monitor.publish(MonitorEvent::HandshakeFailed {
            endpoint: self.endpoint.clone(),
            peer_ident,
            reason: reason.into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    async fn connection() -> (AnyConn, tokio::net::TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, address) = listener.accept().await.unwrap();
        (
            AnyConn::ByteStream {
                stream: crate::socket::dispatch::AnyStream::Tcp(stream),
                peer_ident: omq_proto::monitor::PeerIdent::Socket(address),
                leftover: bytes::Bytes::new(),
                setup: None,
            },
            client,
        )
    }

    #[tokio::test]
    async fn full_mailbox_closes_excess_transport_and_releases_its_admission() {
        let (tx, mut rx) = mpsc::channel(1);
        let admission = Admission::new(2);
        let task = ListenerTask {
            options: Arc::new(Options::default()),
            endpoint: "tcp://127.0.0.1:1".parse().unwrap(),
            cancel: CancellationToken::new(),
            tx,
            admission: Some(admission.clone()),
            monitor: MonitorPublisher::new(),
        };
        let (mut first, _first_client) = connection().await;
        assert!(task.admit(&mut first));
        assert!(task.deliver(first));
        let (mut excess, mut excess_client) = connection().await;
        assert!(task.admit(&mut excess));
        assert!(admission.try_acquire().is_none());
        assert!(
            task.deliver(excess),
            "full mailbox must not stop the listener"
        );
        assert!(admission.try_acquire().is_some());
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), excess_client.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        drop(rx.recv().await);
        let _first_slot = admission.try_acquire().unwrap();
        let _second_slot = admission.try_acquire().unwrap();
        assert!(admission.try_acquire().is_none());
    }
}
