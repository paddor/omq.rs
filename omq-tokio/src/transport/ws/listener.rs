//! Raw accepts and bounded concurrent TLS/HTTP setup.
//!
//! Each listener admits at most 32 pending peers under socket admission.
//! Reservations and the original accept deadline survive actor/driver handoff.
//! Saturation closes excess connections without allocating another setup task;
//! service yields after 32 operations or 128 KiB of logical work. Ready peers
//! have a separate socket-wide `WsOptions::max_ready_peers` limit.

use std::net::SocketAddr;
use std::time::Instant;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio::net::{TcpListener, TcpStream};

use super::{WsAccepted, accept};
use crate::transport::setup::AcceptSetup;
use crate::transport::setup::{Admission, PendingHandshake, SetupState};
use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::{Error, Result};

type PendingAccept = BoxFuture<'static, (SocketAddr, Result<(WsAccepted, SetupState)>)>;

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    endpoint: Endpoint,
    monitor_endpoint: Endpoint,
    pub(crate) tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    setup: AcceptSetup,
    policy: std::sync::Arc<super::upgrade::ServerPolicy>,
    listener_admission: Admission,
    pending: FuturesUnordered<PendingAccept>,
}

impl WsListener {
    pub(crate) fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub(crate) fn set_monitor_endpoint(&mut self, endpoint: Endpoint) {
        self.monitor_endpoint = endpoint;
    }

    pub(crate) async fn accept(&mut self) -> Result<(WsAccepted, SocketAddr, SetupState)> {
        // Every completed upgrade consumed at most one bounded HTTP header.
        // Yield during raw-accept/rejection floods even when all futures are ready.
        let mut budget = omq_proto::flow::DrainBudget::new(32, 128 * 1024);
        loop {
            if !budget.account(4096) {
                tokio::task::yield_now().await;
                budget = omq_proto::flow::DrainBudget::new(32, 128 * 1024);
            }
            tokio::select! {
                biased;
                () = self.setup.cancel.cancelled() => return Err(Error::Closed),
                Some((addr, result)) = self.pending.next(), if !self.pending.is_empty() => {
                    match result {
                        Ok((accepted, setup)) => return Ok((accepted, addr, setup)),
                        Err(error) => self.reject(addr, error.to_string()),
                    }
                }
                result = self.inner.accept() => {
                    let (stream, addr) = result.map_err(Error::Io)?;
                    self.start_setup(stream, addr);
                }
            }
        }
    }

    fn start_setup(&mut self, stream: TcpStream, addr: SocketAddr) {
        let Some(admission) =
            PendingHandshake::acquire(&self.setup.admission, Some(&self.listener_admission))
        else {
            self.reject(addr, "pending-handshake limit reached".into());
            return;
        };
        let Some(deadline) = Instant::now().checked_add(self.setup.timeout) else {
            self.reject(addr, "transport setup timeout exceeds clock range".into());
            return;
        };
        let setup = SetupState {
            deadline: Some(deadline),
            cancel: self.setup.cancel.clone(),
            admission,
        };
        let tls_acceptor = self.tls_acceptor.clone();
        let policy = self.policy.clone();
        self.pending.push(Box::pin(async move {
            let result = tokio::time::timeout_at(
                deadline.into(),
                accept(stream, tls_acceptor.as_ref(), &policy),
            )
            .await
            .map_err(|_| Error::HandshakeFailed("transport setup timeout".into()))
            .and_then(std::convert::identity)
            .map(|accepted| (accepted, setup));
            (addr, result)
        }));
    }

    fn reject(&self, addr: SocketAddr, reason: String) {
        self.setup
            .monitor
            .publish(omq_proto::MonitorEvent::HandshakeFailed {
                endpoint: self.monitor_endpoint.clone(),
                peer_ident: omq_proto::monitor::PeerIdent::Socket(addr),
                reason,
            });
    }
}

pub(crate) async fn bind(
    endpoint: &Endpoint,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    setup: AcceptSetup,
    options: &omq_proto::Options,
) -> Result<WsListener> {
    use std::net::{IpAddr, Ipv4Addr};
    let (host, port, path) = match endpoint {
        Endpoint::Ws { host, port, path } | Endpoint::Wss { host, port, path } => {
            (host, *port, path)
        }
        other => {
            return Err(Error::InvalidEndpoint(format!(
                "WS transport got non-WS endpoint: {other}"
            )));
        }
    };
    omq_proto::proto::ws_handshake::validate_ws_address(host, path)?;
    let policy = std::sync::Arc::new(super::upgrade::ServerPolicy::new(path, options)?);
    let addr = match host {
        Host::Wildcard => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
        Host::Ip(ip) => SocketAddr::new(*ip, port),
        Host::Name(name) => crate::transport::dns::resolve(name, port)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::InvalidEndpoint(format!("DNS lookup failed for {name}")))?,
        _ => unreachable!(),
    };
    let listener = crate::transport::tcp::reuse_addr_bind(addr)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    let resolved = match endpoint {
        Endpoint::Ws { .. } => Endpoint::Ws {
            host: Host::Ip(local.ip()),
            port: local.port(),
            path: path.clone(),
        },
        Endpoint::Wss { .. } => Endpoint::Wss {
            host: Host::Ip(local.ip()),
            port: local.port(),
            path: path.clone(),
        },
        _ => unreachable!(),
    };
    Ok(WsListener {
        inner: listener,
        monitor_endpoint: resolved.clone(),
        endpoint: resolved,
        tls_acceptor,
        setup,
        policy,
        listener_admission: Admission::new(32),
        pending: FuturesUnordered::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::socket::monitor::MonitorPublisher;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn raw_accepts_respect_both_caps_and_release_every_reservation_on_drop() {
        for (socket_limit, expected) in [(3, 3), (64, 32)] {
            let admission = Admission::new(socket_limit);
            let endpoint = "ws://127.0.0.1:0/".parse().unwrap();
            let mut listener = bind(
                &endpoint,
                None,
                AcceptSetup {
                    admission: admission.clone(),
                    timeout: Duration::from_secs(30),
                    cancel: CancellationToken::new(),
                    monitor: MonitorPublisher::new(),
                    io_pool: crate::context::IoPoolHandle::none(),
                },
                &omq_proto::Options::default(),
            )
            .await
            .unwrap();
            let address = listener.inner.local_addr().unwrap();
            let mut clients = Vec::new();
            for _ in 0..=expected {
                clients.push(TcpStream::connect(address).await.unwrap());
                let (stream, peer) = listener.inner.accept().await.unwrap();
                listener.start_setup(stream, peer);
            }
            assert_eq!(listener.pending.len(), expected);
            let free: Vec<_> = (expected..socket_limit)
                .map(|_| admission.try_acquire().unwrap())
                .collect();
            assert!(admission.try_acquire().is_none());
            drop(free);
            drop(listener);
            let released: Vec<_> = (0..socket_limit)
                .map(|_| admission.try_acquire().unwrap())
                .collect();
            assert_eq!(released.len(), socket_limit);
        }
    }
}
