//! Raw accepts and bounded concurrent TLS/HTTP setup.
//!
//! Each listener admits at most 32 pending peers under socket admission.
//! Reservations and the original accept deadline survive actor/driver handoff.
//! Saturation closes excess connections without allocating another setup task;
//! service yields after 32 operations or 128 KiB of logical work. Ready peers
//! have a separate socket-wide `WsOptions::max_ready_peers` limit.

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};

use super::{WsUpgraded, accept};
use crate::transport::setup::{AcceptBudget, AcceptSetup, SetupQueue, SetupState};
use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::{Error, Result};

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    endpoint: Endpoint,
    pub(crate) tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    policy: std::sync::Arc<super::upgrade::ServerPolicy>,
    queue: SetupQueue<WsUpgraded>,
}

impl WsListener {
    pub(crate) fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub(crate) fn set_monitor_endpoint(&mut self, endpoint: Endpoint) {
        self.queue.set_monitor_endpoint(endpoint);
    }

    pub(crate) async fn accept(&mut self) -> Result<(WsUpgraded, SocketAddr, SetupState)> {
        let mut budget = AcceptBudget::new();
        loop {
            budget.charge().await;
            tokio::select! {
                biased;
                ready = self.queue.next_ready() => {
                    if let Some(ready) = ready? {
                        return Ok(ready);
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
        let Ok((setup, deadline)) = self.queue.admit(addr) else {
            return;
        };
        let tls_acceptor = self.tls_acceptor.clone();
        let policy = self.policy.clone();
        self.queue.push(async move {
            let result = tokio::time::timeout_at(
                deadline.into(),
                accept(stream, tls_acceptor.as_ref(), &policy),
            )
            .await
            .map_err(|_| Error::HandshakeFailed("transport setup timeout".into()))
            .and_then(std::convert::identity)
            .map(|accepted| (accepted, setup));
            (addr, result)
        });
    }
}

pub(crate) async fn bind(
    endpoint: &Endpoint,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    setup: AcceptSetup,
    options: &omq_proto::Options,
) -> Result<WsListener> {
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
    let addr = crate::transport::dns::bind_addr(host, port).await?;
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
        queue: SetupQueue::new(setup, resolved.clone(), 32),
        endpoint: resolved,
        tls_acceptor,
        policy,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::socket::monitor::MonitorPublisher;
    use crate::transport::setup::Admission;
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
                    monitor: MonitorPublisher::new(omq_proto::options::DEFAULT_MONITOR_CAPACITY),
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
            assert_eq!(listener.queue.len(), expected);
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
