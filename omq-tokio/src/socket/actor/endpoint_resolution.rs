//! Named endpoint resolution without blocking the socket actor.
//!
//! Initial bind/connect DNS errors go back to the API caller. A successful
//! connect retains the hostname for TLS and future reconnects. Later DNS
//! failures belong to the dialer's silent retry loop, never send/recv.

use omq_proto::Options;
use std::sync::Arc;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{Endpoint, Error, InternalEvent, Result, SocketDriver};
use crate::transport::setup::PendingHandshake;
use omq_proto::endpoint::Host;

const MAX_PENDING_ENDPOINTS: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Bind,
    Connect,
}

pub(super) enum Ack {
    Bind(oneshot::Sender<Result<Endpoint>>),
    Connect(oneshot::Sender<Result<()>>),
}

impl Ack {
    fn kind(&self) -> Kind {
        match self {
            Self::Bind(_) => Kind::Bind,
            Self::Connect(_) => Kind::Connect,
        }
    }

    async fn canceled(&mut self) {
        match self {
            Self::Bind(ack) => ack.cancellation().await,
            Self::Connect(ack) => ack.cancellation().await,
        }
    }

    fn is_canceled(&self) -> bool {
        match self {
            Self::Bind(ack) => ack.is_canceled(),
            Self::Connect(ack) => ack.is_canceled(),
        }
    }

    fn error(self, error: Error) {
        match self {
            Self::Bind(ack) => {
                let _ = ack.send(Err(error));
            }
            Self::Connect(ack) => {
                let _ = ack.send(Err(error));
            }
        }
    }
}

pub(super) struct ResolvedEndpoint {
    pub(super) endpoint: Endpoint,
    pub(super) first_deadline: Option<Instant>,
    pub(super) admission: Option<PendingHandshake>,
}

pub(super) struct PendingEndpoint {
    options: Arc<Options>,
    pub(super) original: Endpoint,
    pub(super) kind: Kind,
    pub(super) cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Drop for PendingEndpoint {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

pub(super) fn needs_dns(endpoint: &Endpoint) -> bool {
    #[cfg(feature = "dart")]
    if let Endpoint::Dart { host, .. } = endpoint {
        return matches!(host, Host::Name(_));
    }
    if endpoint.is_tcp_family() {
        return matches!(
            endpoint.underlying_tcp(),
            Endpoint::Tcp {
                host: Host::Name(_),
                ..
            }
        );
    }
    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        return matches!(
            endpoint.underlying_ws(),
            Endpoint::Ws {
                host: Host::Name(_),
                ..
            } | Endpoint::Wss {
                host: Host::Name(_),
                ..
            }
        );
    }
    #[cfg(feature = "quic")]
    if let Endpoint::Quic { host, .. } = endpoint {
        return matches!(host, Host::Name(_));
    }
    false
}

async fn bind_address(endpoint: Endpoint) -> Result<Endpoint> {
    #[cfg(feature = "dart")]
    if let Endpoint::Dart {
        host: Host::Name(host),
        port,
    } = &endpoint
    {
        let address = first(host, *port).await?;
        return Ok(Endpoint::Dart {
            host: Host::Ip(address.ip()),
            port: *port,
        });
    }
    if endpoint.is_tcp_family() {
        let Endpoint::Tcp {
            host: Host::Name(host),
            port,
        } = endpoint.underlying_tcp()
        else {
            unreachable!()
        };
        let address = first(&host, port).await?;
        return Ok(endpoint.rewrap_tcp(Endpoint::Tcp {
            host: Host::Ip(address.ip()),
            port,
        }));
    }
    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        let underlying = endpoint.underlying_ws();
        let (host, port, path, tls) = match underlying {
            Endpoint::Ws {
                host: Host::Name(host),
                port,
                path,
            } => (host, port, path, false),
            Endpoint::Wss {
                host: Host::Name(host),
                port,
                path,
            } => (host, port, path, true),
            _ => unreachable!(),
        };
        let address = first(&host, port).await?;
        let resolved = if tls {
            Endpoint::Wss {
                host: Host::Ip(address.ip()),
                port,
                path,
            }
        } else {
            Endpoint::Ws {
                host: Host::Ip(address.ip()),
                port,
                path,
            }
        };
        return Ok(endpoint.rewrap_ws(resolved));
    }
    #[cfg(feature = "quic")]
    if let Endpoint::Quic {
        host: Host::Name(host),
        port,
    } = &endpoint
    {
        let address = first(host, *port).await?;
        return Ok(Endpoint::Quic {
            host: Host::Ip(address.ip()),
            port: *port,
        });
    }
    unreachable!("only named network endpoints are dispatched")
}

async fn first(host: &str, port: u16) -> Result<SocketAddr> {
    crate::transport::dns::resolve(host, port)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::InvalidEndpoint(format!("DNS lookup failed for {host}")))
}

struct EndpointTask {
    id: u64,
    kind: Kind,
    endpoint: Endpoint,
    cancel: CancellationToken,
    admission: Option<crate::transport::setup::Admission>,
    setup_timeout: Option<Duration>,
    tx: tokio::sync::mpsc::Sender<InternalEvent>,
}

impl EndpointTask {
    async fn run(self, mut ack: Ack) {
        let result = tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(Error::Closed),
            () = ack.canceled() => Err(Error::Closed),
            result = self.prepare() => result,
        };
        let _ = self
            .tx
            .send(InternalEvent::EndpointResolved {
                id: self.id,
                ack,
                result,
            })
            .await;
    }

    async fn prepare(&self) -> Result<ResolvedEndpoint> {
        // Wait without holding the actor, then carry one peer setup credit
        // from DNS through connect, HTTP/TLS, and ZMTP.
        let admission = match &self.admission {
            Some(limit) => Some(PendingHandshake::from_socket(limit.acquire().await)),
            None => None,
        };
        let now = Instant::now();
        let first_deadline = self
            .setup_timeout
            .map(|timeout| {
                now.checked_add(timeout)
                    .ok_or_else(|| Error::HandshakeFailed("DNS timeout exceeds clock range".into()))
            })
            .transpose()?;
        let deadline = first_deadline
            .or_else(|| now.checked_add(omq_proto::options::DEFAULT_HANDSHAKE_TIMEOUT))
            .ok_or_else(|| Error::HandshakeFailed("DNS timeout exceeds clock range".into()))?;
        let endpoint = tokio::time::timeout_at(deadline.into(), self.resolve())
            .await
            .map_err(|_| Error::HandshakeFailed("DNS resolution timeout".into()))??;
        Ok(ResolvedEndpoint {
            endpoint,
            first_deadline,
            admission,
        })
    }

    async fn resolve(&self) -> Result<Endpoint> {
        loop {
            let result = match self.kind {
                Kind::Bind => bind_address(self.endpoint.clone()).await,
                Kind::Connect => {
                    #[cfg(feature = "dart")]
                    if matches!(self.endpoint, Endpoint::Dart { .. }) {
                        return bind_address(self.endpoint.clone()).await;
                    }
                    super::super::dispatch::preflight_connect_endpoint_resolution(&self.endpoint)
                        .await
                        .map(|()| self.endpoint.clone())
                }
            };
            if matches!(&result, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            } else {
                return result;
            }
        }
    }
}

impl SocketDriver {
    pub(super) fn start_endpoint_resolution(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
        ack: Ack,
    ) {
        if self.pending_endpoints.len() == MAX_PENDING_ENDPOINTS {
            ack.error(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "pending endpoint resolution limit reached",
            )));
            return;
        }
        let kind = ack.kind();
        let admission = (kind == Kind::Connect && self.socket_type != super::SocketType::Stream)
            .then(|| self.setup_admission.clone());
        let setup_timeout = super::endpoints::dial_setup_timeout(&endpoint, &options);
        let id = self.next_peer_id;
        self.next_peer_id += 1;
        let cancel = self.cancel.child_token();
        let task_cancel = cancel.clone();
        let task_endpoint = endpoint.clone();
        let tx = self.internal_tx.clone();
        let task = tokio::spawn(
            EndpointTask {
                id,
                kind,
                endpoint: task_endpoint,
                cancel: task_cancel,
                admission,
                setup_timeout,
                tx,
            }
            .run(ack),
        );
        self.pending_endpoints.insert(
            id,
            PendingEndpoint {
                options,
                original: endpoint,
                kind,
                cancel,
                task,
            },
        );
    }

    pub(super) async fn finish_endpoint_resolution(
        &mut self,
        id: u64,
        ack: Ack,
        result: Result<ResolvedEndpoint>,
    ) {
        let Some(pending) = self.pending_endpoints.remove(&id) else {
            return;
        };
        if pending.cancel.is_cancelled() || ack.is_canceled() || self.closing {
            return;
        }
        match (ack, result) {
            (Ack::Bind(ack), Ok(resolved)) => {
                #[cfg(feature = "dart")]
                if matches!(resolved.endpoint, Endpoint::Dart { .. }) {
                    let _ = ack.send(
                        self.start_dart(
                            resolved.endpoint,
                            pending.original.clone(),
                            pending.options.clone(),
                            false,
                        )
                        .await,
                    );
                    return;
                }
                let _ = ack.send(self.bind(resolved.endpoint, pending.options.clone()).await);
            }
            (Ack::Connect(ack), Ok(resolved)) => {
                #[cfg(feature = "dart")]
                if matches!(resolved.endpoint, Endpoint::Dart { .. }) {
                    let _ = ack.send(
                        self.start_dart(
                            resolved.endpoint,
                            pending.original.clone(),
                            pending.options.clone(),
                            true,
                        )
                        .await
                        .map(|_| ()),
                    );
                    return;
                }
                if !self.should_ignore_duplicate_connect(&resolved.endpoint) {
                    self.start_dial_with_deadline(
                        resolved.endpoint,
                        pending.options.clone(),
                        resolved.first_deadline,
                        resolved.admission,
                    );
                }
                let _ = ack.send(Ok(()));
            }
            (ack, Err(error)) => ack.error(error),
        }
    }

    pub(super) fn cancel_pending_endpoints(&self, endpoint: Option<(&Endpoint, Kind)>) -> bool {
        let mut canceled = false;
        for pending in self.pending_endpoints.values() {
            if endpoint
                .is_none_or(|(target, kind)| target == &pending.original && kind == pending.kind)
            {
                pending.cancel.cancel();
                canceled = true;
            }
        }
        canceled
    }
}

#[cfg(test)]
mod tests;
