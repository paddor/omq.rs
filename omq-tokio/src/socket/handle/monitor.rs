use super::Socket;
use crate::socket::actor::SocketCommand;
use crate::socket::monitor::{ConnectionStatus, MonitorStream, PeerInfo};
use futures::channel::oneshot;
use omq_proto::{
    error::{Error, Result},
    proto::SocketType,
};
use std::sync::atomic::{AtomicUsize, Ordering};

impl Socket {
    /// Subscribe to connection-lifecycle events. Multiple monitors can be
    /// active simultaneously; each sees every event from subscription time
    /// onward. Cheap: backed by a broadcast channel.
    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor.subscribe()
    }

    /// Snapshot the live status of one connected peer by `connection_id`.
    /// `Ok(None)` means no peer with that id exists (never connected, or
    /// already disconnected). `Err(Error::Closed)` means the socket
    /// driver is gone.
    pub async fn connection_info(&self, connection_id: u64) -> Result<Option<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnection { connection_id, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Snapshot the peer addressed by a routing id received on a SERVER socket.
    /// `Ok(None)` means the route is stale or unknown.
    pub async fn peer_info(&self, routing_id: u32) -> Result<Option<PeerInfo>> {
        if self.inner.socket_type != SocketType::Server {
            return Err(Error::Protocol(
                "peer_info is only valid on SERVER sockets".into(),
            ));
        }
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryPeerInfo { routing_id, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Wait until at least `min_peers` peers have completed the ZMTP
    /// handshake, or `timeout` expires. Returns the peer count at the
    /// time the threshold was met, or `Error::Timeout` if the deadline
    /// is reached first.
    ///
    /// This is a data-plane readiness check. It waits for ZMTP peers to
    /// finish handshaking rather than only being accepted by the listener.
    /// `STREAM` peers are ready as soon as the raw TCP connection exists.
    pub async fn wait_connected(
        &self,
        min_peers: usize,
        timeout: std::time::Duration,
    ) -> Result<usize> {
        let deadline = crate::socket::deadline_after(timeout).map(tokio::time::Instant::from_std);
        let changed = &self.inner.ready_peer_count.changed;
        loop {
            // Snapshot first: the actor answers after any readiness change
            // it already published, and notifies for later ones.
            let seen = changed.generation();
            let conns = self.connections().await?;
            let ready = if self.inner.socket_type == SocketType::Stream {
                conns.len()
            } else {
                conns.iter().filter(|conn| conn.peer_info.is_some()).count()
            };
            if ready >= min_peers {
                return Ok(ready);
            }
            match deadline {
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, changed.changed_after(seen))
                        .await
                        .is_err()
                    {
                        return Err(Error::Timeout);
                    }
                }
                None => changed.changed_after(seen).await,
            }
        }
    }

    /// Wait until the socket has received at least `min_subscriptions`
    /// subscription commands from peers, or until `timeout` expires.
    /// Returns the total subscription count at the time the threshold
    /// was met, or `Error::Timeout`.
    ///
    /// Reads an atomic counter incremented by the actor on each
    /// `Subscribe` command, so it reflects fully-processed subscriptions
    /// (after routing registration), not just wire arrival.
    pub async fn wait_subscribed(
        &self,
        min_subscriptions: u64,
        timeout: std::time::Duration,
    ) -> Result<u64> {
        let deadline = crate::socket::deadline_after(timeout).map(tokio::time::Instant::from_std);
        loop {
            let count = self.inner.subscribe_count.load(Ordering::Acquire);
            if count >= min_subscriptions {
                return Ok(count);
            }
            if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                return Err(Error::Timeout);
            }
            let now = tokio::time::Instant::now();
            let poll_deadline = now + std::time::Duration::from_millis(5);
            tokio::time::sleep_until(deadline.map_or(poll_deadline, |d| d.min(poll_deadline)))
                .await;
        }
    }

    /// Snapshot every peer that is ready for data-plane routing. Empty
    /// vec when no peers are ready. Useful for introspection / health
    /// checks.
    pub async fn connections(&self) -> Result<Vec<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnections { ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Generation of the ready connection set. It advances whenever a peer
    /// becomes ready or a ready peer goes away, so every change to
    /// [`Self::connections`] moves it. Unlike monitor events, it never lags.
    pub fn connections_generation(&self) -> u64 {
        self.inner.ready_peer_count.changed.generation()
    }

    /// Wait until the connection generation differs from `seen` and return
    /// the new generation. Take `seen` before the [`Self::connections`]
    /// snapshot it describes, so no change between the two is missed.
    /// Returns `Err(Error::Closed)` once the socket is closing and `seen`
    /// is current.
    pub async fn connections_changed(&self, seen: u64) -> Result<u64> {
        let changed = &self.inner.ready_peer_count.changed;
        tokio::select! {
            biased;
            () = changed.changed_after(seen) => Ok(changed.generation()),
            () = self.inner.cmd_tx.closed() => Err(Error::Closed),
        }
    }
}

/// Ready peer count, shared by a socket's handles and its actor. Every
/// change wakes `wait_connected` and send-readiness waits.
#[derive(Debug, Default)]
pub(crate) struct ReadyPeers {
    count: AtomicUsize,
    changed: crate::engine::signal::StateSignal,
}

impl ReadyPeers {
    pub(crate) fn get(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    pub(crate) fn add(&self) {
        self.count.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_changed();
    }

    pub(crate) fn remove(&self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_changed();
    }

    pub(crate) fn set(&self, count: usize) {
        self.count.store(count, Ordering::Release);
        self.changed.notify_changed();
    }
}
