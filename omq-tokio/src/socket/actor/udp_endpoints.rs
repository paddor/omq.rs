//! UDP endpoints: DISH listeners and RADIO senders without ZMTP peers.

use super::peer_materialize::PeerChannels;
use super::{
    Endpoint, EndpointTask, Error, MonitorEvent, PeerIdent, Result, SocketDriver, SocketType,
    UdpDialerEntry, spawn_dish_listener, spawn_radio_sender,
};

impl SocketDriver {
    /// Returns how many UDP dialers to `endpoint` were removed.
    pub(super) fn remove_udp_dialers_to(&mut self, endpoint: &Endpoint) -> usize {
        // Cancel matching UDP dialers AND tell the SendStrategy the
        // synthetic peer is gone so RADIO stops queuing through it.
        let removed_peers: Vec<u64> = self
            .endpoints
            .udp_dialers
            .extract_if(.., |d| d.task.endpoint == *endpoint)
            .map(|d| d.peer_id)
            .collect();

        let removed = removed_peers.len();
        for pid in removed_peers {
            // UDP dialers use their peer id as route id.
            self.routing.send_strategy.connection_removed(pid, pid);
        }

        removed
    }

    /// Bind a UDP DISH listener. Validates socket type, opens the
    /// socket, registers the listener task, publishes
    /// [`MonitorEvent::Listening`]. UDP listeners do not register a
    /// peer entry - datagrams are pushed straight onto `recv_tx`.
    pub(super) async fn bind_udp(&mut self, endpoint: Endpoint) -> Result<Endpoint> {
        if self.socket_type != SocketType::Dish {
            return Err(Error::Protocol(
                "udp:// bind is only supported on DISH sockets".into(),
            ));
        }

        let network = self.resources.io_pool.network_primary();
        network.validate_datagrams(crate::transport::udp::MAX_DATAGRAM_SIZE)?;
        let sock = crate::transport::udp::bind(&endpoint).await?;
        let local = sock.local_addr()?;

        let resolved = match &endpoint {
            Endpoint::Udp { group, .. } => Endpoint::Udp {
                group: group.clone(),
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
            },
            _ => unreachable!("checked above"),
        };

        self.link.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });

        let cancel = self.link.cancel.child_token();
        let delivery = crate::socket::udp::DishDelivery {
            recv_tx: self.link.recv_tx.clone(),
            joined: self.routing.joined_groups.clone(),
            pool: self.resources.payload_pools.receive(),
        };
        let task = spawn_dish_listener(
            sock,
            delivery,
            cancel.clone(),
            &self.resources.io_pool,
            self.resources.io_pool.reserve_thread(),
        );

        let ret = resolved.clone();
        self.endpoints.udp_listeners.push(EndpointTask {
            endpoint: resolved,
            cancel,
            task,
        });

        Ok(ret)
    }

    /// Establish a UDP RADIO outbound. Validates socket type, opens
    /// the socket, registers a synthetic peer with the `SendStrategy`
    /// so `send` routes through the sender task's inbox.
    pub(super) async fn start_dial_udp(&mut self, endpoint: Endpoint) -> Result<()> {
        if self.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }

        let network = self.resources.io_pool.network_primary();
        network.validate_datagrams(crate::transport::udp::MAX_DATAGRAM_SIZE)?;
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let peer_id = self
            .alloc_id()
            .ok_or_else(|| Error::Config("peer ID exhausted".into()))?;

        let PeerChannels {
            handle,
            control,
            data,
        } = self.peer_channels();
        let cancel = handle.cancel.clone();

        let assignment = self.resources.io_pool.reserve_thread();
        let index = assignment.index();
        let inboxes = crate::socket::udp::RadioInboxes { control, data };
        let task = spawn_radio_sender(
            sock,
            inboxes,
            cancel.clone(),
            &self.resources.io_pool,
            assignment,
        );

        // Register the synthetic peer with SendStrategy as an
        // any-groups RADIO target - UDP DISH never sends JOIN, so the
        // sender must fan out unconditionally. The receiver filters.
        self.routing
            .send_strategy
            .connection_added_any_groups(peer_id, handle, index);

        // Synthesise Connected so users see the same monitor signal
        // they'd get for any other transport. PeerIdent is the
        // post-connect remote address when known.
        let peer_ident = PeerIdent::Path(format!("{endpoint}"));
        self.link.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident,
            connection_id: peer_id,
        });

        self.endpoints.udp_dialers.push(UdpDialerEntry {
            task: EndpointTask {
                endpoint,
                cancel,
                task,
            },
            peer_id,
        });

        Ok(())
    }
}
