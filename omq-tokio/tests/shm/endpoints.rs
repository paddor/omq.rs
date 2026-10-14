//! Multiple bindings, connections, and mixed transports on SHM-capable sockets.

use std::collections::BTreeSet;

use super::{DEADLINE, endpoint, options, receive};
use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Endpoint, Message, Options, Socket, SocketType};

fn transport_options() -> Options {
    let options = options();
    #[cfg(feature = "quic")]
    let options = {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        Options {
            quic: omq_tokio::options::QuicOptions {
                server_cert_pem: Some(certificate.cert.pem().into_bytes()),
                server_key_pem: Some(certificate.signing_key.serialize_pem().into_bytes()),
                trust_pem: Some(certificate.cert.pem().into_bytes()),
                trust_system: false,
                server_name: Some("localhost".into()),
                ..omq_tokio::options::QuicOptions::default()
            },
            ..options
        }
    };
    options
}

fn endpoints() -> Vec<Endpoint> {
    let nonce = rand::random::<u64>();
    let mut endpoints = vec![
        endpoint(),
        endpoint(),
        format!("inproc://shm-mixed-{nonce}").parse().unwrap(),
        format!("ipc:///tmp/omq-shm-mixed-{nonce}.sock")
            .parse()
            .unwrap(),
    ];
    let network = &[
        "tcp://127.0.0.1:0",
        #[cfg(feature = "quic")]
        "quic://127.0.0.1:0",
        #[cfg(feature = "dart")]
        "dart://127.0.0.1:0",
    ];
    endpoints.extend(network.iter().map(|address| address.parse().unwrap()));
    endpoints
}

fn contexts() -> [Context; 3] {
    [
        Context::new(),
        Context::with_config(ContextConfig { io_threads: 2 }),
        Context::current(),
    ]
}

fn body(index: usize, size: usize) -> Message {
    let mut bytes = vec![0xa5; size];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    Message::from_slice(&bytes)
}

async fn bind_all(socket: &Socket) -> Vec<Endpoint> {
    let mut bound = Vec::new();
    for address in endpoints() {
        bound.push(socket.bind(address).await.unwrap());
    }
    bound
}

async fn connect_all(socket: &Socket, bound: &[Endpoint]) {
    for address in bound {
        socket.connect(address.clone()).await.unwrap();
    }
    socket.wait_connected(bound.len(), DEADLINE).await.unwrap();
}

#[tokio::test]
async fn gather_receives_from_every_mixed_binding() {
    for context in contexts() {
        let options = transport_options();
        let gather = context.socket(SocketType::Gather, options.clone());
        let bound = bind_all(&gather).await;
        let mut senders = Vec::new();
        for address in &bound {
            let sender = context.socket(SocketType::Scatter, options.clone());
            sender.connect(address.clone()).await.unwrap();
            sender.wait_connected(1, DEADLINE).await.unwrap();
            senders.push(sender);
        }
        gather.wait_connected(bound.len(), DEADLINE).await.unwrap();
        for size in [16, 1024, 128 * 1024] {
            for (index, sender) in senders.iter().enumerate() {
                sender.send(body(index, size)).await.unwrap();
            }
            let mut seen = BTreeSet::new();
            for _ in &senders {
                let message = receive(&gather).await;
                let bytes = message.part_slice(0).unwrap();
                let index =
                    usize::try_from(u64::from_le_bytes(bytes[..8].try_into().unwrap())).unwrap();
                assert!(index < senders.len() && seen.insert(index));
                assert_eq!(message, body(index, size));
            }
        }
        for sender in senders {
            sender.close().await.unwrap();
        }
        gather.close().await.unwrap();
    }
}

#[tokio::test]
async fn scatter_distributes_over_multiple_shm_and_mixed_connections() {
    for context in contexts() {
        let options = transport_options();
        let scatter = context.socket(SocketType::Scatter, options.clone());
        let mut receivers = Vec::new();
        let mut bound = Vec::new();
        for address in endpoints() {
            let gather = context.socket(SocketType::Gather, options.clone());
            bound.push(gather.bind(address).await.unwrap());
            receivers.push(gather);
        }
        connect_all(&scatter, &bound).await;
        for receiver in &receivers {
            receiver.wait_connected(1, DEADLINE).await.unwrap();
        }
        let count = receivers.len() * 4;
        let (tx, mut rx) = tokio::sync::mpsc::channel(count);
        let mut tasks = tokio::task::JoinSet::new();
        for (index, receiver) in receivers.iter().enumerate() {
            let receiver = receiver.clone_shared();
            let tx = tx.clone();
            tasks.spawn(async move {
                for _ in 0..count {
                    let message = receive(&receiver).await;
                    tx.send((index, message)).await.unwrap();
                }
            });
        }
        drop(tx);
        for index in 0..count {
            scatter.send(body(index, 1024)).await.unwrap();
        }
        let mut received = BTreeSet::new();
        let mut sources = BTreeSet::new();
        for _ in 0..count {
            let (source, message) = tokio::time::timeout(DEADLINE, rx.recv())
                .await
                .unwrap()
                .unwrap();
            let bytes = message.part_slice(0).unwrap();
            let index =
                usize::try_from(u64::from_le_bytes(bytes[..8].try_into().unwrap())).unwrap();
            assert!(index < count && received.insert(index));
            assert_eq!(message, body(index, 1024));
            sources.insert(source);
        }
        assert_eq!(
            sources.len(),
            receivers.len(),
            "a ready transport was starved"
        );
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        scatter.close().await.unwrap();
        for receiver in receivers {
            receiver.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn shared_client_and_server_route_replies_over_every_mixed_endpoint() {
    for context in contexts() {
        for bind_server in [true, false] {
            let options = transport_options();
            let server = context.socket(SocketType::Server, options.clone());
            let client = context.socket(SocketType::Client, options);
            let (listener, connector) = if bind_server {
                (&server, &client)
            } else {
                (&client, &server)
            };
            let bound = bind_all(listener).await;
            connect_all(connector, &bound).await;
            listener
                .wait_connected(bound.len(), DEADLINE)
                .await
                .unwrap();
            for size in [16, 1024, 128 * 1024] {
                let mut routes = BTreeSet::new();
                for index in 0..bound.len() * 2 {
                    let message = body(index, size);
                    client.send(message.clone()).await.unwrap();
                    let request = receive(&server).await;
                    assert_eq!(request.part_slice(0), message.part_slice(0));
                    routes.insert(request.routing_id().unwrap());
                    server.send(request).await.unwrap();
                    assert_eq!(receive(&client).await, message);
                }
                assert_eq!(routes.len(), bound.len());
            }
            client.close().await.unwrap();
            server.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn peer_routes_identities_across_multiple_shm_and_mixed_bindings() {
    for context in contexts() {
        for outbound in [false, true] {
            let options = transport_options();
            let hub = context.socket(
                SocketType::Peer,
                options.clone().identity(Bytes::from_static(b"hub")),
            );
            let endpoints = endpoints();
            let mut peers = Vec::new();
            for (index, address) in endpoints.iter().enumerate() {
                let identity = Bytes::from(format!("peer-{index}"));
                let peer =
                    context.socket(SocketType::Peer, options.clone().identity(identity.clone()));
                let (listener, connector) = if outbound {
                    (&peer, &hub)
                } else {
                    (&hub, &peer)
                };
                let bound = listener.bind(address.clone()).await.unwrap();
                connector.connect(bound).await.unwrap();
                peer.wait_connected(1, DEADLINE).await.unwrap();
                peers.push((identity, peer));
            }
            hub.wait_connected(endpoints.len(), DEADLINE).await.unwrap();
            for size in [16, 1024, 128 * 1024] {
                for (index, (identity, peer)) in peers.iter().enumerate() {
                    peer.send_to(b"hub", body(index, size)).await.unwrap();
                    let mut message = receive(&hub).await;
                    assert_eq!(message.pop_front().unwrap(), *identity);
                    assert_eq!(message, body(index, size));
                    hub.send_to(identity, message).await.unwrap();
                    let mut reply = receive(peer).await;
                    assert_eq!(reply.pop_front().unwrap(), b"hub".as_slice());
                    assert_eq!(reply, body(index, size));
                }
            }
            for (_, peer) in peers {
                peer.close().await.unwrap();
            }
            hub.close().await.unwrap();
        }
    }
}

async fn wait_exclusive_pair(first: &Socket, second: &Socket) {
    // Exclusive sockets can initially admit different competing connections.
    // Traffic checks need both sides to settle on the same endpoint.
    tokio::time::timeout(DEADLINE, async {
        loop {
            let first = first.connections().await.unwrap();
            let second = second.connections().await.unwrap();
            if let ([first], [second]) = (first.as_slice(), second.as_slice())
                && first.endpoint == second.endpoint
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn channel_connects_through_multiple_shm_and_mixed_endpoints() {
    for context in contexts() {
        let options = transport_options();
        let listener = context.socket(SocketType::Channel, options.clone());
        let bound = bind_all(&listener).await;
        let connector = context.socket(SocketType::Channel, options);
        for address in bound {
            connector.connect(address).await.unwrap();
        }
        connector.wait_connected(1, DEADLINE).await.unwrap();
        listener.wait_connected(1, DEADLINE).await.unwrap();
        wait_exclusive_pair(&connector, &listener).await;
        for (index, size) in [16, 1024, 128 * 1024].into_iter().enumerate() {
            let message = body(index, size);
            connector.send(message.clone()).await.unwrap();
            listener.send(receive(&listener).await).await.unwrap();
            assert_eq!(receive(&connector).await, message);
        }
        connector.close().await.unwrap();
        listener.close().await.unwrap();
    }
}

#[tokio::test]
async fn channel_switches_peers_while_multiple_mixed_bindings_remain_live() {
    for context in contexts() {
        let options = transport_options();
        let channel = context.socket(SocketType::Channel, options.clone());
        let bound = bind_all(&channel).await;
        let mut monitor = channel.monitor();
        for (index, address) in bound.into_iter().enumerate() {
            let peer = context.socket(SocketType::Channel, options.clone());
            peer.connect(address).await.unwrap();
            peer.wait_connected(1, DEADLINE).await.unwrap();
            channel.wait_connected(1, DEADLINE).await.unwrap();
            for size in [16, 1024, 128 * 1024] {
                let message = body(index, size);
                peer.send(message.clone()).await.unwrap();
                channel.send(receive(&channel).await).await.unwrap();
                assert_eq!(receive(&peer).await, message);
            }
            peer.close().await.unwrap();
            tokio::time::timeout(DEADLINE, async {
                while channel.ready_peer_count() != 0 {
                    monitor.recv().await.unwrap();
                }
            })
            .await
            .unwrap();
        }
        channel.close().await.unwrap();
    }
}
