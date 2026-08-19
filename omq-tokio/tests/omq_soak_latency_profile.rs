#![cfg(feature = "soak")]
//! Soak: regular `Socket` latency profile across every supported type.
//!
//! Keeps one TCP-bound socket alive per scenario, repeatedly connects a peer,
//! exchanges a round trip, idles long enough for heartbeats to run, closes the
//! peer, and verifies the bound side observes the disappearance.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::options::{ReconnectPolicy, WorkloadProfile};
use omq_tokio::{
    Endpoint, Message, MonitorEvent, MonitorTryRecvError, Options, Socket, SocketType,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(120);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_HEARTBEATS: Duration = Duration::from_millis(80);

#[derive(Clone, Copy, Debug)]
enum Flow {
    Pair,
    DealerRouter,
    ReqRep,
    ClientServer,
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    name: &'static str,
    bind_kind: SocketType,
    connect_kind: SocketType,
    flow: Flow,
}

struct ScenarioState {
    spec: Scenario,
    bound: Socket,
    endpoint: Endpoint,
    monitor: omq_tokio::MonitorStream,
    cycles: u64,
    disconnects: u64,
}

fn latency_options(kind: SocketType, side: &str) -> Options {
    let identity = match kind {
        SocketType::Dealer => Bytes::from(format!("dealer-{side}")),
        SocketType::Client => Bytes::from(format!("client-{side}")),
        SocketType::Req => Bytes::from(format!("req-{side}")),
        SocketType::Rep => Bytes::from(format!("rep-{side}")),
        SocketType::Router => Bytes::from(format!("router-{side}")),
        SocketType::Server => Bytes::from(format!("server-{side}")),
        SocketType::Pair => Bytes::from(format!("pair-{side}")),
        _ => Bytes::new(),
    };
    Options::default()
        .identity(identity)
        .workload_profile(WorkloadProfile::Latency)
        .send_hwm(32)
        .recv_hwm(32)
        .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(5)))
        .handshake_timeout(Duration::from_secs(1))
        .heartbeat_interval(HEARTBEAT_INTERVAL)
        .heartbeat_ttl(HEARTBEAT_TIMEOUT)
        .heartbeat_timeout(HEARTBEAT_TIMEOUT)
        .linger(Duration::ZERO)
}

async fn make_state(spec: Scenario) -> ScenarioState {
    let bound = Socket::new(spec.bind_kind, latency_options(spec.bind_kind, "bind"));
    let mut monitor = bound.monitor();
    let endpoint = bound.bind(soak_common::tcp_ep(0)).await.unwrap();
    wait_for_listening(&mut monitor).await;
    ScenarioState {
        spec,
        bound,
        endpoint,
        monitor,
        cycles: 0,
        disconnects: 0,
    }
}

async fn wait_for_listening(monitor: &mut omq_tokio::MonitorStream) {
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        match tokio::time::timeout(IO_TIMEOUT, monitor.recv()).await {
            Ok(Ok(MonitorEvent::Listening { .. })) => return,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("monitor failed before listening: {error:?}"),
            Err(error) => panic!("socket did not report Listening before timeout: {error:?}"),
        }
        assert!(Instant::now() < deadline, "listening monitor timed out");
    }
}

async fn connect_peer(state: &mut ScenarioState) -> (Socket, omq_tokio::MonitorStream) {
    let peer = Socket::new(
        state.spec.connect_kind,
        latency_options(state.spec.connect_kind, "connect"),
    );
    let mut peer_monitor = peer.monitor();
    peer.connect(state.endpoint.clone()).await.unwrap();
    peer.wait_connected(1, IO_TIMEOUT).await.unwrap();
    state.bound.wait_connected(1, IO_TIMEOUT).await.unwrap();
    wait_for_handshake(&mut peer_monitor).await;
    (peer, peer_monitor)
}

async fn wait_for_handshake(monitor: &mut omq_tokio::MonitorStream) {
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        match tokio::time::timeout(IO_TIMEOUT, monitor.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => return,
            Ok(Ok(MonitorEvent::Disconnected { reason, .. })) => {
                panic!("peer disconnected before handshake: {reason:?}");
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("monitor failed before handshake: {error:?}"),
            Err(error) => panic!("peer did not report handshake before timeout: {error:?}"),
        }
        assert!(Instant::now() < deadline, "handshake monitor timed out");
    }
}

async fn cycle(state: &mut ScenarioState) {
    let (peer, mut peer_monitor) = connect_peer(state).await;
    exchange_once(state.spec, &state.bound, &peer, state.cycles).await;
    drive_idle_heartbeats(&mut state.monitor, &mut peer_monitor, state.spec.name).await;
    peer.close_with_linger(Some(Duration::ZERO)).await.unwrap();
    wait_for_disconnect(&mut state.monitor, state.spec.name).await;
    state.disconnects += 1;
    state.cycles += 1;
}

async fn exchange_once(spec: Scenario, bound: &Socket, peer: &Socket, seq: u64) {
    match spec.flow {
        Flow::Pair => exchange_pair(bound, peer, seq).await,
        Flow::DealerRouter => exchange_dealer_router(spec, bound, peer, seq).await,
        Flow::ReqRep => exchange_req_rep(spec, bound, peer, seq).await,
        Flow::ClientServer => exchange_client_server(spec, bound, peer, seq).await,
    }
}

async fn exchange_pair(bound: &Socket, peer: &Socket, seq: u64) {
    let body = format!("pair-{seq}");
    peer.send(Message::single(body.clone())).await.unwrap();
    assert_eq!(recv("pair-bound", bound).await, Message::single(body));

    bound.send(Message::single("pair-reply")).await.unwrap();
    assert_eq!(recv("pair-peer", peer).await, Message::single("pair-reply"));
}

async fn exchange_dealer_router(spec: Scenario, bound: &Socket, peer: &Socket, seq: u64) {
    if spec.bind_kind == SocketType::Dealer {
        dealer_router_round_trip(bound, peer, seq).await;
    } else {
        dealer_router_round_trip(peer, bound, seq).await;
    }
}

async fn dealer_router_round_trip(dealer: &Socket, router: &Socket, seq: u64) {
    let body = format!("dealer-router-{seq}");
    dealer.send(Message::single(body.clone())).await.unwrap();
    let request = recv("router request", router).await;
    let identity = request.part_bytes(0).unwrap().clone();
    assert_eq!(request.part_bytes(1).unwrap(), body.as_bytes());

    router
        .send(Message::multipart([
            identity,
            Bytes::from_static(b"dealer-router-reply"),
        ]))
        .await
        .unwrap();
    assert_eq!(
        recv("dealer reply", dealer).await,
        Message::single("dealer-router-reply")
    );
}

async fn exchange_req_rep(spec: Scenario, bound: &Socket, peer: &Socket, seq: u64) {
    if spec.bind_kind == SocketType::Req {
        req_rep_round_trip(bound, peer, seq).await;
    } else {
        req_rep_round_trip(peer, bound, seq).await;
    }
}

async fn req_rep_round_trip(req: &Socket, rep: &Socket, seq: u64) {
    let body = format!("req-rep-{seq}");
    req.send(Message::single(body.clone())).await.unwrap();
    assert_eq!(recv("rep request", rep).await, Message::single(body));

    rep.send(Message::single("req-rep-reply")).await.unwrap();
    assert_eq!(
        recv("req reply", req).await,
        Message::single("req-rep-reply")
    );
}

async fn exchange_client_server(spec: Scenario, bound: &Socket, peer: &Socket, seq: u64) {
    if spec.bind_kind == SocketType::Client {
        client_server_round_trip(bound, peer, seq).await;
    } else {
        client_server_round_trip(peer, bound, seq).await;
    }
}

async fn client_server_round_trip(client: &Socket, server: &Socket, seq: u64) {
    let body = format!("client-server-{seq}");
    client.send(Message::single(body.clone())).await.unwrap();
    let request = recv("server request", server).await;
    let routing_id = request.routing_id().expect("SERVER routing id");
    assert_eq!(request.part_bytes(0).unwrap(), body.as_bytes());

    server
        .send(Message::single("client-server-reply").with_routing_id(routing_id))
        .await
        .unwrap();
    assert_eq!(
        recv("client reply", client).await,
        Message::single("client-server-reply")
    );
}

async fn recv(label: &str, socket: &Socket) -> Message {
    tokio::time::timeout(IO_TIMEOUT, socket.recv())
        .await
        .unwrap_or_else(|_| panic!("{label} timed out"))
        .unwrap()
}

async fn drive_idle_heartbeats(
    bound_monitor: &mut omq_tokio::MonitorStream,
    peer_monitor: &mut omq_tokio::MonitorStream,
    scenario: &str,
) {
    let start = Instant::now();
    while start.elapsed() < IDLE_HEARTBEATS {
        assert_no_disconnect(bound_monitor, scenario);
        assert_no_disconnect(peer_monitor, scenario);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn assert_no_disconnect(monitor: &mut omq_tokio::MonitorStream, scenario: &str) {
    loop {
        match monitor.try_recv() {
            Ok(MonitorEvent::Disconnected { reason, .. }) => {
                panic!("{scenario} disconnected during heartbeat idle: {reason:?}");
            }
            Ok(_) => {}
            Err(MonitorTryRecvError::Empty | MonitorTryRecvError::Closed) => return,
            Err(MonitorTryRecvError::Lagged(skipped)) => {
                panic!("{scenario} monitor lagged during soak: skipped {skipped}");
            }
            Err(error) => panic!("{scenario} unexpected monitor error during soak: {error:?}"),
        }
    }
}

async fn wait_for_disconnect(monitor: &mut omq_tokio::MonitorStream, scenario: &str) {
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        match tokio::time::timeout(IO_TIMEOUT, monitor.recv()).await {
            Ok(Ok(MonitorEvent::Disconnected { .. })) => return,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("{scenario} monitor failed before disconnect: {error:?}"),
            Err(error) => panic!("{scenario} did not report disconnect before timeout: {error:?}"),
        }
        assert!(Instant::now() < deadline, "{scenario} disconnect timed out");
    }
}

#[test]
fn soak_latency_profile_all_supported_socket_types() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();

    ctx.block_on(async move {
        let specs = [
            Scenario {
                name: "pair",
                bind_kind: SocketType::Pair,
                connect_kind: SocketType::Pair,
                flow: Flow::Pair,
            },
            Scenario {
                name: "dealer-router",
                bind_kind: SocketType::Router,
                connect_kind: SocketType::Dealer,
                flow: Flow::DealerRouter,
            },
            Scenario {
                name: "router-dealer",
                bind_kind: SocketType::Dealer,
                connect_kind: SocketType::Router,
                flow: Flow::DealerRouter,
            },
            Scenario {
                name: "req-rep",
                bind_kind: SocketType::Rep,
                connect_kind: SocketType::Req,
                flow: Flow::ReqRep,
            },
            Scenario {
                name: "rep-req",
                bind_kind: SocketType::Req,
                connect_kind: SocketType::Rep,
                flow: Flow::ReqRep,
            },
            Scenario {
                name: "client-server",
                bind_kind: SocketType::Server,
                connect_kind: SocketType::Client,
                flow: Flow::ClientServer,
            },
            Scenario {
                name: "server-client",
                bind_kind: SocketType::Client,
                connect_kind: SocketType::Server,
                flow: Flow::ClientServer,
            },
        ];

        let mut states = Vec::new();
        for spec in specs {
            states.push(make_state(spec).await);
        }

        let start = Instant::now();
        let mut last_log = start;
        while start.elapsed() < duration {
            for state in &mut states {
                cycle(state).await;
            }
            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!("[latency-profile] {:.0}s:", start.elapsed().as_secs_f64());
                for state in &states {
                    eprintln!(
                        "  {}: {} cycles, {} disconnects",
                        state.spec.name, state.cycles, state.disconnects
                    );
                }
                last_log = Instant::now();
            }
        }

        for state in &states {
            assert!(state.cycles > 0, "{} had no cycles", state.spec.name);
            assert_eq!(
                state.disconnects, state.cycles,
                "{} missed disconnect events",
                state.spec.name
            );
        }
        for state in states {
            state
                .bound
                .close_with_linger(Some(Duration::ZERO))
                .await
                .unwrap();
        }
    });

    let report = monitor.stop();
    report.assert_no_leak("latency-profile");
}
