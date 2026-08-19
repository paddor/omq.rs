#![cfg(feature = "soak")]
//! Soak: caller-driven exclusive sockets under peer churn.
//!
//! Exercises every exclusive socket type over TCP with both sides using the
//! exclusive API. Each scenario keeps one bound socket alive, repeatedly
//! connects a peer, exchanges one round trip, idles while both sides call
//! `maintain()` to drive heartbeats, then drops the peer and verifies the
//! bound side observes the disappearance.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::exclusive::{
    Event as ExclusiveEvent, Options as ExclusiveOptions, Socket as ExclusiveSocket,
};
use omq_tokio::{Endpoint, Error, Message, ReconnectPolicy, SocketType};
use tokio::sync::broadcast::error::TryRecvError;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(120);
const IO_TIMEOUT: Duration = Duration::from_secs(1);
const IDLE_MAINTAIN: Duration = Duration::from_millis(70);
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

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
    bound: ExclusiveSocket,
    endpoint: Endpoint,
    monitor: tokio::sync::broadcast::Receiver<ExclusiveEvent>,
    cycles: u64,
    disconnects: u64,
}

fn exclusive_options(kind: SocketType, side: &str) -> ExclusiveOptions {
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
    ExclusiveOptions {
        identity,
        connect_timeout: Duration::from_secs(1),
        handshake_timeout: Duration::from_secs(1),
        io_timeout: Some(IO_TIMEOUT),
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(5)),
        heartbeat_interval: Some(HEARTBEAT_INTERVAL),
        heartbeat_ttl: Some(HEARTBEAT_TIMEOUT),
        heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
    }
}

async fn make_state(spec: Scenario) -> ScenarioState {
    let (bound, endpoint) = ExclusiveSocket::bind(
        spec.bind_kind,
        soak_common::tcp_ep(0),
        exclusive_options(spec.bind_kind, "bind"),
    )
    .await
    .unwrap();
    let monitor = bound.monitor();
    ScenarioState {
        spec,
        bound,
        endpoint,
        monitor,
        cycles: 0,
        disconnects: 0,
    }
}

async fn connect_peer(state: &mut ScenarioState) -> ExclusiveSocket {
    let connect = ExclusiveSocket::connect(
        state.spec.connect_kind,
        state.endpoint.clone(),
        exclusive_options(state.spec.connect_kind, "connect"),
    );
    let accept = state.bound.maintain();
    let (connected, accepted) = tokio::join!(connect, accept);
    accepted.unwrap();
    connected.unwrap()
}

async fn cycle(state: &mut ScenarioState) {
    let mut peer = connect_peer(state).await;
    exchange_once(&state.spec, &mut state.bound, &mut peer, state.cycles).await;
    drive_idle_heartbeats(&mut state.bound, &mut peer).await;
    drop(peer);
    wait_for_peer_disappearance(&mut state.bound).await;
    state.disconnects += drain_disconnect_events(&mut state.monitor);
    state.cycles += 1;
}

async fn exchange_once(
    spec: &Scenario,
    bound: &mut ExclusiveSocket,
    peer: &mut ExclusiveSocket,
    seq: u64,
) {
    match spec.flow {
        Flow::Pair => exchange_pair(bound, peer, seq).await,
        Flow::DealerRouter => exchange_dealer_router(spec, bound, peer, seq).await,
        Flow::ReqRep => exchange_req_rep(spec, bound, peer, seq).await,
        Flow::ClientServer => exchange_client_server(spec, bound, peer, seq).await,
    }
}

async fn exchange_pair(bound: &mut ExclusiveSocket, peer: &mut ExclusiveSocket, seq: u64) {
    let body = format!("pair-{seq}");
    peer.send(&Message::single(body.clone())).await.unwrap();
    assert_eq!(bound.recv().await.unwrap(), Message::single(body));
    bound.send(&Message::single("pair-reply")).await.unwrap();
    assert_eq!(peer.recv().await.unwrap(), Message::single("pair-reply"));
}

async fn exchange_dealer_router(
    spec: &Scenario,
    bound: &mut ExclusiveSocket,
    peer: &mut ExclusiveSocket,
    seq: u64,
) {
    if spec.bind_kind == SocketType::Dealer {
        dealer_router_round_trip(bound, peer, seq).await;
    } else {
        dealer_router_round_trip(peer, bound, seq).await;
    }
}

async fn dealer_router_round_trip(
    dealer: &mut ExclusiveSocket,
    router: &mut ExclusiveSocket,
    seq: u64,
) {
    let body = format!("dealer-router-{seq}");
    dealer.send(&Message::single(body.clone())).await.unwrap();
    let request = router.recv().await.unwrap();
    let identity = request.part_bytes(0).unwrap().clone();
    assert_eq!(request.part_bytes(1).unwrap(), body.as_bytes());
    router
        .send(&Message::multipart([
            identity,
            Bytes::from_static(b"dealer-router-reply"),
        ]))
        .await
        .unwrap();
    assert_eq!(
        dealer.recv().await.unwrap(),
        Message::single("dealer-router-reply")
    );
}

async fn exchange_req_rep(
    spec: &Scenario,
    bound: &mut ExclusiveSocket,
    peer: &mut ExclusiveSocket,
    seq: u64,
) {
    if spec.bind_kind == SocketType::Req {
        req_rep_round_trip(bound, peer, seq).await;
    } else {
        req_rep_round_trip(peer, bound, seq).await;
    }
}

async fn req_rep_round_trip(req: &mut ExclusiveSocket, rep: &mut ExclusiveSocket, seq: u64) {
    let body = format!("req-rep-{seq}");
    req.send(&Message::single(body.clone())).await.unwrap();
    assert_eq!(rep.recv().await.unwrap(), Message::single(body));
    rep.send(&Message::single("req-rep-reply")).await.unwrap();
    assert_eq!(req.recv().await.unwrap(), Message::single("req-rep-reply"));
}

async fn exchange_client_server(
    spec: &Scenario,
    bound: &mut ExclusiveSocket,
    peer: &mut ExclusiveSocket,
    seq: u64,
) {
    if spec.bind_kind == SocketType::Client {
        client_server_round_trip(bound, peer, seq).await;
    } else {
        client_server_round_trip(peer, bound, seq).await;
    }
}

async fn client_server_round_trip(
    client: &mut ExclusiveSocket,
    server: &mut ExclusiveSocket,
    seq: u64,
) {
    let body = format!("client-server-{seq}");
    client.send(&Message::single(body.clone())).await.unwrap();
    let request = server.recv().await.unwrap();
    let routing_id = request.routing_id().expect("SERVER routing id");
    assert_eq!(request.part_bytes(0).unwrap(), body.as_bytes());
    server
        .send(&Message::single("client-server-reply").with_routing_id(routing_id))
        .await
        .unwrap();
    assert_eq!(
        client.recv().await.unwrap(),
        Message::single("client-server-reply")
    );
}

async fn drive_idle_heartbeats(bound: &mut ExclusiveSocket, peer: &mut ExclusiveSocket) {
    let start = Instant::now();
    while start.elapsed() < IDLE_MAINTAIN {
        let (bound_result, peer_result) = tokio::join!(bound.maintain(), peer.maintain());
        bound_result.unwrap();
        peer_result.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_peer_disappearance(bound: &mut ExclusiveSocket) {
    let start = Instant::now();
    loop {
        match bound.maintain().await {
            Ok(()) => {}
            Err(Error::Closed | Error::Timeout | Error::Io(_)) => return,
            Err(error) => panic!("unexpected exclusive peer disappearance error: {error}"),
        }
        assert!(
            start.elapsed() < DISCONNECT_TIMEOUT,
            "exclusive bound socket did not observe peer disappearance"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn drain_disconnect_events(monitor: &mut tokio::sync::broadcast::Receiver<ExclusiveEvent>) -> u64 {
    let mut disconnected = 0;
    loop {
        match monitor.try_recv() {
            Ok(ExclusiveEvent::Disconnected { .. }) => disconnected += 1,
            Ok(_) => {}
            Err(TryRecvError::Empty | TryRecvError::Closed) => return disconnected,
            Err(TryRecvError::Lagged(skipped)) => {
                panic!("exclusive monitor lagged during soak: skipped {skipped}");
            }
        }
    }
}

#[test]
fn soak_exclusive_all_supported_types() {
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
                eprintln!("[exclusive] {:.0}s:", start.elapsed().as_secs_f64());
                for state in &states {
                    eprintln!(
                        "  {}: {} cycles, {} disconnects",
                        state.spec.name, state.cycles, state.disconnects
                    );
                }
                last_log = Instant::now();
            }
        }

        for state in &mut states {
            assert!(state.cycles > 0, "{} had no cycles", state.spec.name);
            assert_eq!(
                state.disconnects, state.cycles,
                "{} missed disconnect events",
                state.spec.name
            );
            state.bound.close().await.unwrap();
        }
    });

    let report = monitor.stop();
    report.assert_no_leak("exclusive");
}
