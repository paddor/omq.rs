use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::Socket as InnerSocket;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::notify::PipeNotify;

struct RuntimeState {
    pid: u32,
    handle: Handle,
    _keepalive: flume::Sender<()>,
}

static RUNTIME: Mutex<Option<RuntimeState>> = Mutex::new(None);
static TERMINATED: AtomicBool = AtomicBool::new(false);

pub fn ensure_runtime(io_threads: usize) -> Handle {
    assert!(
        !TERMINATED.load(Ordering::Acquire),
        "omq-rs: runtime terminated"
    );
    let mut guard = RUNTIME.lock().unwrap();
    let pid = std::process::id();
    if let Some(ref rt) = *guard
        && rt.pid == pid
    {
        return rt.handle.clone();
    }
    let (tx, rx) = flume::bounded::<()>(1);
    let (handle_tx, handle_rx) = flume::bounded::<Handle>(1);
    let n = io_threads.max(1);
    thread::Builder::new()
        .name("omq-rust-tokio".into())
        .spawn(move || {
            let rt = if n <= 1 {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("omq-rs: tokio runtime build")
            } else {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(n)
                    .enable_all()
                    .build()
                    .expect("omq-rs: tokio runtime build")
            };
            let _ = handle_tx.send(rt.handle().clone());
            rt.block_on(async move {
                let _ = rx.recv_async().await;
            });
        })
        .expect("omq-rs: spawn tokio thread");
    let handle = handle_rx.recv().expect("omq-rs: runtime handle");
    *guard = Some(RuntimeState {
        pid,
        handle: handle.clone(),
        _keepalive: tx,
    });
    handle
}

fn recv_blocking<T>(rx: flume::Receiver<T>, missing: &'static str) -> T {
    struct RecvBox<U> {
        rx: flume::Receiver<U>,
        result: Option<U>,
    }

    extern "C" fn blocking_recv<U>(data: *mut libc::c_void) -> *mut libc::c_void {
        let rd = unsafe { &mut *data.cast::<RecvBox<U>>() };
        rd.result = rd.rx.recv().ok();
        std::ptr::null_mut()
    }

    let mut rd = RecvBox { rx, result: None };
    unsafe {
        rb_sys::rb_thread_call_without_gvl(
            Some(blocking_recv::<T>),
            (&raw mut rd).cast::<libc::c_void>(),
            None,
            std::ptr::null_mut(),
        );
    }
    rd.result.expect(missing)
}

pub fn spawn_blocking<F, T>(io_threads: usize, fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handle = ensure_runtime(io_threads);
    let (otx, orx) = flume::bounded::<T>(1);
    handle.spawn(async move {
        let out = fut.await;
        let _ = otx.send(out);
    });

    recv_blocking(orx, "omq-rs: runtime dropped result")
}

pub struct Materialized {
    pub socket: Arc<InnerSocket>,

    pub send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    pub recv_cons: Mutex<yring::Consumer<omq_tokio::Message>>,
    pub recv_notify: Arc<PipeNotify>,
    pub send_notify: Arc<PipeNotify>,
    pub recv_space: Arc<tokio::sync::Notify>,
    pub send_waiting: tokio::sync::watch::Sender<bool>,

    pub monitor_rx: flume::Receiver<MonitorEventData>,
    pub monitor_notify: Arc<PipeNotify>,
    pub peer_connected_notify: Arc<PipeNotify>,
    pub all_peers_gone_notify: Arc<PipeNotify>,
    pub subscriber_joined_notify: Arc<PipeNotify>,
    pub tasks: SocketTasks,
}

#[derive(Clone)]
pub struct MonitorEventData {
    pub event_type: &'static str,
    pub endpoint: Option<String>,
    pub detail: Vec<(&'static str, MonitorValue)>,
}

#[derive(Clone)]
pub enum MonitorValue {
    Bytes(Bytes),
    Integer(u64),
    Text(String),
}

/// Map a monitor event to the Ruby-facing name, endpoint, and detail pairs.
/// Event kinds this binding does not know map to "unknown".
fn convert_monitor_event(event: &omq_tokio::MonitorEvent) -> MonitorEventData {
    connection_event(event)
        .or_else(|| attempt_event(event))
        .or_else(|| subscription_event(event))
        .unwrap_or_else(|| MonitorEventData {
            event_type: "unknown",
            endpoint: None,
            detail: vec![],
        })
}

/// Listener and connection lifecycle events.
fn connection_event(event: &omq_tokio::MonitorEvent) -> Option<MonitorEventData> {
    use omq_tokio::MonitorEvent::{Accepted, Connected, Disconnected, Listening};

    let data = match event {
        Listening { endpoint } => MonitorEventData {
            event_type: "listening",
            endpoint: Some(endpoint.to_string()),
            detail: vec![],
        },
        Accepted {
            endpoint,
            connection_id,
            ..
        } => MonitorEventData {
            event_type: "accepted",
            endpoint: Some(endpoint.to_string()),
            detail: vec![("connection_id", MonitorValue::Integer(*connection_id))],
        },
        Connected {
            endpoint,
            connection_id,
            ..
        } => MonitorEventData {
            event_type: "connected",
            endpoint: Some(endpoint.to_string()),
            detail: vec![("connection_id", MonitorValue::Integer(*connection_id))],
        },
        Disconnected {
            endpoint,
            peer,
            reason,
        } => MonitorEventData {
            event_type: "disconnected",
            endpoint: Some(endpoint.to_string()),
            detail: vec![
                ("reason", MonitorValue::Text(format!("{reason:?}"))),
                ("connection_id", MonitorValue::Integer(peer.connection_id)),
            ],
        },
        _ => return None,
    };

    Some(data)
}

/// Connect retries and handshake outcomes.
fn attempt_event(event: &omq_tokio::MonitorEvent) -> Option<MonitorEventData> {
    use omq_tokio::MonitorEvent::{ConnectDelayed, HandshakeFailed, HandshakeSucceeded};

    let data = match event {
        HandshakeSucceeded { endpoint, peer } => {
            let mut detail = vec![("connection_id", MonitorValue::Integer(peer.connection_id))];
            if let Some(ref ident) = peer.peer_identity
                && !ident.is_empty()
            {
                detail.push(("peer_identity", MonitorValue::Bytes(ident.clone())));
            }
            MonitorEventData {
                event_type: "handshake_succeeded",
                endpoint: Some(endpoint.to_string()),
                detail,
            }
        }
        HandshakeFailed {
            endpoint, reason, ..
        } => MonitorEventData {
            event_type: "handshake_failed",
            endpoint: Some(endpoint.to_string()),
            detail: vec![("reason", MonitorValue::Text(reason.clone()))],
        },
        ConnectDelayed {
            endpoint,
            retry_in,
            attempt,
        } => MonitorEventData {
            event_type: "connect_delayed",
            endpoint: Some(endpoint.to_string()),
            detail: vec![
                (
                    "interval",
                    MonitorValue::Text(format!("{:.3}", retry_in.as_secs_f64())),
                ),
                ("attempt", MonitorValue::Integer(u64::from(*attempt))),
            ],
        },
        _ => return None,
    };

    Some(data)
}

/// Subscription and group traffic, plus socket close.
fn subscription_event(event: &omq_tokio::MonitorEvent) -> Option<MonitorEventData> {
    use omq_tokio::MonitorEvent::{
        Closed, JoinReceived, LeaveReceived, SubscribeReceived, UnsubscribeReceived,
    };

    let data = match event {
        SubscribeReceived { prefix, .. } => MonitorEventData {
            event_type: "subscribe_received",
            endpoint: None,
            detail: vec![("prefix", MonitorValue::Bytes(prefix.clone()))],
        },
        UnsubscribeReceived { prefix, .. } => MonitorEventData {
            event_type: "unsubscribe_received",
            endpoint: None,
            detail: vec![("prefix", MonitorValue::Bytes(prefix.clone()))],
        },
        JoinReceived { group } => MonitorEventData {
            event_type: "join_received",
            endpoint: None,
            detail: vec![("group", MonitorValue::Bytes(group.clone()))],
        },
        LeaveReceived { group } => MonitorEventData {
            event_type: "leave_received",
            endpoint: None,
            detail: vec![("group", MonitorValue::Bytes(group.clone()))],
        },
        Closed => MonitorEventData {
            event_type: "closed",
            endpoint: None,
            detail: vec![],
        },
        _ => return None,
    };

    Some(data)
}

async fn push_to_ring(
    recv_prod: &mut yring::Producer<omq_tokio::Message>,
    msg: omq_tokio::Message,
    recv_space: &tokio::sync::Notify,
) {
    let mut m = msg;
    loop {
        match recv_prod.push(m) {
            Ok(()) => break,
            Err(returned) => {
                recv_prod.flush();
                m = returned;
                let notified = recv_space.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match recv_prod.push(m) {
                    Ok(()) => break,
                    Err(r2) => {
                        m = r2;
                        notified.await;
                    }
                }
            }
        }
    }
}

/// Pump ends and wake signals between the Ruby-facing rings and the socket.
pub struct PumpChannels {
    pub send_cons: yring::AsyncConsumer<omq_tokio::Message>,
    pub send_notify: Arc<PipeNotify>,
    pub send_waiting: tokio::sync::watch::Sender<bool>,
    pub recv_prod: yring::Producer<omq_tokio::Message>,
    pub recv_notify: Arc<PipeNotify>,
    pub recv_space: Arc<tokio::sync::Notify>,
}

/// Where the monitor pump reports events and one-shot peer milestones.
pub struct MonitorSinks {
    pub events: flume::Sender<MonitorEventData>,
    pub notify: Arc<PipeNotify>,
    pub peer_connected: Arc<PipeNotify>,
    pub all_peers_gone: Arc<PipeNotify>,
    pub subscriber_joined: Arc<PipeNotify>,
}

/// Background tasks owned by one materialized socket.
pub struct SocketTasks {
    pub send: JoinHandle<()>,
    pub recv: JoinHandle<()>,
    pub monitor: JoinHandle<()>,
}

pub fn materialize(
    io_threads: usize,
    socket_type: omq_tokio::SocketType,
    options: omq_tokio::Options,
    pumps: PumpChannels,
    monitor: MonitorSinks,
) -> (Arc<InnerSocket>, SocketTasks) {
    // Construction only spawns tasks. Keep the GVL while the caller holds
    // its materialization lock; waiting without it would let close take
    // the GVL and block on that same lock.
    let handle = ensure_runtime(io_threads);
    let _guard = handle.enter();
    let sock = Arc::new(InnerSocket::new(socket_type, options));

    let send_pump = tokio::spawn(run_send_pump(
        sock.clone(),
        pumps.send_cons,
        pumps.send_notify,
        pumps.send_waiting,
    ));
    let recv_pump = tokio::spawn(run_recv_pump(
        sock.clone(),
        pumps.recv_prod,
        pumps.recv_notify,
        pumps.recv_space,
    ));

    // Subscribe here, not inside the task: the broadcast receiver keeps
    // events emitted before the task first runs.
    let stream = sock.monitor();
    let monitor_pump = tokio::spawn(run_monitor_pump(sock.clone(), stream, monitor));

    let tasks = SocketTasks {
        send: send_pump,
        recv: recv_pump,
        monitor: monitor_pump,
    };
    (sock, tasks)
}

/// Feed queued Ruby sends to the socket with a drain budget. REP instead
/// only watches send capacity.
async fn run_send_pump(
    s: Arc<InnerSocket>,
    mut send_cons: yring::AsyncConsumer<omq_tokio::Message>,
    sn: Arc<PipeNotify>,
    send_waiting: tokio::sync::watch::Sender<bool>,
) {
    if s.socket_type() == omq_tokio::SocketType::Rep {
        // REP submits at application send. Watch only failed sends;
        // a successful retry cancels an obsolete capacity wait.
        watch_rep_send_capacity(&s, &send_waiting, &sn).await;
        return;
    }

    let mut budget = omq_proto::flow::DrainBudget::new(256, 1024 * 1024);
    while let Some(msg) = futures::StreamExt::next(&mut send_cons).await {
        let byte_len = msg.byte_len();
        // Release the ring slot before the send so Ruby can refill while
        // this message is in flight.
        send_cons.release();
        sn.notify();
        let _ = s.send(msg).await;
        if !budget.account(byte_len) {
            budget.reset();
            tokio::task::yield_now().await;
        }
    }
    sn.notify();
}

/// Move received messages into the Ruby ring in budgeted batches, waking
/// the reader once per batch.
async fn run_recv_pump(
    s: Arc<InnerSocket>,
    mut recv_prod: yring::Producer<omq_tokio::Message>,
    rn: Arc<PipeNotify>,
    rs: Arc<tokio::sync::Notify>,
) {
    loop {
        match s.recv_for_external_recv().await {
            Ok(msg) => {
                let mut budget = omq_proto::flow::DrainBudget::WORKER;
                let mut more = budget.account(msg.byte_len());
                push_to_ring(&mut recv_prod, msg, &rs).await;

                while more && !recv_prod.is_full() {
                    match s.try_recv_for_external_recv() {
                        Ok(msg) => {
                            more = budget.account(msg.byte_len());
                            push_to_ring(&mut recv_prod, msg, &rs).await;
                        }
                        Err(_) => break,
                    }
                }

                recv_prod.flush();
                rn.force_wake();
                if !more {
                    tokio::task::yield_now().await;
                }
            }
            Err(omq_tokio::Error::Closed) => break,
            Err(_) => {}
        }
    }
}

/// Forward monitor events to Ruby and fire the one-shot peer milestones.
async fn run_monitor_pump(
    peer_ready_sock: Arc<InnerSocket>,
    mut stream: omq_tokio::MonitorStream,
    sinks: MonitorSinks,
) {
    let mut peer_count: u32 = 0;
    let mut had_peers = false;
    let mut peer_connected_fired = false;
    let mut subscriber_joined_fired = false;

    loop {
        match stream.recv().await {
            Ok(event) => {
                match &event {
                    omq_tokio::MonitorEvent::HandshakeSucceeded { .. } => {
                        peer_count += 1;
                        had_peers = true;
                        if !peer_connected_fired {
                            peer_connected_fired = true;
                            let _ = peer_ready_sock.connections().await;
                            sinks.peer_connected.force_wake();
                        }
                    }
                    omq_tokio::MonitorEvent::Disconnected { .. } => {
                        peer_count = peer_count.saturating_sub(1);
                        if had_peers && peer_count == 0 {
                            sinks.all_peers_gone.force_wake();
                        }
                    }
                    omq_tokio::MonitorEvent::SubscribeReceived { .. }
                    | omq_tokio::MonitorEvent::JoinReceived { .. }
                        if !subscriber_joined_fired =>
                    {
                        subscriber_joined_fired = true;
                        sinks.subscriber_joined.force_wake();
                    }
                    _ => {}
                }

                let data = convert_monitor_event(&event);
                let _ = sinks.events.try_send(data);
                sinks.notify.notify();
            }
            Err(omq_tokio::MonitorRecvError::Lagged(_)) => {}
            Err(_) => break,
        }
    }
}

async fn watch_rep_send_capacity(
    socket: &InnerSocket,
    requested: &tokio::sync::watch::Sender<bool>,
    notify: &PipeNotify,
) {
    let mut waiting = requested.subscribe();
    while waiting.wait_for(|requested| *requested).await.is_ok() {
        tokio::select! {
            () = socket.wait_send_ready() => {
                requested.send_replace(false);
                notify.force_wake();
            }
            changed = waiting.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }
}

/// Stop the pumps and close the socket, giving queued sends up to the
/// linger time (30 s when unset, at least 10 ms) to flush.
pub fn destroy_socket(
    io_threads: usize,
    sock: Arc<InnerSocket>,
    send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    tasks: SocketTasks,
    linger: Option<Duration>,
) {
    let SocketTasks {
        send: mut send_pump,
        recv: recv_pump,
        monitor: monitor_pump,
    } = tasks;

    recv_pump.abort();
    monitor_pump.abort();
    if sock.socket_type() == omq_tokio::SocketType::Rep {
        // This task only watches capacity; REP has no binding send backlog.
        send_pump.abort();
    }
    drop(recv_pump);
    drop(monitor_pump);
    let handle = ensure_runtime(io_threads);
    let close_timeout = linger
        .unwrap_or(Duration::from_secs(30))
        .max(Duration::from_millis(10));
    let fut = async move {
        drop(send_prod);
        if tokio::time::timeout(close_timeout, &mut send_pump)
            .await
            .is_err()
        {
            send_pump.abort();
            let _ = send_pump.await;
        }

        let s = Arc::try_unwrap(sock).unwrap_or_else(|arc| (*arc).clone());
        let _ = tokio::time::timeout(close_timeout, s.close()).await;
    };

    let (otx, orx) = flume::bounded::<()>(1);
    handle.spawn(async move {
        fut.await;
        let _ = otx.send(());
    });

    recv_blocking(orx, "omq-rs: close failed");
}
