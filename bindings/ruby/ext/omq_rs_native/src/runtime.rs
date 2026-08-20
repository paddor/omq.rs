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

type Job = Box<dyn FnOnce() + Send + 'static>;

struct RuntimeState {
    pid: u32,
    handle: Handle,
    submit: flume::Sender<Job>,
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
    let (tx, rx) = flume::unbounded::<Job>();
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
                while let Ok(job) = rx.recv_async().await {
                    job();
                }
            });
        })
        .expect("omq-rs: spawn tokio thread");
    let handle = handle_rx.recv().expect("omq-rs: runtime handle");
    *guard = Some(RuntimeState {
        pid,
        handle: handle.clone(),
        submit: tx,
    });
    handle
}

fn submit_job(io_threads: usize) -> flume::Sender<Job> {
    let guard = RUNTIME.lock().unwrap();
    if let Some(ref rt) = *guard
        && rt.pid == std::process::id()
    {
        return rt.submit.clone();
    }
    drop(guard);
    ensure_runtime(io_threads);
    RUNTIME.lock().unwrap().as_ref().unwrap().submit.clone()
}

#[cfg(ruby_engine = "mri")]
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

#[cfg(not(ruby_engine = "mri"))]
fn recv_blocking<T>(rx: flume::Receiver<T>, missing: &'static str) -> T {
    rx.recv().expect(missing)
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
    pub send_pump: JoinHandle<()>,
    pub recv_pump: JoinHandle<()>,

    pub monitor_rx: flume::Receiver<MonitorEventData>,
    pub monitor_notify: Arc<PipeNotify>,
    pub peer_connected_notify: Arc<PipeNotify>,
    pub all_peers_gone_notify: Arc<PipeNotify>,
    pub subscriber_joined_notify: Arc<PipeNotify>,
    pub monitor_pump: JoinHandle<()>,
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

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive match keeps monitor event conversion auditable"
)]
fn convert_monitor_event(event: &omq_tokio::MonitorEvent) -> MonitorEventData {
    use omq_tokio::MonitorEvent::{
        Accepted, Closed, ConnectDelayed, Connected, Disconnected, HandshakeFailed,
        HandshakeSucceeded, JoinReceived, LeaveReceived, Listening, SubscribeReceived,
        UnsubscribeReceived,
    };
    match event {
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
        SubscribeReceived { prefix } => MonitorEventData {
            event_type: "subscribe_received",
            endpoint: None,
            detail: vec![("prefix", MonitorValue::Bytes(prefix.clone()))],
        },
        UnsubscribeReceived { prefix } => MonitorEventData {
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
        _ => MonitorEventData {
            event_type: "unknown",
            endpoint: None,
            detail: vec![],
        },
    }
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

#[expect(clippy::too_many_arguments)]
pub fn materialize(
    io_threads: usize,
    socket_type: omq_tokio::SocketType,
    options: omq_tokio::Options,
    send_cons: yring::AsyncConsumer<omq_tokio::Message>,
    mut recv_prod: yring::Producer<omq_tokio::Message>,
    recv_notify: Arc<PipeNotify>,
    send_notify: Arc<PipeNotify>,
    recv_space: Arc<tokio::sync::Notify>,
    monitor_tx: flume::Sender<MonitorEventData>,
    monitor_notify: Arc<PipeNotify>,
    peer_connected_notify: Arc<PipeNotify>,
    all_peers_gone_notify: Arc<PipeNotify>,
    subscriber_joined_notify: Arc<PipeNotify>,
) -> (
    Arc<InnerSocket>,
    JoinHandle<()>,
    JoinHandle<()>,
    JoinHandle<()>,
) {
    let (otx, orx) = flume::bounded(1);
    let tx = submit_job(io_threads);
    let job: Job = Box::new(move || {
        let sock = Arc::new(InnerSocket::new(socket_type, options));

        let s = sock.clone();
        let sn = send_notify.clone();
        let send_pump = tokio::spawn(async move {
            let mut send_cons = send_cons;
            let mut budget = omq_proto::flow::DrainBudget::new(256, 1024 * 1024);
            while let Some(msg) = futures::StreamExt::next(&mut send_cons).await {
                let byte_len = msg.byte_len();
                send_cons.release();
                sn.notify();
                let _ = s.send(msg).await;
                if !budget.account(byte_len) {
                    budget.reset();
                    tokio::task::yield_now().await;
                }
            }
            sn.notify();
        });

        let s = sock.clone();
        let rn = recv_notify.clone();
        let rs = recv_space.clone();
        let recv_pump = tokio::spawn(async move {
            loop {
                match s.recv().await {
                    Ok(msg) => {
                        push_to_ring(&mut recv_prod, msg, &rs).await;

                        while !recv_prod.is_full() {
                            match s.try_recv() {
                                Ok(msg) => push_to_ring(&mut recv_prod, msg, &rs).await,
                                Err(_) => break,
                            }
                        }

                        recv_prod.flush();
                        rn.force_wake();
                    }
                    Err(omq_tokio::Error::Closed) => break,
                    Err(_) => {}
                }
            }
        });

        let monitor_sock = sock.clone();
        let peer_ready_sock = sock.clone();
        let monitor_pump = tokio::spawn(async move {
            let mut stream = monitor_sock.monitor();
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
                                    peer_connected_notify.force_wake();
                                }
                            }
                            omq_tokio::MonitorEvent::Disconnected { .. } => {
                                peer_count = peer_count.saturating_sub(1);
                                if had_peers && peer_count == 0 {
                                    all_peers_gone_notify.force_wake();
                                }
                            }
                            omq_tokio::MonitorEvent::SubscribeReceived { .. }
                            | omq_tokio::MonitorEvent::JoinReceived { .. }
                                if !subscriber_joined_fired =>
                            {
                                subscriber_joined_fired = true;
                                subscriber_joined_notify.force_wake();
                            }
                            _ => {}
                        }

                        let data = convert_monitor_event(&event);
                        let _ = monitor_tx.try_send(data);
                        monitor_notify.notify();
                    }
                    Err(omq_tokio::MonitorRecvError::Lagged(_)) => {}
                    Err(_) => break,
                }
            }
        });

        let _ = otx.send((sock, send_pump, recv_pump, monitor_pump));
    });
    tx.send(job).expect("omq-rs: tokio runtime gone");

    recv_blocking(orx, "omq-rs: materialize failed")
}

pub fn destroy_socket(
    io_threads: usize,
    sock: Arc<InnerSocket>,
    send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    mut send_pump: JoinHandle<()>,
    recv_pump: JoinHandle<()>,
    monitor_pump: JoinHandle<()>,
    linger: Option<Duration>,
) {
    recv_pump.abort();
    monitor_pump.abort();
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
