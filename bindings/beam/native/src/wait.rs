//! Calls that can wait. The NIF returns at once with `{wait, Wait}`, and a
//! task on the socket's IO runtime sends `{omq_wait, Ref, Reply}` to the
//! caller once. The caller waits in `receive`, so it stays killable and holds
//! no scheduler thread. Collecting `Wait` aborts the task.

use std::future::Future;
use std::sync::Arc;
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use omq_tokio::{
    Context, Error as OmqError, Message, MonitorRecvError, MonitorTryRecvError, TrySendError,
};
use rustler::env::SavedTerm;
use rustler::{Binary, Encoder, Env, LocalPid, OwnedEnv, ResourceArc, Term};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::{
    NativeMonitor, NativeSocket, atoms, deadline_after, duration_from_millis, err_term,
    lagged_monitor_event_term, map_error, message_from_parts, message_to_term, monitor_event_term,
    ok, ok_unit,
};

/// Builds a reply in the message environment.
type ReplyFn = Box<dyn for<'a> FnOnce(Env<'a>) -> Term<'a> + Send>;

/// Box a reply builder. The bound gives closures their lifetime-generic
/// signature.
fn reply(build: impl for<'a> FnOnce(Env<'a>) -> Term<'a> + Send + 'static) -> ReplyFn {
    Box::new(build)
}

/// Handle for a waiting task. Collecting it aborts the task.
struct NativeWait {
    task: AbortHandle,
}

#[rustler::resource_impl]
impl rustler::Resource for NativeWait {
    fn destructor(self, _env: Env<'_>) {
        self.task.abort();
    }
}

/// The one reply to a waiting process. Dropped unsent, as when the runtime
/// shuts down, it reports `closed`, so the caller never waits forever.
struct Reply(Option<Target>);

struct Target {
    pid: LocalPid,
    env: OwnedEnv,
    reference: SavedTerm,
}

impl Reply {
    fn new(env: Env<'_>, reference: Term<'_>) -> Self {
        let owned = OwnedEnv::new();
        let reference = owned.save(reference);

        Self(Some(Target {
            pid: env.pid(),
            env: owned,
            reference,
        }))
    }

    fn send(mut self, build: ReplyFn) {
        if let Some(target) = self.0.take() {
            target.deliver(build);
        }
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(target) = self.0.take() {
            target.deliver(reply(|env| err_term(env, atoms::closed(), "socket closed")));
        }
    }
}

impl Target {
    fn deliver(mut self, build: ReplyFn) {
        // `send_and_clear` panics on ERTS threads. A task can drop on one,
        // for example when spawned on a terminated runtime.
        if rustler::thread::is_scheduler_thread() {
            std::thread::spawn(move || self.deliver(build));
            return;
        }

        let reference = &self.reference;
        let _ = self.env.send_and_clear(&self.pid, |env| {
            (atoms::omq_wait(), reference.load(env), build(env))
        });
    }
}

/// Wakes a task parked on `Notify`. A wake before the park leaves a permit,
/// so registering before the last check loses no wakeup.
struct NotifyWake(Notify);

impl Wake for NotifyWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_one();
    }
}

/// Run `work` on `ctx` and reply to the caller with its result.
fn spawn<'a>(
    env: Env<'a>,
    ctx: &Context,
    reference: Term<'a>,
    work: impl Future<Output = ReplyFn> + Send + 'static,
) -> Term<'a> {
    let reply = Reply::new(env, reference);
    let task = ctx.handle().spawn(async move {
        let build = work.await;
        reply.send(build);
    });

    let wait = ResourceArc::new(NativeWait {
        task: task.abort_handle(),
    });
    (atoms::wait(), wait).encode(env)
}

/// Run `work` until `deadline`. Returns false when the deadline passes first.
async fn until(deadline: Option<Instant>, work: impl Future<Output = ()>) -> bool {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
            .await
            .is_ok(),
        None => {
            work.await;
            true
        }
    }
}

/// `{ready, Ms}`: try again with the time left. Infinity is -1. At least
/// 1 ms, so a retry that finds nothing waits and reports a timeout.
fn ready_reply(deadline: Option<Instant>) -> ReplyFn {
    let remaining = deadline.map_or(-1, |deadline| {
        let left = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        i64::try_from(left).unwrap_or(i64::MAX).max(1)
    });

    reply(move |env| (atoms::ready(), remaining).encode(env))
}

fn timeout_reply(env: Env<'_>) -> Term<'_> {
    err_term(env, atoms::timeout(), "operation timed out")
}

fn result_reply<T: Encoder + Send + 'static>(result: Result<T, OmqError>) -> ReplyFn {
    reply(move |env| match result {
        Ok(value) => ok(env, value),
        Err(error) => map_error(env, error),
    })
}

/// Pop a buffered message, or receive one without blocking.
pub(crate) fn try_recv_buffered(socket: &NativeSocket) -> Result<Message, OmqError> {
    let materialized = socket.materialize()?;
    let mut buffer = socket.recv_buffer.lock().unwrap();
    match buffer.pop_front() {
        Some(message) => Ok(message),
        None => materialized.try_recv(),
    }
}

/// Whether a receive would not block. A ready message moves into the
/// socket's buffer for the next receive. With `failed_ready`, a socket that
/// fails, such as on close, counts as ready so the retry reports the error.
pub(crate) fn recv_ready_now(socket: &NativeSocket, failed_ready: bool) -> bool {
    let Ok(materialized) = socket.materialize() else {
        return failed_ready;
    };

    let mut buffer = socket.recv_buffer.lock().unwrap();
    if !buffer.is_empty() {
        return true;
    }

    match materialized.try_recv() {
        Ok(message) => {
            buffer.push_back(message);
            true
        }
        Err(OmqError::WouldBlock) => false,
        Err(_) => failed_ready,
    }
}

/// Resolve once any socket is ready per [`recv_ready_now`].
async fn recv_ready(sockets: Vec<ResourceArc<NativeSocket>>, failed_ready: bool) {
    let wake = Arc::new(NotifyWake(Notify::new()));
    let waker = Waker::from(Arc::clone(&wake));
    let mut registrations = None;

    loop {
        if sockets
            .iter()
            .any(|socket| recv_ready_now(socket, failed_ready))
        {
            return;
        }

        if registrations.is_none() {
            // Register, then recheck before the first park.
            let registered: Vec<_> = sockets
                .iter()
                .filter_map(|socket| socket.materialize().ok())
                .map(|socket| socket.register_recv_waker(waker.clone()))
                .collect();
            registrations = Some(registered);
            continue;
        }

        wake.0.notified().await;
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn recv<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    timeout_ms: i64,
    reference: Term<'a>,
) -> Term<'a> {
    let timeout = if timeout_ms == -2 {
        *socket.rcvtimeo.lock().unwrap()
    } else {
        duration_from_millis(timeout_ms)
    };

    match try_recv_buffered(&socket) {
        Ok(message) => message_to_term(env, message),
        Err(OmqError::WouldBlock) if timeout != Some(Duration::ZERO) => {
            let deadline = timeout.and_then(deadline_after);
            let ctx = socket.ctx.clone();
            spawn(env, &ctx, reference, async move {
                if until(deadline, recv_ready(vec![socket], true)).await {
                    ready_reply(deadline)
                } else {
                    reply(timeout_reply)
                }
            })
        }
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn send<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    parts: Vec<Binary<'a>>,
    routing_id: u32,
    reference: Term<'a>,
) -> Term<'a> {
    let message = message_from_parts(parts, routing_id);
    let timeout = *socket.sndtimeo.lock().unwrap();
    let materialized = match socket.materialize() {
        Ok(materialized) => materialized,
        Err(error) => return map_error(env, error),
    };

    match materialized.try_send(message) {
        Ok(()) => ok_unit(env),
        Err(TrySendError::Full(message)) if timeout != Some(Duration::ZERO) => {
            let deadline = timeout.and_then(deadline_after);
            spawn(env, &socket.ctx, reference, async move {
                let send = materialized.as_ref().send(message);
                let sent = match deadline {
                    Some(deadline) => {
                        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), send)
                            .await
                            .unwrap_or(Err(OmqError::Timeout))
                    }
                    None => send.await,
                };

                reply(move |env| match sent {
                    Ok(()) => ok_unit(env),
                    Err(error) => map_error(env, error),
                })
            })
        }
        Err(TrySendError::Full(_)) => map_error(env, OmqError::WouldBlock),
        Err(TrySendError::Closed) => map_error(env, OmqError::Closed),
        Err(TrySendError::Error(error)) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn wait_any<'a>(
    env: Env<'a>,
    sockets: Vec<ResourceArc<NativeSocket>>,
    timeout_ms: i64,
    reference: Term<'a>,
) -> Term<'a> {
    let ready: Vec<usize> = sockets
        .iter()
        .enumerate()
        .filter_map(|(index, socket)| recv_ready_now(socket, false).then_some(index))
        .collect();
    let Some(first) = sockets.first() else {
        return ok(env, ready);
    };

    if !ready.is_empty() || timeout_ms == 0 {
        return ok(env, ready);
    }

    let deadline = duration_from_millis(timeout_ms).and_then(deadline_after);
    let ctx = first.ctx.clone();
    spawn(env, &ctx, reference, async move {
        if until(deadline, recv_ready(sockets, false)).await {
            ready_reply(deadline)
        } else {
            reply(|env| ok(env, Vec::<usize>::new()))
        }
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn monitor_recv<'a>(
    env: Env<'a>,
    monitor: ResourceArc<NativeMonitor>,
    timeout_ms: i64,
    reference: Term<'a>,
) -> Term<'a> {
    // Another waiter holds the stream while it waits; queue behind it.
    if let Ok(mut stream) = monitor.stream.try_lock() {
        match stream.try_recv() {
            Err(MonitorTryRecvError::Empty) if timeout_ms != 0 => {}
            Err(MonitorTryRecvError::Empty) => return timeout_reply(env),
            Ok(event) => return ok(env, monitor_event_term(env, event)),
            Err(MonitorTryRecvError::Lagged(count)) => {
                return ok(env, lagged_monitor_event_term(env, count));
            }
            Err(_) => return err_term(env, atoms::closed(), "monitor closed"),
        }
    } else if timeout_ms == 0 {
        return timeout_reply(env);
    }

    let deadline = duration_from_millis(timeout_ms).and_then(deadline_after);
    let stream = Arc::clone(&monitor.stream);
    spawn(env, &monitor.ctx, reference, async move {
        let next = async { stream.lock().await.recv().await };
        let received = match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), next)
                    .await
                    .ok()
            }
            None => Some(next.await),
        };

        reply(move |env| match received {
            Some(Ok(event)) => ok(env, monitor_event_term(env, event)),
            Some(Err(MonitorRecvError::Lagged(count))) => {
                ok(env, lagged_monitor_event_term(env, count))
            }
            Some(Err(_)) => err_term(env, atoms::closed(), "monitor closed"),
            None => timeout_reply(env),
        })
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn wait_connected<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    min_peers: usize,
    timeout_ms: i64,
    reference: Term<'a>,
) -> Term<'a> {
    let timeout = duration_from_millis(timeout_ms).unwrap_or(Duration::MAX);
    match socket.materialize() {
        Ok(materialized) => spawn(env, &socket.ctx, reference, async move {
            let connected = materialized
                .as_ref()
                .wait_connected(min_peers, timeout)
                .await;
            result_reply(connected)
        }),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn wait_subscribed<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    min_subscriptions: u64,
    timeout_ms: i64,
    reference: Term<'a>,
) -> Term<'a> {
    let timeout = duration_from_millis(timeout_ms).unwrap_or(Duration::MAX);
    match socket.materialize() {
        Ok(materialized) => spawn(env, &socket.ctx, reference, async move {
            let subscribed = materialized
                .as_ref()
                .wait_subscribed(min_subscriptions, timeout)
                .await;
            result_reply(subscribed)
        }),
        Err(error) => map_error(env, error),
    }
}

/// Called after the reply arrives. Keeps `Wait` alive until then.
#[rustler::nif]
fn wait_done(env: Env<'_>, _wait: ResourceArc<NativeWait>) -> Term<'_> {
    ok_unit(env)
}
