# pyomq Architecture

PyO3 binding for `omq-tokio`. Drop-in pyzmq API for Python (sync and
async). Single stable-ABI wheel (`abi3-py312`, Python 3.12+) via maturin.

## Source layout

```
python/pyomq/
  __init__.py       sync API: Socket, Context, Poller, proxy, select
  asyncio.py        async API: wraps _native.AsyncSocket
  error.py          exception hierarchy (pyzmq-compatible)

src/
  lib.rs            module root: classes, constants, wait_any, proxy,
                    curve_keypair, has_feature
  runtime.rs        tokio runtime on dedicated thread; materialize,
                    wait_any, proxy
  socket.rs         sync Socket + SocketInner + ReadinessSignal (platform
                    abstraction) + Monitor (connection event stream)
  socket_async.rs   AsyncSocket: native admission, fallback send, _try_recv,
                    platform-specific recv wakeup integration
  send.rs           bounded fallback admission and in-flight FIFO barrier
  recv.rs           fair direct/fallback receive drain and sink recycling
  notify.rs         ReadinessSignal: platform-agnostic public API
  notify/
    unix.rs         Unix signal backend: eventfd on Linux, pipe elsewhere
    windows.rs      Windows WindowsSignal: Win32 event handles + async callback
  context.rs        Context / AsyncContext (stateless factories)
  options.rs        setsockopt/getsockopt: Overlay cache, option dispatch
  dispatch.rs       shared bind/connect/subscribe dispatch helpers
  constants.rs      libzmq-compatible socket type + option constants
  conversions.rs    zero-copy PyBytes via PyBytesOwner + Bytes::from_owner
  frame.rs          native Frame/Message object backed by bytes::Bytes
  error.rs          ZMQError with errno (EAGAIN, ETERM, etc.)
  auth.rs           CURVE authenticator: key-list or Python callable
```

## Threading model

```
Python threads ──────────────▶ tokio thread (current_thread, "pyomq-tokio")
  Arc<omq_tokio::Socket>            ├─ send pump per socket (drain yring → socket)
  held in SocketInner               ├─ recv pump per socket (socket → yring, signal readiness)
                                    └─ socket driver tasks (ConnectionDriver, actor)
```

`omq_tokio::Socket` is `Send + Sync` and stored as `Arc<Socket>` in
`SocketInner`. Python wrappers hold an `Arc<SocketInner>`.

Each Python `Context` owns or shadows one native `omq_tokio::Context`,
which is a handle to a shared `ContextCore`. The core owns the tokio
runtime and its `inproc://` registry. Normal `pyomq.Context()` owns that
core and calls native `term()` on close. Shadow contexts and
`Context.from_share_key()` wrappers only drop their handle; they do not
terminate the owner.

`Context.share_key()` returns a process-local opaque integer backed by
the native `u128` `ContextCore` key. `Context.from_share_key(key)` and
`pyomq.asyncio.Context.from_share_key(key)` import another handle to the
same core, so sync and asyncio wrappers can share `inproc://` names
without Python-side endpoint rewriting. The native registry stores weak
refs, so keys do not keep contexts alive.

### Direct admission and bounded fallback

Python calls native `Socket::try_send` without a Tokio runtime context. Native
admission queues the message; drivers own wire framing and I/O. Async sends
use this path for REQ/REP/PAIR/CHANNEL and send capacities at least 256. Small
throughput queues retain the send relay to overlap Python and IO work.

Full or pre-ready sends enter a bounded async yring drained by a send worker.
The queued count includes the worker's in-flight native send, so later inline
sends cannot overtake it. Native admission runs outside binding locks because
dropping a Python buffer exporter may reenter the binding. Accepted sends keep
progressing even when the caller ignores the returned future.

REQ/REP/PAIR/CHANNEL and receive capacities at least 256 use an external native
receive sink unless conflate is enabled. Inproc producers, and eligible wire
drivers, publish directly into the Python receive ring. Other peers use the
bounded receive relay. Python alternates available sources and adopts a new
direct consumer only after its old producer disconnects and the ring drains.
Small throughput queues retain the receive relay for pipeline overlap.

REQ and REP retain the complete transport item until application receive.
Polling and receive relays do not change protocol state. Application receive
validates REQ delimiters or admits the REP peer/envelope/body together. A
multipart receive must finish before the next request or reply send.

### Dispatch for non-I/O operations

For operations that don't go through the relay (bind, connect,
subscribe, unbind, etc.), `runtime::with_socket()` spawns a future on
the tokio runtime via `Handle::spawn()` and blocks the Python thread
on a oneshot channel (with GIL released). Since Socket is Send+Sync,
no thread-local registry or Job indirection is needed.

Socket IDs are allocated by `AtomicU64::fetch_add`. They are monotonic
and never recycled.

## Lazy materialization

Sockets are not created on the tokio thread at construction time.
`Context.socket()` only allocates a `SocketInner` with an `Overlay`
(option cache). The actual `omq_tokio::Socket` is created on the first
I/O call (`bind`, `connect`, `send`, `recv`, etc.) via
`SocketInner::materialize()`.

Materialization:

1. Extract options from the `Overlay` into `omq_tokio::Options`.
2. Create yring producer/consumer pairs (capacities from SNDHWM/RCVHWM).
3. Post job to the tokio thread: build the socket from the context's
   native `ContextCore`, spawn send and recv pump tasks.
4. Store native socket, send admission queue, direct/fallback receive drain,
   readiness signals, sink config, and task handles in `SocketInner`.
5. On fork recovery, forget inherited runtime/task state before creating the
   child socket; inherited queue locks may belong to vanished threads.

This lets Python code do `setsockopt` freely before the socket exists
on the tokio thread.

## Fallback queue workers

Each materialized async socket retains bounded send and receive workers.
Direct traffic does not pass through these workers. Both yield after 256
messages or 1 MiB to preserve runtime progress.

The send worker drains an `AsyncConsumer<Message>` into native `send()`.
`AsyncProducer::poll_ready()` registers full-queue wakes; stream receives
release capacity before returning each item. Completion clears the in-flight
FIFO barrier without acquiring the producer mutex. REQ/REP completion wakes
receivers waiting for native send admission.

The receive worker uses `recv_for_external_recv()` and preserves complete
transport items. It flushes with yring wake hints and signals socket/global
readiness only when required. A full relay waits on `recv_space`; the Python
consumer publishes popped credits and notifies registered producers.

## ReadinessSignal abstraction

The `ReadinessSignal` is a platform-agnostic interface for waking the
Python asyncio loop when socket readiness changes. It wraps a backend
chosen at compile time and abstracts away transport differences.

### Common interface

All backends implement:

- `signal()`: notify waiter(s) that readiness state changed.
- `force_wake()`: unconditional immediate wake (used on socket close).
- `wait_timeout(duration)`: blocking wait with timeout (used by sync code).
- `park_begin()` / `park_end()`: arm/disarm the parking flag (closes
  races in polling loops).

### Unix backend: EventFdSignal (eventfd or pipe)

`EventFdSignal` is the Unix readiness backend (`notify/unix.rs`). On
Linux/Android it uses `eventfd(EFD_NONBLOCK | EFD_CLOEXEC)`. On other Unix
targets, including macOS, it uses a nonblocking close-on-exec pipe. The shared
`ReadinessSignal` owns the `AtomicBool parking` flag.

- `signal()`: writes to the backend fd only if `parking` is true. On the
  hot path (consumer not parked), this is a single atomic load with no
  syscall.
- `park_begin()` / `park_end()`: arm/disarm the parking flag.
- `wait_timeout(dur)`: `poll(2)` on the backend fd with a timeout.
- `force_wake()`: unconditional write. Used on socket close to unblock
  any parked recv.
- `dup_fd()`: duplicate the fd for async recv integration.

The parking flag is set before re-checking the consumer. This closes
the race where a notification arrives between the consumer check and
the park.

**Async integration:** Python's `asyncio.py` calls `_recv_fd()` to get
a dup'd backend fd, then registers it with `loop.add_reader(callback)`.
Direct sinks and receive relays write the fd when wake hints require it; the kernel
wakes the event loop, `callback` fires, and `_try_recv()` is invoked.

### Windows backend: WindowsSignal (Win32 event handles + async callbacks)

`WindowsSignal` wraps Win32 event handles with a callback-based
wakeup model (`notify/windows.rs`).

**State machine:**
- `mode`: stores wakeup configuration (ASYNC, SYNC, or NONE).
  - `WAKEUP_MODE_ASYNC` (1): invoke Python callback when signal() is called.
  - `WAKEUP_MODE_SYNC` (2): set the shadow socket's Python wait event.
- `pending`: latches the wakeup signal (set by `signal()`,
  cleared by `wait_timeout()` or async drain completion).
- `callback_state`: `Idle`, `Scheduled`, or `ScheduledPending`. It prevents
  duplicate callbacks and preserves one follow-up wake while a drain is queued
  or running.

Backend fields are protected by one mutex. Native receive and send-space
callbacks first set an atomic pending flag and notify a Tokio dispatch task.
That task invokes Python hooks outside native producer and binding locks.
Hook replacement also drops old Python references outside the backend mutex.
 A callback dispatch is claimed while
holding that mutex. The claim remains valid if Python clears the wake mode
before the producer acquires the GIL. Callback invocation failures release the
claim so later readiness changes can retry.

**Callback lifecycle:**
1. Python calls `set_wakeup_hooks(async_callback, ...)` to register
   the Python drain callback.
2. Python calls `set_wakeup_mode(WAKEUP_MODE_ASYNC)` when adding a waiter.
3. Rust calls `signal()` when data arrives.
4. If `mode & WAKEUP_MODE_ASYNC`, `signal()` invokes the Python callback
   directly (via PyO3).
5. Python callback drains the waiter queue.
6. Python calls `_mark_send_drain_complete()` / `_mark_recv_drain_complete()`
   to finish the callback claim and re-trigger if more work arrived.

An asyncio socket binds to the first event loop that waits on it. Later waits
from another loop fail instead of routing readiness callbacks to the wrong
loop. A canceled future's done callback removes its waiter and clears the wake
mode when the queue becomes empty.

**Wakeup modes:**
- `WAKEUP_MODE_ASYNC`: callback-based. Used by async code.
- `WAKEUP_MODE_SYNC`: Python event-based. Used by sync shadow sockets.
- `WAKEUP_MODE_NONE`: inactive. No wakeups until mode is re-enabled.

The recv and send signals have independent modes: async code sets
send to ASYNC, while sync code can set recv to SYNC simultaneously.


## Sync send and recv

Sync sockets use the native blocking API. Sends first try native admission,
then release the GIL while waiting for capacity, respecting SNDTIMEO. Receives
use native blocking waits and RCVTIMEO. Multipart remainders live in `rxbuf`;
`recv()` returns one frame and `recv_multipart()` returns the remainder.

Sync polling may stage a raw item using `try_recv_for_external_recv()`.
`prepare_external_recv()` runs only when the application consumes that item,
so polling cannot advance REQ/REP state or select a REP reply route.
`Poller` reports POLLOUT when a nonblocking send would be accepted. Polls with
async sockets subscribe private wait signals to the process receive signal;
each waiting thread wakes on readiness without consuming another thread's wake.

## Async send/recv

Async operations are completion-based with platform-specific wakeup
mechanisms. No Rust futures are bridged to Python asyncio.

### Async send with backpressure

A send first attempts inline native admission when eligible and when no older
fallback is queued or in flight. Full native admission queues the original
message into the bounded fallback. If that ring is full, the binding registers
its send-space waker and returns EAGAIN with a `PendingSend` holding the already
converted message and tracker. Retries reuse those values; cancellation drops
them outside binding locks.

The Python wrapper queues a waiter and arms send readiness before retrying.
Releasing a fallback slot wakes registered senders. Unix signals the fd;
Windows defers Python hook dispatch to its notification task, then schedules
`_drain_send_waiters` through `loop.call_soon_threadsafe`. Close forces wakeups,
releases unaccepted pending values, and drains accepted sends within linger.

### Async recv with waiter queue

**Setup (registration once per socket):**

```python
_register_wakeup_hooks()
  -> sock._set_wakeup_hooks(
       recv_async=_schedule_recv_drain,
       send_async=_schedule_send_drain,
       recv_event=_recv_wakeup_event,
       send_event=_send_wakeup_event
     )
```

This is called once; Rust stores callbacks and event handles.

**Recv with message ready:**

```
AsyncSocket._try_recv()
  -> refresh and fairly drain direct/fallback yring sources
  -> prepare_external_recv() at application admission
      if Some(msg): return msg
      else: return None
```

**Recv with waiter (no message):**

```
AsyncSocket._add_recv_event(try_fn=_try_recv)
  -> _add_waitable(try_fn, waiters=_recv_waiters, set_mode)
```

Similar to send: appends waiter, sets mode, returns future.

**Wakeup path (Unix):**
- Waiter future is pending, registered with `loop.add_reader(fd, callback)`.
- Direct producer or receive relay publishes data and signals when required.
- `EventFdSignal.signal()` writes to the backend fd.
- Kernel wakes asyncio event loop.
- Registered fd callback fires:
  - Calls `_drain_recv_waiters()` directly (no intermediate deferral).
  - `_drain_recv_waiters()` pops waiters, invokes each:
    - Waiter calls `_try_recv()` -> succeeds, future resolved.
- After draining, clears the parking flag and may re-enable recv in the
  ReadinessSignal.

**Wakeup path (Windows):**
- Waiter future is pending.
- Direct producer or receive relay publishes data and signals when required.
- Notification task calls `WindowsSignal.signal()` outside producer locks.
  With `mode & WAKEUP_MODE_ASYNC`, it invokes `_schedule_recv_drain`.
- Callback queues `_drain_recv_waiters()` to the asyncio event loop.
- Event loop invokes `_drain_recv_waiters()` in main thread context:
  - Pops waiters and invokes each.
- After draining, calls `_mark_recv_drain_complete()` to clear Rust state.

### Waiter queue drain logic (platform-independent)

Both Unix and Windows converge on the same Python code for draining:

```python
def _drain_send_waiters(self):
    try:
        waiters = self._send_waiters
        while waiters and waiters[0]():
            waiters.popleft()
    finally:
        self._sock._mark_send_drain_complete()
        # Race window: re-check for notifications that arrived
        # between the loop end and mark_drain_complete().
        while waiters and waiters[0]():
            waiters.popleft()
        # If waiters remain, re-enable async mode.
        if waiters:
            self._set_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC)
```

Each waiter is a closure that attempts the operation (send/recv) and
returns `True` if done or `False` if blocked. The drain stops when a
waiter returns False, preserving queue order (fairness).

## Zero-copy conversions

`PyBytesOwner` holds a `Py<PyBytes>` (preventing GC) and captures the
raw `*const u8` + `len` under the GIL at construction. Because Python
bytes are immutable, the pointer is stable for the object's lifetime.
`Bytes::from_owner(PyBytesOwner)` borrows the buffer without copying.

`Frame`/`Message` is a native Python class backed by `Bytes`.
`recv(copy=False)` and `recv_multipart(copy=False)` return these frames.
Passing such frames to `send` or `send_multipart` clones the `Bytes`
handle, so broker reroute paths avoid converting frame payloads through
Python `bytes`. `bytes(frame)` and `frame.bytes` still allocate a Python
`bytes` object on demand. `frame.buffer` returns a memoryview directly
over the immutable Rust `Bytes` storage via the Python buffer protocol.

Other contiguous buffer types (`bytearray`, `memoryview`, multibyte arrays)
go through `copy_from_slice` by default. With `copy=False`, a `PyUntypedBuffer`
export pins their storage until the last native owner releases it. Strided
buffers are rejected, including with `copy=True`.

Buffer exports are currently released on the thread dropping the last native
owner, including I/O threads. Python-defined `__release_buffer__` callbacks
must not make blocking socket/context calls there. That preexisting limitation
also affects untracked zero-copy sends. Such exporters can use `copy=True` or
schedule their cleanup on an application thread.

## MessageTracker

Tracking follows buffer ownership, not queue admission or delivery. Tracked
zero-copy sends wrap `Bytes` in an owner whose final drop releases the Python
export before notifying a condition variable. The Python tracker owns only
completion tokens or existing Frame trackers, never the payload. Multipart
conversion builds one Python tracker over all parts without intermediate
per-buffer Python trackers. No token or tracking synchronization is allocated
for untracked sends. `wait()` releases the GIL; a single token handles its own
timeout, while aggregates share one monotonic deadline across all sources.

Copied buffer sends return `None`. Sending a `Frame` returns its own tracker;
tracking an untracked frame is an error. Tracked receive frames have completed
trackers, but an inproc frame or exported view can still keep the original
sender's tracker pending. Async queue backpressure allocates a `PendingSend`
that retains the converted message across retries, so generators are consumed
once. Cancellation and socket close release these retained messages.

jupyter-client's `Session.send()` shadows async sockets to sync
(`zmq.Socket.shadow(stream.underlying)`) before calling
`send_multipart`. The sync path returns `None` (or `MessageTracker`
with `track=True`), never a future. No async/sync return type
mismatch.

## Proxy

`runtime::proxy_handles()` uses native blocking sockets through the native
proxy implementation. Python releases the GIL while forwarding. Capture and
control behavior follow the native proxy; no Python message conversion is
required for forwarded items.

## Socket options

`Overlay` is a per-socket option cache that mirrors `omq_proto::Options`
plus wrapper-only fields (RCVTIMEO, SNDTIMEO, LINGER, HWMs).
`setsockopt` writes to the overlay; `materialize()` converts it to
`omq_tokio::Options`.

Post-materialization `setsockopt` for SUBSCRIBE/UNSUBSCRIBE dispatches
to the tokio thread (the socket must process it). Most other options
are read-only after materialization.

Some options are accepted as no-ops for pyzmq compatibility: IMMEDIATE,
IPV6, RATE, PROBE_ROUTER. Some raise ENOSYS: AFFINITY, BACKLOG.

## Authentication

CURVE uses the same bridge pattern. The Python side sets
an authenticator on the overlay via `setsockopt`:

- `None`: clear authenticator.
- Iterable of keys: build a `HashSet` of accepted keys. CURVE keys are
  Z85-encoded strings.
- Callable: wrap as `Py<PyAny>`. Called with a `PeerInfo` pyclass (has
  `.public_key` attribute). Must return truthy/falsy.

At materialization, `build_authenticator()` converts the enum into an
`omq_proto::Authenticator` closure. Callable authenticators acquire the
GIL when invoked from the tokio thread.

## Error mapping

`error.rs` maps `omq_proto::Error` variants to libzmq-compatible errno
codes:

| Rust error        | errno         | Python exception      |
|-------------------|---------------|-----------------------|
| `Closed`          | ETERM (156)   | `ContextTerminated`   |
| `Timeout`         | EAGAIN        | `Again`               |
| `HandshakeFailed` | EPROTO        | `ZMQError`            |
| `Unroutable`      | EHOSTUNREACH  | `ZMQError`            |
| `MessageTooLarge` | EMSGSIZE      | `ZMQError`            |
| `InvalidEndpoint` | EINVAL        | `ZMQError`            |
| `Io(e)`           | `e.raw_os_errno()` or EIO | `ZMQError`   |

The Python exception hierarchy matches pyzmq: `ZMQBaseError` is the
root; `ZMQError` and `ZMQBindError` are siblings under it (not
parent-child). `Again`, `ContextTerminated`, `NotImplementedError` are
subclasses of `ZMQError`.

## Monitor

`Socket.monitor()` returns a `Monitor` object backed by a relay task
that drains the tokio broadcast channel into a `flume::Receiver`. A
`lagged: Arc<AtomicU64>` counter tracks dropped events on overflow.

- `recv(timeout_ms)`: blocking receive, returns a dict
  (`{"event": "listening", "endpoint": "..."}`).
- `recv_nowait()`: non-blocking, returns dict or raises EAGAIN.

## Known limitations

- `wait_ready` and `wait_any` return socket IDs, not file descriptors.

## Direct-path measurements

Linux inproc, three paired two-second samples, event loop on CPU 0 and initial
runtime threads on CPU 1. Baseline is the committed receive/wake changes before
this binding refactor. Rates include draining the pipeline tail. These results
do not establish Windows performance.

| Workload | Before | After | CPU seconds before/after |
| --- | --- | --- | --- |
| Async PUSH/PULL, 64 B, HWM1000 | 0.536 M/s | 0.521 M/s | 3.301 / 2.000 |
| Async PUSH/PULL, 64 B, HWM8 | 0.180 M/s | 0.177 M/s | 3.133 / 2.793 |
| Async REQ/REP, 4 B, p50 | 80.750 us | 77.120 us | 2.466 / 1.997 |
| Async ready poll per receive | 11,492/s | 218,836/s | 2.513 / 2.015 |

Default throughput changed -2.8% while CPU use fell 39%. Small-HWM throughput
changed -1.9% while CPU use fell 11%; its switches rose from 57,821 to 92,130
per sample. Small throughput queues retain both relays to preserve pipeline
overlap. Ready polling avoids executor dispatch; pending polls still use it.
