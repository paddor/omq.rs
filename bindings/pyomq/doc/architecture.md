# pyomq Architecture

PyO3 binding for `omq-tokio`. Drop-in pyzmq API for Python (sync and
async). Single stable-ABI wheel (`abi3-py39`, Python 3.9+) via maturin.

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
  socket_async.rs   AsyncSocket: send (sync yring push), _try_recv,
                    platform-specific recv wakeup integration
  notify/
    mod.rs          ReadinessSignal: platform-agnostic public API
    unix.rs         Unix EventFdSignal: eventfd(2) + parking flag
    windows.rs      Windows WindowsSignal: Win32 event handles + async callback
  context.rs        Context / AsyncContext (stateless factories)
  options.rs        setsockopt/getsockopt: Overlay cache, option dispatch
  dispatch.rs       shared bind/connect/subscribe dispatch helpers
  constants.rs      libzmq-compatible socket type + option constants
  conversions.rs    zero-copy PyBytes via PyBytesOwner + Bytes::from_owner
  error.rs          ZMQError with errno (EAGAIN, ETERM, etc.)
  auth.rs           CURVE authenticator: key-list or Python callable
```

## Threading model

```
Python threads ──────────────▶ tokio thread (current_thread, "pyomq-tokio")
  Arc<omq_tokio::Socket>            ├─ send pump per socket (drain yring → socket)
  held in SocketInner               ├─ recv pump per socket (socket → yring, signal eventfd)
                                    └─ socket driver tasks (ConnectionDriver, actor)
```

`omq_tokio::Socket` is `Send + Sync` and stored as `Arc<Socket>` in
`SocketInner`. Python wrappers hold an `Arc<SocketInner>`.

### Why the yring relay is needed

Although `omq_tokio::Socket` can be shared across threads, its
`send()`/`recv()` methods are async and require the tokio runtime's
scheduler to be actively polling. The socket's internal driver tasks
(ConnectionDriver, actor loop) are spawned with `tokio::spawn` and
need the I/O driver to make progress. Python threads have no tokio
runtime context. Calling `Handle::block_on(socket.send(msg))` from a
non-runtime thread would deadlock: the future pushes into an internal
queue, but the driver task that drains that queue isn't being polled.

The yring SPSC relay bridges the two worlds: Python does a fast
lock-free ring push/pop (no syscall, no async context needed), and
pump tasks on the tokio thread relay between the rings and the actual
`socket.send()`/`recv().await` calls. This also gives natural batching
and avoids per-message cross-thread notifications.

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
3. Post job to the tokio thread: build the socket, spawn send and recv
   pump tasks.
4. Store `Materialized { id, socket, send_prod, recv_cons, recv_ready,
   send_ready, recv_space, send_pump, recv_pump }` in the `SocketInner`.

This lets Python code do `setsockopt` freely before the socket exists
on the tokio thread.

## Queue relay (yring pumps)

Each materialized socket has two pump tasks on the tokio thread:

**Send pump.** Drains the `AsyncProducer<Message>` (fed from Python)
into `socket.send()`. Yields every 256 messages to prevent a single
high-volume socket from starving others on the runtime.

**Recv pump.** Drains `socket.recv()` into a `Producer<Message>` (read
by Python). On ring-full, waits on `recv_space` (`StateSignal`, signaled
by the Python consumer after draining). After pushing, signals the
per-socket `ReadinessSignal` and the process-global recv signal used by
`wait_any`.

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

### Unix backend: EventFdSignal (eventfd)

`EventFdSignal` wraps a Linux `eventfd(EFD_NONBLOCK)` plus an
`AtomicBool parking` flag (`notify/unix.rs`).

- `signal()`: writes to the eventfd only if `parking` is true. On the
  hot path (consumer not parked), this is a single atomic load with no
  syscall.
- `park_begin()` / `park_end()`: arm/disarm the parking flag.
- `wait_timeout(dur)`: `poll(2)` on the eventfd with a timeout.
- `force_wake()`: unconditional write. Used on socket close to unblock
  any parked recv.
- `dup_fd()`: duplicate the fd for async recv integration.

The parking flag is set before re-checking the consumer. This closes
the race where a notification arrives between the consumer check and
the park.

**Async integration:** Python's `asyncio.py` calls `_recv_fd()` to get
a dup'd eventfd, then registers it with `loop.add_reader(callback)`.
The recv pump writes the eventfd whenever it pushes a message; the
kernel wakes the event loop, `callback` fires, and `_try_recv()` is
invoked.

### Windows backend: WindowsSignal (Win32 event handles + async callbacks)

`WindowsSignal` wraps Win32 event handles with a callback-based
wakeup model (`notify/windows.rs`).

**State machine:**
- `mode`: atomic u32 field stores wakeup configuration (ASYNC, SYNC, or NONE).
  - `WAKEUP_MODE_ASYNC` (1): invoke Python callback when signal() is called.
  - `WAKEUP_MODE_SYNC` (2): used by sync code for `wait_timeout()`.
- `pending`: atomic bool latches the wakeup signal (set by `signal()`,
  cleared by `wait_timeout()`).
- `draining`: atomic bool tracks if Python callback is currently executing.
  Additional wakeups during this window are coalesced to prevent callback
  re-entrancy.
- `callback_state`: atomic u8 state machine (IDLE, SCHEDULED, PENDING)
  to prevent duplicate callbacks while draining is in progress.

**Callback lifecycle:**
1. Python calls `set_wakeup_hooks(async_callback, ...)` to register
   the Python drain callback.
2. Python calls `set_wakeup_mode(WAKEUP_MODE_ASYNC)` when adding a waiter.
3. Rust calls `signal()` when data arrives.
4. If `mode & WAKEUP_MODE_ASYNC`, `signal()` invokes the Python callback
   directly (via PyO3).
5. Python callback drains the waiter queue.
6. Python calls `_mark_send_drain_complete()` / `_mark_recv_drain_complete()`
   to clear the draining flag and re-trigger if more work arrived.

**Wakeup modes:**
- `WAKEUP_MODE_ASYNC`: callback-based. Used by async code.
- `WAKEUP_MODE_SYNC`: event handle-based. Used by sync code's `wait_timeout()`.
- `WAKEUP_MODE_NONE`: inactive. No wakeups until mode is re-enabled.

The recv and send signals have independent modes: async code sets
send to ASYNC, while sync code can set recv to SYNC simultaneously.


## Sync send path

```
Socket.send(bytes, flags)
  -> build_or_buffer(bytes, flags)
      if SNDMORE: buffer frame, return
      else: assemble Message from buffered frames + this frame
  -> send_message(msg)
      prod.push_and_flush(msg)
      if Ok: done (fast path, GIL held)
      if Err (ring full): release GIL, loop:
          sleep 10 us, retry push_and_flush
          check SNDTIMEO deadline -> raise EAGAIN on timeout
```

SNDMORE frames accumulate in a `SendBuffer` (`Vec<Bytes>`). The final
`send` (no SNDMORE flag) flushes all buffered frames plus the final
frame into one multipart `Message`. (Platform-independent; yring is
populated by both Unix and Windows backends.)

## Sync recv path

```
Socket.recv(flags)
  -> if rxbuf not empty: pop head frame, return (no lock contention)
  -> recv_message()
      lock consumer, try pop (fast path)
      if Some(msg): return first frame, store rest in rxbuf
      else:
          # Platform: Unix uses eventfd + poll(2)
          # Platform: Windows uses ReadinessSignal.wait_timeout() with Win32 handles
          release GIL, slow path:
              park_begin()
              re-check consumer (closes race)
              loop:
                  wait_timeout(100 ms or remaining RCVTIMEO)
                  re-check consumer
                  if msg: park_end(), return
                  check RCVTIMEO deadline -> raise EAGAIN
```

Each `recv()` returns one frame. If the message is multipart, remaining
frames go into `rxbuf` and are returned by subsequent `recv()` calls.
`recv_multipart()` returns all frames at once. Both platforms use
`ReadinessSignal.wait_timeout()`, which internally handles eventfd
on Unix or Win32 event handles on Windows.

## Async send/recv

Async operations are completion-based with platform-specific wakeup
mechanisms. No Rust futures are bridged to Python asyncio.

### Async send with backpressure

**Happy path (ring not full):**

```
AsyncSocket.send(data, flags)
  -> prod.push_and_flush(msg)
      if Ok: return
```

The send yring is an `AsyncProducer`, allowing non-blocking push from
Python.

**Backpressure (ring full):**

```
AsyncSocket.send(data, flags)
  -> prod.push_and_flush(msg)
      if Err(ring full):
        raise EAGAIN to Python wrapper
```

Python wrapper (`asyncio.py`) catches EAGAIN and enters the waiter queue
pattern:

1. Calls `_add_waitable(try_fn=socket.send, waiters=_send_waiters, set_mode)`.
2. `set_mode` lambda calls `_set_wakeup_modes(send_mode=WAKEUP_MODE_ASYNC)`.
3. Appends a waiter closure to `_send_waiters` deque.
4. Returns future for caller to await.

**Wakeup path (Unix):**
- Waiter future is pending.
- recv pump on tokio thread finishes forwarding a message, freeing ring space.
- recv pump calls `send_ready.signal()`.
- `EventFdSignal.signal()` writes to the eventfd (because `parking=true`).
- Kernel wakes asyncio event loop via epoll/select.
- Loop fires the registered callback, which calls `_drain_send_waiters()`.
- `_drain_send_waiters()` pops waiters from queue and invokes each:
  - Waiter calls `try_fn()` (socket.send) -> succeeds, future resolved.
- After draining, calls `_mark_send_drain_complete()` to clear Rust callback state.

**Wakeup path (Windows):**
- Waiter future is pending.
- recv pump on tokio thread finishes forwarding a message, freeing ring space.
- recv pump calls `send_ready.signal()`.
- `WindowsSignal.signal()` sees `mode & WAKEUP_MODE_ASYNC` and directly
  invokes the Python callback (stored during `set_wakeup_hooks()`).
- Callback is `_schedule_send_drain`, which calls
  `loop.call_soon_threadsafe(self._drain_send_waiters)`.
- Asyncio loop invokes `_drain_send_waiters()` in main thread context:
  - Pops waiters and invokes each, same as Unix.
- After draining, calls `_mark_send_drain_complete()` which clears the
  Rust draining flag and re-triggers if follow-up work arrived.

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
  -> socket.recv_nowait() from yring
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
- send pump on tokio thread drains yring, calls `recv_ready.signal()`.
- `EventFdSignal.signal()` writes to eventfd.
- Kernel wakes asyncio event loop.
- Registered fd callback fires:
  - Calls `_drain_recv_waiters()` directly (no intermediate deferral).
  - `_drain_recv_waiters()` pops waiters, invokes each:
    - Waiter calls `_try_recv()` -> succeeds, future resolved.
- After draining, clears the parking flag and may re-enable recv in the
  ReadinessSignal.

**Wakeup path (Windows):**
- Waiter future is pending.
- send pump on tokio thread drains yring, calls `recv_ready.signal()`.
- `WindowsSignal.signal()` sees `mode & WAKEUP_MODE_ASYNC` and directly
  invokes the Python callback (`_schedule_recv_drain`).
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

Other buffer types (`bytearray`, `memoryview`) go through
`copy_from_slice` because their contents can be mutated from Python.

## MessageTracker

pyzmq's `track=True` tracks whether the zero-copy send buffer has
been flushed to the wire (so the caller knows when it's safe to
mutate the buffer). pyomq copies on send (no zero-copy send path),
so the buffer is always safe to reuse immediately.
`send(track=True)` returns a `MessageTracker` that reports done
immediately.

jupyter-client's `Session.send()` shadows async sockets to sync
(`zmq.Socket.shadow(stream.underlying)`) before calling
`send_multipart`. The sync path returns `None` (or `MessageTracker`
with `track=True`), never a future. No async/sync return type
mismatch.

## Proxy

`runtime::proxy()` takes exclusive control of the participating sockets:

1. Abort send/recv pumps on frontend, backend, and optional
   capture/control sockets.
2. Drain any buffered messages from the yring queues.
3. Spawn `proxy_loop()` on the tokio runtime with `futures::select!` on
   `fe.recv()`, `be.recv()`, and optional `ctrl.recv()`.
4. Forward messages between frontend and backend. Capture socket
   receives copies of all forwarded messages.
5. Control commands: PAUSE (spin-wait for RESUME), TERMINATE/KILL (exit
   loop).
6. Block the calling Python thread until the loop exits.

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

- `Poller` registers POLLIN only; POLLOUT is ignored.
- `send(copy=False)` and `send(track=True)` raise `NotImplementedError`.
- `wait_any` returns socket IDs, not file descriptors.
