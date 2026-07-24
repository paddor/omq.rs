# pyomq Performance

Throughput measured with 2-thread TCP loopback (PUSH/PULL, 8 B payload).

## Timeline

### omq-compio baseline: 1.54M msg/s

pyomq started on omq-compio (io_uring, single-threaded). The compio
runtime ran on a dedicated background thread. A yring SPSC relay bridged
Python threads to the compio thread. Send: push to ring, return. Recv:
pop from ring, park on eventfd if empty.

Migrated away from omq-compio because it (probably) only runs on Linux (io_uring).
macOS support currently broken and Windows untested.

Tokio is supposed to be much more mature.

### omq-tokio + yring + tokio::time::sleep: 59k msg/s (27x regression)

Same yring relay architecture, swapped compio for tokio. The send pump
yielded every 64 messages with `tokio::time::sleep(Duration::from_micros(10))`.
On a `current_thread` runtime, tokio's timer wheel rounds sub-ms sleeps
up to ~1ms. 64 msgs per 1ms yield = 64k msg/s. Matched the measured
throughput exactly.

### Fix: yield_now() instead of sleep: 1.26M msg/s

Replaced `tokio::time::sleep(10 us)` with `tokio::task::yield_now()`
in both the send pump (yield interval) and recv pump (backpressure
retry). Restored throughput to within 18% of the compio baseline.

### Attempted: multi_thread runtime, no yring, Handle::block_on

Hypothesis: the yring relay adds per-message overhead. Eliminating it
by calling `Handle::block_on(socket.send/recv)` directly from the
Python thread should be faster.

Results:

| Approach | Throughput |
|----------|-----------|
| block_on for send + recv | 254k msg/s |
| try_recv fast path + block_on fallback for recv | 584k msg/s |
| try_send (fire-and-forget) + block_on fallback for recv | 390k msg/s |
| Pre-filled try_recv only (no block_on) | 1.76M msg/s |

`Handle::block_on` has ~2 us per-call overhead: entering the runtime
context, cross-thread future dispatch, and wakeup notification. In a
balanced streaming workload, the receiver misses `try_recv` frequently
and falls back to `block_on`, capping throughput at ~500k.

`try_send` through the command channel was slower than `send().await`
because it missed the SPSC ring bypass that `send().await` uses for
PUSH/PULL. Adding the SPSC bypass to `try_send` didn't help for TCP
(bypass only applies to inproc with a single connected peer).

The pre-filled test (1.76M) proved that `try_recv` without `block_on`
is faster than the yring approach. The bottleneck is the `block_on`
fallback when the internal channel is empty.

Dead end. Reverted to yring + yield_now(). Kept multi_thread runtime.

### Attempted: hybrid try_send/try_recv + yring fallback

Hypothesis: use `try_send`/`try_recv` on the fast path (no yring
overhead), fall back to yring only under backpressure.

Results:

- try_send for send + try_recv for recv: 765k. `try_send` on TCP goes
  through the command channel (capacity = send_hwm), fills fast, spins.
- yring for send + try_recv for recv: 700k-1.3M, inconsistent. The
  recv pump and `try_recv` compete on the same internal channel. When
  the pump wins, the message goes through the yring. When `try_recv`
  wins, it's fast. The race causes unpredictable throughput.

Dead end. The pump and `try_recv` can't coexist on the same socket
because they're both consumers of the socket's internal recv channel.

### Send backpressure: readiness signal instead of spin

When the send yring is full, the Python thread parks on `send_ready`
instead of spinning with `thread::yield_now()`. Linux uses `eventfd`,
macOS/other Unix uses a nonblocking pipe, and Windows uses a Win32
event/callback backend. The send pump signals after each batch drain
(every 256 messages). No CPU waste under backpressure.

### Send pump yield interval: 64 -> 256

The send pump yields every N messages to let connection drivers run on
the single-threaded tokio runtime. 64 was too frequent. 256 is the
sweet spot: fewer yields = less overhead. 512 starves the drivers.

### Always multi_thread runtime

The initial implementation used `current_thread` when
`available_parallelism() <= 1`, `multi_thread` otherwise. Simplified
to always `multi_thread` with `worker_threads(n.max(1))`. The
Handle::block_on dead end above was about removing the yring, not
the runtime type.

### Current: multi_thread + yring + yield_now() + batched send_ready: ~1.5M msg/s

The yring relay avoids `block_on` entirely. Python does lock-free ring
push/pop. Pump tasks on the tokio thread batch send/recv operations.
The readiness notification for recv uses an atomic flag to skip the
syscall when the consumer isn't parked. Send backpressure parks on the
same readiness signal instead of spinning.

### Attempted: recv prefetch (drain yring batch under one lock)

Hypothesis: when `recv_message()` pops one message from the yring, also
drain up to N more into a `VecDeque` on `SocketInner`. Subsequent
`recv()` calls skip the yring lock entirely and pop from the buffer.

Results (8 B TCP, N=15):

| Config | 8 B | 128 B | 1024 B |
|--------|----:|------:|-------:|
| No prefetch | 1.48M | 1.40M | 1.20M |
| Prefetch 15 | 1.60M | 1.52M | 1.21M |
| Prefetch 63 | 1.54M | 1.53M | 1.21M |
| Prefetch 127 | 1.58M | 1.56M | 1.21M |
| Prefetch 255 | 1.60M | 1.51M | 1.20M |

~8% gain at small sizes, negligible at 1 KiB+. Plateaus past 15.

Reverted. The gains are too small to justify the complexity, and the
prefetch buffer breaks `poll()`/`wait_any()`: those check the yring and
readiness signal but don't know about the prefetch buffer. Messages
drained into the buffer become invisible to poll, causing hangs in
ZMQStream/tornado integrations (jupyter-client test suite).

A correct implementation would need `poll_ready` to also check the
prefetch buffer. We prototyped this and it works, but the interaction
surface (every readiness check must know about the buffer) is fragile
for ~8% on a path that's not the bottleneck.

### Proxy: batched drain via try_recv

The proxy loop (`proxy_loop` in `runtime.rs`) uses `futures::select!`
to race `recv()` on frontend and backend. After the first message
arrives, it drains up to 63 more via `try_recv()` before going back to
`select!`. This avoids re-entering the select macro per message under
load.

Measured improvement: ~10% on REQ/REP proxy (latency-bound). Negligible
on PUSH/PULL proxy (TCP-bound, not async-loop-bound).

### Proxy benchmark: native sender/receiver

The proxy benchmark previously used Python sender/receiver in one
subprocess. Python's per-message send/recv overhead (~1.5M msg/s)
bottlenecked the measurement. Switching to a native omq-compio
sender/receiver (`omq_bench_proxy_client`) reveals the true proxy
throughput: ~2.81M msg/s for pyomq vs ~1.53M for pyzmq (1.83x).
REQ/REP: ~8.4k vs ~4.5k (1.86x).

### Materialized slot: Mutex -> RwLock

`SocketInner::materialized` guards the lazily-initialized socket state
(ring producers/consumers, readiness signals, pump handles). Every `send()`
and `recv()` locks it to reach `send_prod` / `recv_cons`.

With `Mutex`, the send thread (Python sender) and recv thread (Python
receiver) contend on every message. At 1.5M msg/s, even an uncontended
`Mutex::lock()` costs ~20 ns. Under contention (two threads alternating
on the same lock), throughput dropped to ~560k msg/s (2.7x regression).

`RwLock` eliminates the contention: both threads take shared read locks
simultaneously, only materialization and close need an exclusive write
lock. Throughput restored to ~1.4M msg/s.

The `send_prod` and `recv_cons` inside `Materialized` are separate
`Mutex`es. They don't contend because only one thread touches each
(sender touches `send_prod`, receiver touches `recv_cons`).

### Async send/recv: remove tokio bridge entirely

`AsyncSocket::send()` and `recv()` were routing every message through
`tokio_future_into_py`, which spawns on the tokio runtime and bridges
back via `call_soon_threadsafe`. Each bridge hop costs ~100 µs (GIL
acquisition + pipe write to wake the asyncio loop). With two recv
round-trips per REQ/REP exchange, async latency was ~195 µs.

The fix: send pushes directly into the yring (synchronous, no tokio).
Recv uses `_try_recv` (poll yring) + `_recv_fd` (Unix readiness fd
registered with `loop.add_reader`). No Rust futures bridged to Python. Removed
`async_send_message`, `async_recv_message`, `tokio_future_into_py`
from the send/recv path (kept only for control-plane ops like bind).

Async REQ/REP latency: 195 µs → 82 µs p50.

### yring AsyncProducer::flush(): unconditional wake

`AsyncProducer::flush()` used a `was_empty` optimization: only wake the
consumer when the ring transitions from empty to non-empty. The consumer
could drain and re-register its waker between two producer flushes,
making `was_empty` false while the consumer was parked. Result: the send
pump's `AsyncConsumer` stream never got woken, 1024 messages stuck in
the send yring, inproc PUSH/PULL hung ~2-4% of the time.

Fix: flush unconditionally wakes the consumer. One extra
`AtomicWaker::wake()` per flush (a no-op when no waker is registered).

### Recv pump: StateSignal instead of yield_now spin-loop

The recv pump spun with `tokio::task::yield_now()` when the recv yring
was full. Under specific timing this prevented forward progress.
Replaced with `recv_space: StateSignal` signaled by the Python
consumer after each `prefetch_and_pop`. The recv pump awaits the signal
instead of spinning.

### Python _check_fork() removed from hot path

`Socket.send()` and `.recv()` called `_check_fork()` on every call,
which does `os.getpid()` + comparison. At 1.5M calls/s the Python
function call overhead is measurable. Fork detection is now handled
in Rust via the PID stored in `Materialized`.

## Wins

- `tokio::task::yield_now()` instead of `tokio::time::sleep(10 us)`.
  Timer wheel granularity was the root cause of the 27x regression.
- Send backpressure: readiness signal park instead of spin loop.
- Send pump yield interval 64 -> 256, notify once per batch.
- yring capacity matches HWM (was hardcoded 65536).
- `Socket::try_send` now returns the message on `Full` (via
  `TrySendError::Full(msg)`). Previously the message was consumed
  and lost on WouldBlock.
- `Socket::try_send` uses the SPSC ring bypass for single-peer socket
  types (same fast path as `send().await`).
