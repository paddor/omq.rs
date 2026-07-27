# libzmq Send, Linger, and HWM Semantics

Reference: libzmq 4.3.5.

## No-Peer Sends

Terms:

- **Ready peer:** ZMTP handshake completed; pipe accepts data messages.
- **Bound no-peer:** `bind()` socket with no ready peer.
- **Connected no-peer:** `connect()` socket before first connection or during
  reconnect.
- **Mute:** socket cannot route. Blocking send waits. Nonblocking send returns
  `EAGAIN`, except lossy socket types.

`ZMQ_IMMEDIATE` only affects connected no-peer sends:

- `0`: queue to incomplete or reconnecting connect pipes.
- `1`: require a completed connection; otherwise mute.

| Case | libzmq and `omq-libzmq` | `omq-tokio` API |
|------|-------------------------|-----------------|
| `PUSH`/`DEALER`/`REQ`/`CLIENT`/`SCATTER` bound, no ready peer | Mute. Nonblocking send returns `EAGAIN`. No accept-then-drop. | `send().await` waits. `try_send()` returns `Full`. |
| Same sockets connected, no ready peer, `ZMQ_IMMEDIATE=0` | Queue to incomplete or reconnecting pipe, bounded by HWM. | Queue to connect-side pre-ready pipe, bounded by `send_hwm`. |
| Same sockets connected, no ready peer, `ZMQ_IMMEDIATE=1` | Mute. Nonblocking send returns `EAGAIN`. | No `ZMQ_IMMEDIATE` option. `connect()` always creates a pre-ready pipe. |
| Bound socket lost all peers | Mute once no ready pipe remains. | Bind-only sockets wait; `try_send()` returns `Full`. |
| Connected socket lost peer | `ZMQ_IMMEDIATE=0` queues to the reconnecting pipe. `ZMQ_IMMEDIATE=1` mutes until reconnect completes. | Queue to connect-side pre-ready pipe while reconnecting. Messages already accepted by a failed transport have no delivery guarantee. |
| `PUB`/`XPUB`/`RADIO` no matching ready subscriber | Lossy. Send succeeds and drops, unless `XPUB_NODROP` exposes mute. | Lossy default. `xpub_nodrop` exposes backpressure. |
| `ROUTER` unknown identity | Drop by default. `ROUTER_MANDATORY` returns `EHOSTUNREACH`. | Drop by default. `router_mandatory` returns `Error::Unroutable`. |
| `PAIR`/`CHANNEL` no ready peer | Mute. Nonblocking send returns `EAGAIN`. No connect-side pre-ready queue. | `send().await` waits. `try_send()` returns `Full`. |

Bound no-peer `PUSH` is not lossy. It waits or returns `EAGAIN`; it does not
accept and drop the message.

## Linger

libzmq default `ZMQ_LINGER=-1` waits forever for queued outbound messages.
`zmq_close()` returns immediately. `zmq_ctx_term()` waits for linger work.

`omq-tokio` default linger is zero. `Socket::close().await` waits for the
configured linger and joins the driver. Dropping the last handle starts linger
in the background. With `Context::current()`, the caller runtime must keep
polling for that close to progress.

`omq-libzmq` maps the C API to libzmq defaults:

- default `ZMQ_LINGER=-1` maps to `linger_forever()`;
- explicit `ZMQ_LINGER=0` maps to zero linger;
- `zmq_close()` returns quickly, and context termination waits for active
  linger work.

Finite or forever linger keeps endpoints alive while draining. Late peers can
receive queued connect-side sends before the deadline. Bound no-peer sends are
mute, so no bound no-peer queue exists. Zero linger cancels endpoints and drops
queued sends immediately.

## HWM

libzmq `ZMQ_SNDHWM` and `ZMQ_RCVHWM` are message-count limits per pipe, not
byte limits. A complete multipart message consumes one HWM credit. HWM is
backpressure, not a memory cap; memory grows with peer count and message size.

`omq-tokio` exposes `Options::send_hwm` and `Options::recv_hwm` as
message-count settings. Data queues are split:

- connect-side pre-ready pipes: capped by `send_hwm`;
- round-robin materialized peer pipes: capped by `send_hwm`;
- fan-out lane rings: capped by `send_hwm`;
- routed/fan-out transmit slots: capped separately by message count and bytes;
- actor/control channels: separate from data HWM pipes.

Effective queued capacity can exceed one `send_hwm` when a socket owns multiple
pipes, rings, or transmit slots. Treat HWM as per-pipe backpressure, not an
exact socket queue length or byte cap.
