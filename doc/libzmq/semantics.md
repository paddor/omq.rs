# libzmq Send, Linger, and HWM Semantics

Reference point: libzmq 4.3.5.

## No-Peer Sends

Terminology:

- **Ready peer:** ZMTP handshake completed and the pipe can accept data-plane
  messages.
- **Bound no-peer:** socket called `bind()` but no peer is currently ready.
- **Connected no-peer:** socket called `connect()` but no peer is currently
  ready. This includes first connect-before-bind and reconnect after loss.
- **Mute:** socket cannot currently route a send. Blocking sends wait;
  nonblocking sends return `EAGAIN`, except lossy socket types.

`ZMQ_IMMEDIATE` controls connected no-peer sends. Default is `0`.

- `ZMQ_IMMEDIATE=0`: queue to incomplete/reconnecting connect pipes.
- `ZMQ_IMMEDIATE=1`: only queue to completed connections. No ready peer means
  mute.

Behavior by case:

| Socket case | libzmq behavior | `omq-libzmq` behavior | Native `omq-tokio` behavior |
|-------------|-----------------|-----------------------|-----------------------------|
| `PUSH`/`DEALER`/`REQ`/`CLIENT`/`SCATTER` bound, no ready peer | Mute. Blocking `send()` waits. `DONTWAIT` or `SNDTIMEO=0` returns `EAGAIN`. No message is accepted and dropped. | Same. | Same, except native nonblocking reports `TrySendError::Full(msg)`. |
| Same sockets connected, no ready peer, `ZMQ_IMMEDIATE=0` | Accepts and queues to the incomplete/reconnecting pipe, bounded by HWM. | Same. | Queues to a connect-side pre-ready pipe allocated by `connect()`, bounded by `send_hwm`. |
| Same sockets connected, no ready peer, `ZMQ_IMMEDIATE=1` | Mute. Blocking `send()` waits; nonblocking returns `EAGAIN`. | Same. | No native `ZMQ_IMMEDIATE` option; native always uses the pre-ready pipe. |
| Bound socket had a peer, then all peers disconnected | Mute once no ready pipe remains. Blocking `send()` waits; nonblocking returns `EAGAIN`. | Same. | Same for sockets with only bind endpoints, except native nonblocking reports `TrySendError::Full(msg)`. |
| Connected socket had a peer, then peer disconnected | With `ZMQ_IMMEDIATE=0`, queues to the reconnecting pipe. With `ZMQ_IMMEDIATE=1`, mute until reconnect completes. | Same for sends issued after reconnect is scheduled. | Queues to a connect-side pre-ready pipe while reconnecting. Native has no `ZMQ_IMMEDIATE` option. Messages already accepted by a dead transport are not delivery-guaranteed. |
| `PUB`/`XPUB`/`RADIO` no matching ready subscriber | Lossy. Send succeeds and message is dropped, unless `XPUB_NODROP` makes mute visible. | Same for supported options. | Same lossy default; `xpub_nodrop` exposes direct-path backpressure. |
| `ROUTER` unknown identity | Drops by default. With `ROUTER_MANDATORY`, returns `EHOSTUNREACH`. | Same public behavior. | Returns `Error::Unroutable` when `router_mandatory` is set. |
| `PAIR`/`CHANNEL` no ready peer | Mute. Blocking send waits; nonblocking returns `EAGAIN`. | Blocking send waits; nonblocking returns `EAGAIN`. No connect-side pre-ready queue. | `send().await` waits for the exclusive peer; `try_send()` returns `Full`. |

Important edge case: libzmq does not accept then drop `PUSH` messages when a
bound socket has no ready peer. The call does not complete until a pipe can
take the message, or it returns `EAGAIN` under nonblocking or timed send.

## Linger

libzmq default `ZMQ_LINGER` is `-1`: wait forever for queued outbound messages
to drain. `zmq_close()` returns immediately. `zmq_ctx_term()` waits for socket
linger work to finish, or forever for `-1`.

Native OMQ default linger is zero. `Socket::close().await` waits for configured
linger itself, then joins the driver. Dropping the last handle starts configured
linger in the background. With `Context::current()`, the caller's runtime must
keep polling for that background close to make progress.

`omq-libzmq` maps the C API back to libzmq defaults:

- default `ZMQ_LINGER=-1` maps to native `linger_forever()`;
- explicit `ZMQ_LINGER=0` maps to native zero linger;
- `zmq_close()` returns quickly and context termination waits for active linger
  work.

Finite or forever linger keeps bind/connect endpoints alive while draining.
Late peers can receive queued connect-side pre-ready sends before the deadline.
Bound no-peer sends are mute, so there is no bound no-peer queue to drain. Zero
linger cancels endpoints and drops queued sends immediately.

## HWM

libzmq `ZMQ_SNDHWM` and `ZMQ_RCVHWM` are message-count limits, not byte
limits. A complete multipart message consumes one HWM credit. A 16-byte
message and a 16-MiB message both consume one credit.

libzmq accounting is per pipe. Total process memory can grow with peer count:
roughly `hwm * ready_pipe_count * average_message_size`, plus codec and OS
buffers. HWM is backpressure, not a memory cap.

Native OMQ exposes `Options::send_hwm` and `Options::recv_hwm` as message-count
settings, but implementation queues are split:

- connect-side pre-ready pipes: capped by `send_hwm`;
- round-robin materialized peer pipes: capped by `send_hwm`;
- fan-out lane rings: capped by `send_hwm`;
- routed/fan-out transmit slots: capped separately by message count and bytes;
- actor/control channels are separate and are not data HWM pipes.

Therefore native effective queued capacity can exceed one `send_hwm` when a
socket owns multiple pipes/rings or transmit slots. Treat native HWM as
per-pipe backpressure, not as an exact total socket queue length and not as a
byte cap.
