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
| `PUSH`/`DEALER`/`REQ`/`CLIENT`/`SCATTER` bound, no ready peer | Mute. Blocking `send()` waits. `DONTWAIT` or `SNDTIMEO=0` returns `EAGAIN`. No message is accepted and dropped. | Same. | Different: queues complete messages in the no-peer fallback up to `send_hwm`, then applies `on_mute`. |
| Same sockets connected, no ready peer, `ZMQ_IMMEDIATE=0` | Accepts and queues to the incomplete/reconnecting pipe, bounded by HWM. | Same. | Similar: native has no `ZMQ_IMMEDIATE` option and queues in fallback. |
| Same sockets connected, no ready peer, `ZMQ_IMMEDIATE=1` | Mute. Blocking `send()` waits; nonblocking returns `EAGAIN`. | Same. | No native `ZMQ_IMMEDIATE` option. |
| Bound socket had a peer, then all peers disconnected | Mute once no ready pipe remains. Blocking `send()` waits; nonblocking returns `EAGAIN`. | Same. | Different: falls back to no-peer queue up to `send_hwm`. |
| Connected socket had a peer, then peer disconnected | With `ZMQ_IMMEDIATE=0`, queues to the reconnecting pipe. With `ZMQ_IMMEDIATE=1`, mute until reconnect completes. | Same. | Queues in fallback while reconnecting. |
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
Late peers can receive queued no-peer sends before the deadline. Zero linger
cancels endpoints and drops queued sends immediately.

## HWM

libzmq `ZMQ_SNDHWM` and `ZMQ_RCVHWM` are message-count limits, not byte
limits. A complete multipart message consumes one HWM credit. A 16-byte
message and a 16-MiB message both consume one credit.

libzmq accounting is per pipe. Total process memory can grow with peer count:
roughly `hwm * ready_pipe_count * average_message_size`, plus codec and OS
buffers. HWM is backpressure, not a memory cap.

Native OMQ exposes `Options::send_hwm` and `Options::recv_hwm` as socket-level
message-count settings, but implementation queues are split:

- no-peer/pre-connect fallback queue: capped by `send_hwm`;
- round-robin per-peer send pipes: capped by `send_hwm.max(16)`;
- fan-out and routed peers: per-peer queues and transmit slots add more
  buffering;
- internal `yring` capacities are rounded for batching.

Therefore native effective queued capacity can exceed `send_hwm`. This is
deliberate today. Treat native HWM as the point where backpressure starts, not
as an exact total queue length and not as a byte cap.
