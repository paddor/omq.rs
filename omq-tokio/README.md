# omq-tokio

Tokio backend for OMQ, actor-based with one or more IO threads. Works on
Linux, macOS, and Windows. Install with `cargo add omq-tokio --rename omq`.

Built on [omq-proto](https://crates.io/crates/omq-proto) and
[tokio](https://crates.io/crates/tokio).

## Highlights

| | |
|-|-|
| Multi-threaded | Concurrent `send`/`recv` from multiple tasks is safe |
| Actor with bypass | `SocketDriver` owns mutable socket state. Sends bypass it for every socket type; receives bypass it except for ROUTER and REP. |
| Arena encoding | Small messages (< 4 KiB by default) pack into a `FrameBuffer` arena. Larger payloads use zero-copy gather-write. Override with `Options::arena_threshold`. |
| Bounded wakeups | Per-peer transmit slots and `yring` send pipes use `DataSignal` to coalesce wakeups without losing readiness. |

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_pushpull_tcp.svg" alt="PUSH/PULL throughput: TCP implementations" width="850">
</p>

## Usage

```rust
use omq_tokio::{Context, SocketType, Options, Message};

let ctx = Context::new();

let push = ctx.socket(SocketType::Push, Options::default());
push.bind("tcp://127.0.0.1:5555").await?;

let pull = ctx.socket(SocketType::Pull, Options::default());
pull.connect("tcp://127.0.0.1:5555").await?;

push.send(Message::single("hello")).await?;
let msg = pull.recv().await?;
```

Use `Socket::new(...)` when you want the socket driver on the caller's
active tokio runtime. Use `ctx.socket(...)` when OMQ should own IO runtime
threads.

Full [API documentation](https://docs.rs/omq-tokio/latest/omq_tokio/).

| Socket method | Purpose |
|---|---|
| `bind`, `connect` | Listen or connect using an endpoint URI |
| `send`, `try_send` | Send a message |
| `recv`, `try_recv` | Receive one complete message |
| `message` | Stream messages asynchronously |
| `sink` | Send messages from a stream |
| `identity_routing` | Receive ROUTER/PEER bodies with an owning `Identity`; `identity.peer_info()` exposes verified authentication and separate transport diagnostics |
| `IdentitySocket::recv_from`, `try_recv_from` | Receive `(Identity, Message)`; retaining the identity keeps metadata only |
| `recv_with_receipt`, `try_recv_with_receipt` | Receive `(ReceiveReceipt, Message)`; `None` selects any lane, `Some(&lane)` selects one |
| `recv_many_into` | Receive into reusable batch storage |
| `blocking::Socket::dispatch` | Handle messages on the calling thread |
| `close` | Close with configured linger |

On PEER, PULL, and GATHER, keeping a receipt pauses its receive lane; dropping
it resumes drainage. `receipt.lane()` returns a `ReceiveLane` for targeted
receives, and `unshift(receipt, message)` leaves the message pending on that lane.

### QUIC and WebTransport

The `quic` feature adds `quic://` endpoints. See the
[QUIC example](../examples/quic.rs) for certificate and trust setup.

With `webtransport`, the same listener also accepts WebTransport over HTTP/3
at `https://host:port/omq` (or `quic.webtransport_path`). Browser clients
implement the stream mapping in the [QUIC RFC](../doc/quic-rfc.md) and need
`quic.allowed_origins`. Draft-02 (Chromium, Firefox) and draft-07 (Safari)
are supported; draft-16 needs `RESET_STREAM_AT`, which Quinn does not ship
yet.

### Shared memory (Linux)

`shm://` endpoints exchange messages through shared memory between local processes.

Prepare payloads directly in shared memory with a producer pool:

```rust
let pool = omq_tokio::shm_payload_pool([(1024, 128), (16384, 16)])?;
let message = pool.message(1024, |body| body.fill(7))?;
socket.send(message).await?;
```

See [SHM transport](../omq-shm/README.md).

## Internals

[`doc/architecture.md`](../doc/architecture.md) covers the actor shape,
send/recv bypass, routing strategies, and arena encoding threshold.

## License

ISC
