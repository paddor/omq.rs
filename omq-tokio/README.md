# omq-tokio

Tokio backend for [omq](https://crates.io/crates/omq). Multi-threaded, actor-based.
Default backend when you `cargo add omq`. Works on Linux, macOS, and Windows.

Built on [omq-proto](https://crates.io/crates/omq-proto) and
[tokio](https://crates.io/crates/tokio).

## Highlights

| | |
|-|-|
| Multi-threaded | Concurrent `send`/`recv` from multiple tasks is safe |
| Actor with bypass | `SocketDriver` owns mutable socket state. Common send/recv paths bypass it for non-REQ/REP sockets. |
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

`cargo add omq` picks this backend by default.

Full [API documentation](https://docs.rs/omq-tokio/latest/omq_tokio/).

| Socket method | Purpose |
|---|---|
| `bind`, `connect` | Listen or connect using an endpoint URI |
| `send`, `try_send` | Send a message |
| `recv`, `try_recv` | Receive one complete message |
| `message` | Stream messages asynchronously |
| `sink` | Send messages from a stream |
| `recv_many_into` | Receive into reusable batch storage |
| `blocking::Socket::dispatch` | Handle messages on the calling thread |
| `close` | Close with configured linger |

### QUIC

The `quic` feature adds `quic://` endpoints. See the
[QUIC example](../examples/quic.rs) for certificate and trust setup.

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
