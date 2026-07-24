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
| Arena encoding | Small messages (< 4 KiB) pack into a `FrameBuffer` arena. Larger payloads use zero-copy gather-write. |
| Bounded wakeups | Per-peer transmit slots and `yring` send pipes use `DataSignal` to coalesce wakeups without losing readiness. |

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_pushpull_tcp.svg" alt="PUSH/PULL throughput: TCP implementations" width="850">
</p>

## Usage

```rust
use omq_tokio::{Context, SocketType, Options, Message};

let ctx = Context::new();

let push = ctx.socket(SocketType::Push, Options::default());
push.bind("tcp://127.0.0.1:5555".parse()?).await?;

let pull = ctx.socket(SocketType::Pull, Options::default());
pull.connect("tcp://127.0.0.1:5555".parse()?).await?;

push.send(Message::single("hello")).await?;
let msg = pull.recv().await?;
```

Use `Socket::new(...)` when you want the socket driver on the caller's
active tokio runtime. Use `ctx.socket(...)` when OMQ should own IO runtime
threads.

`cargo add omq` picks this backend by default.

## Internals

[`doc/architecture.md`](../doc/architecture.md) covers the actor shape,
send/recv bypass, routing strategies, and arena encoding threshold.

## License

ISC
