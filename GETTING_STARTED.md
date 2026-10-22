# Getting Started

A crash course in ZeroMQ-style messaging with OMQ. For the full background,
read the [ZeroMQ Guide](https://zguide.zeromq.org/); its patterns are ported
in [examples/zguide/](examples/zguide/).

## Sockets are message queues

An OMQ socket is not a TCP socket. It is a queue of complete messages with a
routing pattern attached. You pick a socket type, `bind` or `connect` it to
one or more endpoints, then `send` and `recv` messages. The socket handles
connections, reconnects, framing, and backpressure in the background.

```rust
use omq::{Context, Message, Options, Result, SocketType};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Context::new();

    let pull = ctx.socket(SocketType::Pull, Options::default());
    let endpoint = pull.bind("tcp://127.0.0.1:0").await?;

    let push = ctx.socket(SocketType::Push, Options::default());
    push.connect(endpoint).await?;
    push.send(Message::single("hello")).await?;

    assert_eq!(&pull.recv().await?[0], b"hello");
    Ok(())
}
```

## What the socket promises

- **Messages are atomic.** A message has one or more parts (frames) and
  arrives whole or not at all.
- **Bind and connect are independent of direction.** Either side may bind.
  `connect()` works before the peer binds; the socket retries until it can.
- **Peers come and go silently.** Disconnects, restarts, and slow peers never
  make `send` or `recv` fail. Sockets reconnect with backoff and replay
  subscriptions. Errors mean misuse or a closed socket.
- **Queues are bounded.** The high-water mark (`send_hwm`, `recv_hwm`) caps
  queued messages per peer. When a peer is full, round-robin and exclusive
  sockets block (or `try_send` returns `Full`); PUB, XPUB, and RADIO drop for
  that subscriber unless `xpub_nodrop` is set.
- **Fairness.** Sends round-robin across ready peers; receives fair-queue
  across connections. A fast peer cannot starve a slow one.
- **Heartbeats find dead peers, not slow ones.** A peer that answers PINGs is
  alive, however slowly it reads.
- **One socket, many endpoints.** A socket can bind TCP and IPC at once and
  connect to several peers.

## Socket types

Socket types come in compatible pairs. The handshake rejects mismatches.

| Pattern | Pair | Behavior |
| --- | --- | --- |
| Pipeline | PUSH -> PULL | PUSH round-robins messages; PULL fair-queues them |
| Pipeline, single-part | SCATTER -> GATHER | Like PUSH/PULL, one part per message |
| Publish/subscribe | PUB -> SUB | PUB sends to every SUB whose prefix matches the first part |
| Raw pub/sub | XPUB <-> XSUB | Subscriptions become messages, for building proxies |
| Groups | RADIO -> DISH | RADIO sends to exact groups that DISH joined; also over UDP |
| Request/reply | REQ <-> REP | Strict send/recv alternation; REP replies to the asker |
| Async request/reply | DEALER <-> ROUTER | No alternation; ROUTER prefixes each message with the peer identity |
| Client/server | CLIENT <-> SERVER | Single-part DEALER/ROUTER; SERVER routes by `routing_id` |
| Exclusive | PAIR <-> PAIR, CHANNEL <-> CHANNEL | Exactly one peer; CHANNEL is single-part |
| Peer-to-peer | PEER <-> PEER | Many peers, routed by identity in both directions |
| Raw TCP | STREAM | Talks to non-ZMQ TCP clients; identity frame selects the peer |

Publish/subscribe:

```rust
let sub = ctx.socket(SocketType::Sub, Options::default());
sub.subscribe("weather.").await?;      // prefix match; "" means everything
sub.connect("tcp://127.0.0.1:5556").await?;
let msg = sub.recv().await?;           // ["weather.zurich", ...]
```

Request/reply:

```rust
let rep = ctx.socket(SocketType::Rep, Options::default());
rep.bind("tcp://*:5555").await?;
let request = rep.recv().await?;
rep.send(Message::single("world")).await?;    // goes back to the asker
```

ROUTER sees `[identity, ...body]` on receive and routes a send by its first
part. PUB keeps nothing for subscribers that have not connected yet, so a late
SUB misses earlier publications. A connecting PUSH, by contrast, queues
messages until its peer is ready.

## Transports

The endpoint URI picks the transport; socket code stays the same.

| URI | Use |
| --- | --- |
| `tcp://host:port` | Network |
| `ipc:///path` | Local processes (Unix sockets, Windows named pipes) |
| `inproc://name` | Threads in one process, no serialization |
| `shm://name` | Local processes over shared memory (Linux) |
| `ws://`, `wss://` | WebSocket, optionally TLS |
| `quic://host:port` | QUIC with TLS 1.3 (`quic` feature) |
| `udp://host:port` | RADIO/DISH datagrams |
| `dart://host:port` | Reliable ordered messages over UDP (`dart` feature) |
| `lz4+tcp://`, `zstd+tcp://` | Compressed TCP (`lz4`, `zstd` features) |

PLAIN authenticates peers with a username and password; CURVE authenticates
and encrypts (`plain` and `curve` features). Both sides must use the same
mechanism.

## Contexts and threads

A `Context` owns OMQ's background I/O threads and the `inproc://` namespace.
`Context::new()` starts one I/O thread; the application calls the socket from
any runtime or thread, and OMQ does the network work on its own threads.
`Context::current()` runs OMQ inside your existing Tokio runtime instead. A
blocking API exists for non-async code. See
[architecture.md](doc/architecture.md#threads-and-runtimes).

Sockets can be cloned and shared between tasks. Sends through one clone keep
their order; clone once per sending task, not per message.

## Knowing who is connected

- `wait_connected(n, timeout)` waits for `n` ready peers.
- `connections()` lists ready peers; `connections_changed(seen)` waits for
  the set to change, without missing updates.
- `monitor()` streams lifecycle events for diagnostics. It is lossy under
  load; do not gate application logic on it.

## Next steps

- [README.md](README.md): install, performance, and feature overview.
- [doc/architecture.md](doc/architecture.md): how OMQ works inside.
- [doc/bindings.md](doc/bindings.md): C and other languages.
- [doc/libzmq/semantics.md](doc/libzmq/semantics.md): differences from
  libzmq.
