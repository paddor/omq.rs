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

### QUIC

The `quic` feature adds `quic://host:port` for native OMQ peers. It carries
ZMTP on one bidirectional stream and liveness records on a second stream.

A bind needs a TLS server certificate chain and private key. A native OMQ
connector verifies the certificate name and chain against the platform store
and/or `trust_pem`. Use `server_name` when the endpoint host differs from the
certificate name. QUIC does not offer an insecure verification override or
mTLS. With the `plain` feature, PLAIN authenticates a client inside the
encrypted QUIC connection:

```rust
let mut server = Options::default().plain_server_credentials([
    ("alice", "client-secret"),
]);
server.quic.server_cert_pem = Some(cert_pem);
server.quic.server_key_pem = Some(key_pem);
let pull = ctx.socket(SocketType::Pull, server);
pull.bind("quic://0.0.0.0:4433".parse()?).await?;

let mut client = Options::default().plain_client("alice", "client-secret");
client.quic.trust_pem = Some(ca_pem);
client.quic.trust_system = false; // private CA only
let push = ctx.socket(SocketType::Push, client);
push.connect("quic://server.example.com:4433".parse()?).await?;
```

The server certificate must cover `server.example.com`. For dynamic
admission, `Options::plain_server(|peer: &MechanismPeerInfo| ...)` receives
the username, password, and remote IP during the handshake. CURVE remains
available when client public keys are preferred; configure a CURVE server
authenticator to restrict admitted keys. The C compatibility layer can use
ZAP at `inproc://zeromq.zap.01` with a configured ZAP domain for PLAIN,
CURVE, or NULL admission.

`connect()` reports initial DNS errors immediately. TLS and ZMTP
handshake failures appear on the socket monitor while automatic reconnect
continues. Setup has a 10-second default deadline; reconnect starts at
100 ms with jitter. `Options` retains `Debug`, with the QUIC private key
and PLAIN client password redacted.

Each peer is its own QUIC connection placed on one OMQ IO thread; one
connection's packet work does not spread across threads. Heartbeat options
drive a separate liveness stream, so a slow local consumer is not mistaken
for a dead peer. OMQ compression is never used on this transport.

## Internals

[`doc/architecture.md`](../doc/architecture.md) covers the actor shape,
send/recv bypass, routing strategies, and arena encoding threshold.

## License

ISC
