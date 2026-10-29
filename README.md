<img src="doc/omq-logo.svg" alt="OMQ" width="525" />

**ZeroMQ messaging, built in Rust.**

Connect threads, processes, hosts, and languages without a broker.
Your application sends messages, OMQ handles the network. Queues all the way!

- **libzmq compatibility.** Familiar [ZeroMQ](https://zeromq.org) socket
  patterns, compatible wire protocol, and a drop-in C API.
- **High throughput, low latency.** Millions of messages per second,
  low p99 latency, and scalable background IO. See the [benchmarks](#performance).
- **Compression.** LZ4 and adaptive Zstd transports save bandwidth without changing
  application messages.
- **One API across transports.** inproc, IPC, TCP, UDP, WebSocket, QUIC, DART, and
  SHM. Sockets handle framing, reconnection, and back-pressure.
- **Language support.** Rust plus [bindings](#workspace) for C/C++, Python,
  Ruby, Go, Java, Node.js, .NET, Lua, BEAM, Zig, and Crystal.
  [OMQ.ts](pure/typescript/) is a TypeScript browser client.

## Usage

Same socket types, same connect/bind/send/recv:

```sh
cargo add omq-tokio --rename omq
cargo add tokio --features macros,rt-multi-thread
```

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

    let msg = pull.recv().await?;
    assert_eq!(&msg[0], b"hello");
    Ok(())
}
```

The application can await `bind`, `connect`, `send`, and `recv` on any runtime
like Tokio or Compio. OMQ handles the network on its own background IO threads.
Port `0` selects a free TCP port, returned by `bind()`.

New to ZeroMQ? Start with [GETTING_STARTED.md](GETTING_STARTED.md). More
examples in [examples/zguide/](examples/zguide/), a port of the ZeroMQ Guide
patterns to OMQ.

## Performance

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/tcp.svg" alt="PUSH/PULL throughput over TCP: OMQ and libzmq" width="950">
</p>

<details>
<summary>REQ/REP latency</summary>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/reqrep/tcp.svg" alt="REQ/REP latency over TCP: OMQ and libzmq" width="950">
</p>
</details>

<details>
<summary>PUB/SUB throughput</summary>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/tcp.svg" alt="PUB/SUB throughput over TCP: OMQ and libzmq" width="950">
</p>
</details>

<details>
<summary>Compressed PUSH/PULL throughput</summary>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/compression_tcp.svg" alt="LZ4 and Zstd PUSH/PULL throughput over TCP" width="950">
</p>

[Full compression transport benchmarks (LZ4 and Zstd)](BENCHMARKS_COMPRESSION.md)
</details>

[Full comparison charts](COMPARISONS.md) |
[Other protocol comparisons][other-protocol-comparisons]

## Behavior under load

- **Abstraction.** Connect-before-bind. Transparent reconnection. Peer failures never become
  send/recv errors.
- **Back-pressure.** HWM limits queues. Block or drop according to socket semantics.
- **Fairness.** Round-robin sends and fair-queued receives.
- **Natural batching.** Internal batches coalesce wakeups and writes. No explicit batches
  from application needed.
- **Efficient payloads.** Inline tiny messages, by-value inproc, gather writes for large payloads,
  opt-in buffer pools.

OMQ normally runs transport work on dedicated background IO threads,
separate from application threads. Embedded and exclusive APIs are also
available:

<details>
<summary>Runtime placement</summary>

| API | IO placement |
|-----|--------------|
| `Context::new().socket(...)` | OMQ-owned background IO threads, async API |
| `Context::new().blocking_socket(...)` | OMQ-owned background IO threads, synchronous API |
| `Context::current().socket(...)` | Existing tokio runtime |
| `omq_tokio::exclusive::Socket::{connect, bind}(...)` | Caller task, one TCP peer, no socket driver task |

</details>

Owned contexts support independent aggregate ingress/egress byte limits,
configured before the first socket; exhaustion applies backpressure.
Limits count transport bytes after compression and each fan-out copy;
IPC, inproc, and SHM are excluded.

```rust,ignore
let ctx = omq::Context::new();
ctx.set_network_rate_limits(omq::NetworkRateLimits {
    ingress: None,
    egress: Some(omq::ByteRateLimit::new(10_000_000, 100_000)),
})?;
```

## Cargo features

The default build includes the NULL mechanism and TCP / IPC / inproc / UDP,
plus SHM on Linux.
It needs no libzmq, libsodium, or C compiler. Optional features:

| Feature | Adds |
|---------|------|
| `plain` | PLAIN username/password authentication (RFC 24) |
| `curve` | CURVE authentication and encryption (RFC 26) |
| `lz4` | `lz4+tcp://` compression ([RFC](doc/lz4-rfc.md)) |
| `zstd` | Experimental `zstd+tcp://` compression, with a level that adapts to the sender bottleneck |
| `ws` | WebSocket (`ws://`) and verified TLS (`wss://`) |
| `quic` | TLS-encrypted QUIC (`quic://`): [overview](doc/udp_transports.md#quic), [example](examples/quic.rs) |
| `webtransport` | Browser WebTransport over HTTP/3 on `quic://` listeners ([RFC](doc/quic-rfc.md#11-webtransport-profile)) |
| `dart` | Reliable ordered UDP (`dart://`) for low latency: [overview](doc/udp_transports.md#dart), [RFC](doc/dart-rfc.md) |

## Workspace

Seven Cargo workspace crates. The protocol core, tokio backend, and
WebTransport crates forbid unsafe Rust. SHM transport and native interfaces
isolate unsafe operations.

| Crate | What it does |
|-------|--------------|
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x: codec, messages, mechanisms, subscriptions |
| [`omq-tokio`](omq-tokio/) | Tokio backend for Linux, macOS, and Windows |
| [`omq-shm`](omq-shm/) | Linux shared-memory transport |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq` dynamic/static library) |
| [`omq-bench`](omq-bench/) | Benchmark runner and SVG chart generator |
| [`omq-webtrans-proto`](vendor/omq-webtrans-proto/), [`omq-webtrans-quinn`](vendor/omq-webtrans-quinn/) | WebTransport over HTTP/3, adapted from `webtrans` (MIT) |

Language bindings:

| Language | Binding |
|----------|---------|
| C/C++ | [`omq-libzmq`](omq-libzmq/): libzmq-compatible C API |
| Python | [`pyomq`](bindings/pyomq/): synchronous and asyncio APIs |
| Ruby | [`omq-rs`](bindings/ruby/): scheduler-aware synchronous API |
| Go | [`OMQ.go`](bindings/go/): goroutine-safe API |
| Java | [`OMQ.java`](bindings/java/): synchronous and async APIs |
| Node.js | [`OMQ.node`](bindings/node/): native addon |
| .NET | [`OMQ.Net`](bindings/dotnet/): managed wrapper |
| Lua | [`OMQ.lua`](bindings/lua/): native module |
| Erlang, Elixir, Gleam | [`OMQ.beam`](bindings/beam/): NIF and wrappers |
| Zig | [`OMQ.zig`](bindings/zig/): C API wrapper |
| Crystal | [`omq-binding.cr`](https://github.com/paddor/omq-binding.cr): native binding |

Pure implementations speak ZMTP without the Rust engine:

| Language | Implementation |
|----------|----------------|
| TypeScript | [`OMQ.ts`](pure/typescript/): browser client over `ws://`, `wss://`, `lz4+ws://`, and WebTransport |

## Sister projects

- [OMQ.rb](https://github.com/zeromq/omq.rb): Ruby implementation with
  compatible compression and an optional Rust backend.

## Further reading

- [COMPARISONS.md](COMPARISONS.md): cross-implementation comparison charts.
- [PROTOCOL_COMPARISONS.md][other-protocol-comparisons]: throughput and
  request/reply-like latency against other messaging and RPC protocols.
- [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md): LZ4 and Zstd
  transport throughput on bandwidth-limited links.
- [doc/architecture.md](doc/architecture.md): components, threads, and
  message paths.
- [doc/bindings.md](doc/bindings.md): C and language bindings.
- [doc/libzmq/semantics.md](doc/libzmq/semantics.md): exact compatibility
  notes for no-peer sends, linger, and HWM.
- [doc/lz4-rfc.md](doc/lz4-rfc.md): LZ4 compression transport wire
  format and dictionary shipping rules.
- [doc/quic-rfc.md](doc/quic-rfc.md): native QUIC transport, TLS verification,
  liveness, reconnect rules, and the WebTransport profile.
- [doc/udp_transports.md](doc/udp_transports.md): UDP, QUIC, and DART purposes,
  configuration, and benchmarks over loopback and lossy links.
- [omq-shm/README.md](omq-shm/README.md): shared-memory transport on Linux.

[other-protocol-comparisons]: https://github.com/paddor/omq.rs/blob/other-moms/PROTOCOL_COMPARISONS.md

## Platform and requirements

- Rust 1.93 or newer (edition 2024).
- Linux (`x86_64`, `aarch64`, `i686-unknown-linux-gnu`, `armv7-unknown-linux-gnueabihf`)
- macOS (ARM/Intel)
- Windows

Linux is the primary development and benchmarking platform.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build and test commands, and [RUNNING_BENCHMARKS.md](RUNNING_BENCHMARKS.md) for benchmarks.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.
