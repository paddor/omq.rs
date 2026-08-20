# omq-rs Ruby Architecture

`omq-rs` is a Ruby extension over Rust `omq-tokio`. It does not use libzmq,
FFI, or CZMQ.

- Ruby owns API shape, socket classes, option hashes, block helpers, fiber
  scheduler integration, and Ruby exception mapping.
- Rust owns contexts, sockets, routing, queues, transports, ZMTP, reconnect,
  authentication, compression, monitor conversion, and I/O threads.
- The native extension is loaded as `OMQ::Rust::Native`.

## Runtime

```text
Ruby caller
  -> OMQ::Rust::Socket method
  -> native extension method
  -> yring queue or blocking runtime job
  -> omq_tokio::Socket
  -> OMQ runtime thread(s)
  -> connection tasks and transport I/O
```

The extension owns one process-local Tokio runtime. `OMQ::Rust.io_threads=`
sets worker count before sockets materialize. After fork, runtime lookup is
pid-aware and creates a fresh runtime for the child process.

Sockets materialize lazily on first I/O, bind, connect, wait, monitor, or fd
request. Options and CURVE authenticators must be configured before
materialization.

## Source Map

- `lib/omq/rs.rb`: top-level `OMQ.rs` helper
- `lib/omq/rs/socket.rb`: Ruby socket classes and scheduler-aware waits
- `ext/omq_rs_native/src/lib.rs`: extension entry point and module functions
- `ext/omq_rs_native/src/socket.rs`: native socket methods and Ruby value
  conversion
- `ext/omq_rs_native/src/runtime.rs`: Tokio runtime, pumps, monitor conversion
- `ext/omq_rs_native/src/options.rs`: Ruby option hash conversion
- `ext/omq_rs_native/src/auth.rs`: CURVE authenticator bridge
- `ext/omq_rs_native/src/notify.rs`: pipe-backed wakeups for Ruby waits

## Data Path

Ruby sends are converted to native `Message` values and queued through an
async SPSC ring. A Rust send pump drains that ring and calls `Socket::send` on
the OMQ runtime.

Receives use a Rust producer / Ruby consumer SPSC ring. A Rust receive pump
awaits `Socket::recv`, fills the ring, opportunistically drains more messages
with `try_recv`, then wakes Ruby through a pipe fd. Ruby pops messages and
converts frames to frozen binary strings.

SERVER receives prepend routing id; RADIO/DISH messages expose group plus body;
ROUTER, STREAM, and PEER keep their routing frame semantics. Native never keeps
Ruby string pointers after conversion.

## Scheduler Integration

Blocking waits release the GVL while waiting for runtime jobs. Receive and send
readiness use pipe fds, so MRI fiber schedulers can suspend only the current
fiber. Without a scheduler, waits block the calling Ruby thread.

TruffleRuby currently lacks Ruby's `Fiber.scheduler` API, so concurrent waits
there should use threads.

## Lifecycle

Each socket is a typed Ruby data object holding `RustSocket`. `close` is
idempotent. GC finalization closes the native socket as a leak fallback.

Close aborts receive and monitor pumps, drains pending sends until linger
expires, then closes the OMQ socket. Ractors may create sockets, but each
socket must stay owned by the Ractor that created it.

## Native Features

PLAIN, CURVE, LZ4, zstd, WebSocket, IPC, and inproc support are compiled as
Cargo features. `OMQ::Rust.has(:feature)` reports availability. Rust performs
protocol negotiation, authentication, compression, and monitor event decoding.
