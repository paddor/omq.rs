# OMQ Go Architecture

The Go binding is an idiomatic package over a small C ABI implemented by
`bindings/go/native`. The native crate uses `omq-tokio`, not `omq-libzmq`.

## Threading

`Open(Config{IOThreads: n})` creates an OMQ context with `n` owned IO
threads. Go caller goroutines never own transport, reconnect, compression,
or ZMTP state.

The public API is goroutine-safe. Socket close marks native state closed and
wakes later operations through typed errors. Native handles are kept alive
until Go finalizers release the small handle object, so close does not race
with an in-flight cgo call by freeing memory underneath it.

## Calls

Public send and receive APIs are scalar:

- `Send(ctx, Message)`
- `Recv(ctx)`
- `RecvInto(ctx, []byte)`
- `SendTimeout(Message, time.Duration)`
- `RecvTimeout(time.Duration)`
- `TrySend(Message)`
- `TryRecv()`
- `TryRecvInto([]byte)`

Timeout helpers are convenience wrappers around context-aware loops:

- `timeout == 0`: nonblocking
- `timeout < 0`: wait forever
- `timeout > 0`: deadline

Go cancellation is handled before entering cgo and between nonblocking
native attempts. User goroutines do not need to close a socket from another
goroutine to interrupt a blocked receive.

`Socket.Run(ctx, fn)` executes `fn` on the socket owner goroutine, pinned to
one OS thread. It is the low-latency path for tight loops. `BoundSocket`
keeps scalar methods, but skips the per-call owner-channel handoff while
preserving the one-native-thread-per-socket rule.

## Data Path

Go copies outbound message parts into C-owned memory before calling native
code. Native never stores Go pointers. Inbound message parts are allocated
by Rust and copied into Go-owned `[]byte` before the native message is freed.

Hot `BoundSocket` sends for single-part `PUSH` and `SCATTER` copy the Go
payload into a native SPSC send ring. A native worker drains descriptors in
batches and submits them with OMQ's private `try_send_many` path. Native
keeps each ring slot alive until the OMQ `Message` drops, so Go buffers are
never retained by Rust.

Hot `BoundSocket` receives use a native SPSC receive ring. Rust refills the
ring with `try_recv_many_into`, `recv_many_into`, or `recv_many_timeout_into`
and Go drains scalar `RecvInto` calls from ring descriptors. No public batch
API is exposed.

OPTIMIZE: the current hot path still copies payload bytes at the Go/native
boundary. Single-part sends copy into the native send ring. Single-part
receives copy from OMQ `Message` into the native receive ring and then into
the caller's `RecvInto` buffer. This keeps cgo pointer ownership simple, but
large payload throughput is limited by memory bandwidth. Future work should
measure owned native send buffers that Go fills before transfer to OMQ, plus
a direct native `RecvInto` path that decodes single-part messages into the
caller buffer when batching would not be harmed.

The general scalar API remains context-aware and goroutine-safe. It pays the
owner-channel and cgo transition costs on each operation. Use `Socket.Run`
when a socket is on a hot path.

## Channels

`Socket.Channels(ctx, opts)` is an edge adapter over the scalar API.
Tx-only sockets expose nil `Rx`; Rx-only sockets expose nil `Tx`.
The adapter supports bounded buffers and explicit overrun policy for
receive delivery.

The channel API is Go-friendly, but Go channels are not marketed as pure
lock-free primitives. The hot native receive refill path avoids per-message
cgo calls by batching internally.
