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
- `SendTimeout(Message, time.Duration)`
- `RecvTimeout(time.Duration)`
- `TrySend(Message)`
- `TryRecv()`

Timeout helpers are convenience wrappers around context-aware loops:

- `timeout == 0`: nonblocking
- `timeout < 0`: wait forever
- `timeout > 0`: deadline

Go cancellation is handled before entering cgo and between nonblocking
native attempts. User goroutines do not need to close a socket from another
goroutine to interrupt a blocked receive.

## Data Path

Go copies outbound message parts into C-owned memory before calling native
code. Native never stores Go pointers. Inbound message parts are allocated
by Rust and copied into Go-owned `[]byte` before the native message is freed.

Receive uses hidden batching. When the native receive cache is empty,
Rust calls `try_recv_many_into`, `recv_many_into`, or
`recv_many_timeout_into` with a reused scratch vector, then returns one
message to Go. No public batch API is exposed.

## Channels

`Socket.Channels(ctx, opts)` is an edge adapter over the scalar API.
Tx-only sockets expose nil `Rx`; Rx-only sockets expose nil `Tx`.
The adapter supports bounded buffers and explicit overrun policy for
receive delivery.

The channel API is Go-friendly, but Go channels are not marketed as pure
lock-free primitives. The hot native receive refill path avoids per-message
cgo calls by batching internally.
