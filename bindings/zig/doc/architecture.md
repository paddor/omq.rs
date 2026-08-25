# OMQ.zig Architecture

OMQ.zig is a Zig 0.16 package over the `omq-libzmq` C ABI. It imports
`omq-libzmq/include/zmq.h` with `@cImport` and links `libomq_zmq`.

The binding has no native shim. Public Zig types are thin owners around C
handles:

- `Context` owns `zmq_ctx_t`.
- `Socket` owns `zmq_socket`.
- `Frame`, `Message`, and `FrameMessage` own received or copied frame data.
- `Poller` stores Zig-side registrations and calls `zmq_poll`.
- `Monitor` is a PAIR socket wrapper around `zmq_socket_monitor`.

All transport, reconnect, routing, ZMTP, compression, security, and queue
behavior lives below the ABI in `omq-libzmq`, `omq-tokio`, and `omq-proto`.
Zig callers only enqueue sends, receive complete messages, set options, and
manage explicit ownership.

Returned byte slices are allocator-owned. `recvAlloc`, `recvFrameAlloc`,
`recvMultipartAlloc`, `getBytesAlloc`, and `getStringAlloc` allocate with the
caller-provided allocator. Matching `deinit` methods release aggregate types.

String inputs that cross the ABI are duplicated as sentinel-terminated temporary
buffers. Raw byte options and message payloads pass pointer plus length.

Errors are mapped from thread-local `zmq_errno()` into a Zig error set. Unknown
errno values collapse to `error.Unknown`; `lastErrno()` remains available for
direct ABI diagnostics.

Context sharing uses OMQ extension keys. `Context.shareKey()` exports the
process-local context key and `Context.fromShareKey()` imports another handle
to the same native context, preserving shared `inproc://` namespaces.
