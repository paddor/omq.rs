# OMQ.Net architecture

OMQ.Net is a managed wrapper over the `omq-libzmq` C ABI.

- .NET owns API shape, validation, lifetime wrappers, option conversion, and
  exception translation.
- `omq-libzmq` owns contexts, sockets, queues, routing, transports, ZMTP,
  reconnect, authentication, and native I/O threads.
- The binding does not reimplement ZMTP or transport readiness in C#.

## Runtime and ownership

```text
.NET caller
  -> Socket / Context method
  -> serialized managed call
  -> omq-libzmq C ABI
  -> native context I/O threads
  -> transport and protocol tasks
```

`Context` owns the native context and all sockets created from it. `Socket`
owns one `SafeHandle`. Explicit `Dispose` is idempotent; SafeHandle release is
the fallback for abandoned handles. Socket calls hold a per-socket lock until
the native operation completes.

Pollers acquire native-handle leases before calling `zmq_poll`, so closing a
socket cannot free its handle during an in-flight poll. Context shutdown closes
owned sockets after waking native operations.

## Data path

- Outbound spans, strings, and message frames are copied into temporary
  managed/native buffers before the C call.
- `Receive` copies inbound frames into managed `byte[]` values before each
  native `zmq_msg_t` is closed.
- `ReceiveInto` pins the caller's span for the native call and writes into it
  directly. Native code does not retain the pointer.
- Multipart receive drains the complete native message while the socket lock
  is held, preserving frame order and routing ID.
- No managed pointer is retained by native code after the call returns.

The copying boundary is deliberate: it keeps ownership simple and makes
dispose, cancellation, and GC interactions explicit. Zero-copy APIs are not
part of the current public contract.

## Async and cancellation

`SendAsync(Message)` uses the OMQ-native async task ABI. Cancellation requests
native task cancellation and keeps the callback state rooted until completion.
`SendAsync(ReadOnlyMemory<byte>)` and `ReceiveAsync` use nonblocking socket
operations plus short cancellable poll slices. `Poller.WaitAsync` uses the same
bounded-slice model.

Cancellation never frees a native async task or callback state while native
code may still reference it. Dispose and context shutdown remain the explicit
way to interrupt native operations that have no native cancellation handle.

## Monitors and proxies

`Monitor` enables the native socket monitor and reads its inproc PAIR endpoint.
Events are decoded into managed `MonitorEvent` values; disposing the monitor
first disables the source and then closes the reader.

`Proxy` delegates blocking proxy/device loops directly to the native ABI. Run
these methods on a dedicated thread or task.
