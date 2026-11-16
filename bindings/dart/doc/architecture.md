# OMQ.dart architecture

OMQ.dart is pure Dart over the `omq-libzmq` C ABI: `@Native` declarations
bound to the `libomq_zmq` code asset that `hook/build.dart` registers. The
hook downloads the prebuilt library of a pinned omq-libzmq release and
checks it against SHA-256 sums shipped in the package; user-defines can
bundle another file or defer to the system loader.

- Dart owns API shape, option conversion, exception mapping, and async
  waits.
- `omq-libzmq` owns contexts, sockets, queues, routing, transports, ZMTP,
  reconnect, security, and native I/O threads.

## Ownership and threads

`Context` owns the native context and tracks its sockets; `close()` closes
them, then terminates the context. `Socket` owns one native socket. Both are
closed explicitly; there are no finalizers.

An isolate runs one call at a time, so sockets need no locks. An isolate may
resume on another OS thread, so each socket allows thread migration. A
socket belongs to the isolate that created it. Isolates share a native
context through `Context.fromShareKey`.

## Calls

Calls that never block are leaf calls. Blocking calls, such as sync sends
and receives, bind, connect, close, and context termination, are not, so the
VM can run GC and other isolates meanwhile.

A Dart FFI callback must not run inside a leaf call. An inproc send runs the
receiver's ready callback inline on the sending thread, so a socket with an
`inproc://` endpoint sends through non-leaf calls.

## Data path

- Leaf sends pass the `Uint8List` address straight to `zmq_send`. Non-leaf
  sends copy the frame into per-isolate native scratch first.
- Receives call `omq_recv_into` with a per-isolate native buffer and copy
  each frame into a new `Uint8List`. A frame too large for the buffer
  arrives in an overflow `zmq_msg_t` and is copied from there.
- The frame info carries the more flag, the SERVER routing ID, and the DISH
  group.
- Native code keeps no pointer to Dart memory after a call returns.

Multipart sends stage frames natively until the final frame. A message is
committed once its first frame is accepted. If the final frame would block,
the socket keeps it and finishes the message before any later send, in the
background if no send follows.

## Async

One `NativeCallable.listener` per isolate serves every socket; the callback
userdata is the socket's id. A socket registers on its first wait, so
sync-only sockets never arm native write wakes.

A wait reads `ZMQ_EVENTS`, which arms the callback, and awaits a completer.
The callback runs on an OMQ I/O thread and only posts to the isolate's event
loop. The posted call wakes that socket's waiters, which retry. If an
operation would block while `ZMQ_EVENTS` reports ready, the wait falls back
to a short delay instead of spinning.

The listener keeps the isolate alive only while a wait is pending. `close()`
fails pending waits with `OmqClosedException`. Once `zmq_close` returns, no
callback runs, and late posts for closed sockets are dropped. A socket left
open when its isolate exits can still be called back, which the VM does not
allow, so close it first.
