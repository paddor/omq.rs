# OMQ.php Architecture

PHP extension for `omq-libzmq`. `bindings/php/native` is a Rust `cdylib`
built with ext-php-rs that registers the module `zmq` with ext-zmq 1.1.3's
classes. It links `omq-libzmq` as a crate, not as `libzmq.so.5`.

## Source Layout

```text
native/src/lib.rs        module entry, MINIT/MSHUTDOWN
native/src/context.rs    ZMQContext, persistent context registry
native/src/socket.rs     ZMQSocket, persistent socket registry, send/recv
native/src/sockopt.rs    option kinds and conversion
native/src/poll.rs       ZMQPoll over sockets and PHP streams
native/src/device.rs     ZMQDevice loop with idle and timer callbacks
native/src/errors.rs     ZMQException and its subclasses
native/src/zmq_class.rs  ZMQ constants and static helpers
native/src/fd_stream.c   PHP stream macros ext-php-rs does not bind
tests/                   PHP tests and ext-zmq's .phpt suite
```

## Ownership

- `ContextShared` owns a native context; `SocketShared` owns a native socket
  and holds an `Arc` to its context, so a context outlives its sockets.
  PHP objects (`ZMQContext`, `ZMQSocket`, `ZMQPoll` items) hold `Arc`s too.
- Persistent contexts are keyed by IO thread count, persistent sockets by
  context, type, and persistent id. Both registries are process globals that
  outlive requests. A new persistent socket joins its registry only after
  `on_new_socket` returns.
- MSHUTDOWN clears the socket registry, then the context registry, because
  terminating a context waits for its sockets.

## Processes and Threads

MINIT registers classes only. Contexts start their IO threads on first use,
so PHP-FPM workers forked after MINIT never inherit running threads. Each
context records the pid that created it. A forked child skips the parent's
registry entries and never closes or terminates the parent's handles.

NTS PHP runs one thread per process, which keeps libzmq's rule of one
socket per thread. `omq-libzmq` IO threads own transport, reconnect,
ZMTP, compression, and routing.

## Data Path

Send:

```text
ZMQSocket::send
  -> ext-php-rs method dispatch
  -> borrow the zend_string bytes
  -> zmq_send (copies into an OMQ message)
  -> omq-tokio send pipe
  -> IO thread encodes and writes transport
```

Receive:

```text
IO thread reads and decodes transport
  -> omq-libzmq receive ring
  -> recv_frame_with lends the frame bytes
  -> ZendStr::new copies them into the returned PHP string
```

Receive skips `zmq_msg_t`: each frame is copied once, into the PHP string.
EAGAIN returns `false`, as in ext-zmq; other errors throw.

## Small-Message Cost

For 16 byte TCP PUSH/PULL, the per-call cost is:

- ext-php-rs dispatch and its bailout guard around each call.
- `omq-libzmq` C ABI checks and OMQ routing, HWM, and timeout checks.
- One PHP string allocation per received frame, as in ext-zmq.

ext-zmq is closer to:

```text
zend handler -> libzmq zmq_msg_recv -> zend_string copy
```

OMQ.php is closer to:

```text
ext-php-rs handler -> omq-libzmq recv_frame_with -> zend_string copy
```
