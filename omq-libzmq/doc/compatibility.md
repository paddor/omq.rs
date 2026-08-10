# omq-libzmq Compatibility

`omq-libzmq` ships a libzmq 4.3.6-compatible `zmq.h` and exports every C
symbol declared there. `tests/abi_matrix.rs` enforces that the header, Rust
exports, and tracked socket option matrix stay in sync.

## Supported Surface

- Core context, socket, bind/connect, send/recv, message, proxy, poll, poller,
  timer, atomic counter, stopwatch, thread helper, Curve/Z85, and monitor v1
  APIs are exported.
- Transports: `inproc://`, `tcp://`, `ipc://`, `ws://`, and `wss://`.
- Security mechanisms: NULL, PLAIN, CURVE. GSSAPI option constants exist for
  ABI compatibility but GSSAPI authentication is not implemented.
- Socket types: PAIR, PUB/SUB, REQ/REP, DEALER/ROUTER, PULL/PUSH, XPUB/XSUB,
  STREAM, SERVER/CLIENT, RADIO/DISH, GATHER/SCATTER, PEER, CHANNEL.
- C++ users should use upstream cppzmq directly. The staged compat package
  provides `libzmq` pkg-config and CMake aliases.

## Unsupported Or Partial APIs

- `ZMQ_DGRAM` is declared for header compatibility but `zmq_socket()` rejects it
  with `EINVAL`.
- `zmq_connect_peer()` and `zmq_disconnect_peer()` return `ENOTSUP`.
- `zmq_socket_monitor_versioned()` supports monitor version 1 only. Monitor v2
  and `ZMQ_EVENT_PIPES_STATS` are not implemented.
- `zmq_socket_monitor_pipes_stats()` and `zmq_socket_get_peer_state()` return
  `ENOTSUP`.
- `zmq_sendiov()` and `zmq_recviov()` return `ENOTSUP`.
- `zmq_ppoll()` delegates to `zmq_poll()` only when `sigmask` is null. A
  non-null signal mask returns `ENOTSUP`.
- `zmq_poller_fd()` returns `EINVAL`; the poller is implemented over
  `zmq_poll()` and has no native fd.
- QUIC, SCTP, VMCI, NORM, PGM/EPGM, and DGRAM transports are not implemented.

## Option Behavior

Socket options fall into three groups:

- Functional round-trip options, such as HWM, timeouts, linger, identity,
  reconnect, heartbeats, TCP keepalive, buffers, IPv6, PLAIN/CURVE, and WSS TLS
  options.
- State options, such as `ZMQ_TYPE`, `ZMQ_EVENTS`, `ZMQ_FD`, `ZMQ_RCVMORE`,
  `ZMQ_LAST_ENDPOINT`, and `ZMQ_MECHANISM`.
- Compatibility defaults/no-ops for legacy or transport-specific options that
  do not map to omq-tokio behavior, such as ZAP, SOCKS, GSSAPI, VMCI, NORM,
  multicast tuning, batch sizes, busy-poll, and most XPUB manual knobs.

`ZMQ_RECONNECT_STOP` is stored and returned. Only the connection-refused bit is
currently wired into backend behavior.

`ZMQ_THREAD_SAFE` returns 0. Match libzmq discipline: one application thread per
socket.
