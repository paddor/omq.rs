# omq-libzmq

libzmq-compatible C interface backed by [omq-tokio](https://crates.io/crates/omq-tokio).

Exposes `zmq_socket`, `zmq_bind`, `zmq_connect`, `zmq_send`, `zmq_recv`, and
friends with the same ABI as libzmq, allowing C/C++ programs (and FFI bindings
in other languages) to link against omq instead of libzmq.

## Features

- **Transports:** `inproc://`, `tcp://`, `ipc://` (including Windows named
  pipes), `ws://`, `wss://`
- **Socket Types:** Standard ZMQ types except DGRAM (PUSH/PULL, PUB/SUB,
  REQ/REP, DEALER/ROUTER, etc.)
- **Security:** NULL, PLAIN, and CURVE policy through standard ZAP; CURVE
  also supports OMQ's native inline authenticator
- **Compression:** LZ4 and Zstd over TCP
- **Cross-Platform:** Linux, macOS, Windows, BSD
- **API Compatibility:** Drop-in ABI target with documented compatibility gaps

See [WINDOWS.md](WINDOWS.md) for Windows targets, DLL linking, named-pipe IPC,
and polling differences.

`ZMQ_PLAIN_SERVER` selects the PLAIN mechanism and delegates admission to a
context-local ZAP REP or ROUTER handler at `inproc://zeromq.zap.01`; without
one, authentication fails closed. A non-empty `ZMQ_ZAP_DOMAIN` enables ZAP
filtering for NULL and CURVE. Status 300 closes silently; 400 and 500 send the
matching mechanism ERROR. Successful user IDs and metadata are exposed by
`zmq_msg_gets()` on received messages.

`omq_socket_set_plain_server_credentials()` is the OMQ-specific fixed
credential allowlist used by high-level bindings. It accepts an array of
`omq_plain_credential_t` values. `ZMQ_PLAIN_USERNAME` and
`ZMQ_PLAIN_PASSWORD` remain client options.

32-bit Linux support covers `i686-unknown-linux-gnu` and
`armv7-unknown-linux-gnueabihf`. `zmq_msg_t` is 64 bytes and pointer-aligned,
matching libzmq; `zmq_ctx_get(ctx, ZMQ_MSG_T_SIZE)` returns 64.

## C/C++ ABI

The bundled `zmq.h` tracks the libzmq 4.3.6 C ABI and is compiled against
cppzmq by `scripts/test-cppzmq.sh`.

Draft/legacy helper APIs needed by modern bindings are link-compatible.
Unsupported draft behavior returns `ENOTSUP`; this currently includes peer
connect/disconnect, monitor v2 pipe stats, peer state, `zmq_ppoll` with a
signal mask, and deprecated iovec send/recv. `zmq_poller_fd` returns `EINVAL`
because this poller is emulated over `zmq_poll` and has no native pollable fd.

WSS TLS options (`ZMQ_WSS_KEY_PEM`, `ZMQ_WSS_CERT_PEM`, `ZMQ_WSS_TRUST_PEM`,
`ZMQ_WSS_HOSTNAME`, `ZMQ_WSS_TRUST_SYSTEM`) are wired into `wss://` binds and
connects.

OMQ extension option `OMQ_ARENA_THRESHOLD` accepts an `int64_t` byte threshold
for the outbound frame arena. Set it before first bind/connect; `-1` restores
the 4 KiB native default.

Detailed support notes live in [doc/compatibility.md](doc/compatibility.md).

For the rough supported C ABI surface, compatibility gaps, and option behavior,
see [doc/compatibility.md](doc/compatibility.md). The bundled
`include/zmq.h` is the authoritative symbol and constant list.

`libomq_zmq.so` also exposes a small `omq_*` extension API for context sharing
and native async multipart sends. It is OMQ-specific, outside the libzmq ABI,
and documented in [doc/compatibility.md](doc/compatibility.md).

## Build

Produces `libomq_zmq.so` / `libomq_zmq.a` / `libomq_zmq.dylib`.

```sh
cargo build -p omq-libzmq --release
```

Runnable C ZGuide suites live in [examples/zguide/](examples/zguide/):

```sh
omq-libzmq/examples/zguide/run_all.sh
```

Standalone security examples live in [`examples/plain.c`](examples/plain.c)
and [`examples/curve.c`](examples/curve.c). Build them from the repository
root after building `omq-libzmq`:

```sh
for name in plain curve; do
  cc -std=gnu11 -Wall -Wextra -Werror \
    -I omq-libzmq/include "omq-libzmq/examples/$name.c" \
    -L target/debug -Wl,-rpath,"$PWD/target/debug" -lomq_zmq \
    -o "target/$name-example"
done

target/plain-example
target/curve-example
```

cppzmq users can use upstream cppzmq directly; see
[doc/cppzmq.md](doc/cppzmq.md) and [examples/cppzmq/](examples/cppzmq/).

To stage drop-in C/C++ build files:

```sh
omq-libzmq/scripts/stage-compat.sh /tmp/omq-libzmq
```

This writes `include/zmq.h`, `lib/libomq_zmq.*`, `lib/libzmq.*` aliases,
`lib/pkgconfig/{libzmq,omq-libzmq}.pc`, and CMake package configs for
`ZeroMQ::ZeroMQ` and `omq_libzmq::omq_zmq`.

## License

ISC
