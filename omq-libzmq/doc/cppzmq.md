# Using cppzmq With omq-libzmq

`omq-libzmq` exposes the libzmq C ABI. C++ users should use upstream
`cppzmq`; no omq-specific C++ wrapper is needed.

Stage a libzmq-compatible prefix:

```sh
omq-libzmq/scripts/stage-compat.sh /tmp/omq-libzmq
```

Use pkg-config:

```sh
export PKG_CONFIG_PATH=/tmp/omq-libzmq/lib/pkgconfig
c++ app.cpp $(pkg-config --cflags --libs libzmq)
```

Use CMake:

```cmake
find_package(ZeroMQ REQUIRED CONFIG)
target_link_libraries(app PRIVATE ZeroMQ::ZeroMQ)
```

For source-tree examples:

```sh
omq-libzmq/examples/cppzmq/run_all.sh
```

What works well:

- `zmq::context_t`, `zmq::socket_t`, `zmq::message_t`
- exceptions via `zmq::error_t`
- `zmq::poller_t`
- `zmq::monitor_t` for implemented v1 monitor events

Known limits:

- draft peer connect/disconnect and monitor v2 pipe stats are stubs
- deprecated iovec send/recv are link-compatible but return `ENOTSUP`
- CZMQ examples (`zsock_t`, `zmsg_t`, `zpoller_t`) require CZMQ, not cppzmq
