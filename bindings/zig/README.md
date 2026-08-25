# OMQ.zig

Zig 0.16 binding for OMQ backed by `omq-libzmq`.

The binding wraps the libzmq-compatible C ABI from `omq-libzmq/include/zmq.h`.
That ABI is the right base for Zig: `@cImport` can consume it directly, Zig
callers keep explicit allocator ownership, and the surface stays close to
pyomq/libzmq names.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/zig/doc/charts/bindings.svg" alt="OMQ.zig binding performance" width="850">
</p>

## Install

Build `omq-libzmq`, then import the package from `bindings/zig`:

```sh
cargo build --release -p omq-libzmq
cd bindings/zig
zig build test
```

## API Shape

- `Context.init()` creates a context with one IO thread.
- `Context.initWithIoThreads(n)` selects IO threads.
- `Context.socket(omq.PUSH)` creates a socket.
- `Socket.bind(allocator, endpoint)` binds and returns the resolved endpoint.
- `Socket.connect(allocator, endpoint)` connects.
- `Socket.send(data, flags)` sends one frame.
- `Socket.sendMultipart(parts, flags)` sends all frames atomically.
- `Socket.sendFrame(allocator, frame, flags)` sends data plus routing id/group
  metadata.
- `Socket.recvAlloc(allocator, flags)` receives one frame. Caller frees it.
- `Socket.recvFrameAlloc(allocator, flags)` receives data plus `more`,
  `routing_id`, and `group` metadata.
- `Socket.recvMultipartAlloc(allocator, flags)` receives all frames into a
  `Message`. Caller calls `Message.deinit()`.
- `Socket.recvMultipartFramesAlloc(allocator, flags)` receives all frames with
  metadata into a `FrameMessage`. Caller calls `FrameMessage.deinit()`.
- `Socket.subscribe(prefix)` and `Socket.unsubscribe(prefix)` manage SUB
  filters.
- `Socket.join(allocator, group)` and `Socket.leave(allocator, group)` manage
  DISH groups.
- `Socket.sendGroup(allocator, group, data, flags)` sends RADIO messages.
- `Socket.setInt`, `Socket.getInt`, `Socket.setBytes`, and typed helpers cover
  pyomq/libzmq options plus OMQ extensions: on-mute policy, compression knobs,
  arena threshold, PLAIN/CURVE security, identity, timeouts, HWM, and routing.
- `omq.poll()` wraps `zmq_poll`.
- `Poller` gives register/modify/unregister and returns active events.
- `Socket.monitor()` returns a PAIR monitor socket wrapper with parsed events.
- `Context.shareKey()` and `Context.fromShareKey()` mirror pyomq shadow-context
  use cases for OMQ shared contexts.
- `omq.proxy()` and `omq.proxySteerable()` wrap native proxy helpers.
- `omq.curveKeypair()` and `omq.curvePublic()` expose CURVE key helpers.

Example:

```zig
const std = @import("std");
const omq = @import("omq");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const endpoint = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(endpoint);
    try push.connect(allocator, endpoint);

    _ = try push.send("hello", 0);
    const msg = try pull.recvAlloc(allocator, 0);
    defer allocator.free(msg);
}
```

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for test, docs, and benchmark commands.
