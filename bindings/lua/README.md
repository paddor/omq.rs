# OMQ.lua

Lua 5.4 binding for OMQ backed by `omq-libzmq`.

The binding is a Lua module loaded by `/usr/bin/lua`. Public Lua APIs live in
`lua/omq.lua`; the native module is a Rust `cdylib` that calls the
`omq-libzmq` C ABI directly.

Architecture notes: [`doc/architecture.md`](doc/architecture.md).

## Build And Test

Requires `/usr/bin/lua`, Python 3 for charts, and a Rust toolchain.

```sh
./scripts/test-lua.sh
```

Manual equivalent:

```sh
cargo build --manifest-path bindings/lua/native/Cargo.toml
export LUA_PATH="$PWD/bindings/lua/lua/?.lua;;"
export LUA_CPATH="$PWD/bindings/lua/native/target/debug/lib?.so;;"
/usr/bin/lua bindings/lua/tests/test_basic.lua
```

## API Shape

- Socket constants: `PAIR`, `PUB`, `SUB`, `REQ`, `REP`, `DEALER`, `ROUTER`,
  `PULL`, `PUSH`, `XPUB`, `XSUB`.
- Flags: `DONTWAIT`, `SNDMORE`.
- Arena constants: `OMQ_ARENA_THRESHOLD`, `DEFAULT_ARENA_THRESHOLD`.
- `omq.monotonic_seconds()` returns a monotonic timestamp for tests and
  benchmarks.
- `omq.context({ io_threads = 1 })` creates a context.
- `Context:socket("push", opts)` creates a socket by name or numeric constant.
- `Context:term()` closes the context. It errors while sockets or helper peers
  are still live.
- `Context:close()` aliases `Context:term()`.
- `Socket:bind(endpoint)` binds and returns the resolved endpoint, including
  wildcard TCP ports when available.
- `Socket:connect(endpoint)` connects an endpoint.
- `Socket:close()` closes the socket. It is idempotent.
- `Socket:send("bytes", flags)` sends one frame.
- `Socket:send({ "part1", "part2" }, flags)` sends multipart.
- `Socket:send_parts(parts, flags)` sends multipart.
- `Socket:recv(max_size, flags)` receives one frame. If `max_size` is set and
  the frame is larger, the call errors after consuming that frame.
- `Socket:try_recv(max_size)` is nonblocking and returns `nil` when no frame is
  ready.
- `Socket:recv_parts(max_size, flags)` receives all frames in one message.
- `Socket:subscribe(prefix)` adds a SUB prefix.
- `Socket:unsubscribe(prefix)` removes a SUB prefix.
- `Socket:set_linger(ms)`, `Socket:set_send_timeout(ms)`,
  `Socket:set_recv_timeout(ms)`, `Socket:set_send_hwm(value)`,
  `Socket:set_recv_hwm(value)`, `Socket:set_arena_threshold(bytes)`, and
  `Socket:get_arena_threshold()` expose supported socket options.
- Socket option tables support `linger`, `send_timeout`, `recv_timeout`,
  `send_hwm`, `recv_hwm`, `arena_threshold`, and `subscribe`.
- `arena_threshold` uses `4 KiB` by default. `0` means gather-write for all
  payloads. `-1` restores the native default.
- `omq.testing.*` helpers are test-only Rust backend peers. Their join handles
  expose `endpoint()`, `join()`, and `received()`.

Example:

```lua
local omq = require("omq")

local ctx = omq.context({ io_threads = 1 })
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })

local endpoint = pull:bind("tcp://127.0.0.1:*")
push:connect(endpoint)
push:send("hello")
print(pull:recv())

push:close()
pull:close()
ctx:term()
```

## Performance Chart

![OMQ.lua performance](doc/charts/bindings.svg)

Generate a quick local chart:

```sh
bindings/lua/scripts/update_perf.py --quick
```

The script uses the same append-only cache and SVG chart shape as the Go and
Python bindings. It benchmarks `omq.lua` and, when `require("lzmq")` works for
the selected Lua binary, `lzmq`. User-local rocks are visible when `luarocks`
is available.

Limit a run to one implementation:

```sh
bindings/lua/scripts/update_perf.py --quick --impls omq.lua --latency-impls omq.lua
```

Benchmark rows append to:

```text
~/.cache/omq.lua/bindings.jsonl
```
