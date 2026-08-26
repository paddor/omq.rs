# OMQ.lua

Lua 5.4 binding for OMQ backed by `omq-libzmq`.

The binding is a Lua module loaded by `/usr/bin/lua`. Public Lua APIs live in
`lua/omq.lua`; the native module is a Rust `cdylib` that calls the
`omq-libzmq` C ABI directly.

Architecture notes: [`doc/architecture.md`](doc/architecture.md).

## Performance

![OMQ.lua performance](https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/lua/doc/charts/bindings.svg)

Benchmark and build details live in
[`DEVELOPMENT.md`](DEVELOPMENT.md).

## API Shape

- `Context` owns the native OMQ context and IO threads.
- `Context:socket(...)` creates sockets by name or numeric ZMQ constant.
- Socket calls follow the normal libzmq shape: bind/connect, send/receive,
  multipart, pub/sub controls, RADIO/DISH groups, and close.
- Lua strings are message payloads. Lua tables are multipart messages.
- Nonblocking calls use libzmq-style flags and `try_*` helpers.
- Socket options are supplied at creation or through setter methods for HWM,
  linger, timeouts, subscription, and arena threshold tuning.
- Treat each socket as owned by one Lua coroutine/thread at a time. Create
  more sockets for more concurrent flows.
- `omq.testing.*` helpers are test-only Rust backend peers, not application
  APIs.

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
