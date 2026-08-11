---OMQ.lua public API.
---
---The module wraps the native `omq-libzmq` backend and uses `/usr/bin/lua`.
---All public calls either return their documented value or raise a Lua error
---with the backend errno text.
---@class omq
local native = require("omq_native")

local M = {}

M.PAIR = native.PAIR
M.PUB = native.PUB
M.SUB = native.SUB
M.REQ = native.REQ
M.REP = native.REP
M.DEALER = native.DEALER
M.ROUTER = native.ROUTER
M.PULL = native.PULL
M.PUSH = native.PUSH
M.XPUB = native.XPUB
M.XSUB = native.XSUB
M.DONTWAIT = native.DONTWAIT
M.SNDMORE = native.SNDMORE

---Return a monotonic timestamp in fractional seconds.
---This is used by benchmark scripts; application code should not treat the
---epoch as meaningful.
---@return number seconds monotonic seconds since the native module loaded.
function M.monotonic_seconds()
  return native.monotonic_seconds()
end

local socket_types = {
  pair = M.PAIR,
  pub = M.PUB,
  sub = M.SUB,
  req = M.REQ,
  rep = M.REP,
  dealer = M.DEALER,
  router = M.ROUTER,
  pull = M.PULL,
  push = M.PUSH,
  xpub = M.XPUB,
  xsub = M.XSUB,
}

---@class omq.Context
---@field _native userdata
local Context = {}
Context.__index = Context

---@class omq.Socket
---@field _native userdata
local Socket = {}
Socket.__index = Socket

local function socket_type_id(socket_type)
  if type(socket_type) == "number" then
    return socket_type
  end
  local id = socket_types[string.lower(socket_type)]
  if id == nil then
    error("unknown socket type: " .. tostring(socket_type), 3)
  end
  return id
end

local function apply_socket_options(socket, options)
  if options == nil then
    return
  end
  if options.linger ~= nil then
    socket:set_linger(options.linger)
  end
  if options.send_timeout ~= nil then
    socket:set_send_timeout(options.send_timeout)
  end
  if options.recv_timeout ~= nil then
    socket:set_recv_timeout(options.recv_timeout)
  end
  if options.send_hwm ~= nil then
    socket:set_send_hwm(options.send_hwm)
  end
  if options.recv_hwm ~= nil then
    socket:set_recv_hwm(options.recv_hwm)
  end
  if options.subscribe ~= nil then
    socket:subscribe(options.subscribe)
  end
end

---Create an OMQ context.
---@param options? {io_threads?: integer} context options; defaults to one IO thread.
---@return omq.Context context new context object.
function M.context(options)
  local io_threads = nil
  if options ~= nil then
    io_threads = options.io_threads
  end
  return setmetatable({ _native = native.context(io_threads) }, Context)
end

---Create a socket owned by this context.
---@param socket_type string|integer socket type name (`"push"`, `"pull"`, `"req"`, etc.) or numeric ZMQ constant.
---@param options? table socket options applied before bind/connect.
---@return omq.Socket socket new socket object.
function Context:socket(socket_type, options)
  local socket = setmetatable({ _native = self._native:socket(socket_type_id(socket_type)) }, Socket)
  apply_socket_options(socket, options)
  return socket
end

---Terminate the context. Close sockets first.
---@return boolean ok true when the context is closed.
function Context:term()
  return self._native:term()
end

---Alias for `Context:term()`.
---@return boolean ok true when the context is closed.
function Context:close()
  return self:term()
end

---Bind the socket to an endpoint.
---@param endpoint string endpoint string, e.g. `inproc://name` or `tcp://127.0.0.1:*`.
---@return string endpoint bound endpoint; wildcard TCP ports are resolved when available.
function Socket:bind(endpoint)
  return self._native:bind(endpoint)
end

---Connect the socket to an endpoint.
---@param endpoint string endpoint string.
---@return boolean ok true on success.
function Socket:connect(endpoint)
  return self._native:connect(endpoint)
end

---Send a single-part message, or a multipart message if `data` is a table.
---@param data string|string[] payload string or array of string parts.
---@param flags? integer native send flags; defaults to zero.
---@return boolean ok true on success.
function Socket:send(data, flags)
  if type(data) == "table" then
    return self:send_parts(data, flags)
  end
  return self._native:send(data, flags or 0)
end

---Send a multipart message.
---@param parts string[] array of string payload parts.
---@param flags? integer flags applied to the final frame.
---@return boolean ok true on success.
function Socket:send_parts(parts, flags)
  return self._native:send_parts(parts, flags or 0)
end

---Receive one message part.
---@param capacity? integer receive buffer size; defaults to 64 KiB.
---@return string data received payload.
function Socket:recv(capacity)
  return self._native:recv(capacity, 0)
end

---Try to receive one message part without blocking.
---@param capacity? integer receive buffer size; defaults to 64 KiB.
---@return string|nil data payload, or nil when no message is ready.
function Socket:try_recv(capacity)
  return self._native:recv(capacity, M.DONTWAIT)
end

---Receive a complete multipart message.
---@param capacity? integer per-part receive buffer size; defaults to 64 KiB.
---@return string[] parts message parts.
function Socket:recv_parts(capacity)
  return self._native:recv_parts(capacity, 0)
end

---Subscribe a SUB socket to a prefix.
---@param prefix string prefix bytes; empty string subscribes to all messages.
---@return boolean ok true on success.
function Socket:subscribe(prefix)
  return self._native:subscribe(prefix)
end

---Unsubscribe a SUB socket from a prefix.
---@param prefix string prefix bytes.
---@return boolean ok true on success.
function Socket:unsubscribe(prefix)
  return self._native:unsubscribe(prefix)
end

---Set socket linger in milliseconds.
---@param millis integer linger duration; zero drops pending messages on close.
---@return boolean ok true on success.
function Socket:set_linger(millis)
  return self._native:set_linger(millis)
end

---Set send timeout in milliseconds.
---@param millis integer timeout; zero is nonblocking, negative blocks forever.
---@return boolean ok true on success.
function Socket:set_send_timeout(millis)
  return self._native:set_send_timeout(millis)
end

---Set receive timeout in milliseconds.
---@param millis integer timeout; zero is nonblocking, negative blocks forever.
---@return boolean ok true on success.
function Socket:set_recv_timeout(millis)
  return self._native:set_recv_timeout(millis)
end

---Set send high-water mark in messages.
---@param value integer message count.
---@return boolean ok true on success.
function Socket:set_send_hwm(value)
  return self._native:set_send_hwm(value)
end

---Set receive high-water mark in messages.
---@param value integer message count.
---@return boolean ok true on success.
function Socket:set_recv_hwm(value)
  return self._native:set_recv_hwm(value)
end

---Close the socket.
---@return boolean ok true when the socket is closed.
function Socket:close()
  return self._native:close()
end

---@class omq.testing
M.testing = {}

---Start a Rust backend thread that PULL-binds a random TCP endpoint and receives one payload.
---The returned handle exposes `endpoint()` and `join()`. This is a test helper
---for interop between OMQ.lua and an OMQ.rs thread using `omq-libzmq` directly.
---@return userdata handle join handle whose `endpoint()` returns the TCP endpoint and `join()` returns the received payload.
function M.testing.spawn_tcp_pull()
  return native.spawn_tcp_pull()
end

---Start a Rust backend thread that PULL-binds an inproc endpoint on an existing context.
---The returned handle exposes `join()`, which returns the received payload.
---@param context omq.Context context whose inproc namespace is shared with the thread.
---@param endpoint string inproc endpoint to bind in the Rust thread.
---@return userdata handle join handle whose `join()` returns the received payload.
function M.testing.spawn_inproc_pull(context, endpoint)
  return context._native:spawn_inproc_pull(endpoint)
end

return M
