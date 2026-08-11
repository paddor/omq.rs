---OMQ.lua public API.
---
---The module wraps the native `omq-libzmq` backend and uses `/usr/bin/lua`.
---All public calls either return their documented value or raise a Lua error
---with the backend errno text.
---@class omq
local native = require("omq_native")

local M = {}

---Bidirectional exclusive socket type.
---@type integer
M.PAIR = native.PAIR
---Publish socket type.
---@type integer
M.PUB = native.PUB
---Subscribe socket type.
---@type integer
M.SUB = native.SUB
---Synchronous request socket type.
---@type integer
M.REQ = native.REQ
---Synchronous reply socket type.
---@type integer
M.REP = native.REP
---Asynchronous request/reply socket type without strict alternation.
---@type integer
M.DEALER = native.DEALER
---Routed asynchronous request/reply socket type.
---@type integer
M.ROUTER = native.ROUTER
---Pipeline receiver socket type.
---@type integer
M.PULL = native.PULL
---Pipeline sender socket type.
---@type integer
M.PUSH = native.PUSH
---Raw publish socket type that exposes subscription messages.
---@type integer
M.XPUB = native.XPUB
---Raw subscribe socket type.
---@type integer
M.XSUB = native.XSUB
---Nonblocking send/receive flag.
---@type integer
M.DONTWAIT = native.DONTWAIT
---Multipart send flag. Internal frames set this automatically via `send_parts`.
---@type integer
M.SNDMORE = native.SNDMORE
---Native socket option id for outbound frame arena threshold.
---@type integer
M.OMQ_ARENA_THRESHOLD = native.OMQ_ARENA_THRESHOLD
---Default outbound frame arena threshold used by OMQ.lua sockets.
---@type integer
M.DEFAULT_ARENA_THRESHOLD = 4 * 1024

local context_option_keys = {
  io_threads = true,
}

local socket_option_keys = {
  linger = true,
  send_timeout = true,
  recv_timeout = true,
  send_hwm = true,
  recv_hwm = true,
  arena_threshold = true,
  subscribe = true,
}

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
---@field bind fun(self: omq.Socket, endpoint: string): string Bind endpoint and return resolved endpoint.
---@field connect fun(self: omq.Socket, endpoint: string): boolean Connect endpoint.
---@field close fun(self: omq.Socket): boolean Close socket. Idempotent.
---@field send fun(self: omq.Socket, data: string|string[], flags?: integer): boolean Send one frame or multipart table.
---@field send_parts fun(self: omq.Socket, parts: string[], flags?: integer): boolean Send multipart frames.
---@field recv fun(self: omq.Socket, max_size?: integer, flags?: integer): string Receive one frame.
---@field try_recv fun(self: omq.Socket, max_size?: integer): string|nil Nonblocking receive. Returns nil when not ready.
---@field recv_parts fun(self: omq.Socket, max_size?: integer, flags?: integer): string[] Receive all frames in one message.
---@field subscribe fun(self: omq.Socket, prefix: string): boolean Add SUB prefix.
---@field unsubscribe fun(self: omq.Socket, prefix: string): boolean Remove SUB prefix.
---@field set_linger fun(self: omq.Socket, millis: integer): boolean Set close linger in milliseconds.
---@field set_send_timeout fun(self: omq.Socket, millis: integer): boolean Set send timeout in milliseconds.
---@field set_recv_timeout fun(self: omq.Socket, millis: integer): boolean Set receive timeout in milliseconds.
---@field set_send_hwm fun(self: omq.Socket, value: integer): boolean Set outbound high-water mark.
---@field set_recv_hwm fun(self: omq.Socket, value: integer): boolean Set inbound high-water mark.
---@field set_arena_threshold fun(self: omq.Socket, bytes: integer): boolean Set outbound frame arena threshold. `-1` restores native default.
---@field get_arena_threshold fun(self: omq.Socket): integer Return outbound frame arena threshold.

local function socket_type_id(socket_type)
  if type(socket_type) == "number" then
    return socket_type
  end
  if type(socket_type) ~= "string" then
    error("socket type must be a string or numeric constant", 3)
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
  if options.arena_threshold ~= nil then
    socket:set_arena_threshold(options.arena_threshold)
  end
  if options.subscribe ~= nil then
    socket:subscribe(options.subscribe)
  end
end

local function validate_options(options, valid_keys, label)
  if options == nil then
    return
  end
  if type(options) ~= "table" then
    error(label .. " options must be a table", 3)
  end
  for key in pairs(options) do
    if not valid_keys[key] then
      error("unknown " .. label .. " option: " .. tostring(key), 3)
    end
  end
end

---Create an OMQ context.
---@param options? {io_threads?: integer} context options; defaults to one IO thread.
---@return omq.Context context new context object.
function M.context(options)
  validate_options(options, context_option_keys, "context")
  local io_threads = nil
  if options ~= nil then
    io_threads = options.io_threads
  end
  return setmetatable({ _native = native.context(io_threads) }, Context)
end

---Create a socket owned by this context.
---@param socket_type string|integer socket type name (`"push"`, `"pull"`,
---`"req"`, etc.) or numeric ZMQ constant.
---@param options? {linger?: integer, send_timeout?: integer, recv_timeout?: integer,
---send_hwm?: integer, recv_hwm?: integer, arena_threshold?: integer, subscribe?: string}
---socket options applied before bind/connect. `arena_threshold` overrides
---OMQ.lua's native 4 KiB default; `-1` restores the native default.
---@return omq.Socket socket new socket object.
function Context:socket(socket_type, options)
  validate_options(options, socket_option_keys, "socket")
  local socket = self._native:socket(socket_type_id(socket_type))
  local ok, err = pcall(apply_socket_options, socket, options)
  if not ok then
    pcall(function()
      socket:close()
    end)
    error(err, 2)
  end
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

---@class omq.testing
M.testing = {}

---@class omq.testing.Join
---@field endpoint fun(self: omq.testing.Join): string Return bound endpoint for TCP helpers.
---@field join fun(self: omq.testing.Join): string|integer Wait for helper thread and return payload or count.
---@field received fun(self: omq.testing.Join): integer Return payload count received so far.

---Start a Rust backend thread that PULL-binds a random TCP endpoint and receives one payload.
---The returned handle exposes `endpoint()` and `join()`. This is a test helper
---for interop between OMQ.lua and an OMQ.rs thread using `omq-libzmq` directly.
---@return omq.testing.Join handle join handle whose `endpoint()` returns the TCP
---endpoint and `join()` returns the received payload.
function M.testing.spawn_tcp_pull()
  return native.spawn_tcp_pull()
end

---Start a Rust backend thread that PULL-binds an inproc endpoint on an existing context.
---The returned handle exposes `join()`, which returns the received payload.
---@param context omq.Context context whose inproc namespace is shared with the thread.
---@param endpoint string inproc endpoint to bind in the Rust thread.
---@return omq.testing.Join handle join handle whose `join()` returns the received payload.
function M.testing.spawn_inproc_pull(context, endpoint)
  return context._native:spawn_inproc_pull(endpoint)
end

---Start a Rust backend thread that PULL-binds an inproc endpoint and receives `messages` payloads.
---The returned handle exposes `join()`, which returns the received message count.
---@param context omq.Context context whose inproc namespace is shared with the thread.
---@param endpoint string inproc endpoint to bind in the Rust thread.
---@param messages integer number of messages to receive before the thread exits.
---@return omq.testing.Join handle join handle whose `join()` returns the received message count.
function M.testing.spawn_inproc_pull_count(context, endpoint, messages)
  return context._native:spawn_inproc_pull_count(endpoint, messages)
end

---Start a Rust backend thread that PULL-binds an inproc endpoint until `stop_payload` arrives.
---The returned handle exposes `join()`, which returns received message count
---excluding `stop_payload`.
---@param context omq.Context context whose inproc namespace is shared with the thread.
---@param endpoint string inproc endpoint to bind in the Rust thread.
---@param stop_payload string sentinel payload that stops the thread.
---@return omq.testing.Join handle join handle whose `join()` returns the received message count.
function M.testing.spawn_inproc_pull_until_stop(context, endpoint, stop_payload)
  return context._native:spawn_inproc_pull_until_stop(endpoint, stop_payload)
end

return M
