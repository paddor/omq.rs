local bench = arg[1]
local impl = arg[2]
local role = arg[3]
local endpoint = arg[4]
local size = tonumber(arg[5])
local amount = tonumber(arg[6])
local warmup = tonumber(arg[7])

local hwm = 1000000
local timeout_ms = 120000
local max_time_check_messages = 1024
local native = require("omq_native")
local clock = native.monotonic_seconds

local function die(message)
  io.stderr:write(message .. "\n")
  os.exit(1)
end

local function payload(n)
  return string.rep("x", n)
end

local function checked(value, err)
  if value == nil or value == false then
    die(tostring(err or "operation failed"))
  end
  return value
end

local function json_escape(value)
  return tostring(value):gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n")
end

local function json_value(value)
  if type(value) == "string" then
    return '"' .. json_escape(value) .. '"'
  end
  return tostring(value)
end

local function print_result(row, keys)
  local parts = {}
  for _, key in ipairs(keys) do
    parts[#parts + 1] = '"' .. key .. '":' .. json_value(row[key])
  end
  print("RESULT {" .. table.concat(parts, ",") .. "}")
end

local function print_throughput(result_impl, messages, started, ended)
  local seconds = ended - started
  local msgs_s = messages / seconds
  print_result({
    impl = result_impl,
    endpoint = endpoint,
    msg_size = size,
    messages = messages,
    seconds = seconds,
    msgs_s = msgs_s,
    gb_s = msgs_s * size / 1000000000.0,
  }, { "impl", "endpoint", "msg_size", "messages", "seconds", "msgs_s", "gb_s" })
end

local function print_latency(result_impl, messages, samples)
  table.sort(samples)
  local p50_index = math.min(messages, math.floor(messages * 50 / 100) + 1)
  local p99_index = math.min(messages, math.floor(messages * 99 / 100) + 1)
  print_result({
    impl = result_impl,
    endpoint = endpoint,
    msg_size = size,
    messages = messages,
    p50_us = samples[p50_index],
    p99_us = samples[p99_index],
  }, { "impl", "endpoint", "msg_size", "messages", "p50_us", "p99_us" })
end

local function time_check_every(payload_size)
  if payload_size <= 0 then
    return max_time_check_messages
  end
  local n = math.floor((1024 * 1024) / payload_size)
  if n < 1 then
    return 1
  end
  if n > max_time_check_messages then
    return max_time_check_messages
  end
  return n
end

local function open_omq_socket(socket_type, sender)
  local omq = require("omq")
  local ctx = omq.context({ io_threads = 1 })
  local options = { linger = 1000 }
  local arena_threshold = os.getenv("OMQ_BENCH_ARENA_THRESHOLD")
  if arena_threshold ~= nil and arena_threshold ~= "" then
    options.arena_threshold = checked(tonumber(arena_threshold), "invalid OMQ_BENCH_ARENA_THRESHOLD")
  end
  if sender then
    options.send_timeout = timeout_ms
    options.send_hwm = hwm
  else
    options.recv_timeout = timeout_ms
    options.recv_hwm = hwm
  end
  return ctx, ctx:socket(socket_type, options)
end

local function open_lzmq_socket(socket_type, sender)
  local zmq = require("lzmq")
  local ctx = checked(zmq.context({ io_threads = 1 }))
  local socket = checked(ctx:socket(socket_type))
  checked(socket:set_linger(1000))
  if sender then
    checked(socket:set_sndtimeo(timeout_ms))
    checked(socket:set_sndhwm(hwm))
  else
    checked(socket:set_rcvtimeo(timeout_ms))
    checked(socket:set_rcvhwm(hwm))
  end
  return ctx, socket
end

local function recv_omq_for(socket, seconds)
  local messages = 0
  local check_every = time_check_every(size)
  local deadline = clock() + seconds
  local recv = socket.recv
  while true do
    local msg = recv(socket)
    if #msg ~= size then
      die("bad message size")
    end
    messages = messages + 1
    if messages % check_every == 0 and clock() >= deadline then
      return messages
    end
  end
end

local function recv_lzmq_for(socket, seconds)
  local messages = 0
  local check_every = time_check_every(size)
  local deadline = clock() + seconds
  local recv = socket.recv
  while true do
    local msg = checked(recv(socket))
    if #msg ~= size then
      die("bad message size")
    end
    messages = messages + 1
    if messages % check_every == 0 and clock() >= deadline then
      return messages
    end
  end
end

local function run_omq_pull(duration, warmup_seconds)
  local ctx, pull = open_omq_socket("pull", false)
  local bound = pull:bind(endpoint)
  print("READY " .. bound)
  io.stdout:flush()

  if warmup_seconds > 0 then
    recv_omq_for(pull, warmup_seconds)
  end
  local started = clock()
  local messages = recv_omq_for(pull, duration)
  local ended = clock()
  print_throughput("omq.lua", messages, started, ended)
  pull:close()
  ctx:term()
end

local function run_omq_push()
  local ctx, push = open_omq_socket("push", true)
  push:connect(endpoint)
  local msg = payload(size)
  local send = push.send
  while true do
    send(push, msg)
  end
end

local function run_lzmq_pull(duration, warmup_seconds)
  local ctx, pull = open_lzmq_socket(require("lzmq").PULL, false)
  checked(pull:bind(endpoint))
  print("READY " .. endpoint)
  io.stdout:flush()

  if warmup_seconds > 0 then
    recv_lzmq_for(pull, warmup_seconds)
  end
  local started = clock()
  local messages = recv_lzmq_for(pull, duration)
  local ended = clock()
  print_throughput("lzmq", messages, started, ended)
  checked(pull:close(1000))
  checked(ctx:term())
end

local function run_lzmq_push()
  local ctx, push = open_lzmq_socket(require("lzmq").PUSH, true)
  checked(push:connect(endpoint))
  local msg = payload(size)
  local send = push.send
  while true do
    checked(send(push, msg))
  end
end

local function run_omq_rep(count)
  local ctx, rep = open_omq_socket("rep", false)
  rep:bind(endpoint)
  print("READY " .. endpoint)
  io.stdout:flush()
  for _ = 1, count do
    local msg = rep:recv(size)
    if #msg ~= size then
      die("bad message size")
    end
    rep:send(msg)
  end
  rep:close()
  ctx:term()
end

local function run_omq_req(messages, warmup_messages)
  local ctx, req = open_omq_socket("req", true)
  req:set_recv_timeout(timeout_ms)
  req:connect(endpoint)
  local msg = payload(size)
  for _ = 1, warmup_messages do
    req:send(msg)
    local reply = req:recv(size)
    if #reply ~= size then
      die("bad reply size")
    end
  end
  local samples = {}
  for i = 1, messages do
    local started = clock()
    req:send(msg)
    local reply = req:recv(size)
    if #reply ~= size then
      die("bad reply size")
    end
    samples[i] = (clock() - started) * 1000000.0
  end
  print_latency("omq.lua", messages, samples)
  req:close()
  ctx:term()
end

local function run_lzmq_rep(count)
  local ctx, rep = open_lzmq_socket(require("lzmq").REP, false)
  checked(rep:bind(endpoint))
  print("READY " .. endpoint)
  io.stdout:flush()
  for _ = 1, count do
    local msg = checked(rep:recv())
    if #msg ~= size then
      die("bad message size")
    end
    checked(rep:send(msg))
  end
  checked(rep:close(1000))
  checked(ctx:term())
end

local function run_lzmq_req(messages, warmup_messages)
  local ctx, req = open_lzmq_socket(require("lzmq").REQ, true)
  checked(req:set_rcvtimeo(timeout_ms))
  checked(req:connect(endpoint))
  local msg = payload(size)
  for _ = 1, warmup_messages do
    checked(req:send(msg))
    local reply = checked(req:recv())
    if #reply ~= size then
      die("bad reply size")
    end
  end
  local samples = {}
  for i = 1, messages do
    local started = clock()
    checked(req:send(msg))
    local reply = checked(req:recv())
    if #reply ~= size then
      die("bad reply size")
    end
    samples[i] = (clock() - started) * 1000000.0
  end
  print_latency("lzmq", messages, samples)
  checked(req:close(1000))
  checked(ctx:term())
end

if bench == "pushpull" and impl == "omq.lua" and role == "pull" then
  run_omq_pull(amount, warmup)
elseif bench == "pushpull" and impl == "omq.lua" and role == "push" then
  run_omq_push()
elseif bench == "pushpull" and impl == "lzmq" and role == "pull" then
  run_lzmq_pull(amount, warmup)
elseif bench == "pushpull" and impl == "lzmq" and role == "push" then
  run_lzmq_push()
elseif bench == "reqrep" and impl == "omq.lua" and role == "rep" then
  run_omq_rep(amount + warmup)
elseif bench == "reqrep" and impl == "omq.lua" and role == "req" then
  run_omq_req(amount, warmup)
elseif bench == "reqrep" and impl == "lzmq" and role == "rep" then
  run_lzmq_rep(amount + warmup)
elseif bench == "reqrep" and impl == "lzmq" and role == "req" then
  run_lzmq_req(amount, warmup)
else
  die(
    "usage: bench_peer.lua <pushpull|reqrep> <omq.lua|lzmq> "
      .. "<pull|push|req|rep> <endpoint> <size> <duration|messages> <warmup>"
  )
end
