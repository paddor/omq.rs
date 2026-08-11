local omq = require("omq")

local role = arg[1]
local endpoint = arg[2]
local size = tonumber(arg[3])
local duration = tonumber(arg[4])
local warmup = tonumber(arg[5])

local function die(message)
  io.stderr:write(message .. "\n")
  os.exit(1)
end

local function payload(n)
  return string.rep("x", n)
end

if role == "pull" then
  local ctx = omq.context({ io_threads = 1 })
  local pull = ctx:socket("pull", { linger = 1000, recv_timeout = 1000, recv_hwm = 1000000 })
  local bound = pull:bind(endpoint)
  print("READY " .. bound)
  io.stdout:flush()

  local warmup_until = omq.monotonic_seconds() + warmup
  local end_at = warmup_until + duration
  local messages = 0
  while omq.monotonic_seconds() < end_at do
    local msg = pull:recv(size + 16)
    if omq.monotonic_seconds() >= warmup_until then
      messages = messages + 1
      if #msg ~= size then
        die("bad message size")
      end
    end
  end

  print(string.format("RESULT %d %.9f", messages, duration))
  pull:close()
  ctx:term()
elseif role == "push" then
  local ctx = omq.context({ io_threads = 1 })
  local push = ctx:socket("push", { linger = 1000, send_timeout = 1000, send_hwm = 1000000 })
  push:connect(endpoint)
  local msg = payload(size)
  while true do
    push:send(msg)
  end
else
  die("usage: bench_peer.lua <pull|push> <endpoint> <size> <duration> <warmup>")
end
