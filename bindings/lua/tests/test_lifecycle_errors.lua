local omq = require("omq")

local ok, err = pcall(function()
  omq.context({ threads = 1 })
end)
assert(not ok)
assert(string.find(err, "unknown context option: threads", 1, true))

local ctx = omq.context()
ok, err = pcall(function()
  ctx:socket("not-a-socket")
end)
assert(not ok)
assert(string.find(err, "unknown socket type", 1, true))

ok, err = pcall(function()
  ctx:socket({})
end)
assert(not ok)
assert(string.find(err, "socket type must be a string or numeric constant", 1, true))

ok, err = pcall(function()
  ctx:socket("pull", { recv_timo = 10 })
end)
assert(not ok)
assert(string.find(err, "unknown socket option: recv_timo", 1, true))

local pull = ctx:socket("pull", { linger = 0, recv_timeout = 10 })
local endpoint = pull:bind("inproc://lua-lifecycle-errors")
assert(pull:try_recv() == nil)

ok, err = pcall(function()
  ctx:term()
end)
assert(not ok)
assert(string.find(tostring(err), "context has 1 live sockets", 1, true))

local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })
ok, err = pcall(function()
  ctx:term()
end)
assert(not ok)
assert(string.find(tostring(err), "context has 2 live sockets", 1, true))

push:connect(endpoint)
push:send("still-open")
assert(pull:recv() == "still-open")

push:close()
ok, err = pcall(function()
  ctx:term()
end)
assert(not ok)
assert(string.find(tostring(err), "context has 1 live sockets", 1, true))

pull:close()
assert(pull:close())

ok = pcall(function()
  pull:recv()
end)
assert(not ok)

ctx:term()
assert(ctx:close())

local function orphan_socket()
  local orphan_ctx = omq.context()
  return orphan_ctx:socket("pull", { linger = 0 })
end

local orphan = orphan_socket()
collectgarbage("collect")
orphan:close()
orphan = nil
collectgarbage("collect")
