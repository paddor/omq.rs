local omq = require("omq")

local ctx = omq.context()
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })

local endpoint = pull:bind("inproc://lua-recv-capacity")
push:connect(endpoint)

push:send("too-large-for-buffer")
local ok, err = pcall(function()
  pull:recv(4)
end)
assert(not ok)
assert(string.find(tostring(err), "received message exceeded Lua receive buffer", 1, true))

push:close()
pull:close()
ctx:term()
