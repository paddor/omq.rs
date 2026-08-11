local omq = require("omq")

local ctx = omq.context()
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })

local endpoint = pull:bind("inproc://lua-recv-capacity")
push:connect(endpoint)

local large = string.rep("x", 128 * 1024)
push:send(large)
assert(pull:recv() == large)

push:send("too-large-for-buffer")
local ok, err = pcall(function()
  pull:recv(4)
end)
assert(not ok)
assert(string.find(tostring(err), "received message exceeded Lua receive limit", 1, true))

push:send("after-limit-error")
assert(pull:recv(64) == "after-limit-error")

push:close()
pull:close()
ctx:term()
