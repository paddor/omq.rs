local omq = require("omq")

local ctx = omq.context()
local ok, err = pcall(function()
  ctx:socket("not-a-socket")
end)
assert(not ok)
assert(string.find(err, "unknown socket type", 1, true))

local pull = ctx:socket("pull", { linger = 0, recv_timeout = 10 })
pull:bind("inproc://lua-lifecycle-errors")
assert(pull:try_recv() == nil)

pull:close()
assert(pull:close())

ok = pcall(function()
  pull:recv()
end)
assert(not ok)

ctx:term()
assert(ctx:close())
