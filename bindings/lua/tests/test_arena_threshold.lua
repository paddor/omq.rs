local omq = require("omq")

assert(type(omq.OMQ_ARENA_THRESHOLD) == "number")
assert(omq.DEFAULT_ARENA_THRESHOLD == 4 * 1024)

local ctx = omq.context({ io_threads = 1 })
local default = ctx:socket("push", { linger = 0 })
assert(default:get_arena_threshold() == omq.DEFAULT_ARENA_THRESHOLD)
default:close()

local reset_default = ctx:socket("push", { linger = 0, arena_threshold = -1 })
assert(reset_default:get_arena_threshold() == omq.DEFAULT_ARENA_THRESHOLD)
reset_default:close()

local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000, arena_threshold = 2048 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000, arena_threshold = 0 })

local endpoint = pull:bind("inproc://lua-arena-threshold")
assert(push:connect(endpoint))
assert(push:send("arena"))
assert(pull:recv() == "arena")

local ok = pcall(function()
  push:set_arena_threshold(-2)
end)
assert(not ok)

ok = pcall(function()
  ctx:socket("push", { arena_threshold = -2 })
end)
assert(not ok)

push:close()
pull:close()
ctx:term()
