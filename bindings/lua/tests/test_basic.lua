local omq = require("omq")

local ctx = omq.context({ io_threads = 1 })
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })

local endpoint = pull:bind("inproc://lua-basic")
assert(endpoint == "inproc://lua-basic")
assert(push:connect(endpoint))
assert(push:send("hello"))
assert(pull:recv() == "hello")
assert(pull:try_recv() == nil)

push:close()
pull:close()
ctx:term()
