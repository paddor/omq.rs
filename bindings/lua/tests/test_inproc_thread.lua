local omq = require("omq")

local ctx = omq.context({ io_threads = 1 })
local endpoint = "inproc://lua-inproc-thread"
local handle = omq.testing.spawn_inproc_pull(ctx, endpoint)

local push = ctx:socket("push", { linger = 1000, send_timeout = 1000 })
push:connect(endpoint)
push:send("from-lua-inproc-thread")

assert(handle:join() == "from-lua-inproc-thread")

push:close()
ctx:term()
