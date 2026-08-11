local omq = require("omq")

local handle = omq.testing.spawn_tcp_pull()
local ctx = omq.context({ io_threads = 1 })
local push = ctx:socket("push", { linger = 1000, send_timeout = 1000 })

push:connect(handle:endpoint())
push:send("from-lua-thread")
assert(handle:join() == "from-lua-thread")

push:close()
ctx:term()
