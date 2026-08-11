local omq = require("omq")

local ctx = omq.context({ io_threads = 1 })
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 1000, send_timeout = 1000 })

local endpoint = pull:bind("tcp://127.0.0.1:*")
assert(string.find(endpoint, "tcp://", 1, true) == 1)
assert(not string.find(endpoint, "*", 1, true))

push:connect(endpoint)
push:send("tcp-ok")
assert(pull:recv() == "tcp-ok")

push:close()
pull:close()
ctx:term()
