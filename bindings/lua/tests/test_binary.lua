local omq = require("omq")

local ctx = omq.context()
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })

local endpoint = pull:bind("inproc://lua-binary")
push:connect(endpoint)

local payload = "a\0b\0c"
push:send(payload)
assert(pull:recv() == payload)

push:close()
pull:close()
ctx:term()
