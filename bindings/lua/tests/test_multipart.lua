local omq = require("omq")

local ctx = omq.context()
local pull = ctx:socket("pull", { linger = 0, recv_timeout = 1000 })
local push = ctx:socket("push", { linger = 0, send_timeout = 1000 })

local endpoint = pull:bind("inproc://lua-multipart")
push:connect(endpoint)
push:send({ "one", "two", "three" })

local parts = pull:recv_parts()
assert(#parts == 3)
assert(parts[1] == "one")
assert(parts[2] == "two")
assert(parts[3] == "three")

push:close()
pull:close()
ctx:term()
