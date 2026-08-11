local omq = require("omq")

local ctx = omq.context()
local rep = ctx:socket("rep", { linger = 0, recv_timeout = 1000, send_timeout = 1000 })
local req = ctx:socket("req", { linger = 0, recv_timeout = 1000, send_timeout = 1000 })

local endpoint = rep:bind("inproc://lua-reqrep")
req:connect(endpoint)

req:send("ping")
assert(rep:recv() == "ping")
rep:send("pong")
assert(req:recv() == "pong")

req:close()
rep:close()
ctx:term()
