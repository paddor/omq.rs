local omq = require("omq")

local ctx = omq.context()
local pub = ctx:socket("xpub", { linger = 0, send_timeout = 1000, recv_timeout = 1000 })
local sub = ctx:socket("sub", { linger = 0, recv_timeout = 1000, subscribe = "topic:" })

local endpoint = pub:bind("inproc://lua-pubsub")
sub:connect(endpoint)

local event = pub:recv()
assert(string.byte(event, 1) == 1)
assert(string.sub(event, 2) == "topic:")

for _ = 1, 50 do
  pub:send("topic:hello")
end

assert(sub:recv() == "topic:hello")

pub:close()
sub:close()
ctx:term()
