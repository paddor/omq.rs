local omq = require("omq")

local ctx = omq.context()
local sub = ctx:socket("sub", { linger = 0, recv_timeout = 1000, subscribe = "topic:" })
local pub = ctx:socket("pub", { linger = 0, send_timeout = 1000 })

local endpoint = sub:bind("inproc://lua-pubsub")
pub:connect(endpoint)
os.execute("sleep 0.1")

for _ = 1, 50 do
  pub:send("topic:hello")
end

assert(sub:recv() == "topic:hello")

pub:close()
sub:close()
ctx:term()
