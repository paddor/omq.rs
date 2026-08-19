local omq = require("omq")

local ctx = omq.context()

local types = {
  { "pair", omq.PAIR, 0 },
  { "pub", omq.PUB, 1 },
  { "sub", omq.SUB, 2 },
  { "req", omq.REQ, 3 },
  { "rep", omq.REP, 4 },
  { "dealer", omq.DEALER, 5 },
  { "router", omq.ROUTER, 6 },
  { "pull", omq.PULL, 7 },
  { "push", omq.PUSH, 8 },
  { "xpub", omq.XPUB, 9 },
  { "xsub", omq.XSUB, 10 },
  { "stream", omq.STREAM, 11 },
  { "server", omq.SERVER, 12 },
  { "client", omq.CLIENT, 13 },
  { "radio", omq.RADIO, 14 },
  { "dish", omq.DISH, 15 },
  { "gather", omq.GATHER, 16 },
  { "scatter", omq.SCATTER, 17 },
  { "peer", omq.PEER, 19 },
  { "channel", omq.CHANNEL, 20 },
}

for _, entry in ipairs(types) do
  local name, constant, expected = table.unpack(entry)
  assert(constant == expected, name .. " constant mismatch")
  local socket = ctx:socket(name, { linger = 0 })
  socket:close()
  local numeric_socket = ctx:socket(constant, { linger = 0 })
  numeric_socket:close()
end

local server = ctx:socket("server", { linger = 0, recv_timeout = 1000 })
local client = ctx:socket("client", { linger = 0, recv_timeout = 1000 })
local endpoint = server:bind("inproc://lua-client-server")
client:connect(endpoint)

client:send("request")
local request, routing_id = server:recv_routed()
assert(request == "request")
assert(routing_id > 0)
server:send_routed(routing_id, "reply")
assert(client:recv() == "reply")

client:close()
server:close()
ctx:term()
