local omq = require("omq")

local function options()
  return { linger = 0, recv_timeout = 1000, send_timeout = 1000 }
end

local ctx = omq.context()

-- CLIENT/SERVER carries the server routing id as an internal first frame.
do
  local server = ctx:socket("SERVER", options())
  local client = ctx:socket("CLIENT", options())
  local endpoint = server:bind("inproc://lua-client-server-behavior")
  client:connect(endpoint)

  for i = 1, 3 do
    client:send("request-" .. i)
    local request = server:recv_parts()
    assert(#request == 2)
    assert(request[2] == "request-" .. i)
    server:send_parts({ request[1], "reply-" .. i })
    assert(client:recv() == "reply-" .. i)
  end

  client:close()
  server:close()
end

-- SCATTER/GATHER accepts single-frame messages and delivers each one.
do
  local gather = ctx:socket("gather", options())
  local scatter = ctx:socket("scatter", options())
  local endpoint = gather:bind("inproc://lua-scatter-gather-behavior")
  scatter:connect(endpoint)

  for i = 1, 3 do
    scatter:send("scatter-" .. i)
  end
  for i = 1, 3 do
    assert(gather:recv() == "scatter-" .. i)
  end

  gather:close()
  scatter:close()
end

-- CHANNEL is bidirectional and single-frame.
do
  local left = ctx:socket("channel", options())
  local right = ctx:socket("channel", options())
  local endpoint = left:bind("inproc://lua-channel-behavior")
  right:connect(endpoint)

  left:send("left-to-right")
  right:send("right-to-left")
  assert(right:recv() == "left-to-right")
  assert(left:recv() == "right-to-left")

  left:close()
  right:close()
end

-- RADIO/DISH uses explicit group membership and grouped messages.
do
  local radio = ctx:socket("radio", options())
  local dish = ctx:socket("dish", options())
  local endpoint = radio:bind("inproc://lua-radio-dish-behavior")
  dish:join("weather")
  dish:connect(endpoint)

  os.execute("sleep 0.05")
  radio:send_group("weather", "sunny")
  assert(dish:recv() == "sunny")

  dish:leave("weather")
  radio:send_group("weather", "ignored")
  assert(dish:try_recv() == nil)

  dish:close()
  radio:close()
end

ctx:term()
