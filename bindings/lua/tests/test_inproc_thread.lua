local omq = require("omq")

local ctx = omq.context({ io_threads = 1 })
local endpoint = "inproc://lua-inproc-thread"
local handle = omq.testing.spawn_inproc_pull(ctx, endpoint)

local push = ctx:socket("push", { linger = 1000, send_timeout = 1000 })
push:connect(endpoint)
push:send("from-lua-inproc-thread")

assert(handle:join() == "from-lua-inproc-thread")
assert(handle:received() == 1)
local ok, err = pcall(function()
  handle:join()
end)
assert(not ok)
assert(string.find(tostring(err), "join handle already joined", 1, true))

local count_endpoint = "inproc://lua-inproc-thread-count"
local count_handle = omq.testing.spawn_inproc_pull_count(ctx, count_endpoint, 3)
local push_count = ctx:socket("push", { linger = 1000, send_timeout = 1000 })
push_count:connect(count_endpoint)
push_count:send("one")
push_count:send("two")
push_count:send("three")
assert(count_handle:join() == 3)
assert(count_handle:received() == 3)

local stop_endpoint = "inproc://lua-inproc-thread-stop"
local stop_payload = "stop-payload"
local stop_handle = omq.testing.spawn_inproc_pull_until_stop(ctx, stop_endpoint, stop_payload)
local push_stop = ctx:socket("push", { linger = 1000, send_timeout = 1000 })
push_stop:connect(stop_endpoint)
push_stop:send("alpha")
push_stop:send("beta")
push_stop:send(stop_payload)
assert(stop_handle:join() == 2)
assert(stop_handle:received() == 2)

local term_ctx = omq.context({ io_threads = 1 })
local term_endpoint = "inproc://lua-inproc-thread-term"
local term_stop = "stop-term"
local term_handle = omq.testing.spawn_inproc_pull_until_stop(term_ctx, term_endpoint, term_stop)
ok, err = pcall(function()
  term_ctx:term()
end)
assert(not ok)
assert(string.find(tostring(err), "context has 1 live sockets", 1, true))

local term_push = term_ctx:socket("push", { linger = 0, send_timeout = 1000 })
term_push:connect(term_endpoint)
term_push:send(term_stop)
term_push:close()
assert(term_handle:join() == 0)
assert(term_ctx:term())

push_stop:close()
push_count:close()
push:close()
ctx:term()
