local omq = require("omq")

assert(omq.OMQ_MAX_MESSAGE_PARTS == 1020)
assert(omq.OMQ_MAX_PEER_SUBSCRIPTIONS == 1021)

local ctx = omq.context({ io_threads = 1 })
local default = ctx:socket("pull", { linger = 0 })
assert(default:get_max_message_parts() == 65536)
assert(default:get_max_peer_subscriptions() == 1048576)
default:close()

local pull = ctx:socket("pull", { linger = 0, max_message_parts = 16, max_peer_subscriptions = 32 })
assert(pull:get_max_message_parts() == 16)
assert(pull:get_max_peer_subscriptions() == 32)

assert(pull:set_max_message_parts(-1))
assert(pull:get_max_message_parts() == -1)
assert(pull:set_max_peer_subscriptions(-1))
assert(pull:get_max_peer_subscriptions() == -1)

for _, invalid in ipairs({ 0, -2 }) do
  assert(not pcall(function()
    pull:set_max_message_parts(invalid)
  end))
  assert(not pcall(function()
    pull:set_max_peer_subscriptions(invalid)
  end))
  assert(not pcall(function()
    ctx:socket("pull", { max_message_parts = invalid })
  end))
  assert(not pcall(function()
    ctx:socket("pull", { max_peer_subscriptions = invalid })
  end))
end
assert(pull:get_max_message_parts() == -1)
assert(pull:get_max_peer_subscriptions() == -1)

pull:close()
ctx:term()
