local omq = require("omq")

local function have_pyzmq()
  local ok = os.execute("python3 -c 'import zmq' >/dev/null 2>&1")
  return ok == true or ok == 0
end

if not have_pyzmq() then
  io.stderr:write("skip: pyzmq not installed\n")
  os.exit(0)
end

local base = os.tmpname()
local endpoint_file = base .. ".endpoint"
local payload_file = base .. ".payload"
os.remove(endpoint_file)
os.remove(payload_file)

local cmd = string.format(
  "python3 bindings/lua/tests/pyzmq_pull_once.py %q %q &",
  endpoint_file,
  payload_file
)
assert(os.execute(cmd))

local deadline = omq.monotonic_seconds() + 5
local endpoint = nil
while omq.monotonic_seconds() < deadline do
  local f = io.open(endpoint_file, "r")
  if f then
    endpoint = f:read("*a")
    f:close()
    if endpoint and #endpoint > 0 then
      break
    end
  end
  os.execute("sleep 0.05")
end
assert(endpoint and #endpoint > 0, "pyzmq peer did not publish endpoint")

local ctx = omq.context({ io_threads = 1 })
local push = ctx:socket("push", { linger = 1000, send_timeout = 1000 })
push:connect(endpoint)
push:send("hello-pyzmq")
push:close()
ctx:term()

deadline = omq.monotonic_seconds() + 5
while omq.monotonic_seconds() < deadline do
  local f = io.open(payload_file, "rb")
  if f then
    local payload = f:read("*a")
    f:close()
    if payload == "hello-pyzmq" then
      os.remove(endpoint_file)
      os.remove(payload_file)
      os.exit(0)
    end
  end
  os.execute("sleep 0.05")
end

error("pyzmq peer did not receive payload")
