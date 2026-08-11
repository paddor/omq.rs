local omq = require("omq")

if os.getenv("OMQ_LUA_SOAK") ~= "1" then
  print("skip Lua soak: set OMQ_LUA_SOAK=1")
  return
end

local recv_timeout_ms = 200
local report_interval = 10

local all_scenarios = { "tcp", "inproc", "pubsub", "context-churn" }

local function env_number(name, default)
  local value = os.getenv(name)
  if value == nil or value == "" then
    return default
  end
  local n = tonumber(value)
  assert(n ~= nil, name .. " must be numeric")
  return n
end

local function split_csv(value)
  local out = {}
  if value == nil or value == "" then
    return out
  end
  for item in string.gmatch(value, "([^,]+)") do
    item = string.gsub(item, "^%s*(.-)%s*$", "%1")
    if item ~= "" then
      out[item] = true
    end
  end
  return out
end

local function assert_known_scenarios(items)
  for name in pairs(items) do
    local known = false
    for _, scenario in ipairs(all_scenarios) do
      if scenario == name then
        known = true
        break
      end
    end
    assert(known, "unknown soak scenario: " .. name)
  end
end

local function selected_scenarios()
  local only = split_csv(os.getenv("OMQ_LUA_SOAK_SCENARIOS"))
  local skip = split_csv(os.getenv("OMQ_LUA_SOAK_SKIP_SCENARIOS"))
  assert_known_scenarios(only)
  assert_known_scenarios(skip)
  local selected = {}
  local any = false
  for _, name in ipairs(all_scenarios) do
    if (next(only) == nil or only[name]) and not skip[name] then
      selected[name] = true
      any = true
    end
  end
  assert(any, "OMQ_LUA_SOAK_SCENARIOS selected no scenarios")
  return selected
end

local function now()
  return omq.monotonic_seconds()
end

local function transient_error(err)
  local s = tostring(err)
  return string.find(s, "Resource temporarily unavailable", 1, true) ~= nil
    or string.find(s, "timed out", 1, true) ~= nil
    or string.find(s, "Timeout", 1, true) ~= nil
end

local function checked(ok, err)
  if not ok and not transient_error(err) then
    error(err, 2)
  end
  return ok
end

local counters = {
  tcp = 0,
  inproc = 0,
  pubsub = 0,
  contexts = 0,
}

local lifecycle = {}

local function life(name)
  local item = lifecycle[name]
  if item == nil then
    item = { sockets_created = 0, sockets_closed = 0, contexts_created = 0, contexts_closed = 0 }
    lifecycle[name] = item
  end
  return item
end

local function new_socket(ctx, scenario, socket_type, options)
  local item = life(scenario)
  item.sockets_created = item.sockets_created + 1
  local ok, socket_or_err = pcall(function()
    return ctx:socket(socket_type, options)
  end)
  if not ok then
    item.sockets_closed = item.sockets_closed + 1
    error(socket_or_err, 2)
  end
  return socket_or_err
end

local function close_socket(socket, scenario)
  if socket == nil then
    return
  end
  local ok, err = pcall(function()
    socket:close()
  end)
  if not ok then
    error(err, 2)
  end
  life(scenario).sockets_closed = life(scenario).sockets_closed + 1
end

local function new_context(scenario, io_threads)
  local item = life(scenario)
  item.contexts_created = item.contexts_created + 1
  local ok, ctx_or_err = pcall(function()
    return omq.context({ io_threads = io_threads or 1 })
  end)
  if not ok then
    item.contexts_closed = item.contexts_closed + 1
    error(ctx_or_err, 2)
  end
  return ctx_or_err
end

local function close_context(ctx, scenario)
  if ctx == nil then
    return
  end
  local ok, err = pcall(function()
    ctx:term()
  end)
  if not ok then
    error(err, 2)
  end
  life(scenario).contexts_closed = life(scenario).contexts_closed + 1
end

local function payload(kind, seq, size)
  local prefix = string.format("%s:%d:", kind, seq)
  if #prefix >= size then
    return string.sub(prefix, 1, size)
  end
  return prefix .. string.rep("x", size - #prefix)
end

local function read_number_file(path)
  local file = io.open(path, "r")
  if file == nil then
    return nil
  end
  local text = file:read("*a")
  file:close()
  return tonumber(text)
end

local cached_pid = nil
local function pid()
  if cached_pid ~= nil then
    return cached_pid
  end
  local file = io.popen("printf '%s' \"$PPID\"")
  if file == nil then
    return nil
  end
  local text = file:read("*a")
  file:close()
  cached_pid = tonumber(text)
  return cached_pid
end

local function fd_count()
  local p = pid()
  if p == nil then
    return nil
  end
  local file = io.popen("find /proc/" .. p .. "/fd -maxdepth 1 -type l 2>/dev/null | wc -l")
  if file == nil then
    return nil
  end
  local text = file:read("*a")
  file:close()
  return tonumber(text)
end

local function rss_kb()
  local file = io.open("/proc/self/status", "r")
  if file == nil then
    return nil
  end
  for line in file:lines() do
    local value = string.match(line, "^VmRSS:%s+(%d+)%s+kB")
    if value ~= nil then
      file:close()
      return tonumber(value)
    end
  end
  file:close()
  return nil
end

local function resources()
  return {
    lua_heap_kb = collectgarbage("count"),
    rss_kb = rss_kb() or 0,
    fd_count = fd_count() or 0,
  }
end

local function report(prefix, elapsed, res)
  io.stdout:write(string.format(
    "[lua-soak] %s%.0fs tcp=%d inproc=%d pubsub=%d contexts=%d heap=%.1fMB rss=%.1fMB fds=%d\n",
    prefix or "",
    elapsed,
    counters.tcp,
    counters.inproc,
    counters.pubsub,
    counters.contexts,
    res.lua_heap_kb / 1024,
    res.rss_kb / 1024,
    res.fd_count
  ))
  for _, name in ipairs(all_scenarios) do
    local item = life(name)
    io.stdout:write(string.format(
      "[lua-soak-life] %s sockets=%d/%d contexts=%d/%d\n",
      name,
      item.sockets_created,
      item.sockets_closed,
      item.contexts_created,
      item.contexts_closed
    ))
  end
  io.stdout:flush()
end

local function assert_live_resources(baseline, current)
  local max_fd_growth = env_number("OMQ_LUA_SOAK_MAX_FD_GROWTH", 128)
  if baseline.fd_count > 0 and current.fd_count > baseline.fd_count + max_fd_growth then
    error(string.format(
      "fd growth too high: baseline=%d current=%d max_growth=%d",
      baseline.fd_count,
      current.fd_count,
      max_fd_growth
    ))
  end
end

local function drain_pull(socket, max_messages)
  local drained = 0
  for _ = 1, max_messages do
    local msg = socket:try_recv(4096)
    if msg == nil then
      break
    end
    drained = drained + 1
  end
  return drained
end

local function make_tcp(shared)
  local pull = new_socket(shared, "tcp", "pull", {
    linger = 0,
    recv_timeout = recv_timeout_ms,
    recv_hwm = 8192,
  })
  local endpoint = pull:bind("tcp://127.0.0.1:0")
  return {
    pull = pull,
    endpoint = endpoint,
    seq = 0,
    worker = 0,
    next_churn = 0,
    churn_interval = env_number("OMQ_LUA_SOAK_TCP_CHURN_INTERVAL", 0.05),
  }
end

local function run_tcp_cycle(shared, state, workers)
  if now() >= state.next_churn then
    for _ = 1, workers do
      local push = new_socket(shared, "tcp", "push", {
        linger = 1000,
        send_timeout = 1000,
        send_hwm = 8192,
      })
      local ok, err = pcall(function()
        push:connect(state.endpoint)
        local body = payload("tcp-" .. state.worker, state.seq, 256)
        for i = 1, 32 do
          push:send(string.char(i % 256) .. string.sub(body, 2))
          state.seq = state.seq + 1
        end
      end)
      close_socket(push, "tcp")
      if not ok then
        error(err, 2)
      end
      state.worker = state.worker + 1
    end
    state.next_churn = now() + state.churn_interval
  end
  counters.tcp = counters.tcp + drain_pull(state.pull, workers * 64)
end

local function make_inproc(shared)
  local endpoint = "inproc://lua-soak-thread-" .. tostring(math.floor(now() * 1000000))
  local stop = "stop:" .. endpoint
  local handle = omq.testing.spawn_inproc_pull_until_stop(shared, endpoint, stop)
  local push = new_socket(shared, "inproc", "push", {
    linger = 0,
    send_timeout = 5000,
    send_hwm = 8192,
  })
  push:connect(endpoint)
  return {
    push = push,
    handle = handle,
    stop = stop,
    seq = 0,
    sent = 0,
    batch = math.max(1, math.floor(env_number("OMQ_LUA_SOAK_INPROC_BATCH", 64))),
  }
end

local function run_inproc_cycle(state)
  for _ = 1, state.batch do
    state.push:send(payload("inproc", state.seq, 128))
    state.seq = state.seq + 1
    state.sent = state.sent + 1
    counters.inproc = counters.inproc + 1
  end
end

local function run_inproc_cycles(state, workers)
  for _ = 1, workers do
    run_inproc_cycle(state)
  end
end

local function close_inproc(state)
  if state == nil then
    return
  end
  local stop_ok, stop_err = pcall(function()
    state.push:send(state.stop)
  end)
  close_socket(state.push, "inproc")
  local join_ok, got = pcall(function()
    return state.handle:join()
  end)
  assert(stop_ok, stop_err)
  assert(join_ok, got)
  assert(got == state.sent, string.format(
    "inproc thread receive count mismatch: sent=%d got=%d",
    state.sent,
    got
  ))
end

local function make_pubsub(shared)
  local pub = new_socket(shared, "pubsub", "pub", {
    linger = 0,
    send_timeout = 0,
    send_hwm = 8192,
  })
  local endpoint = pub:bind("tcp://127.0.0.1:0")
  return {
    shared = shared,
    pub = pub,
    endpoint = endpoint,
    seq = 0,
    subs = {},
    topics = { "fast.", "slow.", "all.", "rare." },
    last_churn = 0,
  }
end

local function close_first_sub(state)
  if #state.subs == 0 then
    return
  end
  close_socket(state.subs[1], "pubsub")
  table.remove(state.subs, 1)
end

local function add_sub(state)
  local topic = state.topics[(#state.subs % #state.topics) + 1]
  local sub = new_socket(state.shared, "pubsub", "sub", {
    linger = 0,
    recv_timeout = recv_timeout_ms,
    recv_hwm = 8192,
    subscribe = topic,
  })
  sub:connect(state.endpoint)
  table.insert(state.subs, sub)
end

local function run_pubsub_cycle(state)
  for _ = 1, 128 do
    local topic = state.topics[(state.seq % #state.topics) + 1]
    checked(pcall(function()
      state.pub:send(topic .. tostring(state.seq), omq.DONTWAIT)
    end))
    state.seq = state.seq + 1
  end
  for _, sub in ipairs(state.subs) do
    counters.pubsub = counters.pubsub + drain_pull(sub, 256)
  end
  if now() - state.last_churn >= 0.5 then
    state.last_churn = now()
    close_first_sub(state)
    if #state.subs < 10 then
      add_sub(state)
    end
  end
end

local function run_context_churn_cycle()
  local scenario = "context-churn"
  local seq = counters.contexts
  local ctx = new_context(scenario, 1)
  local endpoint = "inproc://lua-soak-context-" .. tostring(seq)
  local pull = new_socket(ctx, scenario, "pull", { linger = 0, recv_timeout = 1000 })
  local push = new_socket(ctx, scenario, "push", { linger = 0, send_timeout = 1000 })
  pull:bind(endpoint)
  push:connect(endpoint)
  push:send("x")
  local msg = pull:recv(16)
  assert(msg == "x", "context churn payload mismatch")
  close_socket(push, scenario)
  close_socket(pull, scenario)
  close_context(ctx, scenario)
  counters.contexts = counters.contexts + 1
end

local function assert_progress(selected)
  if selected.tcp then
    assert(counters.tcp > 0, "tcp soak made no progress")
  end
  if selected.inproc then
    assert(counters.inproc > 0, "inproc soak made no progress")
  end
  if selected.pubsub then
    assert(counters.pubsub > 0, "pubsub soak made no progress")
  end
  if selected["context-churn"] then
    assert(counters.contexts > 0, "context-churn soak made no progress")
  end
end

local function assert_lifecycle_closed()
  for _, name in ipairs(all_scenarios) do
    local item = life(name)
    assert(item.sockets_created == item.sockets_closed, string.format(
      "%s socket lifecycle mismatch: created=%d closed=%d",
      name,
      item.sockets_created,
      item.sockets_closed
    ))
    assert(item.contexts_created == item.contexts_closed, string.format(
      "%s context lifecycle mismatch: created=%d closed=%d",
      name,
      item.contexts_created,
      item.contexts_closed
    ))
  end
end

local duration = env_number("OMQ_LUA_SOAK_DURATION_SECS", 60)
local workers = math.max(1, math.floor(env_number("OMQ_LUA_SOAK_WORKERS", 4)))
local selected = selected_scenarios()
local shared = omq.context({ io_threads = workers })
local baseline = resources()
local deadline = now() + duration
local started = now()
local next_report = started + report_interval
local tcp_state = nil
local inproc_state = nil
local pubsub_state = nil

if selected.tcp then
  tcp_state = make_tcp(shared)
end
if selected.inproc then
  inproc_state = make_inproc(shared)
end
if selected.pubsub then
  pubsub_state = make_pubsub(shared)
end

local ok, err = pcall(function()
  while now() < deadline do
    if tcp_state ~= nil then
      run_tcp_cycle(shared, tcp_state, workers)
    end
    if inproc_state ~= nil then
      run_inproc_cycles(inproc_state, workers)
    end
    if pubsub_state ~= nil then
      run_pubsub_cycle(pubsub_state)
    end
    if selected["context-churn"] then
      run_context_churn_cycle()
    end
    local t = now()
    if t >= next_report then
      local current = resources()
      report("", t - started, current)
      assert_live_resources(baseline, current)
      next_report = t + report_interval
    end
  end
end)

if pubsub_state ~= nil then
  for _, sub in ipairs(pubsub_state.subs) do
    close_socket(sub, "pubsub")
  end
  pubsub_state.subs = {}
  close_socket(pubsub_state.pub, "pubsub")
end
close_inproc(inproc_state)
if tcp_state ~= nil then
  close_socket(tcp_state.pull, "tcp")
end
shared:term()

if not ok then
  error(err, 0)
end

collectgarbage("collect")
collectgarbage("collect")
local final = resources()
report("final ", now() - started, final)
assert_live_resources(baseline, final)
assert_progress(selected)
assert_lifecycle_closed()
