local omq = require("omq")

if os.getenv("OMQ_LUA_SOAK") ~= "1" then
  print("skip Lua soak: set OMQ_LUA_SOAK=1")
  return
end

local recv_timeout_ms = 200

local all_scenarios = { "tcp", "inproc", "pubsub", "protocol-mix", "context-churn" }

local function env_number(name, default)
  local value = os.getenv(name)
  if value == nil or value == "" then
    return default
  end
  local n = tonumber(value)
  assert(n ~= nil, name .. " must be numeric")
  return n
end

local report_interval = math.max(1, env_number("OMQ_LUA_SOAK_REPORT_INTERVAL_SECS", 10))

local resource_limits = {
  warmup_secs = env_number("OMQ_LUA_SOAK_RESOURCE_WARMUP_SECS", 600),
  window_secs = env_number("OMQ_LUA_SOAK_RESOURCE_WINDOW_SECS", 300),
  min_samples = math.max(2, math.floor(env_number("OMQ_LUA_SOAK_RESOURCE_MIN_SAMPLES", 12))),
  max_fd_growth = env_number("OMQ_LUA_SOAK_MAX_FD_GROWTH", 128),
  max_final_fd_growth = env_number("OMQ_LUA_SOAK_MAX_FINAL_FD_GROWTH", 16),
  max_thread_growth = env_number("OMQ_LUA_SOAK_MAX_THREAD_GROWTH", 128),
  max_final_thread_growth = env_number("OMQ_LUA_SOAK_MAX_FINAL_THREAD_GROWTH", 16),
  heap_slope_limit_kib_s = env_number("OMQ_LUA_SOAK_HEAP_SLOPE_LIMIT_KIB_S", 512),
  rss_slope_limit_kib_s = env_number("OMQ_LUA_SOAK_RSS_SLOPE_LIMIT_KIB_S", 1024),
  fd_slope_limit_per_s = env_number("OMQ_LUA_SOAK_FD_SLOPE_LIMIT_PER_SEC", 0.05),
  heap_slope_min_growth_kib = env_number("OMQ_LUA_SOAK_HEAP_SLOPE_MIN_GROWTH_MB", 16) * 1024,
  rss_slope_min_growth_kib = env_number("OMQ_LUA_SOAK_RSS_SLOPE_MIN_GROWTH_MB", 128) * 1024,
  fd_slope_min_growth = env_number("OMQ_LUA_SOAK_FD_SLOPE_MIN_GROWTH", 32),
  heap_residual_floor_kib = env_number("OMQ_LUA_SOAK_HEAP_RESIDUAL_FLOOR_MB", 8) * 1024,
  rss_tail_growth_percent = env_number("OMQ_LUA_SOAK_RSS_TAIL_GROWTH_PERCENT", 25),
  rss_tail_growth_min_kib = env_number("OMQ_LUA_SOAK_RSS_TAIL_GROWTH_MIN_MB", 128) * 1024,
}

local progress_limits = {
  grace_secs = env_number("OMQ_LUA_SOAK_PROGRESS_GRACE_SECS", 30),
  stall_secs = env_number("OMQ_LUA_SOAK_PROGRESS_STALL_SECS", 60),
}

local exchange_timeout_secs = env_number("OMQ_LUA_SOAK_EXCHANGE_TIMEOUT_SECS", 120)
local protocol_mix_timeout_secs =
  math.max(1, env_number("OMQ_LUA_SOAK_PROTOCOL_MIX_TIMEOUT_SECS", math.min(exchange_timeout_secs, 10)))
local log_protocol_mix_resets = os.getenv("OMQ_LUA_SOAK_LOG_PROTOCOL_MIX_RESETS") == "1"
local protocol_mix_payload_bytes =
  math.max(1, math.floor(env_number("OMQ_LUA_SOAK_PROTOCOL_MIX_PAYLOAD_BYTES", 2048)))

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

local function try_send_transient(socket, message)
  local ok, err = pcall(function()
    socket:send(message)
  end)
  if ok then
    return true
  end
  if transient_error(err) then
    return false
  end
  error(err, 2)
end

local function send_until_deadline(socket, message, seconds)
  local deadline = now() + seconds
  local last_err = nil
  local multipart = type(message) == "table"
  while now() < deadline do
    local ok, err = pcall(function()
      socket:send(message)
    end)
    if ok then
      return true
    end
    if not transient_error(err) then
      error(err, 2)
    end
    if multipart then
      error(err, 2)
    end
    last_err = err
  end
  error(last_err or "send deadline expired", 2)
end

local function recv_until_deadline(socket, seconds, ...)
  local args = { ... }
  local deadline = now() + seconds
  local last_err = nil
  while now() < deadline do
    local ok, value = pcall(function()
      return socket:try_recv(table.unpack(args))
    end)
    if ok and value ~= nil then
      return value
    end
    if ok then
      last_err = nil
    elseif not transient_error(value) then
      error(value, 2)
    else
      last_err = value
    end
  end
  error(last_err or "recv deadline expired", 2)
end

local function recv_parts_until_deadline(socket, seconds)
  local deadline = now() + seconds
  local last_err = nil
  while now() < deadline do
    local ok, value = pcall(function()
      return socket:recv_parts(nil, omq.DONTWAIT)
    end)
    if ok and value ~= nil and #value > 0 then
      return value
    end
    if ok then
      last_err = nil
    elseif not transient_error(value) then
      error(value, 2)
    else
      last_err = value
    end
  end
  error(last_err or "recv_parts deadline expired", 2)
end

local counters = {
  tcp = 0,
  inproc = 0,
  pubsub = 0,
  protocol = 0,
  contexts = 0,
}

local function counter_value(name)
  if name == "tcp" then
    return counters.tcp
  end
  if name == "inproc" then
    return counters.inproc
  end
  if name == "pubsub" then
    return counters.pubsub
  end
  if name == "protocol-mix" then
    return counters.protocol
  end
  if name == "context-churn" then
    return counters.contexts
  end
  error("unknown soak scenario: " .. tostring(name), 2)
end

local function scenario_list(selected)
  local out = {}
  for _, name in ipairs(all_scenarios) do
    if selected[name] then
      table.insert(out, name)
    end
  end
  return table.concat(out, ",")
end

local function progress_tracker(selected)
  local tracker = {}
  for _, name in ipairs(all_scenarios) do
    if selected[name] then
      tracker[name] = {
        last = counter_value(name),
        last_change = 0,
      }
    end
  end
  return tracker
end

local function assert_progress_active(selected, tracker, elapsed)
  if elapsed < progress_limits.grace_secs then
    return
  end
  for _, name in ipairs(all_scenarios) do
    if selected[name] then
      local state = tracker[name]
      local value = counter_value(name)
      if value < state.last then
        error(string.format(
          "%s soak counter regressed: previous=%d current=%d",
          name,
          state.last,
          value
        ))
      end
      if value > state.last then
        state.last = value
        state.last_change = elapsed
      elseif elapsed - state.last_change >= progress_limits.stall_secs then
        error(string.format(
          "%s soak stalled: counter=%d unchanged for %.0fs",
          name,
          value,
          elapsed - state.last_change
        ))
      end
    end
  end
end

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
  local ok, socket_or_err = pcall(function()
    return ctx:socket(socket_type, options)
  end)
  if not ok then
    error(socket_or_err, 2)
  end
  item.sockets_created = item.sockets_created + 1
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
  local ok, ctx_or_err = pcall(function()
    return omq.context({ io_threads = io_threads or 1 })
  end)
  if not ok then
    error(ctx_or_err, 2)
  end
  item.contexts_created = item.contexts_created + 1
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

local function starts_with(value, prefix)
  return string.sub(value, 1, #prefix) == prefix
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

local function proc_status()
  local file = io.open("/proc/self/status", "r")
  if file == nil then
    return { rss_kb = nil, vm_data_kb = nil, threads = nil }
  end
  local out = { rss_kb = nil, vm_data_kb = nil, threads = nil }
  for line in file:lines() do
    out.rss_kb = out.rss_kb or tonumber(string.match(line, "^VmRSS:%s+(%d+)%s+kB"))
    out.vm_data_kb = out.vm_data_kb or tonumber(string.match(line, "^VmData:%s+(%d+)%s+kB"))
    out.threads = out.threads or tonumber(string.match(line, "^Threads:%s+(%d+)"))
  end
  file:close()
  return out
end

local function resources()
  local status = proc_status()
  return {
    lua_heap_kb = collectgarbage("count"),
    rss_kb = status.rss_kb or 0,
    vm_data_kb = status.vm_data_kb or 0,
    threads = status.threads or 0,
    fd_count = fd_count() or 0,
  }
end

local resource_samples = {
  heap = {},
  rss = {},
  fds = {},
}

local function append_sample(samples, elapsed, value)
  table.insert(samples, { elapsed = elapsed, value = value })
end

local function sample_resources(elapsed, res)
  append_sample(resource_samples.heap, elapsed, res.lua_heap_kb)
  append_sample(resource_samples.rss, elapsed, res.rss_kb)
  append_sample(resource_samples.fds, elapsed, res.fd_count)
end

local function saturating_sub(a, b)
  if a > b then
    return a - b
  end
  return 0
end

local function max_sample_value(samples, fallback)
  local max_value = fallback
  for _, sample in ipairs(samples) do
    if sample.value > max_value then
      max_value = sample.value
    end
  end
  return max_value
end

local function slope_per_second(samples)
  local n = #samples
  if n < 2 then
    return nil
  end
  local sum_x = 0
  local sum_y = 0
  local sum_x2 = 0
  local sum_xy = 0
  local origin = samples[1].elapsed
  for _, sample in ipairs(samples) do
    local x = sample.elapsed - origin
    local y = sample.value
    sum_x = sum_x + x
    sum_y = sum_y + y
    sum_x2 = sum_x2 + x * x
    sum_xy = sum_xy + x * y
  end
  local denom = n * sum_x2 - sum_x * sum_x
  if denom == 0 then
    return nil
  end
  return (n * sum_xy - sum_x * sum_y) / denom
end

local function live_growth_window(samples)
  if #samples < resource_limits.min_samples then
    return nil
  end
  local current_elapsed = samples[#samples].elapsed
  if current_elapsed < resource_limits.warmup_secs + resource_limits.window_secs then
    return nil
  end
  local window_start = current_elapsed - resource_limits.window_secs
  local out = {}
  for _, sample in ipairs(samples) do
    if sample.elapsed >= window_start then
      table.insert(out, sample)
    end
  end
  if #out < resource_limits.min_samples then
    return nil
  end
  return out
end

local function live_growth_error(name, samples, limit_per_sec, min_growth)
  local window = live_growth_window(samples)
  if window == nil then
    return nil
  end
  local growth = saturating_sub(window[#window].value, window[1].value)
  if growth < min_growth then
    return nil
  end
  local slope = slope_per_second(window)
  if slope ~= nil and slope > limit_per_sec then
    return string.format(
      "live %s growth detected: slope=%.1f/s growth=%.1fMB limit=%.1f/s",
      name,
      slope,
      growth / 1024,
      limit_per_sec
    )
  end
  return nil
end

local function live_fd_growth_error(samples)
  local window = live_growth_window(samples)
  if window == nil then
    return nil
  end
  local growth = saturating_sub(window[#window].value, window[1].value)
  if growth < resource_limits.fd_slope_min_growth then
    return nil
  end
  local slope = slope_per_second(window)
  if slope ~= nil and slope > resource_limits.fd_slope_limit_per_s then
    return string.format(
      "live fd growth detected: slope=%.4f/s growth=%d limit=%.4f/s",
      slope,
      growth,
      resource_limits.fd_slope_limit_per_s
    )
  end
  return nil
end

local function percent_growth(growth, baseline)
  if baseline <= 0 then
    return 0
  end
  return growth / baseline * 100
end

local function tail_growth_window(samples)
  local out = {}
  for _, sample in ipairs(samples) do
    if sample.elapsed >= resource_limits.warmup_secs then
      table.insert(out, sample)
    end
  end
  if #out < resource_limits.min_samples then
    return nil, nil
  end
  return out[1].value, max_sample_value(out, out[1].value)
end

local function report(prefix, elapsed, res)
  io.stdout:write(string.format(
    "[lua-soak] %s%.0fs tcp=%d inproc=%d pubsub=%d protocol=%d contexts=%d heap=%.1fMB rss=%.1fMB vmdata=%.1fMB fds=%d threads=%d\n",
    prefix or "",
    elapsed,
    counters.tcp,
    counters.inproc,
    counters.pubsub,
    counters.protocol,
    counters.contexts,
    res.lua_heap_kb / 1024,
    res.rss_kb / 1024,
    res.vm_data_kb / 1024,
    res.fd_count,
    res.threads
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
  if baseline.fd_count > 0 and current.fd_count > baseline.fd_count + resource_limits.max_fd_growth then
    error(string.format(
      "fd growth too high: baseline=%d current=%d max_growth=%d",
      baseline.fd_count,
      current.fd_count,
      resource_limits.max_fd_growth
    ))
  end
  if baseline.threads > 0 and current.threads > baseline.threads + resource_limits.max_thread_growth then
    error(string.format(
      "thread growth too high: baseline=%d current=%d max_growth=%d",
      baseline.threads,
      current.threads,
      resource_limits.max_thread_growth
    ))
  end
  local err = live_growth_error(
    "heap",
    resource_samples.heap,
    resource_limits.heap_slope_limit_kib_s,
    resource_limits.heap_slope_min_growth_kib
  )
  if err ~= nil then
    error(err)
  end
  err = live_growth_error(
    "RSS",
    resource_samples.rss,
    resource_limits.rss_slope_limit_kib_s,
    resource_limits.rss_slope_min_growth_kib
  )
  if err ~= nil then
    error(err)
  end
  err = live_fd_growth_error(resource_samples.fds)
  if err ~= nil then
    error(err)
  end
end

local function assert_final_resources(baseline, final)
  if baseline.fd_count > 0 and final.fd_count > baseline.fd_count + resource_limits.max_final_fd_growth then
    error(string.format(
      "final fd growth too high: baseline=%d final=%d max_growth=%d",
      baseline.fd_count,
      final.fd_count,
      resource_limits.max_final_fd_growth
    ))
  end
  if baseline.threads > 0 and final.threads > baseline.threads + resource_limits.max_final_thread_growth then
    error(string.format(
      "final thread growth too high: baseline=%d final=%d max_growth=%d",
      baseline.threads,
      final.threads,
      resource_limits.max_final_thread_growth
    ))
  end

  local heap_peak = max_sample_value(resource_samples.heap, baseline.lua_heap_kb)
  local heap_threshold = math.max(heap_peak / 20, resource_limits.heap_residual_floor_kib)
  local heap_growth = saturating_sub(final.lua_heap_kb, baseline.lua_heap_kb)
  if heap_growth > heap_threshold then
    error(string.format(
      "heap residual too high: baseline=%.1fMB final=%.1fMB growth=%.1fMB limit=%.1fMB",
      baseline.lua_heap_kb / 1024,
      final.lua_heap_kb / 1024,
      heap_growth / 1024,
      heap_threshold / 1024
    ))
  end

  local rss_baseline, rss_tail_max = tail_growth_window(resource_samples.rss)
  if rss_baseline ~= nil then
    local tail_growth = saturating_sub(rss_tail_max, rss_baseline)
    local final_growth = saturating_sub(final.rss_kb, rss_baseline)
    if tail_growth >= resource_limits.rss_tail_growth_min_kib
      and final_growth >= resource_limits.rss_tail_growth_min_kib
      and percent_growth(tail_growth, rss_baseline) > resource_limits.rss_tail_growth_percent
      and percent_growth(final_growth, rss_baseline) > resource_limits.rss_tail_growth_percent then
      error(string.format(
        "RSS residual too high: tail_growth=%.1fMB final_growth=%.1fMB limit=%.1f%%",
        tail_growth / 1024,
        final_growth / 1024,
        resource_limits.rss_tail_growth_percent
      ))
    end
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

local function drain_pubsub(sub, max_messages)
  local drained = 0
  for _ = 1, max_messages do
    local msg = sub.socket:try_recv(4096)
    if msg == nil then
      break
    end
    assert(starts_with(msg, sub.topic), string.format(
      "pubsub topic mismatch: topic=%s msg=%s",
      sub.topic,
      msg
    ))
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
        linger = 0,
        send_timeout = 100,
        send_hwm = 8192,
      })
      local ok, err = pcall(function()
        push:connect(state.endpoint)
        local body = payload("tcp-" .. state.worker, state.seq, 256)
        for i = 1, 32 do
          if not try_send_transient(push, string.char(i % 256) .. string.sub(body, 2)) then
            break
          end
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
    linger = 5000,
    send_timeout = 100,
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
    if not try_send_transient(state.push, payload("inproc", state.seq, 128)) then
      break
    end
    state.seq = state.seq + 1
    state.sent = state.sent + 1
  end
  counters.inproc = state.handle:received()
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
    send_until_deadline(state.push, state.stop, 30)
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
  counters.inproc = got
end

local function make_pubsub(shared)
  local pub = new_socket(shared, "pubsub", "xpub", {
    linger = 0,
    recv_timeout = recv_timeout_ms,
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
    max_subs = 4,
    next_topic = 1,
    last_churn = 0,
  }
end

local function close_first_sub(state)
  if #state.subs == 0 then
    return
  end
  close_socket(state.subs[1].socket, "pubsub")
  table.remove(state.subs, 1)
end

local function wait_subscribed(state, topic)
  local deadline = now() + env_number("OMQ_LUA_SOAK_PUBSUB_READY_TIMEOUT_SECS", 5)
  while now() < deadline do
    local ok, event = pcall(function()
      return state.pub:recv(1024)
    end)
    if ok then
      local action = string.byte(event, 1)
      local prefix = string.sub(event, 2)
      if action == 1 and prefix == topic then
        return
      end
    elseif not transient_error(event) then
      error(event, 2)
    end
  end
  error("pubsub subscription readiness timed out: " .. topic, 2)
end

local function add_sub(state)
  local topic = "topic." .. tostring(state.next_topic) .. "."
  state.next_topic = state.next_topic + 1
  local sub = new_socket(state.shared, "pubsub", "sub", {
    linger = 0,
    recv_timeout = recv_timeout_ms,
    recv_hwm = 8192,
    subscribe = topic,
  })
  sub:connect(state.endpoint)
  local ok, err = pcall(function()
    wait_subscribed(state, topic)
  end)
  if not ok then
    close_socket(sub, "pubsub")
    error(err, 2)
  end
  table.insert(state.subs, { socket = sub, topic = topic })
end

local function run_pubsub_cycle(state)
  for _ = 1, 128 do
    local topic = "idle."
    if #state.subs > 0 then
      topic = state.subs[(state.seq % #state.subs) + 1].topic
    end
    checked(pcall(function()
      state.pub:send(topic .. tostring(state.seq), omq.DONTWAIT)
    end))
    state.seq = state.seq + 1
  end
  for _, sub in ipairs(state.subs) do
    counters.pubsub = counters.pubsub + drain_pubsub(sub, 256)
  end
  if now() - state.last_churn >= 0.5 then
    state.last_churn = now()
    if #state.subs >= state.max_subs then
      close_first_sub(state)
    end
    if #state.subs < state.max_subs then
      add_sub(state)
    end
  end
end

local function run_context_churn_cycle()
  local scenario = "context-churn"
  local seq = counters.contexts
  local ctx = nil
  local pull = nil
  local push = nil

  local function close_push()
    local socket = push
    push = nil
    close_socket(socket, scenario)
  end

  local function close_pull()
    local socket = pull
    pull = nil
    close_socket(socket, scenario)
  end

  local function close_ctx()
    local context = ctx
    ctx = nil
    close_context(context, scenario)
  end

  local ok, err = pcall(function()
    ctx = new_context(scenario, 1)
    local endpoint = "inproc://lua-soak-context-" .. tostring(seq)
    pull = new_socket(ctx, scenario, "pull", { linger = 0, recv_timeout = 1000 })
    push = new_socket(ctx, scenario, "push", { linger = 0, send_timeout = 1000 })
    pull:bind(endpoint)
    push:connect(endpoint)
    send_until_deadline(push, "x", exchange_timeout_secs)
    local msg = recv_until_deadline(pull, exchange_timeout_secs, 5000)
    assert(msg == "x", "context churn payload mismatch")
    close_push()
    close_pull()
    close_ctx()
  end)

  if not ok then
    local errors = {}
    local function cleanup_local(label, fn)
      local cleanup_ok, cleanup_err = pcall(fn)
      if not cleanup_ok then
        table.insert(errors, label .. ": " .. tostring(cleanup_err))
      end
    end
    cleanup_local("context-churn push", close_push)
    cleanup_local("context-churn pull", close_pull)
    cleanup_local("context-churn context", close_ctx)
    if #errors > 0 then
      error(tostring(err) .. "\ncleanup errors:\n" .. table.concat(errors, "\n"), 2)
    end
    error(err, 2)
  end

  counters.contexts = counters.contexts + 1
end

local protocol_mix_instance = 0

local function make_protocol_mix(shared)
  local scenario = "protocol-mix"
  protocol_mix_instance = protocol_mix_instance + 1
  local send_timeout_ms = math.max(1000, math.floor(protocol_mix_timeout_secs * 1000))
  local pull = new_socket(shared, scenario, "pull", { linger = 0, recv_timeout = send_timeout_ms })
  local push = new_socket(shared, scenario, "push", { linger = 0, send_timeout = send_timeout_ms })
  local ipc = pull:bind("ipc://@omq-lua-soak-" .. tostring(pid()) .. "-" .. tostring(protocol_mix_instance))
  push:connect(ipc)

  local rep = new_socket(shared, scenario, "rep", { linger = 0, recv_timeout = send_timeout_ms })
  local req = new_socket(shared, scenario, "req", {
    linger = 0,
    recv_timeout = send_timeout_ms,
    send_timeout = send_timeout_ms,
  })
  req:connect(rep:bind("tcp://127.0.0.1:0"))

  local left = new_socket(shared, scenario, "pair", {
    linger = 0,
    recv_timeout = send_timeout_ms,
    send_timeout = send_timeout_ms,
  })
  local right = new_socket(shared, scenario, "pair", {
    linger = 0,
    recv_timeout = send_timeout_ms,
    send_timeout = send_timeout_ms,
  })
  right:connect(left:bind("tcp://127.0.0.1:0"))
  return {
    sockets = { pull, push, rep, req, left, right },
    pull = pull,
    push = push,
    rep = rep,
    req = req,
    left = left,
    right = right,
    seq = 0,
    large = string.rep("z", protocol_mix_payload_bytes),
  }
end

local function run_protocol_mix_cycle(state, timeout_secs)
  local sequence = tostring(state.seq)
  local body = state.seq % 64 == 0 and state.large or sequence
  send_until_deadline(state.push, { sequence, body }, timeout_secs)
  local parts = recv_parts_until_deadline(state.pull, timeout_secs)
  assert(#parts == 2 and parts[1] == sequence and #parts[2] == #body, "IPC multipart mismatch")

  send_until_deadline(state.req, "request-" .. sequence, timeout_secs)
  assert(
    recv_until_deadline(state.rep, timeout_secs) == "request-" .. sequence,
    "REQ/REP request mismatch"
  )
  send_until_deadline(state.rep, "reply-" .. sequence, timeout_secs)
  assert(
    recv_until_deadline(state.req, timeout_secs) == "reply-" .. sequence,
    "REQ/REP reply mismatch"
  )

  send_until_deadline(state.left, "left-" .. sequence, timeout_secs)
  assert(
    recv_until_deadline(state.right, timeout_secs) == "left-" .. sequence,
    "PAIR forward mismatch"
  )
  send_until_deadline(state.right, "right-" .. sequence, timeout_secs)
  assert(
    recv_until_deadline(state.left, timeout_secs) == "right-" .. sequence,
    "PAIR reverse mismatch"
  )
  state.seq = state.seq + 1
  counters.protocol = counters.protocol + 4
end

local function close_protocol_mix(state)
  if state == nil then
    return
  end
  for i = #state.sockets, 1, -1 do
    close_socket(state.sockets[i], "protocol-mix")
  end
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
  if selected["protocol-mix"] then
    assert(counters.protocol > 0, "protocol mix made no progress")
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

local cleanup_errors = {}

local function cleanup(label, fn)
  local ok, err = pcall(fn)
  if not ok then
    table.insert(cleanup_errors, label .. ": " .. tostring(err))
  end
end

local function cleanup_error()
  if #cleanup_errors == 0 then
    return nil
  end
  return table.concat(cleanup_errors, "\n")
end

local function run_step(label, fn)
  local ok, err = pcall(fn)
  if not ok then
    error(label .. ": " .. tostring(err), 2)
  end
end

local duration = env_number("OMQ_LUA_SOAK_DURATION_SECS", 60)
assert(duration > 0, "OMQ_LUA_SOAK_DURATION_SECS must be > 0")
local workers = math.max(1, math.floor(env_number("OMQ_LUA_SOAK_WORKERS", 4)))
local selected = selected_scenarios()
local progress = progress_tracker(selected)
local started = now()
local shared = omq.context({ io_threads = workers })
local baseline = resources()
sample_resources(0, baseline)
io.stdout:write(string.format(
  "[lua-soak] start duration=%.0fs workers=%d scenarios=%s report=%.0fs stall=%.0fs\n",
  duration,
  workers,
  scenario_list(selected),
  report_interval,
  progress_limits.stall_secs
))
io.stdout:flush()
local deadline = started + duration
local next_report = started + report_interval
local tcp_state = nil
local inproc_state = nil
local pubsub_state = nil
local protocol_state = nil

if selected.tcp then
  tcp_state = make_tcp(shared)
end
if selected.inproc then
  inproc_state = make_inproc(shared)
end
if selected.pubsub then
  pubsub_state = make_pubsub(shared)
end
if selected["protocol-mix"] then
  protocol_state = make_protocol_mix(shared)
end

local ok, err = pcall(function()
  while now() < deadline do
    if tcp_state ~= nil then
      run_step("tcp", function()
        run_tcp_cycle(shared, tcp_state, workers)
      end)
    end
    if inproc_state ~= nil then
      run_step("inproc", function()
        run_inproc_cycles(inproc_state, workers)
      end)
    end
    if pubsub_state ~= nil then
      run_step("pubsub", function()
        run_pubsub_cycle(pubsub_state)
      end)
    end
    if protocol_state ~= nil then
      local remaining = deadline - now()
      if remaining <= 1 then
        break
      end
      local protocol_timeout_secs = math.min(protocol_mix_timeout_secs, remaining)
      local protocol_ok, protocol_err = pcall(function()
        run_protocol_mix_cycle(protocol_state, protocol_timeout_secs)
      end)
      if not protocol_ok then
        if not transient_error(protocol_err) then
          error("protocol-mix: " .. tostring(protocol_err), 2)
        end
        if now() >= deadline then
          break
        end
        if log_protocol_mix_resets then
          io.stdout:write("[lua-soak] protocol-mix reset: " .. tostring(protocol_err) .. "\n")
          io.stdout:flush()
        end
        close_protocol_mix(protocol_state)
        protocol_state = make_protocol_mix(shared)
      end
    end
    if selected["context-churn"] then
      run_step("context-churn", function()
        run_context_churn_cycle()
      end)
    end
    local t = now()
    if t >= next_report then
      local current = resources()
      local elapsed = t - started
      sample_resources(elapsed, current)
      report("", elapsed, current)
      assert_live_resources(baseline, current)
      assert_progress_active(selected, progress, elapsed)
      next_report = t + report_interval
    end
  end
end)

cleanup("pubsub", function()
  if pubsub_state == nil then
    return
  end
  for _, sub in ipairs(pubsub_state.subs) do
    close_socket(sub.socket, "pubsub")
  end
  pubsub_state.subs = {}
  close_socket(pubsub_state.pub, "pubsub")
end)
cleanup("protocol-mix", function()
  close_protocol_mix(protocol_state)
end)
cleanup("inproc", function()
  close_inproc(inproc_state)
end)
cleanup("tcp", function()
  if tcp_state ~= nil then
    close_socket(tcp_state.pull, "tcp")
  end
end)
cleanup("shared context", function()
  shared:term()
end)

local cleanup_err = cleanup_error()

if not ok then
  if cleanup_err ~= nil then
    error(tostring(err) .. "\ncleanup errors:\n" .. cleanup_err, 0)
  end
  error(err, 0)
end
if cleanup_err ~= nil then
  error("cleanup errors:\n" .. cleanup_err, 0)
end

collectgarbage("collect")
collectgarbage("collect")
local final = resources()
sample_resources(now() - started, final)
report("final ", now() - started, final)
assert_live_resources(baseline, final)
assert_progress_active(selected, progress, now() - started)
assert_final_resources(baseline, final)
assert_progress(selected)
assert_lifecycle_closed()
