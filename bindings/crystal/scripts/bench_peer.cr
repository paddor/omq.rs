require "json"
require "../src/omq"

HWM                     = 1_000_000
TIMEOUT                 = 120.seconds
MAX_TIME_CHECK_MESSAGES = 1024

def die(message : String) : NoReturn
  STDERR.puts(message)
  exit 1
end

def parse_duration(value : String) : Time::Span
  Time::Span.new(nanoseconds: (value.to_f * 1_000_000_000).to_i64)
end

def ready(endpoint : String)
  puts "READY #{endpoint}"
  STDOUT.flush
end

def print_result(impl : String, endpoint : String, size : Int32, messages : Int64,
                 seconds : Float64? = nil, msgs_s : Float64? = nil, gb_s : Float64? = nil,
                 p50_us : Float64? = nil, p99_us : Float64? = nil)
  json = JSON.build do |builder|
    builder.object do
      builder.field "impl", impl
      builder.field "endpoint", endpoint
      builder.field "msg_size", size
      builder.field "messages", messages
      builder.field "seconds", seconds if seconds
      builder.field "msgs_s", msgs_s if msgs_s
      builder.field "gb_s", gb_s if gb_s
      builder.field "p50_us", p50_us if p50_us
      builder.field "p99_us", p99_us if p99_us
    end
  end
  puts "RESULT #{json}"
end

def payload(size : Int32) : String
  String.build(size) do |io|
    size.times { |i| io.write_byte((i & 0xff).to_u8) }
  end
end

def socket(ctx : OMQ::Context, socket_type : String, sender : Bool) : OMQ::Socket
  if sender
    ctx.socket(socket_type, linger: 0, send_hwm: HWM)
  else
    ctx.socket(socket_type, linger: 0, recv_timeout: 1000, recv_hwm: HWM)
  end
end

def run_pull(impl : String, endpoint : String, size : Int32, duration : Time::Span, warmup : Time::Span)
  ctx = OMQ.context
  pull = socket(ctx, "pull", sender: false)
  bound = pull.bind(endpoint)
  ready(bound)

  warmup_deadline = Time.instant + warmup
  deadline = warmup_deadline + duration
  messages = 0_i64
  start = warmup_deadline
  checks = 0

  loop do
    now = Time.instant
    break if now >= deadline
    msg = pull.recv
    die("bad message size") unless msg && msg.bytesize == size
    if now >= warmup_deadline
      start = now if messages == 0
      messages += 1
    end
    checks += 1
    if checks >= MAX_TIME_CHECK_MESSAGES
      checks = 0
      break if Time.instant >= deadline
    end
  end

  elapsed = [Time.instant - start, 1.nanosecond].max.total_seconds
  rate = messages / elapsed
  print_result(impl, endpoint, size, messages, elapsed, rate, rate * size / 1_000_000_000.0)
  pull.close
  ctx.term
end

def run_push(_impl : String, endpoint : String, size : Int32)
  ctx = OMQ.context
  push = socket(ctx, "push", sender: true)
  push.connect(endpoint)
  msg = payload(size)
  loop { push.send(msg) }
end

def run_rep(_impl : String, endpoint : String)
  ctx = OMQ.context
  rep = socket(ctx, "rep", sender: true)
  bound = rep.bind(endpoint)
  ready(bound)
  loop do
    msg = rep.recv
    rep.send(msg || "")
  end
end

def percentile(sorted : Array(Float64), fraction : Float64) : Float64
  return 0.0 if sorted.empty?
  index = ((sorted.size - 1) * fraction).round.to_i
  sorted[index]
end

def run_req(impl : String, endpoint : String, size : Int32, duration : Time::Span, warmup : Time::Span)
  ctx = OMQ.context
  req = socket(ctx, "req", sender: true)
  req.connect(endpoint)
  msg = payload(size)
  latencies = [] of Float64

  warmup_deadline = Time.instant + warmup
  deadline = warmup_deadline + duration
  messages = 0_i64

  loop do
    now = Time.instant
    break if now >= deadline
    start = Time.instant
    req.send(msg)
    reply = req.recv
    done = Time.instant
    die("bad reply size") unless reply && reply.bytesize == size
    if done >= warmup_deadline
      latencies << (done - start).total_microseconds
      messages += 1
    end
  end

  sorted = latencies.sort
  print_result(
    impl,
    endpoint,
    size,
    messages,
    p50_us: percentile(sorted, 0.50),
    p99_us: percentile(sorted, 0.99)
  )
  req.close
  ctx.term
end

die("usage: bench_peer <pushpull|reqrep> <omq.cr> <push|pull|req|rep> <endpoint> <size> <duration> <warmup>") unless ARGV.size == 7

bench = ARGV[0]
impl = ARGV[1]
role = ARGV[2]
endpoint = ARGV[3]
size = ARGV[4].to_i
duration = parse_duration(ARGV[5])
warmup = parse_duration(ARGV[6])

die("bad impl: #{impl}") unless impl == "omq.cr"
die("invalid size") if size < 0

case {bench, role}
when {"pushpull", "pull"}
  run_pull(impl, endpoint, size, duration, warmup)
when {"pushpull", "push"}
  run_push(impl, endpoint, size)
when {"reqrep", "rep"}
  run_rep(impl, endpoint)
when {"reqrep", "req"}
  run_req(impl, endpoint, size, duration, warmup)
else
  die("bad bench/role: #{bench}/#{role}")
end
