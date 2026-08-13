require "json"
require "zeromq/lib_zmq"

HWM                     = 1_000_000
MAX_TIME_CHECK_MESSAGES =      1024

def die(message : String) : NoReturn
  STDERR.puts(message)
  exit 1
end

def check_rc(name : String, rc : Int)
  return if rc >= 0
  errno = LibZMQ.errno
  die("#{name}: #{String.new(LibZMQ.strerror(errno))}")
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

def set_i32(socket : Pointer(Void), option : Int, value : Int)
  raw_value = value.to_i32
  check_rc(
    "zmq_setsockopt",
    LibZMQ.setsockopt(socket, option.to_i32, pointerof(raw_value).as(Void*), sizeof(Int32))
  )
end

def context : Pointer(Void)
  raw = LibZMQ.ctx_new
  die("zmq_ctx_new failed") if raw.null?
  check_rc("zmq_ctx_set", LibZMQ.ctx_set(raw, ZMQ::IO_THREADS, 1))
  raw
end

def socket(ctx : Pointer(Void), socket_type : Int32, sender : Bool) : Pointer(Void)
  sock = LibZMQ.socket(ctx, socket_type)
  die("zmq_socket failed") if sock.null?
  set_i32(sock, ZMQ::LINGER, 0)
  if sender
    set_i32(sock, ZMQ::SNDHWM, HWM)
  else
    set_i32(sock, ZMQ::RCVHWM, HWM)
  end
  sock
end

def send_frame(sock : Pointer(Void), msg : String)
  check_rc(
    "zmq_send",
    LibZMQ.send(sock, msg.to_unsafe.as(Void*), msg.bytesize, 0)
  )
end

def recv_frame(sock : Pointer(Void), size : Int32) : Nil
  buffer = Bytes.new(size)
  rc = LibZMQ.recv(sock, buffer.to_unsafe.as(Void*), buffer.size, 0)
  check_rc("zmq_recv", rc)
  die("bad message size") unless rc == size
end

def run_pull(impl : String, endpoint : String, size : Int32, duration : Time::Span, warmup : Time::Span)
  ctx = context
  pull = socket(ctx, ZMQ::PULL, sender: false)
  check_rc("zmq_bind", LibZMQ.bind(pull, endpoint.to_unsafe))
  ready(endpoint)

  warmup_deadline = Time.instant + warmup
  deadline = warmup_deadline + duration
  messages = 0_i64
  start = warmup_deadline
  checks = 0

  loop do
    now = Time.instant
    break if now >= deadline
    recv_frame(pull, size)
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
  check_rc("zmq_close", LibZMQ.close(pull))
  check_rc("zmq_ctx_destroy", LibZMQ.ctx_destroy(ctx))
end

def run_push(_impl : String, endpoint : String, size : Int32)
  ctx = context
  push = socket(ctx, ZMQ::PUSH, sender: true)
  check_rc("zmq_connect", LibZMQ.connect(push, endpoint.to_unsafe))
  msg = payload(size)
  loop { send_frame(push, msg) }
end

def run_rep(_impl : String, endpoint : String, size : Int32)
  ctx = context
  rep = socket(ctx, ZMQ::REP, sender: true)
  check_rc("zmq_bind", LibZMQ.bind(rep, endpoint.to_unsafe))
  ready(endpoint)
  msg = payload(size)
  loop do
    recv_frame(rep, size)
    send_frame(rep, msg)
  end
end

def percentile(sorted : Array(Float64), fraction : Float64) : Float64
  return 0.0 if sorted.empty?
  index = ((sorted.size - 1) * fraction).round.to_i
  sorted[index]
end

def run_req(impl : String, endpoint : String, size : Int32, duration : Time::Span, warmup : Time::Span)
  ctx = context
  req = socket(ctx, ZMQ::REQ, sender: true)
  check_rc("zmq_connect", LibZMQ.connect(req, endpoint.to_unsafe))
  msg = payload(size)
  latencies = [] of Float64

  warmup_deadline = Time.instant + warmup
  deadline = warmup_deadline + duration
  messages = 0_i64

  loop do
    now = Time.instant
    break if now >= deadline
    start = Time.instant
    send_frame(req, msg)
    recv_frame(req, size)
    done = Time.instant
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
  check_rc("zmq_close", LibZMQ.close(req))
  check_rc("zmq_ctx_destroy", LibZMQ.ctx_destroy(ctx))
end

die("usage: bench_peer_zeromq <pushpull|reqrep> <zeromq-crystal> <push|pull|req|rep> <endpoint> <size> <duration> <warmup>") unless ARGV.size == 7

bench = ARGV[0]
impl = ARGV[1]
role = ARGV[2]
endpoint = ARGV[3]
size = ARGV[4].to_i
duration = parse_duration(ARGV[5])
warmup = parse_duration(ARGV[6])

die("bad impl: #{impl}") unless impl == "zeromq-crystal"
die("invalid size") if size < 0

case {bench, role}
when {"pushpull", "pull"}
  run_pull(impl, endpoint, size, duration, warmup)
when {"pushpull", "push"}
  run_push(impl, endpoint, size)
when {"reqrep", "rep"}
  run_rep(impl, endpoint, size)
when {"reqrep", "req"}
  run_req(impl, endpoint, size, duration, warmup)
else
  die("bad bench/role: #{bench}/#{role}")
end
