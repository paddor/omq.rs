#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"

$stdout.sync = true

backend, pattern, role, endpoint, size, duration, warmup_duration = ARGV
size = Integer(size)
duration = Float(duration)
warmup_duration = Float(warmup_duration)
payload = ("x" * size).b.freeze

case backend
when "omq-rs"
  require "omq/rs"
  implementation_version = OMQ::Rust::VERSION

  build = lambda do |type, bind|
    socket = OMQ.rs(type, linger: 2)
    resolved = bind ? socket.bind(endpoint) : socket.connect(endpoint)
    puts "ENDPOINT #{resolved}" if bind
    socket.wait_for_peer(timeout: 5) unless bind
    socket
  end
  send_message = ->(socket, message) { socket << message }
  recv_message = ->(socket) { socket.recv }
when "cztop"
  require "cztop"
  implementation_version = Gem.loaded_specs.fetch("cztop").version.to_s

  build = lambda do |type, bind|
    klass = CZTop::Socket.const_get(type.to_s.upcase)
    socket = klass.new(bind ? endpoint.sub(/:0\z/, ":*") : ">#{endpoint}", linger: 2)
    puts "ENDPOINT #{socket.last_endpoint}" if bind
    socket
  end
  send_message = ->(socket, message) { socket << message }
  recv_message = ->(socket) { socket.receive }
when "ffi-rzmq"
  require "ffi-rzmq"
  implementation_version = Gem.loaded_specs.fetch("ffi-rzmq").version.to_s
  context = ZMQ::Context.new

  build = lambda do |type, bind|
    socket = context.socket(ZMQ.const_get(type.to_s.upcase))
    socket.setsockopt(ZMQ::LINGER, 2000)
    if bind
      socket.bind(endpoint.sub(/:0\z/, ":*"))
      last_endpoint = []
      socket.getsockopt(ZMQ::LAST_ENDPOINT, last_endpoint)
      puts "ENDPOINT #{last_endpoint.fetch(0).delete_suffix("\0")}"
    else
      socket.connect(endpoint)
    end
    socket
  end
  send_message = ->(socket, message) { socket.send_string(message) }
  recv_message = lambda do |socket|
    message = String.new
    socket.recv_string(message)
    message
  end
else
  abort "unknown backend: #{backend}"
end

elapsed = nil
count = 0

case [pattern, role]
when ["pushpull", "pull"]
  socket = build.call(:pull, true)
  warmup_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + warmup_duration
  loop do
    64.times { recv_message.call(socket) }
    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= warmup_deadline
  end
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  deadline = started + duration
  loop do
    64.times { recv_message.call(socket) }
    count += 64
    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
  end
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
when ["pushpull", "push"]
  socket = build.call(:push, false)
  loop { send_message.call(socket, payload) }
when ["reqrep", "rep"]
  socket = build.call(:rep, true)
  loop do
    message = recv_message.call(socket)
    send_message.call(socket, message)
  end
when ["reqrep", "req"]
  socket = build.call(:req, false)
  warmup_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + warmup_duration
  loop do
    send_message.call(socket, payload)
    recv_message.call(socket)
    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= warmup_deadline
  end
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  deadline = started + duration
  loop do
    send_message.call(socket, payload)
    recv_message.call(socket)
    count += 1
    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
  end
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
else
  abort "unknown benchmark role: #{pattern}/#{role}"
end

result = {
  backend: backend,
  pattern: pattern,
  size: size,
  count: count,
  target_duration: duration,
  warmup_duration: warmup_duration,
  elapsed: elapsed,
  messages_per_second: count / elapsed,
  implementation_version: implementation_version,
}
if backend == "cztop"
  result[:czmq_version] = CZTop::Socket::CZMQ_VERSION
  result[:libzmq_version] = CZTop::Socket::ZMQ_VERSION
elsif backend == "ffi-rzmq"
  result[:libzmq_version] = LibZMQ.version.values.join(".")
end
result[:megabytes_per_second] = count * size / elapsed / 1_000_000.0 if pattern == "pushpull"
result[:microseconds_per_round_trip] = elapsed * 1_000_000.0 / count if pattern == "reqrep"
puts "RESULT #{JSON.generate(result)}"

socket.close
