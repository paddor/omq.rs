# frozen_string_literal: true

require_relative "test_helper"

class SoakTest < Minitest::Test
  REPORT_INTERVAL = 10
  LARGE_PAYLOAD = ("omq-rs-ruby-soak" * 65_536).b.freeze

  def test_mixed_workloads
    skip "set OMQ_RUBY_SOAK=1 to run Ruby soak" unless ENV["OMQ_RUBY_SOAK"] == "1"

    duration = Float(ENV.fetch("OMQ_RUBY_SOAK_DURATION_SECS", ENV.fetch("OMQ_SOAK_DURATION_SECS", "60")))
    deadline = monotonic + duration
    baseline = resources
    samples = []
    counters = Hash.new(0)
    fixtures = stable_fixtures
    cycle = 0
    next_report = monotonic + REPORT_INTERVAL

    while monotonic < deadline
      cycle += 1
      exercise_push_pull(fixtures.fetch(:tcp), cycle, counters, :tcp)
      exercise_push_pull(fixtures.fetch(:ipc), cycle, counters, :ipc)
      exercise_push_pull(fixtures.fetch(:inproc), cycle, counters, :inproc)
      exercise_req_rep(fixtures.fetch(:reqrep), cycle, counters)
      exercise_pair(fixtures.fetch(:pair), cycle, counters)
      exercise_pub_sub(fixtures.fetch(:pubsub), cycle, counters)
      exercise_push_pull(fixtures.fetch(:lz4), cycle, counters, :lz4)
      exercise_push_pull(fixtures.fetch(:zstd), cycle, counters, :zstd)
      exercise_push_pull(fixtures.fetch(:plain), cycle, counters, :plain)
      exercise_push_pull(fixtures.fetch(:curve), cycle, counters, :curve)
      exercise_socket_churn(cycle, counters) if (cycle % 10).zero?
      exercise_reconnect(counters) if (cycle % 25).zero?

      now = monotonic
      next unless now >= next_report

      current = resources
      samples << [now, current]
      warn format_report(duration - [deadline - now, 0].max, counters, current)
      next_report = now + REPORT_INTERVAL
    end

    fixtures.each_value { |pair| pair.reverse_each(&:close) }
    @sockets.clear
    GC.start(full_mark: true, immediate_sweep: true)
    sleep 0.1

    required = %i[tcp ipc inproc lz4 zstd plain curve reqrep pair pubsub fanout multipart]
    required.each { |name| assert_operator counters[name], :>, 0, "#{name} made no progress" }
    assert_resource_growth(baseline, resources, samples)
  ensure
    fixtures&.each_value { |pair| pair.reverse_each(&:close) }
  end

  private

  def stable_fixtures
    fixtures = {
      tcp: push_pull("tcp://127.0.0.1:0"),
      ipc: push_pull("ipc://@omq-ruby-soak-#{Process.pid}"),
      inproc: push_pull("inproc://omq-ruby-soak-#{Process.pid}"),
      reqrep: req_rep,
      pair: pair,
      pubsub: pub_sub,
      lz4: push_pull("lz4+tcp://127.0.0.1:0"),
      zstd: push_pull("zstd+tcp://127.0.0.1:0"),
      plain: push_pull("tcp://127.0.0.1:0", pull: {
        plain_server: true, plain_auth: [%w[soak secret]]
      }, push: {
        plain_username: "soak", plain_password: "secret"
      }),
      curve: curve_push_pull,
    }
    fixtures.each_value { |sockets| sockets.each { |value| @sockets.delete(value) } }
    fixtures
  end

  def push_pull(bind_endpoint, pull: {}, push: {})
    receiver = socket(:pull, recv_timeout: 5, **pull)
    endpoint = receiver.bind(bind_endpoint)
    sender = socket(:push, send_timeout: 5, **push)
    sender.connect(endpoint).wait_for_peer(timeout: 5)
    [sender, receiver]
  end

  def curve_push_pull
    server_public, server_secret = OMQ::Rust.curve_keypair
    client_public, client_secret = OMQ::Rust.curve_keypair
    push_pull(
      "tcp://127.0.0.1:0",
      pull: {
        curve_server: true,
        curve_publickey: server_public,
        curve_secretkey: server_secret,
        curve_auth: [client_public],
      },
      push: {
        curve_serverkey: server_public,
        curve_publickey: client_public,
        curve_secretkey: client_secret,
      },
    )
  end

  def req_rep
    rep = socket(:rep, recv_timeout: 5, send_timeout: 5)
    endpoint = rep.bind("tcp://127.0.0.1:0")
    req = socket(:req, recv_timeout: 5, send_timeout: 5)
    req.connect(endpoint).wait_for_peer(timeout: 5)
    [req, rep]
  end

  def pair
    first = socket(:pair, recv_timeout: 5, send_timeout: 5)
    endpoint = first.bind("tcp://127.0.0.1:0")
    second = socket(:pair, recv_timeout: 5, send_timeout: 5)
    second.connect(endpoint).wait_for_peer(timeout: 5)
    [first, second]
  end

  def pub_sub
    pub = socket(:pub, send_timeout: 5)
    endpoint = pub.bind("tcp://127.0.0.1:0")
    first = socket(:sub, recv_timeout: 5).subscribe("soak.").connect(endpoint)
    second = socket(:sub, recv_timeout: 5).subscribe("soak.").connect(endpoint)
    pub.wait_for_subscriber(timeout: 5)
    [pub, first, second]
  end

  def exercise_push_pull(pair, cycle, counters, name)
    sender, receiver = pair
    payload = cycle.to_s
    sender.send(name.to_s, payload)
    assert_equal [name.to_s, payload], receiver.recv
    counters[name] += 1
    counters[:multipart] += 1

    return unless name == :tcp && (cycle % 25).zero?

    sending = Thread.new { sender << LARGE_PAYLOAD }
    assert_equal [LARGE_PAYLOAD], receiver.recv
    assert sending.join(5), "#{name} large send stalled"
    sending.value
    counters[:large] += 1
  end

  def exercise_req_rep(pair, cycle, counters)
    req, rep = pair
    req << cycle.to_s
    assert_equal [cycle.to_s], rep.recv
    rep << "ack-#{cycle}"
    assert_equal ["ack-#{cycle}"], req.recv
    counters[:reqrep] += 1
  end

  def exercise_pair(pair, cycle, counters)
    first, second = pair
    first << "a-#{cycle}"
    assert_equal ["a-#{cycle}"], second.recv
    second << "b-#{cycle}"
    assert_equal ["b-#{cycle}"], first.recv
    counters[:pair] += 2
  end

  def exercise_pub_sub(sockets, cycle, counters)
    pub, *subscribers = sockets
    message = "soak.#{cycle}"
    pub << message
    subscribers.each { |sub| assert_equal [message], sub.recv }
    counters[:pubsub] += subscribers.length
    counters[:fanout] += 1
  end

  def exercise_socket_churn(cycle, counters)
    pull = OMQ.rs(:pull, linger: 0, recv_timeout: 2)
    push = OMQ.rs(:push, linger: 0, send_timeout: 2)
    endpoint = pull.bind("inproc://omq-ruby-churn-#{Process.pid}-#{cycle}")
    push.connect(endpoint)
    push << cycle.to_s
    assert_equal [cycle.to_s], pull.recv
    counters[:socket_churn] += 1
  ensure
    push&.close
    pull&.close
  end

  def exercise_reconnect(counters)
    pull = OMQ.rs(:pull, linger: 0, recv_timeout: 5)
    endpoint = pull.bind("tcp://127.0.0.1:0")
    push = OMQ.rs(:push, linger: 0, send_timeout: 5, reconnect_interval: 0.01)
    push.connect(endpoint).wait_for_peer(timeout: 5)
    push << "before"
    assert_equal ["before"], pull.recv
    pull.close

    pull = OMQ.rs(:pull, linger: 0, recv_timeout: 5)
    pull.bind(endpoint)
    push << "after"
    assert_equal ["after"], pull.recv
    counters[:reconnect] += 1
  ensure
    push&.close
    pull&.close
  end

  def resources
    rss_kib = File.read("/proc/self/status")[/^VmRSS:\s+(\d+)\s+kB$/, 1].to_i
    {
      rss: rss_kib * 1024,
      fds: Dir.children("/proc/self/fd").length,
      threads: Thread.list.length,
    }
  end

  def assert_resource_growth(baseline, final, samples)
    assert_operator final.fetch(:fds) - baseline.fetch(:fds), :<=, 16, "file descriptor leak"
    assert_operator final.fetch(:threads) - baseline.fetch(:threads), :<=, 4, "thread leak"

    return if samples.length < 12

    warm = samples.drop(samples.length / 5).map { |_, value| value.fetch(:rss) }
    base = warm.first([warm.length / 10, 1].max).sum / [warm.length / 10, 1].max
    tail = warm.last([warm.length / 5, 1].max).max
    growth = tail - base
    assert growth < 32 * 1024 * 1024 || growth.to_f / base < 0.25,
      "RSS leak: baseline=#{base} tail=#{tail} growth=#{growth}"
  end

  def format_report(elapsed, counters, current)
    values = counters.sort.map { |name, count| "#{name}=#{count}" }.join(" ")
    format(
      "[ruby-soak] %.0fs %s rss=%.1fMB fds=%d threads=%d",
      elapsed,
      values,
      current.fetch(:rss).to_f / 1_048_576,
      current.fetch(:fds),
      current.fetch(:threads),
    )
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
