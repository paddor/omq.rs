# frozen_string_literal: true

require_relative "test_helper"

class LifecycleTest < Minitest::Test
  def test_close_during_materialization_finishes
    skip "TracePoint :c_call is unsupported on TruffleRuby" if RUBY_ENGINE == "truffleruby"
    script = <<~'RUBY'
      50.times do |index|
        socket = OMQ.rs(:pull, linger: 0)
        target = index.even? ? socket : socket.instance_variable_get(:@native)
        called = Queue.new
        trace = TracePoint.new(:c_call) do |event|
          called << true if event.method_id == :materialize
        end
        worker = Thread.new do
          trace.enable(target_thread: Thread.current) do
            if target.equal?(socket)
              socket.bind("inproc://close-materializing-#{index}")
            else
              target.materialize
            end
          rescue IOError, RuntimeError
            raise unless socket.closed?
          end
        end
        called.pop
        target.close
        worker.value
        raise "socket stayed open" unless socket.closed?
      end
    RUBY
    pid = Process.spawn(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__),
                        "-romq/rs", "-e", script)
    _, status = Timeout.timeout(10) { Process.wait2(pid) }
    pid = nil
    assert status.success?, "close during materialization failed"
  ensure
    if pid
      Process.kill("KILL", pid)
      Process.wait(pid)
    end
  end

  def test_explicit_close_releases_file_descriptors
    assert_fd_growth_at_most(2) do
      20.times { OMQ.rs(:pull, linger: 0).close }
    end
  end

  def test_gc_releases_file_descriptors
    assert_fd_growth_at_most(2) do
      20.times { abandon_socket }
      GC.start(full_mark: true, immediate_sweep: true)
    end
  end

  def test_curve_callback_workers_stop_on_close
    workers = []
    10.times do
      public_key, secret_key = OMQ::Rust.curve_keypair
      socket = OMQ.rs(
        :pull,
        linger: 0,
        curve_server: true,
        curve_publickey: public_key,
        curve_secretkey: secret_key,
      )
      before = auth_workers
      socket.set_curve_auth { true }
      created = auth_workers - before
      assert_equal 1, created.length
      workers.concat(created)
      socket.close
    end

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    while workers.any?(&:alive?) && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      Thread.pass
    end
    assert_empty workers.select(&:alive?)
  end

  def test_curve_callback_workers_release_after_authentication
    2.times do
      server_public, server_secret = OMQ::Rust.curve_keypair
      client_public, client_secret = OMQ::Rust.curve_keypair
      server = socket(
        :pull,
        recv_timeout: 2,
        curve_server: true,
        curve_publickey: server_public,
        curve_secretkey: server_secret,
      )
      client = socket(
        :push,
        curve_serverkey: server_public,
        curve_publickey: client_public,
        curve_secretkey: client_secret,
      )
      server.set_curve_auth { true }
      endpoint = tcp_endpoint(server)
      client.connect(endpoint).wait_for_peer(timeout: 2)

      client << "authenticated"
      assert_equal ["authenticated"], server.recv
      client.close
      server.close
    end
  end

  def test_curve_callback_workers_do_not_deadlock_gc
    baseline = auth_workers.length
    10.times { abandon_curve_callback_socket }

    GC.start(full_mark: true, immediate_sweep: true)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    while auth_workers.length > baseline && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      Thread.pass
    end
    assert_operator auth_workers.length, :<=, baseline + 1
  end

  private

  def auth_workers
    Thread.list.select { |thread| thread.name == "omq-auth" }
  end

  def abandon_socket
    OMQ.rs(:pull, linger: 0)
    nil
  end

  def abandon_curve_callback_socket
    public_key, secret_key = OMQ::Rust.curve_keypair
    socket = OMQ.rs(
      :pull,
      curve_server: true,
      curve_publickey: public_key,
      curve_secretkey: secret_key,
    )
    socket.set_curve_auth { true }
    nil
  end

  def assert_fd_growth_at_most(limit)
    OMQ.rs(:pull, linger: 0).close
    baseline = fd_count
    yield
    GC.start(full_mark: true, immediate_sweep: true)
    sleep 0.05
    growth = fd_count - baseline

    assert_operator growth, :<=, limit, "file descriptor growth: #{growth}"
  end

  def fd_count
    path = File.directory?("/proc/self/fd") ? "/proc/self/fd" : "/dev/fd"
    Dir.children(path).length
  end
end
