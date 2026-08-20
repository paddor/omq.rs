# frozen_string_literal: true

require_relative "test_helper"

class LifecycleTest < Minitest::Test
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
    baseline = Thread.list.length
    10.times do
      public_key, secret_key = OMQ::Rust.curve_keypair
      socket = OMQ.rs(
        :pull,
        linger: 0,
        curve_server: true,
        curve_publickey: public_key,
        curve_secretkey: secret_key,
      )
      socket.set_curve_auth { true }
      socket.close
    end

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    while Thread.list.length > baseline && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      Thread.pass
    end
    assert_operator Thread.list.length, :<=, baseline
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
    baseline = Thread.list.length
    10.times { abandon_curve_callback_socket }

    GC.start(full_mark: true, immediate_sweep: true)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    while Thread.list.length > baseline && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      Thread.pass
    end
    assert_operator Thread.list.length, :<=, baseline + 1
  end

  private

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
