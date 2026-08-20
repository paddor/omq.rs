# frozen_string_literal: true

require_relative "test_helper"
require "async"

class FiberSchedulerTest < Minitest::Test
  def test_recv_yields_to_another_fiber
    Async do |task|
      pull = socket(:pull, recv_timeout: 2)
      endpoint = tcp_endpoint(pull)
      push = socket(:push)
      push.connect(endpoint).wait_for_peer(timeout: 2)

      task.async do
        sleep 0.02
        push << "from-fiber"
      end

      assert_equal ["from-fiber"], pull.recv
    end.wait
  end

  def test_multiple_waiting_sockets
    Async do |task|
      pairs = 3.times.map do
        pull = socket(:pull, recv_timeout: 2)
        endpoint = tcp_endpoint(pull)
        push = socket(:push)
        push.connect(endpoint).wait_for_peer(timeout: 2)
        [push, pull]
      end

      readers = pairs.map do |_push, pull|
        task.async { pull.recv }
      end
      pairs.each_with_index { |(push, _pull), index| push << "message-#{index}" }

      assert_equal 3.times.map { |index| ["message-#{index}"] }, readers.map(&:wait)
    end.wait
  end

  def test_curve_auth_allowlist
    Async do
      server, client, client_public = curve_pair(:pull, :push)
      server.set_curve_auth([client_public])
      endpoint = tcp_endpoint(server)
      client.connect(endpoint).wait_for_peer(timeout: 2)

      client << "allowed"
      assert_equal ["allowed"], server.recv
    end.wait
  end

  def test_curve_auth_callback
    Async do
      server, client, client_public = curve_pair(:pull, :push)
      server.set_curve_auth { |peer| peer.public_key == client_public }
      endpoint = tcp_endpoint(server)
      client.connect(endpoint).wait_for_peer(timeout: 2)

      client << "allowed"
      assert_equal ["allowed"], server.recv
    end.wait
  end

  def test_curve_auth_callback_receives_identity
    Async do
      captured = []
      server, client, client_public = curve_pair(:router, :dealer, identity: "async-client")
      server.set_curve_auth do |peer|
        captured << peer.identity
        peer.public_key == client_public && peer.identity == "async-client"
      end
      endpoint = tcp_endpoint(server)
      client.connect(endpoint).wait_for_peer(timeout: 2)

      client << "probe"
      assert_equal ["async-client", "probe"], server.recv
      assert_equal ["async-client"], captured
    end.wait
  end

  private

  def curve_pair(server_type, client_type, identity: nil)
    server_public, server_secret = OMQ::Rust.curve_keypair
    client_public, client_secret = OMQ::Rust.curve_keypair
    server = socket(
      server_type,
      recv_timeout: 2,
      curve_server: true,
      curve_publickey: server_public,
      curve_secretkey: server_secret,
    )
    client = socket(
      client_type,
      recv_timeout: 2,
      identity: identity,
      curve_serverkey: server_public,
      curve_publickey: client_public,
      curve_secretkey: client_secret,
    )
    [server, client, client_public]
  end
end
