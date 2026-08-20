# frozen_string_literal: true

require_relative "test_helper"
require "open3"

class InteropTest < Minitest::Test
  PEER = File.expand_path("interop_peer.py", __dir__)

  def setup
    super
    skip "pyzmq unavailable" unless pyzmq_available?
  end

  def test_ruby_push_to_pyzmq_pull
    Open3.popen3(python, PEER, "pull") do |_stdin, stdout, stderr, wait|
      endpoint = stdout.gets&.strip
      flunk stderr.read unless endpoint

      push = socket(:push)
      push.connect(endpoint).wait_for_peer(timeout: 2)
      push << "from-ruby"

      assert_equal "from-ruby", stdout.gets&.strip
      assert wait.value.success?, stderr.read
    end
  end

  def test_pyzmq_push_to_ruby_pull
    pull = socket(:pull, recv_timeout: 2)
    endpoint = tcp_endpoint(pull)

    _output, error, status = Open3.capture3(python, PEER, "push", endpoint)

    assert status.success?, error
    assert_equal ["from", "pyzmq"], pull.recv
  end

  def test_ruby_curve_server_with_pyzmq_client
    server_public, server_secret = OMQ::Rust.curve_keypair
    pull = socket(
      :pull,
      recv_timeout: 5,
      curve_server: true,
      curve_publickey: server_public,
      curve_secretkey: server_secret,
    )
    endpoint = tcp_endpoint(pull)

    _output, error, status = Open3.capture3(
      python,
      PEER,
      "curve_push",
      endpoint,
      server_public,
    )

    assert status.success?, error
    assert_equal ["from-pyzmq-curve"], pull.recv
  end

  def test_pyzmq_curve_server_with_ruby_client
    Open3.popen3(python, PEER, "curve_pull") do |_stdin, stdout, stderr, wait|
      endpoint = stdout.gets&.strip
      server_public = stdout.gets&.strip
      flunk stderr.read unless endpoint && server_public

      client_public, client_secret = OMQ::Rust.curve_keypair
      push = socket(
        :push,
        curve_serverkey: server_public,
        curve_publickey: client_public,
        curve_secretkey: client_secret,
      )
      push.connect(endpoint).wait_for_peer(timeout: 5)
      push << "from-ruby-curve"

      assert_equal "from-ruby-curve", stdout.gets&.strip
      assert wait.value.success?, stderr.read
    end
  end

  def test_ruby_curve_rep_with_pyzmq_req
    server_public, server_secret = OMQ::Rust.curve_keypair
    rep = socket(
      :rep,
      recv_timeout: 5,
      curve_server: true,
      curve_publickey: server_public,
      curve_secretkey: server_secret,
    )
    endpoint = tcp_endpoint(rep)

    Open3.popen3(python, PEER, "curve_req", endpoint, server_public) do |_stdin, stdout, stderr, wait|
      assert_equal ["ping"], rep.recv
      rep << "pong"
      assert_equal "pong", stdout.gets&.strip
      assert wait.value.success?, stderr.read
    end
  end

  def test_pyzmq_curve_rep_with_ruby_req
    Open3.popen3(python, PEER, "curve_rep") do |_stdin, stdout, stderr, wait|
      endpoint = stdout.gets&.strip
      server_public = stdout.gets&.strip
      flunk stderr.read unless endpoint && server_public

      client_public, client_secret = OMQ::Rust.curve_keypair
      req = socket(
        :req,
        recv_timeout: 5,
        curve_serverkey: server_public,
        curve_publickey: client_public,
        curve_secretkey: client_secret,
      )
      req.connect(endpoint).wait_for_peer(timeout: 5)
      req << "ping"

      assert_equal "ping", stdout.gets&.strip
      assert_equal ["pong"], req.recv
      assert wait.value.success?, stderr.read
    end
  end

  private

  def python
    ENV.fetch("OMQ_PYTHON3", "python3")
  end

  def pyzmq_available?
    system(python, "-c", "import zmq", out: File::NULL, err: File::NULL)
  end
end
