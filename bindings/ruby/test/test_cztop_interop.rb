# frozen_string_literal: true

require_relative "test_helper"

class CZTopInteropTest < Minitest::Test
  def setup
    super
    skip "cztop FFI is unsupported on TruffleRuby" if RUBY_ENGINE == "truffleruby"

    require "cztop"
    skip "cztop CURVE unavailable" unless CZTop::CURVE.available?
  rescue LoadError, StandardError => error
    skip "cztop unavailable: #{error.message}"
  end

  def test_cztop_curve_client_to_omq_server
    server_public, server_secret = OMQ::Rust.curve_keypair
    pull = socket(
      :pull,
      recv_timeout: 2,
      curve_server: true,
      curve_publickey: server_public,
      curve_secretkey: server_secret,
    )
    endpoint = tcp_endpoint(pull)
    _, client_secret = CZTop::CURVE.keypair
    push = CZTop::Socket::PUSH.new(
      curve: {
        secret_key: client_secret,
        server_key: CZTop::CURVE.z85_decode(server_public),
      },
    )

    push.connect(endpoint)
    push << "from-cztop-curve"

    assert_equal ["from-cztop-curve"], pull.recv
  ensure
    push&.close
  end

  def test_omq_curve_client_to_cztop_server
    auth = CZTop::CURVE::Auth.new(allow_any: true)
    server_public, server_secret = CZTop::CURVE.keypair
    pull = CZTop::Socket::PULL.new(curve: { secret_key: server_secret })
    pull.read_timeout = 2
    pull.bind("tcp://127.0.0.1:*")
    client_public, client_secret = OMQ::Rust.curve_keypair
    push = socket(
      :push,
      curve_serverkey: CZTop::CURVE.z85_encode(server_public),
      curve_publickey: client_public,
      curve_secretkey: client_secret,
    )

    push.connect(pull.last_endpoint).wait_for_peer(timeout: 2)
    push << "from-omq-curve"

    assert_equal ["from-omq-curve"], pull.receive
  ensure
    pull&.close
    auth&.stop
  end
end
