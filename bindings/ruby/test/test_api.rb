# frozen_string_literal: true

require_relative "test_helper"

class ApiTest < Minitest::Test
  def test_namespace_and_factories
    assert_same OMQ::Rust, OMQ.rs
    assert_instance_of OMQ::Rust::PUSH, socket(:push)
    assert_instance_of OMQ::Rust::PULL, track(OMQ.rs::PULL.new(linger: 0))
    assert_kind_of OMQ::Rust::Socket, socket(:dealer)
  end

  def test_unknown_socket_type
    error = assert_raises(ArgumentError) { OMQ.rs(:unknown) }
    assert_match(/unknown socket type/, error.message)
  end

  def test_all_socket_classes_materialize
    OMQ::Rust::SOCKET_TYPES.each do |type|
      klass = OMQ::Rust.const_get(type.to_s.upcase)
      instance = track(klass.new(linger: 0))

      assert_equal type, instance.socket_type
      refute instance.closed?
    end
  end

  def test_block_form_closes_socket
    yielded = nil
    result = OMQ.rs(:pull, linger: 0) do |pull|
      yielded = pull
      :done
    end

    assert_equal :done, result
    assert yielded.closed?
  end

  def test_io_threads
    original = OMQ::Rust.io_threads
    OMQ::Rust.io_threads = 2

    assert_equal 2, OMQ::Rust.io_threads
    assert_raises(ArgumentError) { OMQ::Rust.io_threads = 0 }
  ensure
    OMQ::Rust.io_threads = original
  end

  def test_current_socket_options
    dealer = socket(
      :dealer,
      workload_profile: :latency,
      send_hwm: 10,
      recv_hwm: 11,
      recv_rate_limit: {messages_per_second: 1_000, burst: 100},
      recv_ip_rate_limit: {rate: 5_000, burst: 500},
      handshake_timeout: 2,
      max_pending_handshakes: 16,
      large_message_threshold: 65_536,
      arena_threshold: 8_192,
      transmit_slot_cap: 4 * 1024 * 1024,
      on_mute: :drop_oldest,
      reconnect_stop_conn_refused: true,
    )

    refute dealer.closed?
  end

  def test_invalid_options
    assert_raises(ArgumentError) { OMQ.rs(:pull, send_hwm: -1) }
    assert_raises(ArgumentError) { OMQ.rs(:pull, workload_profile: :fast) }
    assert_raises(ArgumentError) { OMQ.rs(:pull, recv_rate_limit: {rate: 10}) }
  end

  private

  def track(socket)
    (@sockets ||= []) << socket
    socket
  end
end
