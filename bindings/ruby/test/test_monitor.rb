# frozen_string_literal: true

require_relative "test_helper"

class MonitorTest < Minitest::Test
  def test_monitor_subscribes_before_bind
    64.times do
      OMQ.rs(:pull, linger: 0) do |pull|
        monitor = pull.monitor
        endpoint = pull.bind("tcp://127.0.0.1:0")
        event = monitor.recv(timeout: 2)
        assert_equal :listening, event.fetch(:event)
        assert_equal endpoint, event.fetch(:endpoint)
      end
    end
  end

  def test_event_published_after_empty_poll_wakes_receiver
    published = false
    event = { event: :listening }
    on_poll = lambda do |events, writer, armed|
      unless published
        published = true
        events << event
        writer.write("x") if armed
      end
    end

    with_monitor_pipe(on_poll: on_poll) do |monitor|
      assert_equal event, monitor.recv(timeout: 0.05)
    end
  end

  def test_stale_notification_does_not_end_receive
    event = { event: :handshake_succeeded }
    arms = 0
    on_arm = lambda do |events, writer|
      arms += 1
      events << event if arms == 2
      writer.write("x")
    end

    with_monitor_pipe(on_arm: on_arm) do |monitor|
      assert_equal event, monitor.recv(timeout: 0.05)
    end
  end

  def test_stale_notifications_preserve_receive_deadline
    on_arm = ->(_events, writer) { writer.write("x") }

    with_monitor_pipe(on_arm: on_arm) do |monitor|
      assert_raises(IO::TimeoutError) { monitor.recv(timeout: 0.02) }
    end
  end

  private

  # Publication and pipe signaling are separate operations in the native pump.
  # Exercise their ordering through the receive API with a real readiness FD.
  def with_monitor_pipe(on_arm: nil, on_poll: nil)
    pull = socket(:pull)
    monitor = pull.monitor
    original = pull.instance_variable_get(:@native)
    reader, writer = IO.pipe
    events = []
    armed = false
    native = Object.new
    native.define_singleton_method(:closed?) { false }
    native.define_singleton_method(:monitor_fd) do
      armed = true
      on_arm&.call(events, writer)
      reader.fileno
    end
    native.define_singleton_method(:try_recv_monitor) do
      event = events.shift
      on_poll&.call(events, writer, armed)
      event
    end
    pull.instance_variable_set(:@native, native)
    yield monitor
  ensure
    pull&.instance_variable_set(:@native, original) if original
    reader&.close
    writer&.close
  end
end
