# frozen_string_literal: true

require_relative "test_helper"

class RactorTest < Minitest::Test
  def setup
    skip "Ruby 4.0+ Ractor API required" if Gem::Version.new(RUBY_VERSION) < Gem::Version.new("4.0")
  end

  def test_socket_construction_inside_ractor
    result = Ractor.new do
      OMQ.rs(:pull, linger: 0).close
      :ok
    end.value

    assert_equal :ok, result
  end

  def test_io_thread_configuration_inside_ractor
    expected = OMQ::Rust.io_threads
    result = Ractor.new do
      current = OMQ::Rust.io_threads
      OMQ::Rust.io_threads = current
      OMQ::Rust.io_threads
    end.value

    assert_equal expected, result
  end

  def test_tcp_req_rep_with_ractor_owned_socket
    rep = socket(:rep, recv_timeout: 2)
    endpoint = tcp_endpoint(rep)
    worker = Ractor.new(endpoint) do |address|
      req = OMQ.rs(:req, linger: 0, recv_timeout: 2)
      req.connect(address).wait_for_peer(timeout: 2)
      req << "from-ractor"
      response = req.recv
      req.close
      response
    end

    assert_equal ["from-ractor"], rep.recv
    rep << "from-main"
    assert_equal ["from-main"], worker.value
  end

  def test_inproc_req_rep_across_ractors
    rep = socket(:rep, recv_timeout: 2)
    endpoint = "inproc://omq-rs-ractor-#{object_id}"
    rep.bind(endpoint)
    worker = Ractor.new(endpoint) do |address|
      req = OMQ.rs(:req, linger: 0, recv_timeout: 2)
      req.connect(address).wait_for_peer(timeout: 2)
      req << "inproc-ractor"
      response = req.recv
      req.close
      response
    end

    assert_equal ["inproc-ractor"], rep.recv
    rep << "inproc-main"
    assert_equal ["inproc-main"], worker.value
  end

  def test_concurrent_ractors_share_runtime
    server = socket(:server, recv_timeout: 2)
    endpoint = tcp_endpoint(server)
    workers = 4.times.map do |id|
      Ractor.new(endpoint, id) do |address, worker_id|
        client = OMQ.rs(:client, linger: 0, recv_timeout: 2)
        client.connect(address).wait_for_peer(timeout: 2)
        client << worker_id.to_s
        response = client.recv.fetch(0)
        client.close
        response
      end
    end

    4.times do
      routing_id, worker_id = server.recv
      server.send(routing_id, "ack-#{worker_id}")
    end

    assert_equal %w[ack-0 ack-1 ack-2 ack-3], workers.map(&:value).sort
  end
end
