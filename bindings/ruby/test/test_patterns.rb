# frozen_string_literal: true

require_relative "test_helper"

class PatternsTest < Minitest::Test
  def test_push_pull_single_and_multipart
    pull = socket(:pull, recv_timeout: 2)
    endpoint = tcp_endpoint(pull)
    push = socket(:push)
    push.connect(endpoint).wait_for_peer(timeout: 2)
    assert_same push, push.wait_for_peer(timeout: 0)

    push << "one"
    push.send("two", "three")

    assert_equal ["one"], pull.recv
    assert_equal ["two", "three"], pull.recv
  end

  def test_req_rep
    rep = socket(:rep, recv_timeout: 2)
    endpoint = tcp_endpoint(rep)
    req = socket(:req, recv_timeout: 2)
    req.connect(endpoint).wait_for_peer(timeout: 2)

    req << "question"
    assert_equal ["question"], rep.recv
    rep << "answer"
    assert_equal ["answer"], req.recv
  end

  def test_req_rep_enforces_alternation
    req = socket(:req)
    rep = socket(:rep)

    assert req.try_send("first")
    assert_raises(RuntimeError) { req.try_send("second") }
    assert_raises(RuntimeError) { rep.try_send("early") }
  end

  def test_pub_sub
    pub = socket(:pub)
    endpoint = tcp_endpoint(pub)
    sub = socket(:sub, recv_timeout: 2)
    sub.subscribe("weather.").connect(endpoint)
    pub.wait_for_subscriber(timeout: 2)
    assert_same pub, pub.wait_for_subscriber(timeout: 0)

    pub.send("news.world", "ignored")
    pub.send("weather.ch", "sunny")

    assert_equal ["weather.ch", "sunny"], sub.recv
  end

  def test_client_server_routing
    server = socket(:server, recv_timeout: 2)
    endpoint = tcp_endpoint(server)
    client = socket(:client, recv_timeout: 2, identity: "ruby-client")
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client << "request"
    routing_id, body = server.recv
    assert_instance_of Integer, routing_id
    assert_equal "request", body

    peer = server.peer_info(routing_id)
    assert_equal "ruby-client", peer[:peer_identity]
    assert_equal :client, peer[:socket_type]
    assert_equal [3, 1], peer[:zmtp_version]
    assert_match(/127\.0\.0\.1:/, peer[:peer_address])
    assert_nil server.peer_info(0xffff_ffff)

    server.send(routing_id, "reply")
    assert_equal ["reply"], client.recv
  end

  def test_peer_info_is_server_only
    assert_raises(RuntimeError) { socket(:pair).peer_info(1) }
  end

  def test_radio_dish
    radio = socket(:radio)
    endpoint = tcp_endpoint(radio)
    dish = socket(:dish, recv_timeout: 2)
    dish.join("weather").connect(endpoint)
    radio.wait_for_subscriber(timeout: 2)

    radio.publish("news", "ignored")
    radio.publish("weather", "rain")

    assert_equal ["weather", "rain"], dish.recv
  end

  def test_scatter_gather
    gather = socket(:gather, recv_timeout: 2)
    endpoint = tcp_endpoint(gather)
    scatter = socket(:scatter)
    scatter.connect(endpoint).wait_for_peer(timeout: 2)

    scatter << "work"
    assert_equal ["work"], gather.recv
    assert_raises(ArgumentError) { scatter.send("bad", "multipart") }
  end

  def test_peer_identity_routing
    first = socket(:peer, identity: "first", recv_timeout: 2)
    endpoint = tcp_endpoint(first)
    second = socket(:peer, identity: "second", recv_timeout: 2)
    second.connect(endpoint).wait_for_peer(timeout: 2)

    second.send("first", "hello")
    assert_equal ["second", "hello"], first.recv

    first.send("second", "reply")
    assert_equal ["first", "reply"], second.recv
  end

  def test_inproc
    pull = socket(:pull, recv_timeout: 2)
    pull.bind("inproc://omq-rs-test-#{object_id}")
    push = socket(:push)
    push.connect("inproc://omq-rs-test-#{object_id}")

    push << "local"
    assert_equal ["local"], pull.recv
  end

  def test_try_send_and_try_recv
    pull = socket(:pull)
    endpoint = tcp_endpoint(pull)
    push = socket(:push)
    push.connect(endpoint).wait_for_peer(timeout: 2)

    assert_nil pull.try_recv
    assert push.try_send("ready")

    Timeout.timeout(2) do
      Thread.pass until (message = pull.try_recv)
      assert_equal ["ready"], message
    end
  end

  def test_low_hwm_send_readiness_rearms
    pull = socket(:pull, recv_timeout: 2, recv_hwm: 2)
    endpoint = tcp_endpoint(pull)
    push = socket(:push, send_timeout: 2, send_hwm: 2)
    push.connect(endpoint).wait_for_peer(timeout: 2)

    sender = Thread.new { 2_000.times { push << "message" } }
    2_000.times { assert_equal ["message"], pull.recv }

    assert sender.join(2), "sender did not finish"
  end

  def test_receive_timeout
    pull = socket(:pull, recv_timeout: 0.02)

    assert_raises(IO::TimeoutError) { pull.recv }
  end

  def test_close_wakes_receive
    pull = socket(:pull)
    result = Thread.new do
      pull.recv
    rescue StandardError => error
      error
    end

    sleep 0.02
    pull.close

    assert_instance_of IOError, result.value
  end

  def test_close_interrupts_peer_wait
    push = socket(:push)
    push.connect("tcp://127.0.0.1:1")

    waiter = Thread.new do
      push.wait_for_peer(timeout: 2)
    rescue StandardError => error
      error
    end
    sleep 0.01 until waiter.status == "sleep"
    push.close

    error = waiter.value
    assert_instance_of IOError, error
    assert_equal "socket closed", error.message
  end
end
