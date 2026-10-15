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

  def test_rep_prefetch_does_not_redirect_an_unanswered_request
    rep = socket(:rep, recv_timeout: 2)
    endpoint = tcp_endpoint(rep)
    first = socket(:req, recv_timeout: 2)
    second = socket(:req, recv_timeout: 2)
    [first, second].each { |client| client.connect(endpoint).wait_for_peer(timeout: 2) }

    first << "first"
    rep.wait_readable(timeout: 2)
    assert_equal ["first"], rep.recv
    second << "second"
    rep.wait_readable(timeout: 2)

    rep << "first-reply"
    assert_equal ["first-reply"], first.recv
    assert_equal ["second"], rep.recv
    rep << "second-reply"
    assert_equal ["second-reply"], second.recv
  end

  def test_rep_reply_route_is_claimed_before_next_receive
    rep = socket(:rep, recv_timeout: 2)
    endpoint = tcp_endpoint(rep)
    clients = 3.times.map { socket(:req, recv_timeout: 2) }
    clients.each { |client| client.connect(endpoint).wait_for_peer(timeout: 2) }

    30.times do |round|
      clients.each_with_index { |client, index| client << "#{round}:#{index}" }
      clients.length.times do
        request = rep.recv
        rep.send(request)
      end
      clients.each_with_index do |client, index|
        assert_equal ["#{round}:#{index}"], client.recv
      end
    end
  end

  def test_rep_discards_envelopes_without_a_body
    rep = socket(:rep, recv_timeout: 2)
    endpoint = tcp_endpoint(rep)
    dealer = socket(:dealer, recv_timeout: 2)
    dealer.connect(endpoint).wait_for_peer(timeout: 2)

    dealer.send("")
    dealer.send("route", "")
    dealer.send("route", "", "")
    assert_equal [""], rep.recv
    rep.send("reply")
    assert_equal ["route", "", "reply"], dealer.recv
  end

  def test_rep_retries_a_full_reply_without_losing_its_envelope
    count = 32
    body = "reply" * 26_000
    rep = socket(:rep, recv_timeout: 5, send_timeout: 5, send_hwm: 1, sndbuf: 65_536)
    endpoint = tcp_endpoint(rep)
    dealer = socket(:dealer, recv_timeout: 5, recv_hwm: 1, rcvbuf: 65_536)
    dealer.connect(endpoint).wait_for_peer(timeout: 2)
    count.times { |index| dealer.send(index.to_s, "", "request") }

    accepted = 0
    count.times do
      assert_equal ["request"], rep.recv
      break unless rep.try_send(body)

      accepted += 1
    end
    assert_operator accepted, :<, count, "REP never reached backpressure"

    receiver = Thread.new do
      count.times do |index|
        assert_equal [index.to_s, "", body], dealer.recv
      end
    end
    rep.send(body)
    (count - accepted - 1).times do
      assert_equal ["request"], rep.recv
      rep.send(body)
    end
    assert receiver.join(10), "replies did not drain"
    receiver.value
  ensure
    receiver&.kill
    receiver&.join
  end

  def test_req_discards_replies_from_an_unselected_router
    req = socket(:req, identity: "requester", recv_timeout: 2)
    endpoint = tcp_endpoint(req)
    routers = 2.times.map { socket(:router, recv_timeout: 2) }
    routers.each { |router| router.connect(endpoint).wait_for_peer(timeout: 2) }
    req.send("request")
    selected = nil
    Timeout.timeout(2) do
      until selected
        selected = routers.find { |router| router.try_recv }
        Thread.pass unless selected
      end
    end
    unrelated = (routers - [selected]).first
    unrelated.send("requester", "", "unrelated")
    req.wait_readable(timeout: 2)
    assert_nil req.try_recv
    assert_raises(RuntimeError) { req.try_send("another request") }

    selected.send("requester", "", "reply")
    assert_equal ["reply"], req.recv
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
