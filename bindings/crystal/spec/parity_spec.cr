require "socket"
require "./spec_helper"

describe "OMQ.cr parity" do
  it "exports all supported socket types" do
    expected = {
      "pair"    => OMQ::PAIR,
      "pub"     => OMQ::PUB,
      "sub"     => OMQ::SUB,
      "req"     => OMQ::REQ,
      "rep"     => OMQ::REP,
      "dealer"  => OMQ::DEALER,
      "router"  => OMQ::ROUTER,
      "pull"    => OMQ::PULL,
      "push"    => OMQ::PUSH,
      "xpub"    => OMQ::XPUB,
      "xsub"    => OMQ::XSUB,
      "stream"  => OMQ::STREAM,
      "server"  => OMQ::SERVER,
      "client"  => OMQ::CLIENT,
      "radio"   => OMQ::RADIO,
      "dish"    => OMQ::DISH,
      "gather"  => OMQ::GATHER,
      "scatter" => OMQ::SCATTER,
      "dgram"   => OMQ::DGRAM,
      "peer"    => OMQ::PEER,
      "channel" => OMQ::CHANNEL,
    }
    OMQ::SOCKET_TYPES.should eq(expected)

    ctx = OMQ.context
    sockets = [] of OMQ::Socket
    expected.each do |name, socket_type|
      if name == "dgram"
        expect_raises(OMQ::Error) { ctx.socket(name) }
      else
        socket = ctx.socket(name, linger: 0)
        socket.type.should eq(socket_type)
        sockets << socket
      end
    end
    sockets.each(&.close)
    ctx.term
  end

  it "roundtrips generic and convenience socket options" do
    ctx = OMQ.context
    socket = ctx.socket(
      "dealer",
      linger: 0,
      send_timeout: 11,
      recv_timeout: 12,
      send_hwm: 13,
      recv_hwm: 14,
      arena_threshold: 2048,
      identity: "dealer-opt",
      reconnect_interval: 15,
      reconnect_interval_max: 16,
      heartbeat_interval: 17,
      heartbeat_ttl: 18,
      heartbeat_timeout: 19,
      handshake_interval: 20,
      max_message_size: 21_i64,
      router_mandatory: true,
      conflate: true,
      tcp_keepalive: 1,
      tcp_keepalive_count: 2,
      tcp_keepalive_idle: 3,
      tcp_keepalive_interval: 4,
      send_buffer: 1024,
      recv_buffer: 2048,
      xpub_verbose: true,
      xpub_nodrop: true,
      ipv6: true,
      immediate: true,
      backlog: 32,
      connect_timeout: 33,
      probe_router: true,
      req_correlate: true,
      req_relaxed: true,
      router_handover: true,
      reconnect_stop: OMQ::RECONNECT_STOP_CONN_REFUSED,
      plain_username: "alice",
      plain_password: "secret",
      wss_key_pem: "key",
      wss_cert_pem: "cert",
      wss_trust_pem: "trust",
      wss_hostname: "example.test",
      wss_trust_system: false
    )

    socket.get_option_i32(OMQ::LINGER).should eq(0)
    socket.get_option_i32(OMQ::SNDTIMEO).should eq(11)
    socket.get_option_i32(OMQ::RCVTIMEO).should eq(12)
    socket.get_option_i32(OMQ::SNDHWM).should eq(13)
    socket.get_option_i32(OMQ::RCVHWM).should eq(14)
    socket.get_arena_threshold.should eq(2048)
    socket.get_identity.should eq("dealer-opt")
    socket.get_option_i32(OMQ::RECONNECT_IVL).should eq(15)
    socket.get_option_i32(OMQ::RECONNECT_IVL_MAX).should eq(16)
    socket.get_option_i32(OMQ::HEARTBEAT_IVL).should eq(17)
    socket.get_option_i32(OMQ::HEARTBEAT_TTL).should eq(18)
    socket.get_option_i32(OMQ::HEARTBEAT_TIMEOUT).should eq(19)
    socket.get_option_i32(OMQ::HANDSHAKE_IVL).should eq(20)
    socket.get_option_i64(OMQ::MAXMSGSIZE).should eq(21)
    socket.get_option_i32(OMQ::ROUTER_MANDATORY).should eq(1)
    socket.get_option_i32(OMQ::CONFLATE).should eq(1)
    socket.get_option_i32(OMQ::TCP_KEEPALIVE).should eq(1)
    socket.get_option_i32(OMQ::TCP_KEEPALIVE_CNT).should eq(2)
    socket.get_option_i32(OMQ::TCP_KEEPALIVE_IDLE).should eq(3)
    socket.get_option_i32(OMQ::TCP_KEEPALIVE_INTVL).should eq(4)
    socket.get_option_i32(OMQ::SNDBUF).should eq(1024)
    socket.get_option_i32(OMQ::RCVBUF).should eq(2048)
    socket.get_option_i32(OMQ::XPUB_VERBOSE).should eq(1)
    socket.get_option_i32(OMQ::XPUB_NODROP).should eq(1)
    socket.get_option_i32(OMQ::IPV6).should eq(1)
    socket.get_option_i32(OMQ::IMMEDIATE).should eq(1)
    socket.get_option_i32(OMQ::BACKLOG).should eq(32)
    socket.get_option_i32(OMQ::CONNECT_TIMEOUT).should eq(33)
    socket.get_option_i32(OMQ::PROBE_ROUTER).should eq(1)
    socket.get_option_i32(OMQ::REQ_CORRELATE).should eq(1)
    socket.get_option_i32(OMQ::REQ_RELAXED).should eq(1)
    socket.get_option_i32(OMQ::ROUTER_HANDOVER).should eq(1)
    socket.get_option_i32(OMQ::RECONNECT_STOP).should eq(OMQ::RECONNECT_STOP_CONN_REFUSED)
    socket.get_option_i32(OMQ::MECHANISM).should eq(OMQ::PLAIN_MECHANISM)
    socket.get_option_string(OMQ::PLAIN_USERNAME).should eq("alice")
    socket.get_option_string(OMQ::PLAIN_PASSWORD).should eq("secret")
    socket.get_option_bytes(OMQ::WSS_KEY_PEM).should eq("key".to_slice)
    socket.get_option_bytes(OMQ::WSS_CERT_PEM).should eq("cert".to_slice)
    socket.get_option_bytes(OMQ::WSS_TRUST_PEM).should eq("trust".to_slice)
    socket.get_option_string(OMQ::WSS_HOSTNAME).should eq("example.test")
    socket.get_option_i32(OMQ::WSS_TRUST_SYSTEM).should eq(0)

    keypair = OMQ.curve_keypair
    socket.set_curve_client(keypair, keypair.public_key)
    socket.get_option_i32(OMQ::MECHANISM).should eq(OMQ::CURVE_MECHANISM)
    socket.get_option_string(OMQ::CURVE_PUBLICKEY, 41).should eq(keypair.public_key)
    socket.get_option_string(OMQ::CURVE_SECRETKEY, 41).should eq(keypair.secret_key)
    socket.get_option_string(OMQ::CURVE_SERVERKEY, 41).should eq(keypair.public_key)

    socket.close
    ctx.term
  end

  it "supports dealer/router and client/server routing" do
    ctx = OMQ.context

    router = ctx.socket("router", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    dealer = ctx.socket("dealer", linger: 0, recv_timeout: 1000, send_timeout: 1000, identity: "dealer-1")
    endpoint = router.bind(unique_endpoint("dealer-router"))
    dealer.connect(endpoint)
    dealer.send("hello")
    request = router.recv_parts
    request.should eq(["dealer-1", "hello"])
    router.send_parts([request[0], "world"])
    dealer.recv.should eq("world")
    dealer.close
    router.close

    server = ctx.socket("server", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    client = ctx.socket("client", linger: 0, recv_timeout: 1000, send_timeout: 1000, identity: "client-1")
    endpoint = server.bind(unique_endpoint("client-server"))
    client.connect(endpoint)
    client.send("ping")
    request = server.recv_parts
    request.should eq(["client-1", "ping"])
    server.send_parts([request[0], "pong"])
    client.recv.should eq("pong")
    client.close
    server.close

    ctx.term
  end

  it "supports req/rep and xpub/xsub" do
    ctx = OMQ.context

    rep = ctx.socket("rep", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    req = ctx.socket("req", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    endpoint = rep.bind(unique_endpoint("req-rep"))
    req.connect(endpoint)
    req.send("ping")
    rep.recv.should eq("ping")
    rep.send("pong")
    req.recv.should eq("pong")
    req.close
    rep.close

    xpub = ctx.socket("xpub", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    xsub = ctx.socket("xsub", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    endpoint = xpub.bind(unique_endpoint("xpub-xsub"))
    xsub.connect(endpoint)
    xsub.send(Bytes[1] + "topic".to_slice)
    subscription = xpub.recv_bytes
    subscription.should eq(Bytes[1] + "topic".to_slice)
    xpub.send("topic-data")
    xsub.recv.should eq("topic-data")
    xsub.close
    xpub.close

    ctx.term
  end

  it "supports scatter/gather, pair, channel, and peer" do
    ctx = OMQ.context

    gather = ctx.socket("gather", linger: 0, recv_timeout: 1000)
    scatter = ctx.socket("scatter", linger: 0, send_timeout: 1000)
    endpoint = gather.bind(unique_endpoint("scatter-gather"))
    scatter.connect(endpoint)
    scatter.send("work")
    gather.recv.should eq("work")
    scatter.close
    gather.close

    pair_a = ctx.socket("pair", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    pair_b = ctx.socket("pair", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    endpoint = pair_a.bind(unique_endpoint("pair"))
    pair_b.connect(endpoint)
    pair_a.send("one")
    pair_b.recv.should eq("one")
    pair_b.send("two")
    pair_a.recv.should eq("two")
    pair_b.close
    pair_a.close

    channel_a = ctx.socket("channel", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    channel_b = ctx.socket("channel", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    endpoint = channel_a.bind(unique_endpoint("channel"))
    channel_b.connect(endpoint)
    channel_a.send("left")
    channel_b.recv.should eq("left")
    channel_b.send("right")
    channel_a.recv.should eq("right")
    channel_b.close
    channel_a.close

    peer_a = ctx.socket("peer", linger: 0, recv_timeout: 1000, send_timeout: 1000, identity: "peer-a")
    peer_b = ctx.socket("peer", linger: 0, recv_timeout: 1000, send_timeout: 1000, identity: "peer-b")
    endpoint = peer_a.bind(unique_endpoint("peer"))
    peer_b.connect(endpoint)
    sleep 100.milliseconds
    peer_b.send_parts(["peer-a", "hello a"])
    peer_a.recv_parts.should eq(["peer-b", "hello a"])
    peer_a.send_parts(["peer-b", "hello b"])
    peer_b.recv_parts.should eq(["peer-a", "hello b"])
    peer_b.close
    peer_a.close

    ctx.term
  end

  it "supports radio/dish group helpers" do
    ctx = OMQ.context
    radio = ctx.socket("radio", linger: 0, send_timeout: 1000)
    dish = ctx.socket("dish", linger: 0, recv_timeout: 1000)

    endpoint = radio.bind(unique_endpoint("radio-dish"))
    dish.join("weather")
    dish.connect(endpoint)
    sleep 50.milliseconds
    radio.send_group("news", "ignored")
    radio.send_group("weather", "sunny")
    dish.recv.should eq("sunny")

    dish.leave("weather")
    sleep 50.milliseconds
    radio.send_group("weather", "rain")
    expect_raises(OMQ::Again) { dish.recv }

    dish.close
    radio.close
    ctx.term
  end

  it "rejects multipart on single-part sockets" do
    ctx = OMQ.context
    {"client" => OMQ::CLIENT, "scatter" => OMQ::SCATTER, "channel" => OMQ::CHANNEL}.each do |name, socket_type|
      socket = ctx.socket(socket_type, linger: 0, send_timeout: 0)
      socket.bind(unique_endpoint("single-#{name}"))
      expect_raises(OMQ::Error) { socket.send_parts(["a", "b"]) }
      socket.close
    end

    radio = ctx.socket("radio", linger: 0, send_timeout: 0)
    radio.bind(unique_endpoint("radio-missing-group"))
    expect_raises(OMQ::Error) { radio.send("missing-group") }
    radio.close

    pull = ctx.socket("pull", linger: 0)
    pull.bind(unique_endpoint("join-wrong-type"))
    expect_raises(OMQ::Error) { pull.join("g") }
    pull.close
    ctx.term
  end

  it "supports poll and poller wrappers" do
    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
    push = ctx.socket("push", linger: 0, send_timeout: 1000)
    endpoint = pull.bind(unique_endpoint("poll"))
    push.connect(endpoint)
    push.send("ready")

    items = [OMQ::PollItem.new(pull, OMQ::POLLIN)]
    OMQ.poll(items, 1000).should eq(1)
    items[0].readable?.should be_true
    pull.recv.should eq("ready")

    poller = OMQ::Poller.new
    poller.add(pull)
    push.send("again")
    event = poller.wait(1000)
    event.should_not be_nil
    event.not_nil!.socket.should eq(pull)
    event.not_nil!.readable?.should be_true
    pull.recv.should eq("again")
    poller.size.should eq(1)
    poller.close

    push.close
    pull.close
    ctx.term
  end

  it "supports stream raw TCP sockets" do
    ctx = OMQ.context
    stream = ctx.socket("stream", linger: 0, recv_timeout: 1000, send_timeout: 1000)
    endpoint = stream.bind("tcp://127.0.0.1:*")
    address = endpoint.sub("tcp://", "")
    host, port = address.split(":")
    raw = TCPSocket.new(host, port.to_i)
    raw.write("hello".to_slice)
    raw.flush

    connected = stream.recv_parts_bytes
    identity = connected[0]
    connected.size.should eq(2)
    connected[1].empty?.should be_true
    data = stream.recv_parts_bytes
    data[0].should eq(identity)
    String.new(data[1]).should eq("hello")

    stream.send_parts([identity, "world".to_slice])
    reply = Bytes.new(5)
    raw.read_fully(reply)
    String.new(reply).should eq("world")

    raw.close
    stream.close
    ctx.term
  end

  it "exposes curve, z85, monitor, peer, and bind lifecycle helpers" do
    version = OMQ.version
    version[0].should eq(4)
    OMQ.has("curve").should be_true

    encoded = OMQ.z85_encode(Bytes[1, 2, 3, 4])
    OMQ.z85_decode(encoded).should eq(Bytes[1, 2, 3, 4])
    keypair = OMQ.curve_keypair
    OMQ.curve_public(keypair.secret_key).should eq(keypair.public_key)

    counter = OMQ::LibZMQ.atomic_counter_new
    counter.null?.should be_false
    OMQ::LibZMQ.atomic_counter_set(counter, 1)
    OMQ::LibZMQ.atomic_counter_inc(counter).should eq(1)
    OMQ::LibZMQ.atomic_counter_value(counter).should eq(2)
    OMQ::LibZMQ.atomic_counter_dec(counter).should eq(1)
    counter_slot = counter
    OMQ::LibZMQ.atomic_counter_destroy(pointerof(counter_slot))
    counter_slot.null?.should be_true

    timers = OMQ::LibZMQ.timers_new
    timers.null?.should be_false
    OMQ::LibZMQ.timers_timeout(timers).should eq(-1)
    timer_id = OMQ::LibZMQ.timers_add(timers, 0, ->(_id : LibC::Int, _arg : Void*) { }, Pointer(Void).null)
    timer_id.should be >= 0
    OMQ::LibZMQ.timers_execute(timers).should eq(0)
    timers_slot = timers
    OMQ::LibZMQ.timers_destroy(pointerof(timers_slot)).should eq(0)
    timers_slot.null?.should be_true

    msg = Bytes.new(OMQ::ZMQ_MSG_T_SIZE)
    bytes = "owned-by-crystal".to_slice
    OMQ::LibZMQ.msg_init_data(
      msg.to_unsafe.as(Void*),
      bytes.to_unsafe.as(Void*),
      bytes.size,
      nil,
      Pointer(Void).null
    ).should eq(0)
    OMQ::LibZMQ.msg_close(msg.to_unsafe.as(Void*)).should eq(0)

    watch = OMQ::LibZMQ.stopwatch_start
    watch.null?.should be_false
    OMQ::LibZMQ.stopwatch_intermediate(watch).should be >= 0
    OMQ::LibZMQ.stopwatch_stop(watch).should be >= 0

    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0)
    endpoint = pull.bind(unique_endpoint("unbind"))
    pull.unbind(endpoint).should be_true
    pull.bind(endpoint).should eq(endpoint)

    push = ctx.socket("push", linger: 0)
    push.connect(endpoint).should be_true
    push.disconnect(endpoint).should be_true
    expect_raises(OMQ::Error) { push.connect_peer(endpoint) }
    expect_raises(OMQ::Error) { push.disconnect_peer(1_u32) }
    expect_raises(OMQ::Error) { push.monitor_versioned(unique_endpoint("monitor-v2"), event_version: OMQ::CURRENT_EVENT_VERSION_DRAFT) }
    expect_raises(OMQ::Error) { push.monitor_pipes_stats }
    expect_raises(OMQ::Error) { push.peer_state("peer".to_slice) }

    push.close
    pull.close

    shared = OMQ.context_from_share_key(ctx.share_key)
    shared.get(OMQ::IO_THREADS).should eq(1)
    shared.get_ext_i32(OMQ::IO_THREADS).should eq(1)
    shared.set_string(OMQ::THREAD_NAME_PREFIX, "crystal").should be_true
    shared.get_ext_string(OMQ::THREAD_NAME_PREFIX).should eq("")
    shared.term
    ctx.term
  end
end

private def unique_endpoint(name : String) : String
  "inproc://crystal-parity-#{name}-#{Random.rand(1_000_000_000)}"
end
