# frozen_string_literal: true

require_relative "test_helper"

class FeaturesTest < Minitest::Test
  ZSTD_DICT = [
    "37a430ecbeaadd5c811120841042664644444444244902002114c418638c21841042",
    "082184104208214444444444444444240900005110638c31c618630c21c418636666",
    "864692040080000000c000000000010000",
  ].join.then { |hex| [hex].pack("H*") }.freeze

  def test_compiled_features
    %i[ipc inproc plain curve lz4 zstd ws].each do |feature|
      assert OMQ::Rust.has(feature), feature
    end
    refute OMQ::Rust.has(:unknown)
  end

  def test_curve_key_generation
    public_key, secret_key = OMQ::Rust.curve_keypair

    assert_equal 40, public_key.bytesize
    assert_equal 40, secret_key.bytesize
    assert_equal public_key, OMQ::Rust.curve_public(secret_key)
    assert_raises(ArgumentError) { OMQ::Rust.curve_public("bad") }
  end

  def test_curve_keypairs_are_unique
    first = OMQ::Rust.curve_keypair
    second = OMQ::Rust.curve_keypair

    refute_equal first.first, second.first
    refute_equal first.last, second.last
  end

  def test_curve_push_pull
    server, client, = curve_pair(:pull, :push)
    endpoint = tcp_endpoint(server)
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client << "encrypted"
    assert_equal ["encrypted"], server.recv
  end

  def test_curve_multipart
    server, client, = curve_pair(:pull, :push)
    endpoint = tcp_endpoint(server)
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client.send("one", "two", "three")
    assert_equal %w[one two three], server.recv
  end

  def test_curve_req_rep
    server, client, = curve_pair(:rep, :req)
    endpoint = tcp_endpoint(server)
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client << "ping"
    assert_equal ["ping"], server.recv
    server << "pong"
    assert_equal ["pong"], client.recv
  end

  def test_curve_pub_sub
    server, client, = curve_pair(:pub, :sub)
    client.subscribe("secure.")
    endpoint = tcp_endpoint(server)
    client.connect(endpoint)
    server.wait_for_subscriber(timeout: 2)

    server.send("ignored.", "no")
    server.send("secure.", "yes")
    assert_equal ["secure.", "yes"], client.recv
  end

  def test_curve_allowlist
    server, client, client_public = curve_pair(:pull, :push)
    server.set_curve_auth([client_public])
    endpoint = tcp_endpoint(server)
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client << "allowed"
    assert_equal ["allowed"], server.recv
  end

  def test_curve_allowlist_can_reject
    server, client, = curve_pair(:pull, :push)
    other_public, = OMQ::Rust.curve_keypair
    server.set_curve_auth([other_public])
    endpoint = tcp_endpoint(server)
    client.connect(endpoint)
    client << "rejected"

    assert_raises(IO::TimeoutError) { server.recv }
  end

  def test_curve_callback_receives_key_and_identity
    captured = []
    server, client, client_public = curve_pair(:router, :dealer, identity: "ruby-client")
    server.set_curve_auth do |peer|
      captured << peer
      peer.public_key == client_public && peer.identity == "ruby-client"
    end
    endpoint = tcp_endpoint(server)
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client << "allowed"
    assert_equal ["ruby-client", "allowed"], server.recv
    assert_equal client_public, captured.first.public_key
    assert_equal "ruby-client", captured.first.identity
  end

  def test_curve_callback_can_reject
    server, client, = curve_pair(:pull, :push)
    server.set_curve_auth { false }
    endpoint = tcp_endpoint(server)
    client.connect(endpoint)
    client << "rejected"

    assert_raises(IO::TimeoutError) { server.recv }
  end

  def test_curve_auth_none_accepts_all
    server, client, = curve_pair(:pull, :push)
    server.set_curve_auth(nil)
    endpoint = tcp_endpoint(server)
    client.connect(endpoint).wait_for_peer(timeout: 2)

    client << "open"
    assert_equal ["open"], server.recv
  end

  def test_curve_auth_must_precede_materialization
    server, = curve_pair(:pull, :push)
    tcp_endpoint(server)

    assert_raises(RuntimeError) { server.set_curve_auth(nil) }
  end

  def test_curve_wrong_server_key_is_rejected
    server_public, server_secret = OMQ::Rust.curve_keypair
    wrong_server_public, = OMQ::Rust.curve_keypair
    client_public, client_secret = OMQ::Rust.curve_keypair
    pull = socket(
      :pull,
      recv_timeout: 0.25,
      curve_server: true,
      curve_publickey: server_public,
      curve_secretkey: server_secret,
    )
    push = socket(
      :push,
      curve_serverkey: wrong_server_public,
      curve_publickey: client_public,
      curve_secretkey: client_secret,
    )
    endpoint = tcp_endpoint(pull)
    push.connect(endpoint)
    push << "rejected"

    assert_raises(IO::TimeoutError) { pull.recv }
  end

  def test_curve_configuration_validation
    assert_raises(ArgumentError) { socket(:pull, curve_server: true) }

    public_key, = OMQ::Rust.curve_keypair
    _, secret_key = OMQ::Rust.curve_keypair
    assert_raises(ArgumentError) do
      socket(
        :pull,
        curve_server: true,
        curve_publickey: public_key,
        curve_secretkey: secret_key,
      )
    end
  end

  def test_plain_push_pull
    pull = socket(
      :pull,
      recv_timeout: 2,
      plain_server: true,
      plain_auth: [%w[alice secret], %w[bob hunter2]],
    )
    endpoint = tcp_endpoint(pull)
    push = socket(:push, plain_username: "bob", plain_password: "hunter2")
    push.connect(endpoint).wait_for_peer(timeout: 2)

    push << "plain"
    assert_equal ["plain"], pull.recv
  end

  def test_plain_auth_allowlist_validation
    invalid = [
      [["missing-password"]],
      [["has space", "secret"]],
      [["alice", "line\nbreak"]],
      [["alice", "\u00E9"]],
      [["x" * 256, "secret"]],
    ]
    invalid.each do |credentials|
      assert_raises(ArgumentError, TypeError) do
        socket(:pull, plain_server: true, plain_auth: credentials)
      end
    end
  end

  def test_plain_req_rep
    rep = socket(:rep, recv_timeout: 2, plain_server: true, plain_auth: [%w[alice secret]])
    endpoint = tcp_endpoint(rep)
    req = socket(
      :req,
      recv_timeout: 2,
      plain_username: "alice",
      plain_password: "secret",
    )
    req.connect(endpoint).wait_for_peer(timeout: 2)

    req << "ping"
    assert_equal ["ping"], rep.recv
    rep << "pong"
    assert_equal ["pong"], req.recv
  end

  def test_plain_pub_sub
    pub = socket(:pub, plain_server: true, plain_auth: [%w[alice secret]])
    endpoint = tcp_endpoint(pub)
    sub = socket(
      :sub,
      recv_timeout: 2,
      plain_username: "alice",
      plain_password: "secret",
    )
    sub.subscribe("plain.").connect(endpoint)
    pub.wait_for_subscriber(timeout: 2)

    pub.send("plain.", "message")
    assert_equal ["plain.", "message"], sub.recv
  end

  def test_plain_multipart
    pull = socket(
      :pull,
      recv_timeout: 2,
      plain_server: true,
      plain_auth: [%w[alice secret]],
    )
    endpoint = tcp_endpoint(pull)
    push = socket(:push, plain_username: "alice", plain_password: "secret")
    push.connect(endpoint).wait_for_peer(timeout: 2)

    push.send("a", "bb", "ccc")
    assert_equal %w[a bb ccc], pull.recv
  end

  def test_plain_server_requires_auth_policy
    pull = socket(:pull, plain_server: true)
    error = assert_raises(RuntimeError) { tcp_endpoint(pull) }
    assert_match(/explicit authentication/, error.message)
  end

  def test_plain_auth_callback_receives_credentials
    seen = []
    pull = socket(
      :pull,
      recv_timeout: 2,
      plain_server: true,
      plain_auth: proc do |peer|
        seen << [peer.username, peer.password, peer.peer_address]
        peer.username == "alice" && peer.password == "secret"
      end,
    )
    endpoint = tcp_endpoint(pull)
    push = socket(:push, plain_username: "alice", plain_password: "secret")
    push.connect(endpoint).wait_for_peer(timeout: 2)

    push << "authenticated"
    assert_equal ["authenticated"], pull.recv
    assert_equal [["alice", "secret", "127.0.0.1"]], seen
  end

  def test_zstd_custom_level_and_dictionary
    pull = socket(:pull, recv_timeout: 2)
    endpoint = pull.bind("zstd+tcp://127.0.0.1:0")
    push = socket(:push, compression_level: 1, compression_dict: ZSTD_DICT)
    push.connect(endpoint).wait_for_peer(timeout: 2)
    payload = compressed_payload(1, 4096)

    push << payload
    assert_equal [payload], pull.recv
  end

  def test_zstd_auto_training
    pull = socket(:pull, recv_timeout: 2)
    endpoint = pull.bind("zstd+tcp://127.0.0.1:0")
    push = socket(:push, compression_auto_train: true)
    push.connect(endpoint).wait_for_peer(timeout: 2)
    messages = 130.times.map { |sequence| compressed_payload(sequence) }

    messages.each { |message| push << message }
    assert_equal messages, messages.map { pull.recv.fetch(0) }
  end

  def test_lz4_transport
    pull = socket(:pull, recv_timeout: 2)
    endpoint = pull.bind("lz4+tcp://127.0.0.1:0")
    push = socket(:push)
    push.connect(endpoint).wait_for_peer(timeout: 2)
    payload = compressed_payload(1)

    push << payload
    assert_equal [payload], pull.recv
  end

  def test_compression_option_validation
    assert_raises(ArgumentError) { socket(:push, compression_level: 5) }
    assert_raises(ArgumentError) { socket(:push, compression_level: 1 << 40) }
    assert_raises(ArgumentError) { socket(:push, compression_dict: "x" * 8_193) }
  end

  def test_monitor
    pull = socket(:pull)
    monitor = pull.monitor

    assert_nil monitor.recv_nowait
    endpoint = tcp_endpoint(pull)
    assert_instance_of Integer, pull.monitor_fd
    listening = monitor.recv(timeout: 2)
    assert_equal :listening, listening[:event]
    assert_equal endpoint, listening[:endpoint]

    push = socket(:push)
    push.connect(endpoint).wait_for_peer(timeout: 2)
    handshake = loop do
      event = monitor.recv(timeout: 2)
      break event if event[:event] == :handshake_succeeded
    end
    refute_nil handshake
    assert_instance_of Integer, handshake[:connection_id]
  end

  def test_monitor_each_stops_when_socket_closes
    pull = socket(:pull)
    events = pull.monitor.each
    pull.close

    assert_raises(StopIteration) { events.next }
  end

  def test_wake_recv_makes_wait_readable_return
    pull = socket(:pull)
    pull.bind("inproc://ruby-wake-recv")

    waiter = Thread.new { pull.wait_readable(timeout: 2) }
    sleep 0.01 until waiter.status == "sleep"
    pull.wake_recv

    assert_equal true, waiter.value
    assert_nil pull.try_recv
  ensure
    pull&.close
  end

  private

  def curve_pair(server_type, client_type, identity: nil)
    server_public, server_secret = OMQ::Rust.curve_keypair
    client_public, client_secret = OMQ::Rust.curve_keypair
    server = socket(
      server_type,
      recv_timeout: 0.25,
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

  def compressed_payload(sequence, size = 1_024)
    prefix = %({"kind":"quote","symbol":"OMQ","seq":#{sequence},"pad":").b
    suffix = '"}'.b
    prefix + ("A" * (size - prefix.bytesize - suffix.bytesize)) + suffix
  end
end
