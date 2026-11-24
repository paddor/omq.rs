"""pyomq.auth: pyzmq's zmq.auth API on native handshake callbacks."""

import asyncio
import contextlib
import errno
import logging
import os
import sys

import pyomq as zmq
import pyomq.asyncio
import pytest
from pyomq.auth import (
    CURVE_ALLOW_ANY,
    Authenticator,
    create_certificates,
    load_certificate,
    load_certificates,
)
from pyomq.auth.asyncio import AsyncioAuthenticator
from pyomq.auth.thread import ThreadAuthenticator
from pyomq.utils import z85

needs_curve = pytest.mark.skipif(not zmq.has("curve"), reason="curve not compiled")
needs_plain = pytest.mark.skipif(not zmq.has("plain"), reason="plain not compiled")


@pytest.fixture
def ctx():
    context = zmq.Context()
    yield context
    context.destroy(linger=0)


@pytest.fixture
def auth(ctx):
    authenticator = ThreadAuthenticator(ctx)
    authenticator.start()
    yield authenticator
    authenticator.stop()


def handshake(monitor):
    """True when the server admits the next client, False when it refuses."""
    while True:
        event = monitor.recv(timeout_ms=5000)["event"]
        if event == "handshake_succeeded":
            return True
        if event == "handshake_failed":
            return False


def curve_server(ctx, keys, socket_type=zmq.PULL):
    public, secret = keys
    server = ctx.socket(socket_type)
    server.curve_publickey = public
    server.curve_secretkey = secret
    server.curve_server = 1
    return server


def curve_client(ctx, server_public, keys, socket_type=zmq.PUSH):
    public, secret = keys
    client = ctx.socket(socket_type)
    client.linger = 0
    client.curve_serverkey = server_public
    client.curve_publickey = public
    client.curve_secretkey = secret
    return client


def admits(ctx, server, make_client):
    """Bind `server`, connect one client, and report the server's decision."""
    monitor = server.monitor()
    server.bind("tcp://127.0.0.1:0")
    client = make_client()
    client.connect(server.last_endpoint)
    return handshake(monitor)


def curve_admits(ctx, server_keys, client_keys, configure=None):
    server = curve_server(ctx, server_keys)
    if configure:
        configure(server)
    return admits(ctx, server, lambda: curve_client(ctx, server_keys[0], client_keys))


def null_admits(ctx, zap_domain=b""):
    server = ctx.socket(zmq.PULL)
    server.zap_domain = zap_domain
    return admits(ctx, server, lambda: ctx.socket(zmq.PUSH))


def plain_admits(ctx, username, password):
    server = ctx.socket(zmq.PULL)
    server.plain_server = 1

    def client():
        socket = ctx.socket(zmq.PUSH)
        socket.linger = 0
        socket.plain_username = username
        socket.plain_password = password
        return socket

    return admits(ctx, server, client)


# ── z85 ──────────────────────────────────────────────────────────────


def test_z85_matches_rfc_32_vector():
    raw = bytes([0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B])
    assert z85.encode(raw) == b"HelloWorld"
    assert z85.decode(b"HelloWorld") == raw
    assert z85.decode("HelloWorld") == raw


def test_z85_matches_pyzmq():
    pyzmq_z85 = pytest.importorskip("zmq.utils.z85")
    for raw in (b"", os.urandom(4), os.urandom(32), b"\xff" * 8):
        assert z85.encode(raw) == pyzmq_z85.encode(raw)
        assert z85.decode(pyzmq_z85.encode(raw)) == raw


@pytest.mark.parametrize(
    ("call", "value"),
    [
        (z85.encode, b"abc"),
        (z85.decode, b"abcd"),
        (z85.decode, b"abc~d"),
        (z85.decode, b"#####"),
        (z85.decode, "HellöWorld"),
    ],
)
def test_z85_rejects_malformed_input(call, value):
    with pytest.raises(ValueError):
        call(value)


# ── Certificates ─────────────────────────────────────────────────────


@needs_curve
def test_certificates_round_trip(tmp_path):
    public_file, secret_file = create_certificates(tmp_path, "alice", {"name": "a"})
    assert public_file == str(tmp_path / "alice.key")
    assert secret_file == str(tmp_path / "alice.key_secret")

    public, secret = load_certificate(secret_file)
    assert secret is not None
    assert zmq.curve_public(secret) == public
    assert load_certificate(public_file) == (public, None)
    assert load_certificates(tmp_path) == {public: True}
    if sys.platform != "win32":
        assert os.stat(secret_file).st_mode & 0o777 == 0o600


@needs_curve
def test_certificates_interoperate_with_pyzmq(tmp_path):
    pyzmq_certs = pytest.importorskip("zmq.auth.certs")
    _, ours = create_certificates(tmp_path, "ours")
    _, theirs = pyzmq_certs.create_certificates(tmp_path, "theirs")
    assert pyzmq_certs.load_certificate(ours) == load_certificate(ours)
    assert load_certificate(theirs) == pyzmq_certs.load_certificate(theirs)
    assert load_certificates(tmp_path) == pyzmq_certs.load_certificates(tmp_path)


@needs_curve
def test_certificate_overwrite_restricts_the_secret_file(tmp_path):
    if sys.platform == "win32":
        pytest.skip("POSIX file modes")
    secret_file = tmp_path / "alice.key_secret"
    secret_file.write_text("old")
    secret_file.chmod(0o644)
    create_certificates(tmp_path, "alice")
    assert os.stat(secret_file).st_mode & 0o777 == 0o600


def test_certificate_parsing_matches_key_names_exactly(tmp_path):
    key = "rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7"
    cert = tmp_path / "x.key"
    cert.write_text(
        '#   public-key = "commented"\n'
        "metadata\n"
        '    public-key-hint = "not a key"\n'
        "curve\n"
        f'    public-key = "{key}"\n'
    )
    assert load_certificate(cert) == (key.encode(), None)
    cert.write_text(f'curve\n    public-key = "{key[:-1]}="\n')
    assert load_certificate(cert) == (key[:-1].encode() + b"=", None)


def test_certificate_errors(tmp_path):
    with pytest.raises(OSError):
        load_certificate(tmp_path / "missing.key")
    with pytest.raises(OSError):
        load_certificates(tmp_path / "missing")
    empty = tmp_path / "empty.key"
    empty.write_text("metadata\ncurve\n")
    with pytest.raises(ValueError):
        load_certificate(empty)


# ── CURVE ────────────────────────────────────────────────────────────


@needs_curve
def test_curve_admits_keys_from_certificate_directory(ctx, auth, tmp_path):
    create_certificates(tmp_path, "client")
    client_keys = load_certificate(tmp_path / "client.key_secret")
    auth.configure_curve(location=tmp_path)
    server_keys = zmq.curve_keypair()

    assert curve_admits(ctx, server_keys, client_keys)
    assert not curve_admits(ctx, server_keys, zmq.curve_keypair())


@needs_curve
def test_curve_rejects_every_client_until_configured(ctx, auth):
    assert not curve_admits(ctx, zmq.curve_keypair(), zmq.curve_keypair())


@needs_curve
def test_curve_allow_any(ctx, auth):
    auth.configure_curve(location=CURVE_ALLOW_ANY)
    assert curve_admits(ctx, zmq.curve_keypair(), zmq.curve_keypair())


@needs_curve
def test_curve_callback_receives_domain_and_z85_key(ctx, auth):
    client_keys = zmq.curve_keypair()
    seen = []

    class Provider:
        def callback(self, domain, key):
            seen.append((domain, key))
            return key == client_keys[0]

    auth.configure_curve_callback("global", Provider())
    server_keys = zmq.curve_keypair()

    def in_global(server):
        server.zap_domain = b"global"

    assert curve_admits(ctx, server_keys, client_keys, in_global)
    assert not curve_admits(ctx, server_keys, zmq.curve_keypair(), in_global)
    assert seen[0] == ("global", client_keys[0])


@needs_curve
def test_curve_domain_selects_certificates(ctx, auth, tmp_path):
    create_certificates(tmp_path, "client")
    client_keys = load_certificate(tmp_path / "client.key_secret")
    auth.configure_curve(domain="global", location=tmp_path)
    server_keys = zmq.curve_keypair()

    def in_domain(domain):
        return lambda server: setattr(server, "zap_domain", domain)

    assert curve_admits(ctx, server_keys, client_keys, in_domain(b"global"))
    assert not curve_admits(ctx, server_keys, client_keys, in_domain(b"other"))
    # An empty domain means "*", which has no certificates here.
    assert not curve_admits(ctx, server_keys, client_keys)


@needs_curve
def test_curve_clients_must_also_pass_the_address_filter(ctx, auth):
    auth.configure_curve(location=CURVE_ALLOW_ANY)
    auth.deny("127.0.0.1")
    assert not curve_admits(ctx, zmq.curve_keypair(), zmq.curve_keypair())


@needs_curve
def test_explicit_socket_policy_takes_precedence(ctx, auth):
    client_keys = zmq.curve_keypair()

    def explicit(server):
        server.set_curve_auth([client_keys[0]])

    assert curve_admits(ctx, zmq.curve_keypair(), client_keys, explicit)


@needs_curve
def test_failing_policy_rejects_and_logs(ctx, auth, caplog):
    class Provider:
        def callback(self, domain, key):
            raise RuntimeError("database down")

    auth.configure_curve_callback(credentials_provider=Provider())
    with caplog.at_level(logging.ERROR, logger="pyomq.auth"):
        assert not curve_admits(ctx, zmq.curve_keypair(), zmq.curve_keypair())
    assert "database down" in caplog.text


@needs_curve
def test_socket_created_before_start_is_covered_at_bind(ctx):
    server_keys = zmq.curve_keypair()
    server = curve_server(ctx, server_keys)
    authenticator = Authenticator(ctx)
    authenticator.start()
    try:
        assert not admits(
            ctx,
            server,
            lambda: curve_client(ctx, server_keys[0], zmq.curve_keypair()),
        )
    finally:
        authenticator.stop()


@needs_curve
def test_stop_rejects_new_peers_and_restart_resumes(ctx):
    authenticator = ThreadAuthenticator(ctx)
    authenticator.start()
    authenticator.configure_curve(location=CURVE_ALLOW_ANY)
    server_keys = zmq.curve_keypair()
    server = curve_server(ctx, server_keys)
    monitor = server.monitor()
    server.bind("tcp://127.0.0.1:0")

    def connect():
        client = curve_client(ctx, server_keys[0], zmq.curve_keypair())
        client.connect(server.last_endpoint)
        return handshake(monitor)

    assert authenticator.is_alive()
    assert connect()
    authenticator.stop()
    assert not authenticator.is_alive()
    assert not connect()
    authenticator.start()
    try:
        assert connect()
    finally:
        authenticator.stop()


@needs_curve
def test_start_refuses_a_curve_server_already_in_use(ctx):
    server = curve_server(ctx, zmq.curve_keypair())
    server.bind("tcp://127.0.0.1:0")
    authenticator = Authenticator(ctx)
    with pytest.raises(RuntimeError, match="before the authenticator started"):
        authenticator.start()

    server.close()
    authenticator.start()
    authenticator.stop()


@needs_curve
def test_curve_policy_cannot_change_after_socket_use(ctx):
    server = curve_server(ctx, zmq.curve_keypair())
    server.bind("tcp://127.0.0.1:0")
    with pytest.raises(ValueError, match="before socket use"):
        server.set_curve_auth(None)


@needs_curve
def test_failed_first_bind_keeps_the_socket_covered(ctx):
    server_keys = zmq.curve_keypair()
    server = ctx.socket(zmq.PULL)
    server.curve_server = 1
    with pytest.raises(ValueError, match="CURVE_SECRETKEY"):
        server.bind("tcp://127.0.0.1:0")

    # The failed bind never put the socket in use, so start() covers it.
    authenticator = Authenticator(ctx)
    authenticator.start()
    try:
        server.curve_publickey, server.curve_secretkey = server_keys
        assert not admits(
            ctx,
            server,
            lambda: curve_client(ctx, server_keys[0], zmq.curve_keypair()),
        )
    finally:
        authenticator.stop()


@needs_curve
@pytest.mark.parametrize("use", ["send", "poll", "fd"])
def test_server_in_use_before_start_fails_closed_at_bind(ctx, use):
    server = curve_server(ctx, zmq.curve_keypair(), zmq.DEALER)
    with contextlib.suppress(zmq.Again):
        {
            "send": lambda: server.send(b"x", zmq.NOBLOCK),
            "poll": lambda: server.poll(0),
            "fd": lambda: server.getsockopt(zmq.FD),
        }[use]()
    authenticator = Authenticator(ctx)
    authenticator.start()
    try:
        with pytest.raises(ValueError, match="before socket use"):
            server.bind("tcp://127.0.0.1:0")
    finally:
        authenticator.stop()


@needs_curve
def test_uncovered_socket_in_use_before_start_still_connects(ctx):
    server_keys = zmq.curve_keypair()
    client = curve_client(ctx, server_keys[0], zmq.curve_keypair())
    with contextlib.suppress(zmq.Again):
        client.send(b"early", zmq.NOBLOCK)  # puts the client in use
    authenticator = Authenticator(ctx)
    authenticator.start()
    try:
        authenticator.configure_curve(location=CURVE_ALLOW_ANY)
        server = curve_server(ctx, server_keys)
        server.rcvtimeo = 5000
        server.bind("tcp://127.0.0.1:0")
        client.connect(server.last_endpoint)
        client.sndtimeo = 5000
        client.send(b"hello")
        assert server.recv() == b"hello"
    finally:
        authenticator.stop()


@needs_curve
def test_context_term_stops_the_authenticator():
    context = zmq.Context()
    authenticator = ThreadAuthenticator(context)
    authenticator.start()
    assert authenticator.is_alive()
    context.term()
    assert not authenticator.is_alive()
    authenticator.stop()

    with zmq.Context() as other:
        replacement = ThreadAuthenticator(other)
        replacement.start()
        assert replacement.is_alive()
        replacement.stop()


@needs_curve
def test_other_contexts_are_not_covered(ctx, auth):
    with zmq.Context() as other:
        assert curve_admits(other, zmq.curve_keypair(), zmq.curve_keypair())


def test_one_authenticator_per_context(ctx, auth):
    with pytest.raises(zmq.ZMQError) as raised:
        Authenticator(zmq.Context.shadow(ctx)).start()
    assert raised.value.errno == errno.EADDRINUSE


@needs_curve
def test_curve_callbacks_see_the_peer_address(ctx):
    client_keys = zmq.curve_keypair()
    seen = []

    def remember(server):
        server.set_curve_auth(lambda peer: seen.append(peer.peer_address) or True)

    assert curve_admits(ctx, zmq.curve_keypair(), client_keys, remember)
    assert seen == ["127.0.0.1"]


# ── NULL ─────────────────────────────────────────────────────────────


def test_null_admits_by_default(ctx, auth):
    assert null_admits(ctx, b"global")


def test_null_address_filter_needs_a_zap_domain(ctx, auth):
    auth.deny("127.0.0.1")
    assert not null_admits(ctx, b"global")
    assert null_admits(ctx)


def test_null_allow_list(ctx, auth):
    auth.allow("127.0.0.1")
    assert null_admits(ctx, b"global")
    with pytest.raises(ValueError):
        auth.deny("127.0.0.2")


def test_null_rejects_addresses_outside_the_allow_list(ctx, auth):
    auth.allow("192.0.2.1")
    assert not null_admits(ctx, b"global")


def test_zap_domain_option():
    with zmq.Context() as context, context.socket(zmq.PULL) as socket:
        assert socket.zap_domain == b""
        socket.zap_domain = b"global"
        assert socket.getsockopt(zmq.ZAP_DOMAIN) == b"global"
        with pytest.raises(ValueError):
            socket.zap_domain = b"x" * 256


# ── PLAIN ────────────────────────────────────────────────────────────


@needs_plain
def test_plain_passwords(ctx, auth):
    passwords = {"admin": "secret"}
    auth.configure_plain(passwords=passwords)
    assert plain_admits(ctx, b"admin", b"secret")
    assert not plain_admits(ctx, b"admin", b"wrong")
    assert not plain_admits(ctx, b"guest", b"secret")

    passwords["guest"] = "hello"
    assert plain_admits(ctx, b"guest", b"hello")


@needs_plain
def test_plain_without_passwords_rejects(ctx, auth):
    assert not plain_admits(ctx, b"admin", b"secret")


@needs_plain
def test_plain_server_still_needs_a_policy_without_authenticator(ctx):
    server = ctx.socket(zmq.PULL)
    server.plain_server = 1
    with pytest.raises(ValueError, match="pyomq.auth authenticator"):
        server.bind("tcp://127.0.0.1:0")


@needs_plain
def test_plain_accepts_bytes_passwords(ctx, auth):
    auth.configure_plain(passwords={"admin": b"secret"})
    assert plain_admits(ctx, b"admin", b"secret")
    assert not plain_admits(ctx, b"admin", b"wrong")


@needs_plain
def test_plain_policy_override_may_be_async(ctx):
    class AsyncPlain(ThreadAuthenticator):
        async def _authenticate_plain(self, domain, username, password):
            await asyncio.sleep(0)
            return username == "admin", b"OK"

    authenticator = AsyncPlain(ctx)
    authenticator.start()
    try:
        assert plain_admits(ctx, b"admin", b"anything")
        assert not plain_admits(ctx, b"guest", b"anything")
    finally:
        authenticator.stop()


@needs_plain
def test_pyzmq_plain_client(ctx, auth):
    pyzmq = pytest.importorskip("zmq")
    auth.configure_plain(passwords={"admin": "secret"})
    server = ctx.socket(zmq.PULL)
    server.plain_server = 1
    server.rcvtimeo = 5000
    server.bind("tcp://127.0.0.1:0")
    with pyzmq.Context() as zctx, zctx.socket(pyzmq.PUSH) as client:
        client.linger = 0
        client.plain_username = b"admin"
        client.plain_password = b"secret"
        client.connect(server.last_endpoint.decode())
        client.send(b"hello")
        assert server.recv() == b"hello"


# ── asyncio ──────────────────────────────────────────────────────────


@needs_curve
async def test_asyncio_authenticator_awaits_provider_on_its_loop():
    loop = asyncio.get_running_loop()
    client_keys = zmq.curve_keypair()
    seen = []

    class Provider:
        async def callback(self, domain, key):
            seen.append(asyncio.get_running_loop() is loop)
            await asyncio.sleep(0)
            return key == client_keys[0]

    with pyomq.asyncio.Context() as context:
        authenticator = AsyncioAuthenticator(context)
        authenticator.start()
        authenticator.configure_curve_callback(credentials_provider=Provider())
        try:
            server_keys = zmq.curve_keypair()
            server = curve_server(context, server_keys)
            server.bind("tcp://127.0.0.1:0")
            client = curve_client(context, server_keys[0], client_keys)
            client.connect(server.last_endpoint)
            await client.send(b"hello")
            assert await asyncio.wait_for(server.recv(), 5) == b"hello"
            assert seen == [True]
        finally:
            authenticator.stop()


def test_asyncio_authenticator_loop_argument_is_deprecated(ctx):
    with pytest.warns(DeprecationWarning):
        AsyncioAuthenticator(ctx, loop=object())


@needs_curve
def test_stuck_awaitable_times_out_and_rejects(ctx, auth, monkeypatch, caplog):
    monkeypatch.setattr("pyomq.auth.base._AWAIT_TIMEOUT", 0.2)

    class Provider:
        async def callback(self, domain, key):
            await asyncio.sleep(60)
            return True

    auth.configure_curve_callback(credentials_provider=Provider())
    with caplog.at_level(logging.ERROR, logger="pyomq.auth"):
        assert not curve_admits(ctx, zmq.curve_keypair(), zmq.curve_keypair())
    assert "TimeoutError" in caplog.text


@needs_curve
async def test_asyncio_stuck_awaitable_times_out(monkeypatch):
    monkeypatch.setattr("pyomq.auth.base._AWAIT_TIMEOUT", 0.2)

    class Provider:
        async def callback(self, domain, key):
            await asyncio.sleep(60)
            return True

    with pyomq.asyncio.Context() as context:
        authenticator = AsyncioAuthenticator(context)
        authenticator.start()
        authenticator.configure_curve_callback(credentials_provider=Provider())
        try:
            server_keys = zmq.curve_keypair()
            server = curve_server(context, server_keys)
            monitor = server.monitor()
            server.bind("tcp://127.0.0.1:0")
            client = curve_client(context, server_keys[0], zmq.curve_keypair())
            client.connect(server.last_endpoint)
            assert not await asyncio.to_thread(handshake, monitor)
        finally:
            authenticator.stop()
