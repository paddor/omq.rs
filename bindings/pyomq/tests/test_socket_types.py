"""Coverage for all 19 ZMTP socket types (11 standard + 8 draft).

Each test constructs a real omq socket pair and exchanges at least one
message. The point is not perf or edge cases - those live in the
focussed test files - but to assert that every type code maps through
the binding to a working peer.
"""

import socket as stdsocket
import time

import pytest

import pyomq

STANDARD = [
    pyomq.PAIR,
    pyomq.PUB,
    pyomq.SUB,
    pyomq.REQ,
    pyomq.REP,
    pyomq.DEALER,
    pyomq.ROUTER,
    pyomq.PULL,
    pyomq.PUSH,
    pyomq.XPUB,
    pyomq.XSUB,
]

DRAFT = [
    pyomq.SERVER,
    pyomq.CLIENT,
    pyomq.RADIO,
    pyomq.DISH,
    pyomq.GATHER,
    pyomq.SCATTER,
    pyomq.PEER,
    pyomq.CHANNEL,
]


def _free_tcp() -> str:
    s = stdsocket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return f"tcp://127.0.0.1:{p}"


def _inproc(name: str) -> str:
    return f"inproc://socket-types-{name}-{time.monotonic_ns()}"


@pytest.mark.parametrize("kind", STANDARD + DRAFT)
def test_construct_each_type(kind):
    """Every constant maps through Context.socket without erroring."""
    ctx = pyomq.Context()
    s = ctx.socket(kind)
    s.close()
    ctx.term()


def test_push_pull():
    ep = _inproc("push-pull")
    ctx = pyomq.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    pull.bind(ep)
    push.connect(ep)
    push.send(b"hello")
    assert pull.recv() == b"hello"
    push.close(); pull.close(); ctx.term()


def test_pub_sub():
    ep = _free_tcp()
    ctx = pyomq.Context()
    pub = ctx.socket(pyomq.PUB)
    sub = ctx.socket(pyomq.SUB)
    pub.bind(ep)
    sub.connect(ep)
    sub.subscribe(b"")
    time.sleep(0.05)
    for _ in range(10):
        pub.send(b"ping")
        try:
            sub.setsockopt(pyomq.RCVTIMEO, 200)
            assert sub.recv() == b"ping"
            break
        except pyomq.Again:
            continue
    else:
        pytest.fail("SUB never received")
    pub.close(); sub.close(); ctx.term()


def test_xpub_xsub():
    """XSUB → XPUB subscribe surfaces; XPUB → XSUB publish round-trips."""
    ep = _free_tcp()
    ctx = pyomq.Context()
    xpub = ctx.socket(pyomq.XPUB)
    xsub = ctx.socket(pyomq.XSUB)
    xpub.bind(ep)
    xsub.connect(ep)
    xsub.subscribe(b"")
    # Drain the subscribe notification at XPUB. RFC says it surfaces as
    # `\x01<prefix>` (here just `\x01`).
    xpub.setsockopt(pyomq.RCVTIMEO, 1000)
    sub_msg = xpub.recv()
    assert sub_msg == b"\x01"
    xsub.setsockopt(pyomq.RCVTIMEO, 1000)
    for _ in range(20):
        xpub.send(b"hello")
        try:
            assert xsub.recv() == b"hello"
            break
        except pyomq.Again:
            time.sleep(0.05)
    else:
        pytest.fail("XSUB never received")
    xpub.close(); xsub.close(); ctx.term()


def test_req_rep():
    ep = _inproc("req-rep")
    ctx = pyomq.Context()
    rep = ctx.socket(pyomq.REP)
    req = ctx.socket(pyomq.REQ)
    rep.bind(ep)
    req.connect(ep)
    req.send(b"q")
    assert rep.recv() == b"q"
    rep.send(b"a")
    assert req.recv() == b"a"
    req.close(); rep.close(); ctx.term()


def test_dealer_router():
    ep = _inproc("dealer-router")
    ctx = pyomq.Context()
    router = ctx.socket(pyomq.ROUTER)
    router.bind(ep)
    dealer = ctx.socket(pyomq.DEALER)
    dealer.setsockopt(pyomq.IDENTITY, b"dlr-1")
    dealer.connect(ep)
    time.sleep(0.05)
    dealer.send(b"hello")
    parts = router.recv_multipart()
    assert parts == [b"dlr-1", b"hello"]
    router.send_multipart([b"dlr-1", b"world"])
    assert dealer.recv() == b"world"
    dealer.close(); router.close(); ctx.term()


def test_pair_pair():
    ep = _inproc("pair")
    ctx = pyomq.Context()
    a = ctx.socket(pyomq.PAIR)
    b = ctx.socket(pyomq.PAIR)
    a.bind(ep)
    b.connect(ep)
    a.send(b"x")
    assert b.recv() == b"x"
    b.send(b"y")
    assert a.recv() == b"y"
    a.close(); b.close(); ctx.term()


def test_client_server():
    ep = _inproc("client-server")
    ctx = pyomq.Context()
    server = ctx.socket(pyomq.SERVER)
    server.bind(ep)
    client = ctx.socket(pyomq.CLIENT)
    client.setsockopt(pyomq.IDENTITY, b"cli-1")
    client.connect(ep)
    time.sleep(0.05)
    client.send(b"ping")
    parts = server.recv_multipart()
    assert parts == [b"cli-1", b"ping"]
    server.send_multipart([b"cli-1", b"pong"])
    assert client.recv() == b"pong"
    client.close(); server.close(); ctx.term()


def test_scatter_gather():
    ep = _inproc("scatter-gather")
    ctx = pyomq.Context()
    gather = ctx.socket(pyomq.GATHER)
    gather.bind(ep)
    scatter = ctx.socket(pyomq.SCATTER)
    scatter.connect(ep)
    time.sleep(0.05)
    for i in range(3):
        scatter.send(f"m{i}".encode())
    got = sorted([gather.recv() for _ in range(3)])
    assert got == [b"m0", b"m1", b"m2"]
    scatter.close(); gather.close(); ctx.term()


def test_channel_pair():
    ep = _inproc("channel")
    ctx = pyomq.Context()
    a = ctx.socket(pyomq.CHANNEL)
    b = ctx.socket(pyomq.CHANNEL)
    a.bind(ep)
    b.connect(ep)
    time.sleep(0.05)
    a.send(b"hi")
    assert b.recv() == b"hi"
    b.send(b"there")
    assert a.recv() == b"there"
    a.close(); b.close(); ctx.term()


def test_peer_peer():
    ep = _inproc("peer")
    ctx = pyomq.Context()
    a = ctx.socket(pyomq.PEER)
    a.setsockopt(pyomq.IDENTITY, b"peer-a")
    a.bind(ep)
    b = ctx.socket(pyomq.PEER)
    b.setsockopt(pyomq.IDENTITY, b"peer-b")
    b.connect(ep)
    time.sleep(0.05)
    b.send_multipart([b"peer-a", b"hello a"])
    parts = a.recv_multipart()
    assert parts == [b"peer-b", b"hello a"]
    a.send_multipart([b"peer-b", b"hello b"])
    parts = b.recv_multipart()
    assert parts == [b"peer-a", b"hello b"]
    a.close(); b.close(); ctx.term()


def test_radio_dish_udp_with_groups():
    """RADIO/DISH over UDP: DISH filters by joined group (RFC 48)."""
    s = stdsocket.socket(stdsocket.AF_INET, stdsocket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    ep = f"udp://127.0.0.1:{port}"

    ctx = pyomq.Context()
    dish = ctx.socket(pyomq.DISH)
    dish.bind(ep)
    dish.join(b"weather")

    radio = ctx.socket(pyomq.RADIO)
    radio.connect(ep)
    time.sleep(0.05)

    # Drop: not in joined groups.
    radio.send_multipart([b"news", b"ignored"])
    # Deliver.
    radio.send_multipart([b"weather", b"sunny"])

    dish.setsockopt(pyomq.RCVTIMEO, 500)
    parts = dish.recv_multipart()
    assert parts == [b"weather", b"sunny"]

    dish.leave(b"weather")
    radio.close(); dish.close(); ctx.term()
