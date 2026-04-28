"""REQ/REP envelope handling and DEALER/ROUTER identity routing."""

import pyomq as zmq


def test_req_rep_roundtrip(tcp_endpoint):
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        rep.bind(tcp_endpoint)
        req.connect(tcp_endpoint)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
    finally:
        req.close()
        rep.close()
        ctx.term()


def test_dealer_router_identity_routes_back(tcp_endpoint):
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    dealer = ctx.socket(zmq.DEALER)
    try:
        dealer.setsockopt(zmq.IDENTITY, b"client-A")
        router.bind(tcp_endpoint)
        dealer.connect(tcp_endpoint)
        # DEALER sends; ROUTER recv exposes the identity as the first frame.
        dealer.send(b"hello")
        parts = router.recv_multipart()
        assert parts[0] == b"client-A"
        assert parts[-1] == b"hello"
        # ROUTER replies addressed to the same identity.
        router.send_multipart([b"client-A", b"hi-back"])
        assert dealer.recv() == b"hi-back"
    finally:
        dealer.close()
        router.close()
        ctx.term()
