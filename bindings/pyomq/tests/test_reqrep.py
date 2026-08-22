"""REQ/REP envelope handling and DEALER/ROUTER identity routing."""

import threading

import pyomq as zmq


def test_req_rep_roundtrip(tcp_endpoint):
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        ep = rep.bind(tcp_endpoint)
        req.connect(ep)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
    finally:
        req.close()
        rep.close()
        ctx.term()


def test_rep_accepts_reply_while_client_closes(tcp_endpoint):
    ctx = zmq.Context(io_threads=2)
    rep = ctx.socket(zmq.REP)
    rep.setsockopt(zmq.RCVTIMEO, 1_000)
    rep.setsockopt(zmq.SNDTIMEO, 1_000)
    rep.setsockopt(zmq.LINGER, 0)
    endpoint = rep.bind(tcp_endpoint)
    failure = []

    def serve():
        try:
            for _ in range(21):
                rep.send(rep.recv())
        except Exception as error:  # noqa: BLE001 - preserve thread failure
            failure.append(error)

    server = threading.Thread(target=serve)
    server.start()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.LINGER, 0)
    try:
        req.connect(endpoint)
        for sequence in range(20):
            request = f"request-{sequence}".encode()
            req.send(request)
            assert req.recv() == request
        req.send(b"last")
        req.close(linger=0)
        server.join(timeout=2)
        assert not server.is_alive()
        assert not failure
    finally:
        req.close(linger=0)
        rep.close(linger=0)
        ctx.term()


def test_dealer_router_identity_routes_back(tcp_endpoint):
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    dealer = ctx.socket(zmq.DEALER)
    try:
        dealer.setsockopt(zmq.IDENTITY, b"client-A")
        ep = router.bind(tcp_endpoint)
        dealer.connect(ep)
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
