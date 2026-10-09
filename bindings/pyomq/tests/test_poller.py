"""Poller tests."""

import threading
import time

import pyomq as zmq


def test_poll_returns_ready(tcp_endpoint):
    ctx = zmq.Context()
    push1 = ctx.socket(zmq.PUSH)
    pull1 = ctx.socket(zmq.PULL)
    pull2 = ctx.socket(zmq.PULL)
    push2 = ctx.socket(zmq.PUSH)
    try:
        pull1.bind(tcp_endpoint)
        ep = pull1.last_endpoint
        push1.connect(ep)

        pull2.bind(tcp_endpoint)
        ep2 = pull2.last_endpoint
        push2.connect(ep2)

        push1.send(b"only-one")
        time.sleep(0.02)

        poller = zmq.Poller()
        poller.register(pull1, zmq.POLLIN)
        poller.register(pull2, zmq.POLLIN)

        events = poller.poll(timeout=1000)
        ready_sockets = [s for s, _ in events]
        assert pull1 in ready_sockets
        assert pull2 not in ready_sockets
        assert pull1.recv() == b"only-one"
    finally:
        push1.close()
        pull1.close()
        push2.close()
        pull2.close()
        ctx.term()


def test_poll_timeout_empty(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)
        events = poller.poll(timeout=50)
        assert events == []
    finally:
        pull.close()
        ctx.term()


def test_poll_huge_timeout_receives_late_message():
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    endpoint = f"inproc://poll-huge-timeout-{id(pull)}"
    try:
        pull.bind(endpoint)
        push.connect(endpoint)
        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)

        thread = threading.Thread(
            target=lambda: (time.sleep(0.02), push.send(b"late")),
            daemon=True,
        )
        thread.start()
        assert poller.poll(timeout=2**64 - 1) == [(pull, zmq.POLLIN)]
        assert pull.recv() == b"late"
        thread.join(timeout=1)
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_pollout_ready_once_connected():
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    try:
        poller = zmq.Poller()
        poller.register(push, zmq.POLLOUT)
        # Like libzmq: no pipe, not writable.
        assert poller.poll(timeout=0) == []
        push.connect("inproc://pollout-ready")
        assert poller.poll(timeout=1000) == [(push, zmq.POLLOUT)]
        assert push.getsockopt(zmq.EVENTS) & zmq.POLLOUT
    finally:
        push.close()
        ctx.term()


def test_pollout_waits_for_bound_peer_and_queue_space():
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        push.setsockopt(zmq.SNDHWM, 4)
        pull.setsockopt(zmq.RCVHWM, 4)
        push.bind("inproc://pollout-space")
        poller = zmq.Poller()
        poller.register(push, zmq.POLLOUT)
        assert poller.poll(timeout=50) == [], "bound PUSH without peers is mute"
        assert not push.getsockopt(zmq.EVENTS) & zmq.POLLOUT

        threading.Timer(0.05, pull.connect, ("inproc://pollout-space",)).start()
        started = time.perf_counter()
        assert poller.poll(timeout=5000) == [(push, zmq.POLLOUT)]
        assert time.perf_counter() - started < 2

        # Internal queues drain asynchronously; refill until it stays full.
        queued = 0
        while True:
            try:
                while True:
                    push.send(b"x", zmq.NOBLOCK)
                    queued += 1
            except zmq.Again:
                pass
            if not poller.poll(timeout=50):
                break

        def drain():
            for _ in range(queued):
                pull.recv()

        threading.Timer(0.05, drain).start()
        started = time.perf_counter()
        assert poller.poll(timeout=5000) == [(push, zmq.POLLOUT)]
        assert time.perf_counter() - started < 2
        push.send(b"y", zmq.NOBLOCK)
    finally:
        push.close(linger=0)
        pull.close(linger=0)
        ctx.term()


def test_pollout_follows_req_rep_alternation():
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        rep.bind("inproc://pollout-req-rep")
        req.connect("inproc://pollout-req-rep")
        assert req.poll(1000, zmq.POLLOUT) == zmq.POLLOUT
        assert rep.poll(0, zmq.POLLOUT) == 0
        req.send(b"q")
        assert req.poll(0, zmq.POLLOUT) == 0
        assert rep.recv() == b"q"
        assert rep.poll(1000, zmq.POLLOUT) == zmq.POLLOUT
        rep.send(b"a")
        assert rep.poll(0, zmq.POLLOUT) == 0
        assert req.recv() == b"a"
        assert req.poll(1000, zmq.POLLOUT) == zmq.POLLOUT
    finally:
        req.close(linger=0)
        rep.close(linger=0)
        ctx.term()


def test_poll_multiple_ready(tcp_endpoint):
    ctx = zmq.Context()
    push1 = ctx.socket(zmq.PUSH)
    pull1 = ctx.socket(zmq.PULL)
    push2 = ctx.socket(zmq.PUSH)
    pull2 = ctx.socket(zmq.PULL)
    try:
        pull1.bind(tcp_endpoint)
        ep = pull1.last_endpoint
        push1.connect(ep)

        pull2.bind(tcp_endpoint)
        ep2 = pull2.last_endpoint
        push2.connect(ep2)

        push1.send(b"msg1")
        push2.send(b"msg2")
        time.sleep(0.1)

        poller = zmq.Poller()
        poller.register(pull1, zmq.POLLIN)
        poller.register(pull2, zmq.POLLIN)

        # Poll until both are ready (may need two rounds if
        # select_all fires before the second message arrives).
        ready_sockets = set()
        for _ in range(5):
            events = poller.poll(timeout=1000)
            for s, _ in events:
                ready_sockets.add(s)
            if pull1 in ready_sockets and pull2 in ready_sockets:
                break
        assert pull1 in ready_sockets
        assert pull2 in ready_sockets
    finally:
        push1.close()
        pull1.close()
        push2.close()
        pull2.close()
        ctx.term()


def test_register_unregister(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)
        push.send(b"hello")
        time.sleep(0.02)

        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)
        poller.unregister(pull)

        events = poller.poll(timeout=50)
        assert events == []
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_modify_flags(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)
        push.send(b"hello")
        time.sleep(0.02)

        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)

        # Disable polling
        poller.modify(pull, 0)
        events = poller.poll(timeout=50)
        assert events == []

        # Re-enable polling
        poller.modify(pull, zmq.POLLIN)
        events = poller.poll(timeout=1000)
        assert len(events) == 1
        assert events[0][0] is pull
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_poll_no_busywait(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)

        cpu_before = time.process_time()
        poller.poll(timeout=300)
        cpu_after = time.process_time()

        cpu_ms = (cpu_after - cpu_before) * 1000
        assert cpu_ms < 50, f"CPU time {cpu_ms:.1f} ms during poll — busy-waiting?"
    finally:
        pull.close()
        ctx.term()


def test_socket_id_exposed():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.bind("tcp://127.0.0.1:0")
        sid = sock._sock.socket_id()
        assert isinstance(sid, int)
        assert sid > 0
    finally:
        sock.close()
        ctx.term()
