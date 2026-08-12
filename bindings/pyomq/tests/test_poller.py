"""Poller tests."""

import time

import pyomq as zmq


def test_poll_returns_ready(tcp_endpoint):
    ctx = zmq.Context()
    push1 = ctx.socket(zmq.PUSH)
    pull1 = ctx.socket(zmq.PULL)
    pull2 = ctx.socket(zmq.PULL)
    push2 = ctx.socket(zmq.PUSH)
    try:
        ep = pull1.bind(tcp_endpoint)
        push1.connect(ep)

        ep2 = pull2.bind(tcp_endpoint)
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
        ep = pull.bind(tcp_endpoint)
        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)
        events = poller.poll(timeout=50)
        assert events == []
    finally:
        pull.close()
        ctx.term()


def test_pollout_ready_immediately():
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    try:
        poller = zmq.Poller()
        poller.register(push, zmq.POLLOUT)
        events = poller.poll(timeout=1000)
        assert events == [(push, zmq.POLLOUT)]
    finally:
        push.close()
        ctx.term()


def test_poll_multiple_ready(tcp_endpoint):
    ctx = zmq.Context()
    push1 = ctx.socket(zmq.PUSH)
    pull1 = ctx.socket(zmq.PULL)
    push2 = ctx.socket(zmq.PUSH)
    pull2 = ctx.socket(zmq.PULL)
    try:
        ep = pull1.bind(tcp_endpoint)
        push1.connect(ep)

        ep2 = pull2.bind(tcp_endpoint)
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
        ep = pull.bind(tcp_endpoint)
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
        ep = pull.bind(tcp_endpoint)
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
        ep = pull.bind(tcp_endpoint)
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
