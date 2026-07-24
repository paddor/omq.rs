"""asyncio facade: pyomq.asyncio.Context / Socket roundtrips."""

import asyncio
import sys
import time

import pytest

import pyomq
import pyomq.asyncio as zmq_async

pytestmark = pytest.mark.event_loop("selector", "proactor")


@pytest.mark.asyncio
async def test_async_push_pull_inproc(inproc_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.bind(inproc_endpoint)
        push.connect(inproc_endpoint)
        push.send(b"hello")
        assert await pull.recv() == b"hello"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_push_pull_tcp(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"tcp-hello")
        assert await pull.recv() == b"tcp-hello"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_send_multipart(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_multipart([b"a", b"b", b"c"])
        assert await pull.recv_multipart() == [b"a", b"b", b"c"]
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_pubsub(tcp_endpoint):
    ctx = zmq_async.Context()
    pub = ctx.socket(pyomq.PUB)
    sub = ctx.socket(pyomq.SUB)
    try:
        ep = pub.bind(tcp_endpoint)
        sub.connect(ep)
        sub.setsockopt(pyomq.SUBSCRIBE, b"hot/")
        await asyncio.sleep(0.2)  # let SUBSCRIBE propagate
        pub.send(b"cold/skip")
        pub.send(b"hot/take")
        sub.setsockopt(pyomq.RCVTIMEO, 1000)
        assert await sub.recv() == b"hot/take"
    finally:
        pub.close()
        sub.close()


@pytest.mark.asyncio
async def test_async_concurrent_recvs(tcp_endpoint):
    """Many concurrent awaits on different Python tasks all wake up."""
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        # Fire off N concurrent recvs. AsyncSocket.recv returns an
        # asyncio.Future directly (not a coroutine), so wrap in
        # ensure_future so asyncio.gather is happy.
        N = 32
        recv_futs = [asyncio.ensure_future(pull.recv()) for _ in range(N)]
        await asyncio.sleep(0.05)  # let them register
        for i in range(N):
            push.send(f"msg-{i}".encode())
        results = sorted(await asyncio.gather(*recv_futs))
        assert results == sorted(f"msg-{i}".encode() for i in range(N))
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_mixed_with_sync(tcp_endpoint):
    """Async sender, sync receiver. Both share the wire."""
    ctx_async = zmq_async.Context()
    ctx_sync = pyomq.Context()
    pull = ctx_sync.socket(pyomq.PULL)
    push = ctx_async.socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"mixed")
        assert pull.recv() == b"mixed"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_close_linger_arg_overrides_socket_linger():
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    try:
        push.linger = -1
        push.bind("inproc://async-close-linger-override")
        push.send(b"queued")
        await asyncio.sleep(0.05)

        start = time.monotonic()
        push.close(linger=0)
        elapsed = time.monotonic() - start

        assert elapsed < 0.5
    finally:
        push.close(linger=0)
        ctx.term()


@pytest.mark.asyncio
async def test_async_sndmore_flag_aggregates(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"a", flags=pyomq.SNDMORE)
        push.send(b"b", flags=pyomq.SNDMORE)
        push.send(b"c")
        assert await pull.recv_multipart() == [b"a", b"b", b"c"]
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_rcvmore_iterates_frames(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_multipart([b"x", b"y", b"z"])
        assert await pull.recv() == b"x"
        assert pull.getsockopt(pyomq.RCVMORE) == 1
        assert await pull.recv() == b"y"
        assert pull.getsockopt(pyomq.RCVMORE) == 1
        assert await pull.recv() == b"z"
        assert pull.getsockopt(pyomq.RCVMORE) == 0
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_context_manager(tcp_endpoint):
    ctx = zmq_async.Context()
    async with ctx.socket(pyomq.PAIR) as a, ctx.socket(pyomq.PAIR) as b:
        ep = a.bind(tcp_endpoint)
        b.connect(ep)
        a.send(b"ping")
        assert await b.recv() == b"ping"
        b.send(b"pong")
        assert await a.recv() == b"pong"


@pytest.mark.asyncio
async def test_async_req_rep_roundtrip(tcp_endpoint):
    ctx = zmq_async.Context()
    rep = ctx.socket(pyomq.REP)
    req = ctx.socket(pyomq.REQ)
    try:
        ep = rep.bind(tcp_endpoint)
        req.connect(ep)
        req.send(b"ping")
        assert await rep.recv() == b"ping"
        rep.send(b"pong")
        assert await req.recv() == b"pong"
    finally:
        req.close()
        rep.close()


@pytest.mark.asyncio
async def test_async_unsubscribe_drops_topic(tcp_endpoint):
    ctx = zmq_async.Context()
    pub = ctx.socket(pyomq.PUB)
    sub = ctx.socket(pyomq.SUB)
    try:
        ep = pub.bind(tcp_endpoint)
        sub.connect(ep)
        sub.subscribe(b"a")
        sub.subscribe(b"b")
        await asyncio.sleep(0.1)
        sub.unsubscribe(b"a")
        await asyncio.sleep(0.1)
        pub.send(b"a-one")
        pub.send(b"b-two")
        sub.setsockopt(pyomq.RCVTIMEO, 500)
        assert await sub.recv() == b"b-two"
    finally:
        pub.close()
        sub.close()


@pytest.mark.asyncio
async def test_async_dealer_router_identity(tcp_endpoint):
    ctx = zmq_async.Context()
    router = ctx.socket(pyomq.ROUTER)
    dealer = ctx.socket(pyomq.DEALER)
    try:
        dealer.setsockopt(pyomq.IDENTITY, b"client-A")
        ep = router.bind(tcp_endpoint)
        dealer.connect(ep)
        dealer.send(b"hello")
        parts = await router.recv_multipart()
        assert parts[0] == b"client-A"
        assert parts[-1] == b"hello"
        router.send_multipart([b"client-A", b"hi-back"])
        assert await dealer.recv() == b"hi-back"
    finally:
        dealer.close()
        router.close()


@pytest.mark.asyncio
async def test_async_push_pull_bulk_tcp(tcp_endpoint):
    """Async recv with sync sender in a thread."""
    import threading

    n = 20_000
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push_sync = pyomq.Context().socket(pyomq.PUSH)
    try:
        ep = pull.bind(tcp_endpoint)
        push_sync.connect(ep)

        def sender():
            import time

            time.sleep(0.05)
            for _ in range(n):
                push_sync.send(b"x" * 128)

        t = threading.Thread(target=sender)
        t.start()

        async def receive_all():
            for _ in range(n):
                await pull.recv()

        await asyncio.wait_for(receive_all(), timeout=30)
        t.join(timeout=5)
    finally:
        push_sync.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_close_wakes_pending_recv(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    try:
        pull.bind(tcp_endpoint)
        recv_task = asyncio.create_task(pull.recv())
        await asyncio.sleep(0.05)
        pull.close()
        with pytest.raises(Exception):
            await recv_task
    except Exception:
        pass
