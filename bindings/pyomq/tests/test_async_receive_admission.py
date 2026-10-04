"""Queued transport items must retain request ownership until application recv."""

import asyncio

import pyomq
import pyomq.asyncio as azmq
import pytest

pytestmark = pytest.mark.event_loop("selector", "proactor")


def test_sync_poll_does_not_admit_request_or_reply(inproc_endpoint):
    ctx = pyomq.Context()
    req, rep = ctx.socket(pyomq.REQ), ctx.socket(pyomq.REP)
    try:
        req.linger = rep.linger = 0
        rep.bind(inproc_endpoint)
        req.connect(inproc_endpoint)
        req.send_multipart([b"request", b"tail"])
        assert rep.poll(2000) == pyomq.POLLIN
        with pytest.raises(pyomq.ZMQError):
            rep.send(b"before-receive")
        assert rep.recv() == b"request"
        with pytest.raises(pyomq.ZMQError):
            rep.send(b"partial-request")
        assert rep.recv() == b"tail"
        rep.send_multipart([b"reply", b"tail"])
        assert req.poll(2000) == pyomq.POLLIN
        with pytest.raises(pyomq.ZMQError):
            req.send(b"before-reply-receive")
        assert req.recv() == b"reply"
        with pytest.raises(pyomq.ZMQError):
            req.send(b"partial-reply")
        assert req.recv() == b"tail"
    finally:
        req.close()
        rep.close()
        ctx.term()


@pytest.mark.parametrize("transport", ["tcp", "ipc", "inproc", "mixed"])
async def test_queued_rep_requests_keep_their_peer(
    transport, ipc_endpoint, inproc_endpoint
):
    ctx = azmq.Context()
    rep = ctx.socket(pyomq.REP)
    requests = [ctx.socket(pyomq.REQ) for _ in range(2)]
    sockets = [rep, *requests]
    try:
        for sock in sockets:
            sock.linger = 0
        endpoint = {
            "tcp": "tcp://127.0.0.1:0",
            "ipc": ipc_endpoint,
            "inproc": inproc_endpoint,
            "mixed": inproc_endpoint,
        }[transport]
        rep.bind(endpoint)
        endpoint = rep.last_endpoint
        for index, request in enumerate(requests):
            if transport == "mixed" and index == 1:
                rep.bind("tcp://127.0.0.1:0")
                endpoint = rep.last_endpoint
            request.connect(endpoint)
        await asyncio.sleep(0.05)
        await requests[0].send_multipart([b"first", b"body"])
        await asyncio.sleep(0.03)
        await requests[1].send_multipart([b"second", b"body"])
        await asyncio.sleep(0.03)
        for _ in range(2):
            message = await asyncio.wait_for(rep.recv_multipart(), 2)
            destination = 0 if message[0] == b"first" else 1
            await rep.send_multipart(message)
            assert (
                await asyncio.wait_for(requests[destination].recv_multipart(), 1)
                == message
            )
    finally:
        for sock in sockets:
            sock.close()
        ctx.term()


async def test_req_state_changes_only_after_complete_application_receive(
    inproc_endpoint,
):
    ctx = azmq.Context()
    req, rep = ctx.socket(pyomq.REQ), ctx.socket(pyomq.REP)
    try:
        req.linger = rep.linger = 0
        rep.bind(inproc_endpoint)
        req.connect(inproc_endpoint)
        await req.send(b"request")
        assert await rep.recv() == b"request"
        await rep.send_multipart([b"reply", b"tail"])
        await asyncio.sleep(0.03)
        with pytest.raises(pyomq.ZMQError):
            req.send(b"too-early")
        assert await req.recv() == b"reply"
        with pytest.raises(pyomq.ZMQError):
            req.send(b"partial-reply")
        assert await req.recv() == b"tail"
        await req.send(b"next")
        assert await rep.recv() == b"next"
    finally:
        req.close()
        rep.close()
        ctx.term()


async def test_inline_and_backpressured_sends_keep_fifo(inproc_endpoint):
    ctx = azmq.Context()
    push, pull = ctx.socket(pyomq.PUSH), ctx.socket(pyomq.PULL)
    try:
        push.linger = pull.linger = 0
        push.sndhwm = 256
        pull.rcvhwm = 2
        pull.bind(inproc_endpoint)
        push.connect(inproc_endpoint)
        await asyncio.sleep(0.03)

        # Fill the native destination and leave an eager fallback in flight.
        async def send_all():
            for index in range(32):
                await push.send(str(index).encode())

        sender = asyncio.create_task(send_all())
        received = []
        for _ in range(32):
            received.append(await asyncio.wait_for(pull.recv(), 2))
        await sender
        assert received == [str(index).encode() for index in range(32)]
        await push.send(b"inline-again")
        assert await pull.recv() == b"inline-again"
    finally:
        push.close()
        pull.close()
        ctx.term()


async def test_direct_sink_survives_fallback_disconnect_and_recycles(inproc_endpoint):
    ctx = azmq.Context()
    pull = ctx.socket(pyomq.PULL)
    producers = [ctx.socket(pyomq.PUSH) for _ in range(3)]
    try:
        pull.linger = 0
        pull.bind(inproc_endpoint)
        for index in range(2):
            producers[index].linger = 0
            producers[index].connect(inproc_endpoint)
            await producers[index].send(str(index).encode())
            assert await asyncio.wait_for(pull.recv(), 2) == str(index).encode()
        producers[1].close()
        await asyncio.sleep(0.03)
        producers[2].linger = 0
        producers[2].connect(inproc_endpoint)
        await producers[2].send(b"fallback")
        await producers[0].send(b"original")
        messages = [await asyncio.wait_for(pull.recv(), 2) for _ in range(2)]
        assert set(messages) == {b"fallback", b"original"}
        producers[0].close()
        await asyncio.sleep(0.03)
        replacement = ctx.socket(pyomq.PUSH)
        producers.append(replacement)
        replacement.linger = 0
        replacement.connect(inproc_endpoint)
        await replacement.send(b"replacement")
        assert await pull.poll(2000) == pyomq.POLLIN
        assert await pull.recv() == b"replacement"
    finally:
        for producer in producers:
            producer.close()
        pull.close()
        ctx.term()


async def test_rep_cannot_send_a_queued_request_before_application_receive(
    inproc_endpoint,
):
    ctx = azmq.Context()
    req, rep = ctx.socket(pyomq.REQ), ctx.socket(pyomq.REP)
    try:
        req.linger = rep.linger = 0
        rep.bind(inproc_endpoint)
        req.connect(inproc_endpoint)
        await req.send_multipart([b"request", b"tail"])
        await asyncio.sleep(0.03)
        with pytest.raises(pyomq.ZMQError):
            rep.send(b"too-early")
        assert bytes(await rep.recv(copy=False)) == b"request"
        with pytest.raises(pyomq.ZMQError):
            rep.send(b"partial-request")
        assert bytes(await rep.recv(copy=False)) == b"tail"
        await rep.send(b"reply")
        assert await req.recv() == b"reply"
    finally:
        req.close()
        rep.close()
        ctx.term()
