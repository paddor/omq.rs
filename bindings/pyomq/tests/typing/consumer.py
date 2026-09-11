"""Consumer contracts, checked against the installed package by three checkers."""

import asyncio
from array import array
from collections.abc import Buffer, Iterable
from typing import assert_type

import pyomq as zmq
import pyomq.asyncio as azmq
from pyomq import FutureResult


class SyncSocket(zmq.Socket):
    pass


class AsyncSocket(azmq.Socket):
    pass


class SyncContext(zmq.Context):
    pass


class AsyncContext(azmq.Context):
    pass


def decode(parts: list[bytes]) -> int:
    return len(parts)


def decode_frames(parts: list[zmq.Frame]) -> str:
    return str(len(parts))


def decode_dynamic(parts: list[bytes] | list[zmq.Frame]) -> float:
    return float(len(parts))


def encode(value: int) -> Iterable[zmq.Sendable]:
    yield str(value).encode()


def sync_api(copy: bool, option: int, buffer: Buffer) -> None:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    assert_type(sock, zmq.Socket)
    assert_type(ctx.socket(zmq.PAIR, SyncSocket), SyncSocket)
    assert_type(SyncContext.instance(), SyncContext)
    assert_type(SyncContext.from_share_key(1), SyncContext)
    with SyncContext() as subclass:
        assert_type(subclass, SyncContext)
    with ctx.socket(zmq.PAIR, SyncSocket) as sub_socket:
        assert_type(sub_socket, SyncSocket)
        assert_type(sub_socket.underlying, SyncSocket)
        assert_type(zmq.Socket.shadow(sub_socket), SyncSocket)
    assert_type(sock.context, zmq.Context)
    assert_type(sock.bind(b"inproc://typed"), bytes)
    assert_type(sock.bind("inproc://typed"), str)
    sock.connect(b"inproc://typed")
    sock.unbind(b"inproc://typed")
    sock.disconnect(b"inproc://typed")
    assert_type(sock.last_endpoint, bytes | None)
    assert_type(sock.recv(), bytes)
    assert_type(sock.recv(copy=False), zmq.Frame)
    assert_type(sock.recv(0, False), zmq.Frame)
    assert_type(sock.recv(copy=copy), bytes | zmq.Frame)
    assert_type(sock.recv_multipart(), list[bytes])
    assert_type(sock.recv_multipart(copy=False), list[zmq.Frame])
    assert_type(sock.recv_multipart(0, False), list[zmq.Frame])
    assert_type(sock.recv_multipart(copy=copy), list[bytes] | list[zmq.Frame])
    assert_type(sock.recv_serialized(decode), int)
    assert_type(sock.recv_serialized(decode_frames, copy=False), str)
    assert_type(sock.recv_serialized(decode_frames, 0, False), str)
    assert_type(sock.recv_serialized(decode_dynamic, copy=copy), float)
    assert_type(sock.send_serialized(42, encode), zmq.MessageTracker | None)
    for data in [
        buffer,
        b"bytes",
        bytearray(),
        memoryview(b""),
        array("I"),
        zmq.Frame(),
    ]:
        assert_type(sock.send(data), zmq.MessageTracker | None)
    parts: list[zmq.Sendable] = [b"one", memoryview(b"two")]
    sock.send_multipart(part for part in parts)
    frame = zmq.Frame(buffer, copy=False, track=True)
    memoryview(frame)
    bytes(frame)
    assert_type(frame.tracker, zmq.MessageTracker | None)
    if frame.tracker is not None:
        assert_type(frame.tracker.done, bool)
        assert_type(frame.tracker.wait(0.1), None)
    assert_type(sock.linger, int)
    assert_type(sock.identity, bytes)
    sock.linger = 0
    sock.identity = b"id"
    assert_type(sock.getsockopt(zmq.LINGER), int)
    assert_type(sock.getsockopt(zmq.IDENTITY), bytes)
    assert_type(sock.getsockopt(zmq.LAST_ENDPOINT), bytes | None)
    assert_type(sock.getsockopt(option), int | bytes | None)
    assert_type(sock.get(zmq.TYPE), int)
    assert_type(sock.getsockopt_string(zmq.IDENTITY), str)
    sock.set_plain_auth(pair for pair in [("user", "password")])
    sock.set_curve_auth(key for key in [b"key"])
    sock.set_curve_auth(lambda peer: peer.public_key == b"key")
    sock.set_plain_auth(lambda peer: peer.username == "user")
    assert_type(sock.connections(), list[zmq.ConnectionInfo])
    assert_type(sock.connection_info(1), zmq.ConnectionInfo | None)
    event = sock.monitor().recv()
    assert_type(event, zmq.MonitorEvent)
    poller = zmq.Poller()
    poller.register(sock)
    assert_type(poller.poll(), list[tuple[zmq.Socket, int]])
    assert_type(
        zmq.select([sock], [], []),
        tuple[list[zmq.Socket], list[zmq.Socket], list[zmq.Socket]],
    )
    assert_type(zmq.ZMQError(zmq.EAGAIN).errno, int | None)
    assert_type(zmq.ZMQError("message").strerror, str)
    assert_type(zmq.Again().errno, int | None)


async def async_api(copy: bool) -> None:
    ctx = azmq.Context()
    sock = ctx.socket(zmq.PAIR)
    assert_type(sock, azmq.Socket)
    assert_type(sock.context, azmq.Context)
    assert_type(ctx.socket(zmq.PAIR, AsyncSocket), AsyncSocket)
    assert_type(AsyncContext.instance(), AsyncContext)
    assert_type(AsyncContext.shadow(ctx), AsyncContext)
    assert_type(AsyncContext.from_share_key(1), AsyncContext)
    async with ctx.socket(zmq.PAIR, AsyncSocket) as sub_socket:
        assert_type(sub_socket, AsyncSocket)
        assert_type(sub_socket.underlying, AsyncSocket)
    sent = sock.send(b"x")
    assert_type(sent, FutureResult[zmq.MessageTracker | None])
    assert_type(await sent, zmq.MessageTracker | None)
    assert_type(sent.result(), zmq.MessageTracker | None)
    assert_type(sent.done(), bool)
    assert_type(sock.recv().result(), bytes)
    assert_type(await sock.recv(), bytes)
    assert_type(await sock.recv(copy=False), zmq.Frame)
    assert_type(await sock.recv(0, False), zmq.Frame)
    assert_type(await sock.recv(copy=copy), bytes | zmq.Frame)
    assert_type(await sock.recv_multipart(), list[bytes])
    assert_type(await sock.recv_multipart(copy=False), list[zmq.Frame])
    assert_type(await sock.recv_multipart(0, False), list[zmq.Frame])
    assert_type(await sock.recv_multipart(copy=copy), list[bytes] | list[zmq.Frame])
    assert_type(await sock.recv_serialized(decode), int)
    assert_type(asyncio.create_task(sock.recv_serialized(decode)), asyncio.Task[int])
    assert_type(await sock.recv_serialized(decode_frames, copy=False), str)
    assert_type(await sock.recv_serialized(decode_frames, 0, False), str)
    assert_type(await sock.recv_serialized(decode_dynamic, copy=copy), float)
    assert_type(await sock.send_serialized(42, encode), zmq.MessageTracker | None)
    shadow = zmq.Socket.shadow(sock)
    assert_type(shadow.recv(), bytes)
    assert_type(shadow.recv(copy=False), zmq.Frame)
    assert_type(shadow.recv_multipart(copy=copy), list[bytes] | list[zmq.Frame])
    assert_type(shadow.send(b"x"), zmq.MessageTracker | None)
    assert_type(shadow.getsockopt(zmq.LINGER), int)
    assert_type(shadow.identity, bytes)
    poller = azmq.Poller()
    poller.register(sock)
    assert_type(await poller.poll(), list[tuple[azmq.Socket, int]])


def stream_api(sock: zmq.Socket, copy: bool) -> None:
    stream = zmq.ZMQStream(sock)
    stream.on_recv(decode)
    stream.on_recv(decode_frames, copy=False)
    stream.on_recv(decode_dynamic, copy=copy)
    assert_type(stream.send(b"x"), zmq.MessageTracker | None)
    stream.send_multipart((part for part in [b"x"]), callback=sent)


def sent(parts: list[zmq.Sendable], tracker: zmq.MessageTracker | None) -> None:
    pass
