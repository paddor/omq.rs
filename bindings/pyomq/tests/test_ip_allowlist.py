"""Listener IP allowlist through sync and asyncio sockets.

The second source address is 127.0.0.2, which Linux routes on loopback.
"""

import asyncio
import errno
import socket
import subprocess
import sys
import textwrap

import pyomq
import pyomq.asyncio
import pytest

pyzmq = pytest.importorskip("zmq")

# PyZMQ requires the selector event loop on Windows.
pytestmark = [
    pytest.mark.event_loop("selector"),
    pytest.mark.skipif(sys.platform != "linux", reason="needs 127.0.0.2 on loopback"),
]

LISTED = "127.0.0.2"
UNLISTED = "127.0.0.1"


def address(endpoint):
    host, port = endpoint.removeprefix("tcp://").rsplit(":", 1)
    return host, int(port)


def raw_bytes_until_close(source, endpoint):
    """Bytes a raw TCP client from `source` receives before the listener
    closes it. A ZMTP listener that admits the client sends its greeting."""
    with socket.create_connection(
        address(endpoint), timeout=5, source_address=(source, 0)
    ) as raw:
        received = b""
        try:
            while chunk := raw.recv(64):
                received += chunk
                if len(received) >= 10:
                    break
        except (ConnectionResetError, TimeoutError):
            pass
        return received


def pyzmq_push(context, source, endpoint, body):
    push = context.socket(pyzmq.PUSH)
    push.linger = 0
    push.connect(f"tcp://{source}:0;{endpoint.removeprefix('tcp://')}")
    push.send(body)
    return push


def wait_denied(monitor):
    while True:
        event = monitor.recv(timeout_ms=5000)
        if event["event"] == "handshake_failed" and "allowlist" in event["reason"]:
            return


@pytest.mark.parametrize("materialize_first", [False, True])
def test_sync_socket_admits_only_listed_pyzmq_peers(materialize_first):
    with (
        pyomq.Context() as ctx,
        ctx.socket(pyomq.PULL) as pull,
        pyzmq.Context() as zctx,
    ):
        pull.rcvtimeo = 5000
        # monitor() builds the native socket, so the setter updates it live.
        monitor = pull.monitor() if materialize_first else None
        pull.set_ip_allowlist([LISTED])
        monitor = monitor or pull.monitor()
        endpoint = pull.bind("tcp://127.0.0.1:0").addr

        assert raw_bytes_until_close(UNLISTED, endpoint) == b""
        denied = pyzmq_push(zctx, UNLISTED, endpoint, b"denied")
        wait_denied(monitor)

        listed = pyzmq_push(zctx, LISTED, endpoint, b"listed")
        assert pull.recv() == b"listed"
        assert pull.poll(300) == 0

        denied.close()
        listed.close()


@pytest.mark.parametrize("materialize_first", [False, True])
async def test_async_socket_admits_only_listed_pyzmq_peers(materialize_first):
    with (
        pyomq.asyncio.Context() as ctx,
        ctx.socket(pyomq.PULL) as pull,
        pyzmq.Context() as zctx,
    ):
        monitor = pull.monitor() if materialize_first else None
        pull.set_ip_allowlist([LISTED])
        monitor = monitor or pull.monitor()
        endpoint = pull.bind("tcp://127.0.0.1:0").addr

        assert raw_bytes_until_close(UNLISTED, endpoint) == b""
        denied = pyzmq_push(zctx, UNLISTED, endpoint, b"denied")
        await asyncio.to_thread(wait_denied, monitor)

        listed = pyzmq_push(zctx, LISTED, endpoint, b"listed")
        assert await asyncio.wait_for(pull.recv(), 5) == b"listed"
        assert await pull.poll(300) == 0

        denied.close()
        listed.close()


@pytest.mark.parametrize("context", [pyomq.Context, pyomq.asyncio.Context])
def test_invalid_entries_raise_and_keep_the_current_list(context):
    with context() as ctx, ctx.socket(pyomq.PULL) as pull:
        pull.set_ip_allowlist([LISTED])
        with pytest.raises(ValueError, match="entry 1"):
            pull.set_ip_allowlist([LISTED, "127.0.0.1:80"])
        for wrong in ["127.0.0.1", b"127.0.0.1", [1], 7]:
            with pytest.raises(TypeError):
                pull.set_ip_allowlist(wrong)

        endpoint = pull.bind("tcp://127.0.0.1:0").addr
        assert raw_bytes_until_close(UNLISTED, endpoint) == b""
        assert raw_bytes_until_close(LISTED, endpoint) != b""


@pytest.mark.parametrize("context", [pyomq.Context, pyomq.asyncio.Context])
def test_setter_after_tcp_bind_raises_until_unbind(context):
    with context() as ctx, ctx.socket(pyomq.PULL) as pull:
        endpoint = pull.bind("tcp://127.0.0.1:0").addr
        with pytest.raises(pyomq.ZMQError) as raised:
            pull.set_ip_allowlist([LISTED])
        assert raised.value.errno == errno.EINVAL
        assert raw_bytes_until_close(UNLISTED, endpoint) != b""

        pull.unbind(endpoint)
        pull.set_ip_allowlist([])
        endpoint = pull.bind("tcp://127.0.0.1:0").addr
        for source in [UNLISTED, LISTED]:
            assert raw_bytes_until_close(source, endpoint) == b""


def test_setter_never_deadlocks_with_close_on_another_thread():
    # A deadlock leaves the GIL held, so it runs in a child with a timeout.
    script = textwrap.dedent(
        """
        import threading
        import pyomq
        import pyomq.asyncio

        for _ in range(300):
            ctx = pyomq.asyncio.Context()
            pull = ctx.socket(pyomq.PULL)
            pull.monitor()
            started = threading.Event()

            def spin():
                started.set()
                try:
                    while True:
                        pull.set_ip_allowlist(["127.0.0.2"])
                except pyomq.ZMQError:
                    pass

            setter = threading.Thread(target=spin)
            setter.start()
            started.wait()
            pull.close()
            setter.join()
            ctx.term()
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True, timeout=30)


@pytest.mark.parametrize("context", [pyomq.Context, pyomq.asyncio.Context])
def test_none_clears_the_list(context):
    with context() as ctx, ctx.socket(pyomq.PULL) as pull:
        pull.set_ip_allowlist([LISTED])
        pull.set_ip_allowlist(None)
        endpoint = pull.bind("tcp://127.0.0.1:0").addr
        assert raw_bytes_until_close(UNLISTED, endpoint) != b""
