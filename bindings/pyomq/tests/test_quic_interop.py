"""Native Rust / pyomq interoperability through verified QUIC connections."""

import asyncio
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from threading import Thread

import pyomq
import pyomq.asyncio
import pytest


def message_cases() -> list[list[bytes]]:
    def pattern(size: int) -> bytes:
        return (bytes(range(251)) * (size // 251 + 1))[:size]

    return [
        [b""],
        [b"\x00\xff\x80"],
        [b"head", b"", b"tail"],
        [pattern(256 * 1024)],
        [pattern(256 * 1024), b"", pattern(4097)],
    ]


@pytest.fixture(scope="module")
def rust_peer() -> Path:
    if not pyomq.has("quic"):
        if os.environ.get("OMQ_QUIC_INTEROP_REQUIRED") == "1":
            pytest.fail("pyomq was built without QUIC")
        pytest.skip("pyomq was built without QUIC")
    configured = os.environ.get("OMQ_QUIC_INTEROP_PEER")
    if configured:
        peer = Path(configured)
    else:
        root = Path(__file__).resolve().parents[3]
        metadata = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version=1"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        target = Path(json.loads(metadata.stdout)["target_directory"])
        suffix = ".exe" if sys.platform == "win32" else ""
        peer = target / "debug" / "examples" / f"quic_interop_peer{suffix}"
    if not peer.is_file():
        reason = "build Rust peer: cargo build -p omq-tokio --features quic --example quic_interop_peer"
        if os.environ.get("OMQ_QUIC_INTEROP_REQUIRED") == "1":
            pytest.fail(reason)
        pytest.skip(reason)
    return peer


@pytest.fixture(scope="module")
def certificates(rust_peer: Path, tmp_path_factory) -> Path:
    directory = tmp_path_factory.mktemp("quic-certs")
    subprocess.run([str(rust_peer), "certs", str(directory)], check=True, timeout=10)
    return directory


class Peer:
    def __init__(self, process: subprocess.Popen[str]):
        self.process = process
        self.lines: Queue[str] = Queue()
        self.reader = Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.lines.put(line.strip())

    def line(self) -> str:
        return self.lines.get(timeout=15)


@contextmanager
def running_peer(binary: Path, command: str, endpoint: str, certificates: Path):
    process = subprocess.Popen(
        [str(binary), command, endpoint, str(certificates)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    peer = Peer(process)
    try:
        yield peer
    finally:
        assert process.stdin is not None
        if process.poll() is None:
            try:
                process.stdin.write("stop\n")
                process.stdin.flush()
            except BrokenPipeError:
                pass
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        peer.reader.join(timeout=5)
        assert process.stderr is not None
        error = process.stderr.read()
        for pipe in [process.stdin, process.stdout, process.stderr]:
            assert pipe is not None
            pipe.close()
        assert process.returncode == 0, error
        assert not peer.reader.is_alive()


def configure(socket, certificates: Path, *, bind: bool):
    socket.linger = 0
    socket.sndtimeo = 10_000
    socket.rcvtimeo = 10_000
    socket.quic_stream_window = 16 * 1024
    socket.quic_trust_system = 0
    if bind:
        socket.quic_cert_pem = (certificates / "server.pem").read_bytes()
        socket.quic_key_pem = (certificates / "server.key").read_bytes()
    else:
        socket.quic_trust_pem = (certificates / "server.pem").read_bytes()
        socket.quic_server_name = b"localhost"


@pytest.mark.parametrize("python_binds", [False, True])
def test_rust_python_quic_reqrep(
    rust_peer: Path, certificates: Path, python_binds: bool
):
    with (
        pyomq.Context() as context,
        context.socket(pyomq.REP if python_binds else pyomq.REQ) as socket,
    ):
        configure(socket, certificates, bind=python_binds)
        if python_binds:
            socket.bind("quic://127.0.0.1:0")
            endpoint = socket.last_endpoint.decode()
            command = "connect-req"
        else:
            endpoint = "quic://127.0.0.1:0"
            command = "bind-rep"
        with running_peer(rust_peer, command, endpoint, certificates) as peer:
            ready = peer.line()
            if python_binds:
                assert ready == "CONNECTED"
                for expected in message_cases():
                    assert socket.recv_multipart() == expected
                    socket.send_multipart(expected)
                assert peer.line() == "OK"
            else:
                socket.connect(ready)
                for expected in message_cases():
                    socket.send_multipart(expected)
                    assert socket.recv_multipart() == expected
                    assert peer.line() == "REPLIED"


@pytest.mark.parametrize("python_binds", [False, True])
async def test_rust_async_python_quic_reqrep(
    rust_peer: Path, certificates: Path, python_binds: bool
):
    with (
        pyomq.asyncio.Context() as context,
        context.socket(pyomq.REP if python_binds else pyomq.REQ) as socket,
    ):
        configure(socket, certificates, bind=python_binds)
        if python_binds:
            socket.bind("quic://127.0.0.1:0")
            endpoint = socket.last_endpoint.decode()
            command = "connect-req"
        else:
            endpoint = "quic://127.0.0.1:0"
            command = "bind-rep"
        with running_peer(rust_peer, command, endpoint, certificates) as peer:
            ready = await asyncio.to_thread(peer.line)
            if python_binds:
                assert ready == "CONNECTED"
                for expected in message_cases():
                    assert await socket.recv_multipart() == expected
                    await socket.send_multipart(expected)
                assert await asyncio.to_thread(peer.line) == "OK"
            else:
                socket.connect(ready)
                for expected in message_cases():
                    await socket.send_multipart(expected)
                    assert await socket.recv_multipart() == expected
                    assert await asyncio.to_thread(peer.line) == "REPLIED"


@pytest.mark.parametrize("rejection", ["untrusted", "wrong-name"])
def test_python_quic_rejects_invalid_server(
    rust_peer: Path, certificates: Path, rejection: str
):
    with running_peer(rust_peer, "listen", "quic://127.0.0.1:0", certificates) as peer:
        endpoint = peer.line()
        with pyomq.Context() as context, context.socket(pyomq.REQ) as socket:
            configure(socket, certificates, bind=False)
            if rejection == "untrusted":
                socket.quic_trust_pem = (certificates / "unrelated.pem").read_bytes()
            else:
                socket.quic_server_name = b"wrong.example"
            monitor = socket.monitor()
            socket.connect(endpoint)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    event = monitor.recv(timeout_ms=1000)
                except pyomq.Again:
                    continue
                if event["event"] == "handshake_failed":
                    assert "certificate" in event["reason"].lower()
                    break
            else:
                pytest.fail("no certificate rejection reported")


def test_quic_options_roundtrip_and_validation(rust_peer: Path):
    with pyomq.Context() as context, context.socket(pyomq.REQ) as socket:
        assert socket.quic_trust_system == 1
        assert socket.quic_stream_window == 1024 * 1024
        assert socket.quic_max_ready_peers == 1024
        for option in [
            pyomq.OMQ_QUIC_CERT_PEM,
            pyomq.OMQ_QUIC_KEY_PEM,
            pyomq.OMQ_QUIC_TRUST_PEM,
            pyomq.OMQ_QUIC_SERVER_NAME,
        ]:
            assert socket.getsockopt(option) == b""
            socket.setsockopt(option, b"example")
            assert socket.getsockopt(option) == b"example"
            socket.setsockopt(option, b"")
            assert socket.getsockopt(option) == b""
        for invalid in [0, 1024, 256 * 1024 * 1024 + 1]:
            with pytest.raises(ValueError):
                socket.quic_stream_window = invalid
            assert socket.quic_stream_window == 1024 * 1024
        with pytest.raises(ValueError):
            socket.quic_max_ready_peers = 0
        with pytest.raises(ValueError):
            socket.quic_server_name = b"\xff"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_quic_options_are_fixed_after_bind(
    rust_peer: Path, certificates: Path, asynchronous: bool
):
    factory = pyomq.asyncio.Context if asynchronous else pyomq.Context
    with factory() as context, context.socket(pyomq.REP) as socket:
        configure(socket, certificates, bind=True)
        socket.bind("quic://127.0.0.1:0")
        with pytest.raises(pyomq.ZMQError):
            socket.quic_stream_window = 32 * 1024
        assert socket.quic_stream_window == 16 * 1024
