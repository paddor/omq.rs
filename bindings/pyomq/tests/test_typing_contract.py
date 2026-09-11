"""Check public types as a consumer, including deliberately invalid calls."""

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "typing"
INVALID = """\
import pyomq as zmq
import pyomq.asyncio as azmq

class BytesOnly:
    def __bytes__(self) -> bytes:
        return b"x"

def invalid(sync: zmq.Socket, async_: azmq.Socket) -> None:
    sync.send("text")  # reject
    sync.send(BytesOnly())  # reject
    async_.send("text")  # reject
    sync.send_multipart(["text"])  # reject
    async_.send_multipart([BytesOnly()])  # reject
    sync.identity = 42  # reject
    sync.linger = b"bytes"  # reject
    sync.set_plain_auth([("user", 1)])  # reject
    sync.set_curve_auth(["not bytes"])  # reject
    zmq.Frame("text")  # reject
    zmq.Socket.shadow(async_).send("text")  # reject
    sync.recv_serialized(lambda parts: len(parts), copy="no")  # reject
"""


def run_checker(checker, paths):
    if importlib.util.find_spec(checker) is None:
        pytest.skip(f"install the type-check dependency group for {checker}")
    arguments = {
        "ty": ["check", "--output-format", "concise"],
        "mypy": ["--show-column-numbers"],
        "pyright": ["--pythonpath", sys.executable],
    }
    return subprocess.run(
        [sys.executable, "-m", checker, *arguments[checker], *map(str, paths)],
        capture_output=True,
        text=True,
        timeout=40,
        check=False,
    )


@pytest.mark.parametrize("checker", ["ty", "mypy", "pyright"])
def test_consumer_contract(checker):
    paths = [FIXTURES / "consumer.py"]
    if checker != "ty":
        paths.append(FIXTURES / "narrowing.py")
    result = run_checker(checker, paths)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("checker", ["ty", "mypy", "pyright"])
def test_invalid_calls_are_rejected(checker, tmp_path):
    path = tmp_path / "invalid.py"
    path.write_text(INVALID)
    result = run_checker(checker, [path])
    assert result.returncode != 0, "invalid calls unexpectedly accepted"
    diagnostics = result.stdout + result.stderr
    for number, line in enumerate(INVALID.splitlines(), 1):
        if "# reject" in line:
            assert re.search(rf"invalid\.py:{number}:\d+", diagnostics), diagnostics


def test_native_stub_matches_runtime():
    pytest.importorskip("mypy.stubtest")
    result = subprocess.run(
        [sys.executable, "-m", "mypy.stubtest", "pyomq._native"],
        capture_output=True,
        text=True,
        timeout=40,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
