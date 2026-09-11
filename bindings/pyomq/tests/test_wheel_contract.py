"""Installed package must carry the native stubs and Python 3.12 ABI contract."""

from importlib.metadata import distribution
from pathlib import Path

import pyomq


def test_installed_typing_files_and_python_floor():
    package = Path(pyomq.__file__).parent
    assert (package / "py.typed").is_file()
    assert (package / "_native.pyi").is_file()
    metadata = distribution("pyomq")
    assert metadata.requires is None
    assert metadata.metadata["Requires-Python"] == ">=3.12"
    wheel = metadata.read_text("WHEEL")
    assert wheel is not None
    assert "Tag: cp312-abi3-" in wheel
