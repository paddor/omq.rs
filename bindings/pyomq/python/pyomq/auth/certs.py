"""CURVE certificate files in the czmq and pyzmq format.

``name.key`` holds the public key and ``name.key_secret`` both keys. Keys
are Z85 text in a ZPL ``curve`` section.
"""

from __future__ import annotations

import datetime
import glob
import os

import pyomq

__all__ = ["create_certificates", "load_certificate", "load_certificates"]

_PUBLIC_BANNER = """\
#   ****  Generated on {0} by pyomq  ****
#   ZeroMQ CURVE Public Certificate
#   Exchange securely, or use a secure mechanism to verify the contents
#   of this file after exchange. Store public certificates in your home
#   directory, in the .curve subdirectory.
"""

_SECRET_BANNER = """\
#   ****  Generated on {0} by pyomq  ****
#   ZeroMQ CURVE **Secret** Certificate
#   DO NOT PROVIDE THIS FILE TO OTHER USERS nor change its permissions.
"""


def create_certificates(
    key_dir: str | os.PathLike[str],
    name: str,
    metadata: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Create a CURVE key pair as ``name.key`` and ``name.key_secret``.

    Returns the paths of the public and secret certificate files. The secret
    file is created readable by its owner only.
    """
    public_key, secret_key = pyomq.curve_keypair()
    base = os.path.join(key_dir, name)
    public_file = f"{base}.key"
    secret_file = f"{base}.key_secret"
    now = datetime.datetime.now(datetime.UTC)

    _write(public_file, _PUBLIC_BANNER.format(now), public_key, None, metadata)
    _write(secret_file, _SECRET_BANNER.format(now), public_key, secret_key, metadata)
    return public_file, secret_file


def load_certificate(
    filename: str | os.PathLike[str],
) -> tuple[bytes, bytes | None]:
    """Return ``(public_key, secret_key)`` from a certificate file.

    ``secret_key`` is ``None`` for a public certificate. Raises ``OSError``
    for a missing file and ``ValueError`` when it holds no public key.
    """
    if not os.path.exists(filename):
        raise OSError(f"Invalid certificate file: {filename}")

    public_key = None
    secret_key = None
    with open(filename, "rb") as file:
        for line in file:
            name, equals, value = line.partition(b"=")
            if not equals:
                continue
            name = name.strip()
            if name == b"public-key":
                public_key = _unquote(value)
            elif name == b"secret-key":
                secret_key = _unquote(value)

    if public_key is None:
        raise ValueError(f"No public key found in {filename}")
    return public_key, secret_key


def load_certificates(directory: str | os.PathLike[str] = ".") -> dict[bytes, bool]:
    """Map the public key of every ``*.key`` file in ``directory`` to True."""
    if not os.path.isdir(directory):
        raise OSError(f"Invalid certificate directory: {directory}")

    certs = {}
    for cert_file in glob.glob(os.path.join(directory, "*.key")):
        public_key, _ = load_certificate(cert_file)
        if public_key:
            certs[public_key] = True
    return certs


def _unquote(value: bytes) -> bytes:
    return value.strip().strip(b"'\"")


def _write(
    filename: str,
    banner: str,
    public_key: bytes,
    secret_key: bytes | None,
    metadata: dict[str, str] | None,
) -> None:
    lines = [banner, "metadata\n"]
    for key, value in (metadata or {}).items():
        lines.append(f"    {_text(key)} = {_text(value)}\n")
    lines.append("curve\n")
    lines.append(f'    public-key = "{public_key.decode("ascii")}"\n')
    if secret_key is not None:
        lines.append(f'    secret-key = "{secret_key.decode("ascii")}"\n')

    mode = 0o600 if secret_key is not None else 0o644
    fd = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    if hasattr(os, "fchmod"):
        os.fchmod(fd, mode)  # an existing file keeps its mode otherwise
    with open(fd, "w", encoding="utf-8") as file:
        file.writelines(lines)


def _text(value: str | bytes) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value
