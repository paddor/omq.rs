"""Z85 encoding (ZeroMQ RFC 32), compatible with pyzmq's ``zmq.utils.z85``.

Each group of 4 bytes, read as a big-endian 32-bit integer, becomes 5
characters.
"""

from __future__ import annotations

import struct

__all__ = ["Z85CHARS", "Z85MAP", "decode", "encode"]

Z85CHARS = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
Z85MAP = {char: value for value, char in enumerate(Z85CHARS)}


def encode(rawbytes: bytes) -> bytes:
    """Encode bytes whose length is a multiple of 4 as Z85."""
    if len(rawbytes) % 4:
        raise ValueError(f"length must be multiple of 4, not {len(rawbytes)}")

    encoded = bytearray()
    for (value,) in struct.iter_unpack(">I", rawbytes):
        group = bytearray(5)
        for index in range(4, -1, -1):
            value, digit = divmod(value, 85)
            group[index] = Z85CHARS[digit]
        encoded += group
    return bytes(encoded)


def decode(z85bytes: bytes | str) -> bytes:
    """Decode Z85 text whose length is a multiple of 5."""
    if isinstance(z85bytes, str):
        try:
            z85bytes = z85bytes.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError(
                "string argument should contain only ASCII characters"
            ) from None
    if len(z85bytes) % 5:
        raise ValueError(f"Z85 length must be multiple of 5, not {len(z85bytes)}")

    decoded = bytearray()
    for start in range(0, len(z85bytes), 5):
        value = 0
        for char in z85bytes[start : start + 5]:
            digit = Z85MAP.get(char)
            if digit is None:
                raise ValueError(f"invalid Z85 character {chr(char)!r}")
            value = value * 85 + digit
        if value > 0xFFFF_FFFF:
            raise ValueError("Z85 group exceeds 32 bits")
        decoded += value.to_bytes(4, "big")
    return bytes(decoded)
