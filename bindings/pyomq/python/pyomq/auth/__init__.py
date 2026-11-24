"""Authentication with the API of pyzmq's ``zmq.auth``.

pyzmq answers ZAP requests on an inproc socket. pyomq installs the same
policy on each socket as a handshake callback. See ``Authenticator``.
Background variants live in ``pyomq.auth.thread`` (``ThreadAuthenticator``)
and ``pyomq.auth.asyncio`` (``AsyncioAuthenticator``).
"""

from .base import CURVE_ALLOW_ANY, Authenticator
from .certs import create_certificates, load_certificate, load_certificates

__all__ = [
    "CURVE_ALLOW_ANY",
    "Authenticator",
    "create_certificates",
    "load_certificate",
    "load_certificates",
]
