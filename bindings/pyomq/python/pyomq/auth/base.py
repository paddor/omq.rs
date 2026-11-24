"""Context-wide authentication with the API of pyzmq's ``zmq.auth``."""

from __future__ import annotations

import asyncio
import hmac
import inspect
import logging
import os
from collections.abc import Awaitable
from typing import Any

import pyomq
from pyomq import _zap
from pyomq._native import PeerInfo
from pyomq.utils import z85

from .certs import load_certificates

__all__ = ["CURVE_ALLOW_ANY", "Authenticator"]

CURVE_ALLOW_ANY = "*"

# (accepted, reason), as in pyzmq's ZAP replies.
type _Decision = tuple[bool, bytes]

# Upper bound for an awaitable policy result, so a stuck coroutine cannot
# hold a handshake worker thread forever. Longer than the default handshake
# timeout (10 s).
_AWAIT_TIMEOUT = 30.0


class Authenticator:
    """Authentication policy for every socket of a context.

    After ``start()``, each CURVE or PLAIN server socket of the context, and
    each NULL socket with a ``zap_domain``, asks this policy during the
    handshake, as libzmq asks a ZAP handler. Unlike pyzmq, no ZAP socket or
    thread is involved: the policy runs on a handshake worker thread.

    - Start the authenticator before those sockets bind or connect.
    - Configuration changes apply to later handshakes.
    - After ``stop()``, those sockets reject new peers.
    - Until policies are added, NULL clients are admitted and PLAIN and
      CURVE clients are rejected.
    - A policy that raises rejects the peer and logs the error. Awaitable
      results get 30 seconds.
    - Terminating the context stops the authenticator.
    - An explicit ``set_curve_auth()`` or ``set_plain_auth()`` on a socket
      takes precedence over the authenticator.
    """

    context: pyomq.Context[Any]
    encoding: str
    allow_any: bool
    credentials_providers: dict[str, Any]
    zap_socket: None
    passwords: dict[str, dict[str, str]]
    certs: dict[str, dict[bytes, Any]]
    log: Any

    def __init__(
        self,
        context: pyomq.Context[Any] | None = None,
        encoding: str = "utf-8",
        log: Any = None,
    ) -> None:
        self.context = context or pyomq.Context.instance()
        self.encoding = encoding
        self.allow_any = False
        self.credentials_providers = {}
        self.zap_socket = None
        self._allowed: set[str] = set()
        self._denied: set[str] = set()
        self.passwords = {}
        self.certs = {}
        self.log = log or logging.getLogger("pyomq.auth")
        self._zap_key: int | None = None

    def start(self) -> None:
        """Authenticate the context's handshakes.

        Raises ``ZMQError`` (``EADDRINUSE``) when the context already has a
        running authenticator. Raises ``RuntimeError`` when a CURVE server or
        a socket with a ZAP domain is already in use without one.
        """
        key = _zap.context_key(self.context)
        _zap.register(key, self)
        self._zap_key = key
        self.log.debug("Starting")

    def stop(self) -> None:
        """Stop authenticating; covered sockets then reject new peers."""
        key, self._zap_key = self._zap_key, None
        if key is not None:
            _zap.unregister(key, self)

    def _is_running(self) -> bool:
        return _zap.is_running(self._zap_key, self)

    def allow(self, *addresses: str) -> None:
        """Admit only these IP addresses. Mutually exclusive with ``deny``.

        NULL clients from these addresses are accepted. PLAIN and CURVE
        clients still have to authenticate.
        """
        if self._denied:
            raise ValueError("Only use allow or deny, not both")
        self.log.debug("Allowing %s", ",".join(addresses))
        self._allowed.update(addresses)

    def deny(self, *addresses: str) -> None:
        """Reject these IP addresses. Mutually exclusive with ``allow``."""
        if self._allowed:
            raise ValueError("Only use allow or deny, not both")
        self.log.debug("Denying %s", ",".join(addresses))
        self._denied.update(addresses)

    def configure_plain(
        self, domain: str = "*", passwords: dict[str, str] | None = None
    ) -> None:
        """Accept these PLAIN ``{username: password}`` pairs for ``domain``.

        ``"*"`` covers every domain. The dict is kept by reference, so later
        changes to it apply.
        """
        if passwords:
            self.passwords[domain] = passwords
        self.log.debug("Configure plain: %s", domain)

    def configure_curve(
        self, domain: str = "*", location: str | os.PathLike[str] = "."
    ) -> None:
        """Accept the CURVE client keys of the ``*.key`` files in ``location``.

        Call it again after adding or removing certificates. Pass
        ``CURVE_ALLOW_ANY`` to accept every client key.
        """
        self.log.debug("Configure curve: %s[%s]", domain, location)
        if location == CURVE_ALLOW_ANY:
            self.allow_any = True
            return

        self.allow_any = False
        try:
            self.certs[domain] = load_certificates(location)
        except Exception as e:
            self.log.error("Failed to load CURVE certs from %s: %s", location, e)

    def configure_curve_callback(
        self, domain: str = "*", credentials_provider: Any = None
    ) -> None:
        """Ask ``credentials_provider.callback(domain, z85_key)`` per client.

        The callback returns a truthy value to accept. It may return an
        awaitable.
        """
        self.allow_any = False
        if credentials_provider is not None:
            self.credentials_providers[domain] = credentials_provider
        else:
            self.log.error("None credentials_provider provided for domain:%s", domain)

    def curve_user_id(self, client_public_key: bytes) -> str:
        """Return the Z85 text of a CURVE client key.

        Kept for pyzmq compatibility. pyomq does not attach a User-Id to
        received messages.
        """
        return z85.encode(client_public_key).decode("ascii")

    def configure_gssapi(self, domain: str = "*", location: str | None = None) -> None:
        """No-op for pyzmq compatibility. pyomq has no GSSAPI mechanism."""

    def _zap_admit(self, mechanism: str, domain: bytes, peer: PeerInfo) -> bool:
        try:
            return self._admit(mechanism, domain.decode(self.encoding, "replace"), peer)
        except Exception:
            self.log.exception("Authentication failed with an error")
            return False

    def _admit(self, mechanism: str, domain: str, peer: PeerInfo) -> bool:
        """Decide one handshake, following pyzmq's ZAP handler."""
        address = peer.peer_address or ""
        allowed = False
        denied = False
        reason = b"NO ACCESS"

        if self._allowed:
            if _address_in(address, self._allowed):
                allowed = True
                self.log.debug("PASSED (allowed) address=%s", address)
            else:
                denied = True
                reason = b"Address not allowed"
                self.log.debug("DENIED (not allowed) address=%s", address)
        elif self._denied:
            if _address_in(address, self._denied):
                denied = True
                reason = b"Address denied"
                self.log.debug("DENIED (denied) address=%s", address)
            else:
                allowed = True
                self.log.debug("PASSED (not denied) address=%s", address)

        if not denied:
            if mechanism == "NULL" and not allowed:
                self.log.debug("ALLOWED (NULL)")
                allowed = True
            elif mechanism == "PLAIN":
                allowed, reason = self._resolve(
                    self._authenticate_plain(
                        domain, peer.username or "", peer.password or ""
                    )
                )
            elif mechanism == "CURVE":
                allowed, reason = self._resolve(
                    self._authenticate_curve(domain, z85.decode(peer.public_key))
                )

        self.log.debug("%s %s", "ALLOWED" if allowed else "DENIED", reason)
        return allowed

    # Subclasses may override the policy methods with coroutines, as
    # pyzmq's _authenticate_curve is one.
    def _authenticate_plain(
        self, domain: str, username: str, password: str
    ) -> _Decision | Awaitable[_Decision]:
        if not self.passwords:
            self.log.debug("DENIED (PLAIN) No passwords defined")
            return False, b"No passwords defined"

        users = self.passwords.get(domain or "*")
        if users is None:
            return False, b"Invalid domain"
        expected = users.get(username)
        if expected is None:
            return False, b"Invalid username"
        if not hmac.compare_digest(password.encode(), _as_bytes(expected)):
            return False, b"Invalid password"

        self.log.debug("ALLOWED (PLAIN) domain=%s username=%s", domain, username)
        return True, b"OK"

    def _authenticate_curve(
        self, domain: str, client_key: bytes
    ) -> _Decision | Awaitable[_Decision]:
        if self.allow_any:
            self.log.debug("ALLOWED (CURVE allow any client)")
            return True, b"OK"

        domain = domain or "*"
        z85_client_key = z85.encode(client_key)
        if self.credentials_providers:
            provider = self.credentials_providers.get(domain)
            if provider is None:
                return False, b"Unknown domain"
            accepted = self._resolve(provider.callback(domain, z85_client_key))
        else:
            keys = self.certs.get(domain)
            if keys is None:
                return False, b"Unknown domain"
            accepted = keys.get(z85_client_key)

        self.log.debug(
            "%s (CURVE) domain=%s client_key=%s",
            "ALLOWED" if accepted else "DENIED",
            domain,
            z85_client_key,
        )
        return (True, b"OK") if accepted else (False, b"Unknown key")

    def _resolve(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            return self._run_awaitable(value)
        return value

    def _run_awaitable(self, awaitable: Awaitable[Any]) -> Any:
        """Wait for an awaitable policy result on a private event loop."""
        return asyncio.run(asyncio.wait_for(_wait(awaitable), _AWAIT_TIMEOUT))


async def _wait(awaitable: Awaitable[Any]) -> Any:
    return await awaitable


def _as_bytes(value: str | bytes) -> bytes:
    return value.encode() if isinstance(value, str) else bytes(value)


def _address_in(address: str, addresses: set[str]) -> bool:
    if address in addresses:
        return True
    # A dual-stack listener reports IPv4 clients as IPv4-mapped IPv6.
    return address.startswith("::ffff:") and address[7:] in addresses
