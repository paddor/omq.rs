"""Context-local inproc namespace isolation."""

import pyomq as zmq
import pyomq.asyncio as zmq_async
from pyomq.testing import rust_thread_send_via_share_key
import pytest


def test_two_contexts_same_inproc_name():
    ctx1 = zmq.Context()
    ctx2 = zmq.Context()
    try:
        s1 = ctx1.socket(zmq.PUSH)
        s2 = ctx2.socket(zmq.PUSH)
        s1.bind("inproc://shared-name")
        s2.bind("inproc://shared-name")
    finally:
        s1.close()
        s2.close()
        ctx1.term()
        ctx2.term()


def test_inproc_namespaced_roundtrip():
    ctx = zmq.Context()
    try:
        push = ctx.socket(zmq.PUSH)
        pull = ctx.socket(zmq.PULL)
        pull.bind("inproc://ns-test")
        push.connect("inproc://ns-test")
        push.send(b"namespaced")
        assert pull.recv() == b"namespaced"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_inproc_cross_context_isolation():
    """Messages sent in ctx1's inproc don't leak to ctx2's same-name inproc."""
    ctx1 = zmq.Context()
    ctx2 = zmq.Context()
    try:
        push1 = ctx1.socket(zmq.PUSH)
        pull1 = ctx1.socket(zmq.PULL)
        pull1.bind("inproc://isolated")
        push1.connect("inproc://isolated")

        push2 = ctx2.socket(zmq.PUSH)
        pull2 = ctx2.socket(zmq.PULL)
        pull2.bind("inproc://isolated")
        push2.connect("inproc://isolated")

        push1.send(b"ctx1")
        push2.send(b"ctx2")

        assert pull1.recv() == b"ctx1"
        assert pull2.recv() == b"ctx2"
    finally:
        for s in (push1, pull1, push2, pull2):
            s.close()
        ctx1.term()
        ctx2.term()


def test_share_key_context_roundtrip():
    ctx = zmq.Context()
    shared = zmq.Context.from_share_key(ctx.share_key())
    try:
        push = ctx.socket(zmq.PUSH)
        pull = shared.socket(zmq.PULL)
        pull.bind("inproc://shared-key")
        push.connect("inproc://shared-key")
        push.send(b"shared")
        assert pull.recv() == b"shared"
    finally:
        push.close()
        pull.close()
        shared.term()
        ctx.term()


def test_share_key_python_context_rust_thread_inproc():
    ctx = zmq.Context()
    try:
        pull = ctx.socket(zmq.PULL)
        pull.setsockopt(zmq.RCVTIMEO, 1000)
        pull.bind("inproc://python-rust-thread")
        rust_thread_send_via_share_key(
            ctx.share_key(),
            "inproc://python-rust-thread",
            b"rust-thread",
        )
        assert pull.recv() == b"rust-thread"
    finally:
        pull.close()
        ctx.term()


def test_share_key_term_does_not_destroy_owner():
    ctx = zmq.Context()
    shared = zmq.Context.from_share_key(ctx.share_key())
    try:
        shared.term()
        push = ctx.socket(zmq.PUSH)
        pull = ctx.socket(zmq.PULL)
        pull.bind("inproc://owner-still-alive")
        push.connect("inproc://owner-still-alive")
        push.send(b"alive")
        assert pull.recv() == b"alive"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_imported_context_observes_owner_term():
    ctx = zmq.Context()
    shared = zmq.Context.from_share_key(ctx.share_key())
    try:
        ctx.term()
        with pytest.raises(zmq.ContextTerminated):
            shared.socket(zmq.PULL).bind("inproc://owner-gone")
    finally:
        shared.term()


def test_imported_socket_observes_owner_term():
    ctx = zmq.Context()
    shared = zmq.Context.from_share_key(ctx.share_key())
    pull = None
    try:
        pull = shared.socket(zmq.PULL)
        pull.setsockopt(zmq.RCVTIMEO, 1000)
        pull.bind("inproc://owner-gone-after-bind")
        ctx.term()
        with pytest.raises(zmq.ContextTerminated):
            pull.recv()
    finally:
        if pull is not None:
            pull.close()
        shared.term()


@pytest.mark.asyncio
async def test_share_key_sync_async_roundtrip():
    ctx = zmq.Context()
    shared = zmq_async.Context.from_share_key(ctx.share_key())
    try:
        pull = ctx.socket(zmq.PULL)
        push = shared.socket(zmq.PUSH)
        pull.bind("inproc://shared-sync-async")
        push.connect("inproc://shared-sync-async")
        await push.send(b"async")
        assert pull.recv() == b"async"
    finally:
        push.close()
        pull.close()
        shared.term()
        ctx.term()
