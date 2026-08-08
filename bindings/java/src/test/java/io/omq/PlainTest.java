package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class PlainTest {
    @Test
    void pushPullOverPlain() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).plainServer("alice", "secret");
             Socket push = context.socket(SocketType.PUSH).plainClient("alice", "secret")) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("hello over plain");

            assertEquals("hello over plain", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void reqRepOverPlain() {
        try (Context context = OMQ.context();
             Socket rep = context.socket(SocketType.REP).plainServer("alice", "secret");
             Socket req = context.socket(SocketType.REQ).plainClient("alice", "secret")) {
            String endpoint = rep.bind("tcp://127.0.0.1:0");
            req.connect(endpoint);
            req.waitConnected(1, Duration.ofSeconds(5));
            req.send("ping");
            assertEquals("ping", rep.receive(Duration.ofSeconds(5)).orElseThrow().text());
            rep.send("pong");
            assertEquals("pong", req.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void multipartOverPlain() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).plainServer("alice", "secret");
             Socket push = context.socket(SocketType.PUSH).plainClient("alice", "secret")) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send(Message.multipart("a".getBytes(), "bb".getBytes(), "ccc".getBytes()));

            Message received = pull.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(3, received.partCount());
            assertEquals("a", new String(received.part(0)));
            assertEquals("bb", new String(received.part(1)));
            assertEquals("ccc", new String(received.part(2)));
        }
    }

    @Test
    void plainRejectsBadPassword() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).plainServer("alice", "secret");
             Socket push = context.socket(SocketType.PUSH).plainClient("alice", "wrong")) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.send("blocked");

            assertTrue(pull.receive(Duration.ofMillis(500)).isEmpty());
        }
    }

    @Test
    void plainAuthenticatorReceivesPeerInfo() {
        AtomicReference<PeerInfo> seen = new AtomicReference<>();
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).plainServer(peer -> {
                 seen.set(peer);
                 return "PLAIN".equals(peer.mechanism().orElse(""))
                         && "alice".equals(peer.username().orElse(""))
                         && "secret".equals(peer.password().orElse(""))
                         && peer.identity().isEmpty();
             });
             Socket push = context.socket(SocketType.PUSH)
                     .identity("plain-client".getBytes(StandardCharsets.UTF_8))
                     .plainClient("alice", "secret")) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("allowed");

            assertEquals("allowed", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertEquals("alice", seen.get().username().orElseThrow());
            assertTrue(seen.get().publicKey().isEmpty());
        }
    }
}
