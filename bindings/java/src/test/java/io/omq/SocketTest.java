package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

final class SocketTest {
    @Test
    void pushPullRoundTrip() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("hello");

            Message received = pull.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals("hello", received.text());
        }
    }

    @Test
    void multipartRoundTrip() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send(Message.multipart(
                    "route".getBytes(StandardCharsets.UTF_8),
                    "body".getBytes(StandardCharsets.UTF_8)));

            Message received = pull.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(2, received.partCount());
            assertArrayEquals("route".getBytes(StandardCharsets.UTF_8), received.part(0));
            assertArrayEquals("body".getBytes(StandardCharsets.UTF_8), received.part(1));
        }
    }

    @Test
    void byteBufferSendDoesNotAdvancePosition() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            ByteBuffer buffer = ByteBuffer.wrap("xxpayloadyy".getBytes(StandardCharsets.UTF_8));
            buffer.position(2);
            buffer.limit(9);
            push.send(buffer);

            assertEquals("payload", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertEquals(2, buffer.position());
            assertEquals(9, buffer.limit());
        }
    }

    @Test
    void trySendReturnsTrueWhenMessageAccepted() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            assertTrue(push.trySend("try"));
            assertEquals("try", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void trySendReturnsFalseWhenNoPeerCanAccept() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            push.bind("tcp://127.0.0.1:0");

            assertFalse(push.trySend("mute"));
        }
    }

    @Test
    void requestReplyRoundTrip() {
        try (Context context = OMQ.context();
             Socket rep = context.socket(SocketType.REP);
             Socket req = context.socket(SocketType.REQ)) {
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
    void receiveTimeoutReturnsEmptyOptional() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("tcp://127.0.0.1:0");
            assertFalse(pull.receive(Duration.ofMillis(20)).isPresent());
        }
    }

    @Test
    void optionsCannotChangeAfterMaterialization() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("tcp://127.0.0.1:0");
            assertThrows(OMQException.class, () -> pull.sendHighWaterMark(1));
        }
    }

    @Test
    void closeIsIdempotent() {
        Context context = OMQ.context();
        Socket pull = context.socket(SocketType.PULL);

        pull.close();
        pull.close();
        context.close();
        context.close();

        assertThrows(ClosedException.class, () -> pull.bind("tcp://127.0.0.1:0"));
        assertThrows(ClosedException.class, () -> context.socket(SocketType.PULL));
    }

    @Test
    void contextCloseClosesOpenSockets() {
        Context context = OMQ.context();
        Socket pull = context.socket(SocketType.PULL);

        context.close();

        assertThrows(ClosedException.class, () -> pull.bind("tcp://127.0.0.1:0"));
    }

    @Test
    void lz4TcpRoundTrip() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).compressionAutoTrain(true);
             Socket push = context.socket(SocketType.PUSH).compressionAutoTrain(true)) {
            String endpoint = pull.bind("lz4+tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("{\"kind\":\"json\",\"value\":42}");

            assertEquals("{\"kind\":\"json\",\"value\":42}",
                    pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }
}
