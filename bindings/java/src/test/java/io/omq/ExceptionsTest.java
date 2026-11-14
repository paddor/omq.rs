package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class ExceptionsTest {
    @Test
    void invalidEndpointRaisesTypedException() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(InvalidEndpointException.class, () -> pull.bind("not-an-endpoint"));
            NameResolutionException error = assertThrows(
                    NameResolutionException.class,
                    () -> pull.bind("tcp://999.999.999.999:0"));
            assertEquals("bind", error.operation());
            assertEquals("tcp://999.999.999.999:0", error.endpoint());
            assertTrue(error.detail().length() > 0);
        }
    }

    @Test
    void bindIoErrorRaisesBindException() {
        try (Context context = OMQ.context();
             Socket socket = context.socket(SocketType.PULL)) {
            String endpoint = "tcp://192.0.2.1:0";

            BindException error = assertThrows(BindException.class, () -> socket.bind(endpoint));
            assertEquals("bind", error.operation());
            assertEquals(endpoint, error.endpoint());
            assertTrue(error.detail().length() > 0);
            assertTrue(error.getMessage().contains(endpoint));
        }
    }

    @Test
    void connectDnsErrorRaisesNameResolutionException() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            NameResolutionException error = assertThrows(
                    NameResolutionException.class,
                    () -> push.connect("tcp://999.999.999.999:5555"));
            assertEquals("connect", error.operation());
            assertEquals("tcp://999.999.999.999:5555", error.endpoint());
            assertTrue(error.detail().length() > 0);
        }
    }

    @Test
    void timeoutIsTypedException() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("tcp://127.0.0.1:0");
            assertTrue(pull.receive(Duration.ofMillis(20)).isEmpty());
            assertTrue(pull.tryReceive().isEmpty());
        }
    }

    @Test
    void closedSocketRaisesTypedException() {
        try (Context context = OMQ.context()) {
            Socket pull = context.socket(SocketType.PULL);
            pull.close();
            assertThrows(ClosedException.class, pull::receive);
        }
    }

    @Test
    void optionValidationUsesJavaArgumentException() {
        try (Context context = OMQ.context();
             Socket dealer = context.socket(SocketType.DEALER)) {
            byte[] tooLong = new byte[256];
            assertThrows(IllegalArgumentException.class, () -> dealer.identity(tooLong));
        }
    }

    @Test
    void maxMessageSizeDropsOversizedReceive() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).maxMessageSize(8);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("too-large-message");

            assertTrue(pull.receive(Duration.ofSeconds(5)).isEmpty());
        }
    }

    @Test
    void maxMessagePartsDropsReceiveOverLimit() {
        byte[] part = {1};
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).maxMessageParts(2);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send(Message.multipart(part, part));
            assertEquals(2, pull.receive(Duration.ofSeconds(5)).orElseThrow().partCount());

            push.send(Message.multipart(part, part, part));
            assertTrue(pull.receive(Duration.ofSeconds(1)).isEmpty());
        }
    }

    @Test
    void noMaxMessagePartsLiftsDefaultLimit() {
        byte[][] parts = new byte[1025][1];
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).noMaxMessageParts();
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send(Message.multipart(parts));
            assertEquals(1025, pull.receive(Duration.ofSeconds(5)).orElseThrow().partCount());
        }
    }

    @Test
    void maxPeerSubscriptionsIgnoresPrefixOverLimit() {
        try (Context context = OMQ.context();
             Socket xpub = context.socket(SocketType.XPUB).maxPeerSubscriptions(1);
             Socket sub = context.socket(SocketType.SUB)) {
            String endpoint = xpub.bind("tcp://127.0.0.1:0");
            sub.subscribe("a");
            sub.connect(endpoint);
            assertArrayEquals(
                    new byte[] {1, 'a'}, xpub.receive(Duration.ofSeconds(5)).orElseThrow().bytes());

            // "b" is over the limit, so XPUB never reports it.
            sub.subscribe("b");
            sub.unsubscribe(new byte[] {'a'});
            sub.subscribe("c");
            assertArrayEquals(
                    new byte[] {0, 'a'}, xpub.receive(Duration.ofSeconds(5)).orElseThrow().bytes());
            assertArrayEquals(
                    new byte[] {1, 'c'}, xpub.receive(Duration.ofSeconds(5)).orElseThrow().bytes());
        }
    }
}
