package io.omq;

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
            assertThrows(OMQException.class, () -> pull.bind("tcp://999.999.999.999:0"));
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
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.close();
            assertThrows(ClosedException.class, pull::receive);
        }
    }

    @Test
    void optionValidationMapsToOmqException() {
        try (Context context = OMQ.context();
             Socket dealer = context.socket(SocketType.DEALER)) {
            byte[] tooLong = new byte[256];
            assertThrows(OMQException.class, () -> dealer.identity(tooLong));
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
}
