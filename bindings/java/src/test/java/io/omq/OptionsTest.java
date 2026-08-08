package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class OptionsTest {
    @Test
    void rejectsNegativeHighWaterMarks() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            assertThrows(IllegalArgumentException.class, () -> push.sendHighWaterMark(-1));
            assertThrows(IllegalArgumentException.class, () -> push.receiveHighWaterMark(-1));
        }
    }

    @Test
    void rejectsNegativeDurations() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            assertThrows(IllegalArgumentException.class, () -> push.linger(Duration.ofMillis(-1)));
            assertThrows(IllegalArgumentException.class,
                    () -> push.heartbeatInterval(Duration.ofMillis(-1)));
            assertThrows(IllegalArgumentException.class,
                    () -> push.handshakeTimeout(Duration.ofMillis(-1)));
        }
    }

    @Test
    void acceptsPreMaterializationOptions() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            push.linger(Duration.ZERO)
                    .lingerForever()
                    .sendHighWaterMark(10)
                    .receiveHighWaterMark(11)
                    .heartbeatInterval(Duration.ofMillis(100))
                    .heartbeatOff()
                    .handshakeTimeout(Duration.ofSeconds(1))
                    .noMaxMessageSize()
                    .compressionThreshold(128)
                    .compressionDefaultThreshold()
                    .compressionLevel(1)
                    .compressionDefaultLevel();
        }
    }

    @Test
    void optionsCannotChangeAfterConnect() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            push.connect("tcp://127.0.0.1:" + TestSupport.freePort());
            assertThrows(OMQException.class, () -> push.linger(Duration.ZERO));
        }
    }

    @Test
    void rejectsNegativeMaxMessageSize() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(IllegalArgumentException.class, () -> pull.maxMessageSize(-1));
        }
    }

    @Test
    void rejectsNegativeCompressionThreshold() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(IllegalArgumentException.class, () -> pull.compressionThreshold(-1));
        }
    }

    @Test
    void positiveSubMillisecondDurationsRoundUp() {
        assertEquals(0, Socket.millis(Duration.ZERO));
        assertEquals(1, Socket.millis(Duration.ofNanos(1)));
        assertEquals(2, Socket.millis(Duration.ofNanos(1_000_001)));
    }

    @Test
    void hugeDurationsSaturate() {
        assertEquals(Long.MAX_VALUE, Socket.millis(Duration.ofSeconds(Long.MAX_VALUE)));
    }
}
