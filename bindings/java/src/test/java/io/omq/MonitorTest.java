package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class MonitorTest {
    @Test
    void monitorReceivesListeningEvent() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Monitor monitor = pull.monitor()) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");

            MonitorEvent event = receiveType(monitor, MonitorEventType.LISTENING);
            assertEquals(endpoint, event.endpoint().orElseThrow());
        }
    }

    @Test
    void monitorReceivesHandshakePeerInfo() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH).identity("push-id".getBytes());
             Monitor monitor = pull.monitor()) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            MonitorEvent event = receiveType(monitor, MonitorEventType.HANDSHAKE_SUCCEEDED);
            PeerInfo peer = event.peer().orElseThrow();
            assertEquals(endpoint, event.endpoint().orElseThrow());
            assertTrue(peer.connectionId().isPresent());
            assertTrue(peer.peerAddress().isPresent());
            assertEquals("Push", peer.socketType().orElseThrow());
            assertEquals("3.1", peer.zmtpVersion().orElseThrow());
        }
    }

    @Test
    void monitorTimeoutAndCloseAreTyped() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            Monitor monitor = pull.monitor();
            assertTrue(monitor.tryReceive().isEmpty());
            assertTrue(monitor.receive(Duration.ofMillis(1)).isEmpty());
            monitor.close();
            assertThrows(ClosedException.class, monitor::tryReceive);
        }
    }

    private static MonitorEvent receiveType(Monitor monitor, MonitorEventType type) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            MonitorEvent event = monitor.receive(Duration.ofMillis(200)).orElse(null);
            if (event != null && event.type() == type) {
                return event;
            }
        }
        throw new AssertionError("timed out waiting for " + type);
    }
}
