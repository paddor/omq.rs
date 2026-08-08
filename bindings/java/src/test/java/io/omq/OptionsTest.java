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
                    .workloadProfile(WorkloadProfile.LATENCY)
                    .defaultWorkloadProfile()
                    .reconnectDisabled()
                    .reconnectInterval(Duration.ofMillis(100))
                    .reconnectExponential(Duration.ofMillis(10), Duration.ofSeconds(1))
                    .reconnectStopConnRefused(true)
                    .sendHighWaterMark(10)
                    .receiveHighWaterMark(11)
                    .heartbeatInterval(Duration.ofMillis(100))
                    .heartbeatTtl(Duration.ofSeconds(3))
                    .noHeartbeatTtl()
                    .heartbeatTimeout(Duration.ofSeconds(5))
                    .defaultHeartbeatTimeout()
                    .heartbeatOff()
                    .handshakeTimeout(Duration.ofSeconds(1))
                    .maxPendingHandshakes(8)
                    .noMaxMessageSize()
                    .conflate(false)
                    .routerMandatory(false)
                    .onMute(OnMute.BLOCK)
                    .onMute(OnMute.DROP_NEWEST)
                    .onMute(OnMute.DROP_OLDEST)
                    .tcpKeepalive(Duration.ofSeconds(60), Duration.ofSeconds(10), 3)
                    .tcpKeepaliveOff()
                    .tcpKeepaliveDefault()
                    .sendBufferSize(65_536)
                    .defaultSendBufferSize()
                    .receiveBufferSize(65_536)
                    .defaultReceiveBufferSize()
                    .compressionDict("dict".getBytes())
                    .noCompressionDict()
                    .compressionThreshold(128)
                    .compressionDefaultThreshold()
                    .compressionLevel(1)
                    .compressionDefaultLevel()
                    .compressionDictCapacity(2_048)
                    .defaultCompressionDictCapacity()
                    .maxReceiveDictSize(8_192)
                    .defaultMaxReceiveDictSize()
                    .compressionOffloadThreshold(8_192)
                    .noCompressionOffload()
                    .largeMessageThreshold(4_096)
                    .disableLargeMessagePath()
                    .arenaThreshold(65_536)
                    .defaultArenaThreshold()
                    .transmitSlotCapacity(2 * 1024 * 1024)
                    .defaultTransmitSlotCapacity()
                    .xpubNoDrop(false);
        }
    }

    @Test
    void optionsCannotChangeAfterConnect() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            push.connect("tcp://127.0.0.1:" + TestSupport.freePort());
            assertThrows(OMQException.class, () -> push.linger(Duration.ZERO));
            assertThrows(OMQException.class, () -> push.routerMandatory(true));
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
    void rejectsInvalidExtendedOptions() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(IllegalArgumentException.class,
                    () -> pull.reconnectExponential(Duration.ofSeconds(2), Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class, () -> pull.maxPendingHandshakes(0));
            assertThrows(IllegalArgumentException.class,
                    () -> pull.tcpKeepalive(Duration.ofSeconds(1), Duration.ofSeconds(1), 0));
            assertThrows(IllegalArgumentException.class, () -> pull.sendBufferSize(-1));
            assertThrows(IllegalArgumentException.class, () -> pull.receiveBufferSize(-1));
            assertThrows(IllegalArgumentException.class, () -> pull.compressionDictCapacity(-1));
            assertThrows(IllegalArgumentException.class, () -> pull.maxReceiveDictSize(-1));
            assertThrows(IllegalArgumentException.class,
                    () -> pull.compressionOffloadThreshold(-1));
            assertThrows(IllegalArgumentException.class, () -> pull.largeMessageThreshold(-1));
            assertThrows(IllegalArgumentException.class, () -> pull.arenaThreshold(-1));
            assertThrows(IllegalArgumentException.class, () -> pull.transmitSlotCapacity(-1));
        }
    }

    @Test
    void validatesNativeOptionLimits() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(OMQException.class, () -> pull.compressionDict(new byte[9_000]));
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
