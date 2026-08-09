package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class CompressionTest {
    private static final byte[] ZSTD_DICT = hex(
            "37a430ecbeaadd5c811120841042664644444444244902002114c418638c21841042"
                    + "082184104208214444444444444444240900005110638c31c618630c21c418636666"
                    + "864692040080000000c000000000010000");
    private static final byte[] LZ4_DICT =
            "omq-quote-symbol-price-volume-json-shared-prefix".getBytes();

    @Test
    void lz4TcpRoundTrip() {
        roundTrip("lz4+tcp://127.0.0.1:0");
    }

    @Test
    void zstdTcpRoundTrip() {
        roundTrip("zstd+tcp://127.0.0.1:0");
    }

    @Test
    void lz4StaticDictRoundTrip() {
        dictRoundTrip("lz4+tcp://127.0.0.1:0", LZ4_DICT);
    }

    @Test
    void zstdStaticDictRoundTrip() {
        dictRoundTrip("zstd+tcp://127.0.0.1:0", ZSTD_DICT);
    }

    @Test
    void lz4AutoTrainedDictRoundTrip() {
        autoTrainRoundTrip("lz4+tcp://127.0.0.1:0");
    }

    @Test
    void zstdAutoTrainedDictRoundTrip() {
        autoTrainRoundTrip("zstd+tcp://127.0.0.1:0");
    }

    @Test
    void compressedMultipartRoundTrip() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).compressionAutoTrain(true);
             Socket push = context.socket(SocketType.PUSH).compressionAutoTrain(true)) {
            String endpoint = pull.bind("lz4+tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send(Message.multipart("meta".getBytes(), "payload".getBytes()));

            Message received = pull.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(2, received.partCount());
            assertEquals("meta", new String(received.part(0)));
            assertEquals("payload", new String(received.part(1)));
        }
    }

    private static void roundTrip(String bindEndpoint) {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).compressionAutoTrain(true);
             Socket push = context.socket(SocketType.PUSH).compressionAutoTrain(true)) {
            String endpoint = pull.bind(bindEndpoint);
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("{\"kind\":\"json\",\"value\":42}");

            assertEquals("{\"kind\":\"json\",\"value\":42}",
                    pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    private static void dictRoundTrip(String bindEndpoint, byte[] dict) {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)
                     .compressionDict(dict)
                     .compressionThreshold(32)
                     .compressionLevel(1)) {
            String endpoint = pull.bind(bindEndpoint);
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            byte[] payload = payload(7, 512);

            push.send(payload);

            assertArrayEquals(payload, pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow());
        }
    }

    private static void autoTrainRoundTrip(String bindEndpoint) {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)
                     .compressionAutoTrain(true)
                     .compressionDictCapacity(4_096);
             Socket push = context.socket(SocketType.PUSH)
                     .compressionAutoTrain(true)
                     .compressionDictCapacity(4_096)
                     .compressionThreshold(32)
                     .compressionLevel(1)) {
            String endpoint = pull.bind(bindEndpoint);
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            for (int i = 0; i < 140; i++) {
                push.send(payload(i, 768));
            }

            for (int i = 0; i < 140; i++) {
                assertArrayEquals(payload(i, 768), pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow());
            }
        }
    }

    private static byte[] payload(int seq, int size) {
        String head = "{\"kind\":\"quote\",\"symbol\":\"OMQ\",\"seq\":" + seq + ",\"pad\":\"";
        String tail = "\"}";
        return (head + "A".repeat(size - head.length() - tail.length()) + tail)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] hex(String input) {
        byte[] out = new byte[input.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(input.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }
}
