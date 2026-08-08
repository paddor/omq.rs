package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class CompressionTest {
    @Test
    void lz4TcpRoundTrip() {
        roundTrip("lz4+tcp://127.0.0.1:0");
    }

    @Test
    void zstdTcpRoundTrip() {
        roundTrip("zstd+tcp://127.0.0.1:0");
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
}
