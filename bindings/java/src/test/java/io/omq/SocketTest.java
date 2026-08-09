package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;
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
    void receiveBytesReturnsSinglePartBody() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send("bytes");

            assertArrayEquals(
                    "bytes".getBytes(StandardCharsets.UTF_8),
                    pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow());
        }
    }

    @Test
    void sendBytesCopiesBeforeCallerCanMutate() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            byte[] body = new byte[128];
            body[0] = 7;
            body[127] = 9;

            push.send(body);
            body[0] = 42;
            body[127] = 43;

            byte[] received = pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(128, received.length);
            assertEquals(7, received[0]);
            assertEquals(9, received[127]);
        }
    }

    @Test
    void sendBytesFastPathPreservesEmptyAndOrder() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send(new byte[0]);
            for (int i = 0; i < 64; i++) {
                byte[] body = new byte[128];
                body[0] = (byte) i;
                body[127] = (byte) (255 - i);
                push.send(body);
            }

            assertEquals(0, pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow().length);
            for (int i = 0; i < 64; i++) {
                byte[] received = pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow();
                assertEquals(128, received.length);
                assertEquals((byte) i, received[0]);
                assertEquals((byte) (255 - i), received[127]);
            }
        }
    }

    @Test
    void sendBytesLargeFallbackPreservesOrderAndRingReuse() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://send-large-fallback-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            byte[] large = new byte[17 * 1024 * 1024 + 3];
            large[0] = 7;
            large[large.length - 1] = 9;

            push.send("before");
            push.send(large);
            push.send("after");

            assertEquals("before", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
            byte[] received = pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(large.length, received.length);
            assertEquals(7, received[0]);
            assertEquals(9, received[received.length - 1]);
            assertEquals("after", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void sendBytesFastPathOrdersBeforeMultipartSend() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://send-mixed-order-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send("first");
            push.send(Message.multipart(utf8("route"), utf8("body")));
            push.send("last");

            assertEquals("first", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
            Message multipart = pull.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(2, multipart.partCount());
            assertArrayEquals(utf8("route"), multipart.part(0));
            assertArrayEquals(utf8("body"), multipart.part(1));
            assertEquals("last", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void receiveBytesRejectsMultipart() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send(Message.multipart("a".getBytes(), "b".getBytes()));

            assertThrows(
                    IllegalStateException.class,
                    () -> pull.receiveBytes(Duration.ofSeconds(5)));
        }
    }

    @Test
    void receiveBytesHandlesMessageLargerThanFfmPayloadRing() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://receive-large-external-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            byte[] body = new byte[5 * 1024 * 1024 + 17];
            body[0] = 7;
            body[body.length - 1] = 9;

            push.send(body);
            byte[] received = pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow();

            assertEquals(body.length, received.length);
            assertEquals(7, received[0]);
            assertEquals(9, received[received.length - 1]);
        }
    }

    @Test
    void tryReceiveBytesReturnsEmptyWhenNoMessageAvailable() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("tcp://127.0.0.1:0");

            assertFalse(pull.tryReceiveBytes().isPresent());
        }
    }

    @Test
    void receiveIntoFillsHeapByteBufferAndAdvancesPosition() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://receive-into-heap-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            byte[] storage = "xx.......yy".getBytes(StandardCharsets.UTF_8);
            ByteBuffer destination = ByteBuffer.wrap(storage);
            destination.position(2);
            destination.limit(9);
            push.send("payload");

            OptionalInt count = pull.receiveInto(destination, Duration.ofSeconds(5));

            assertEquals(7, count.orElseThrow());
            assertEquals(9, destination.position());
            assertEquals("xxpayloadyy", new String(storage, StandardCharsets.UTF_8));
        }
    }

    @Test
    void receiveIntoFillsDirectByteBuffer() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://receive-into-direct-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            ByteBuffer destination = ByteBuffer.allocateDirect(16);
            destination.position(1);
            push.send("direct");

            assertEquals(6, pull.receiveInto(destination));
            assertEquals(7, destination.position());
            destination.flip();
            destination.position(1);
            byte[] out = new byte[6];
            destination.get(out);
            assertArrayEquals("direct".getBytes(StandardCharsets.UTF_8), out);
        }
    }

    @Test
    void receiveIntoTimeoutReturnsEmptyAndKeepsPosition() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("inproc://receive-into-timeout-" + UUID.randomUUID());
            ByteBuffer destination = ByteBuffer.allocate(8);
            destination.position(3);

            assertTrue(pull.receiveInto(destination, Duration.ofMillis(10)).isEmpty());
            assertTrue(pull.tryReceiveInto(destination).isEmpty());
            assertEquals(3, destination.position());
        }
    }

    @Test
    void receiveIntoRejectsTooSmallDestination() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://receive-into-overflow-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            ByteBuffer destination = ByteBuffer.allocate(3);
            push.send("toolong");

            assertThrows(
                    BufferOverflowException.class,
                    () -> pull.receiveInto(destination, Duration.ofSeconds(5)));
            assertEquals(0, destination.position());
        }
    }

    @Test
    void receiveIntoRejectsReadOnlyDestination() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://receive-into-read-only-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            ByteBuffer destination = ByteBuffer.allocate(8).asReadOnlyBuffer();
            push.send("readonly");

            assertThrows(
                    ReadOnlyBufferException.class,
                    () -> pull.receiveInto(destination, Duration.ofSeconds(5)));
            assertEquals(0, destination.position());
        }
    }

    @Test
    void receiveIntoRejectsMultipart() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://receive-into-multipart-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send(Message.multipart("a".getBytes(), "b".getBytes()));

            assertThrows(
                    IllegalStateException.class,
                    () -> pull.receiveInto(ByteBuffer.allocate(8), Duration.ofSeconds(5)));
        }
    }

    @Test
    void receiveManyBytesBlocksForFirstAndDrainsAvailable() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://batch-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send("a");
            push.send("bb");
            push.send("ccc");

            List<byte[]> batch = pull.receiveManyBytes(8, Duration.ofSeconds(5));
            assertEquals(3, batch.size());
            assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), batch.get(0));
            assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), batch.get(1));
            assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8), batch.get(2));
        }
    }

    @Test
    void sendManyBytesSendsSinglePartMessages() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://send-batch-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.sendManyBytes(new byte[][] {
                "a".getBytes(StandardCharsets.UTF_8),
                "bb".getBytes(StandardCharsets.UTF_8),
                "ccc".getBytes(StandardCharsets.UTF_8),
            });

            List<byte[]> batch = pull.receiveManyBytes(8, Duration.ofSeconds(5));
            assertEquals(3, batch.size());
            assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), batch.get(0));
            assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), batch.get(1));
            assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8), batch.get(2));
        }
    }

    @Test
    void sendManyBytesAcceptsEmptyBatch() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            push.connect("inproc://empty-send-batch-" + UUID.randomUUID());

            push.sendManyBytes(new byte[0][]);
        }
    }

    @Test
    void timedSendSucceedsBeforeTimeout() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://timed-send-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            assertTrue(push.send("timed", Duration.ofSeconds(5)));

            assertEquals("timed", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void timedSendReturnsFalseOnTimeout() {
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            push.bind("inproc://timed-send-timeout-" + UUID.randomUUID());

            assertFalse(push.send("blocked", Duration.ofMillis(10)));
        }
    }

    @Test
    void receiveManyBytesIntoFillsReusableArrayWithOffset() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://batch-into-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.sendManyBytes(new byte[][] {
                "a".getBytes(StandardCharsets.UTF_8),
                "bb".getBytes(StandardCharsets.UTF_8),
                "ccc".getBytes(StandardCharsets.UTF_8),
            });

            byte[][] output = new byte[5][];
            int count = pull.receiveManyBytesInto(output, 1, 3, Duration.ofSeconds(5));

            assertEquals(3, count);
            assertNull(output[0]);
            assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), output[1]);
            assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), output[2]);
            assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8), output[3]);
            assertNull(output[4]);
        }
    }

    @Test
    void receiveManyBytesIntoTimeoutReturnsZeroAndKeepsOutput() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("inproc://empty-batch-timeout-" + UUID.randomUUID());

            byte[] sentinel = "sentinel".getBytes(StandardCharsets.UTF_8);
            byte[][] output = new byte[][] {sentinel};

            assertEquals(0, pull.receiveManyBytesInto(output, Duration.ofMillis(10)));
            assertSame(sentinel, output[0]);
        }
    }

    @Test
    void tryReceiveManyBytesIntoReturnsZeroWhenNoMessageAvailable() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("inproc://empty-batch-into-" + UUID.randomUUID());

            byte[][] output = new byte[2][];
            assertEquals(0, pull.tryReceiveManyBytesInto(output));
            assertNull(output[0]);
            assertNull(output[1]);
        }
    }

    @Test
    void receiveManyBytesIntoAcceptsZeroLengthOutput() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertEquals(0, pull.receiveManyBytesInto(new byte[0][]));
            assertEquals(0, pull.tryReceiveManyBytesInto(new byte[0][]));
        }
    }

    @Test
    void receiveManyBytesIntoRejectsMultipart() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://batch-into-multipart-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send(Message.multipart("a".getBytes(), "b".getBytes()));

            assertThrows(
                    IllegalStateException.class,
                    () -> pull.receiveManyBytesInto(new byte[2][], Duration.ofSeconds(5)));
        }
    }

    @Test
    void receiveManyReturnsMultipartMessages() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("inproc://batch-multipart-" + UUID.randomUUID());
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.send(Message.multipart("a".getBytes(), "b".getBytes()));
            push.send("tail");

            List<Message> batch = pull.receiveMany(4, Duration.ofSeconds(5));
            assertEquals(2, batch.size());
            assertEquals(2, batch.get(0).partCount());
            assertEquals("tail", batch.get(1).text());
        }
    }

    @Test
    void tryReceiveManyBytesReturnsEmptyWhenNoMessageAvailable() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("inproc://empty-batch-" + UUID.randomUUID());

            assertTrue(pull.tryReceiveManyBytes(4).isEmpty());
        }
    }

    @Test
    void receiveManyRejectsNonPositiveLimit() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(IllegalArgumentException.class, () -> pull.receiveMany(0));
            assertThrows(IllegalArgumentException.class, () -> pull.tryReceiveManyBytes(0));
            assertThrows(NullPointerException.class, () -> pull.sendManyBytes(new byte[][] {null}));
            assertThrows(NullPointerException.class, () -> pull.receiveManyBytesInto(null));
            assertThrows(IndexOutOfBoundsException.class, () -> pull.receiveManyBytesInto(new byte[1][], -1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> pull.receiveManyBytesInto(new byte[1][], 1, 1));
            assertThrows(IllegalArgumentException.class, () -> pull.receiveManyBytesInto(new byte[1][], 0, -1));
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

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
