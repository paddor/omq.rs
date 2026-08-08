package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class MessageTest {
    @Test
    void bytesAreCopiedOnInput() {
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
        Message message = Message.of(body);
        body[0] = 'x';

        assertEquals("hello", message.text());
    }

    @Test
    void bytesAreCopiedOnOutput() {
        Message message = Message.text("hello");
        byte[] body = message.bytes();
        body[0] = 'x';

        assertEquals("hello", message.text());
    }

    @Test
    void byteBufferInputDoesNotAdvancePosition() {
        ByteBuffer buffer = ByteBuffer.wrap("xxpayloadyy".getBytes(StandardCharsets.UTF_8));
        buffer.position(2);
        buffer.limit(9);

        Message message = Message.of(buffer);

        assertEquals("payload", message.text());
        assertEquals(2, buffer.position());
        assertEquals(9, buffer.limit());
    }

    @Test
    void directByteBufferInput() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(7);
        buffer.put("direct!".getBytes(StandardCharsets.UTF_8));
        buffer.flip();

        assertEquals("direct!", Message.of(buffer).text());
    }

    @Test
    void multipartByteBuffers() {
        Message message = Message.multipart(
                ByteBuffer.wrap("one".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("two".getBytes(StandardCharsets.UTF_8)));

        assertEquals(2, message.partCount());
        assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), message.part(0));
        assertArrayEquals("two".getBytes(StandardCharsets.UTF_8), message.part(1));
    }

    @Test
    void partBufferIsReadOnlyCopy() {
        Message message = Message.text("copy");
        ByteBuffer buffer = message.partBuffer(0);

        assertTrue(buffer.isReadOnly());
        assertEquals("copy", StandardCharsets.UTF_8.decode(buffer).toString());
    }

    @Test
    void multipartRejectsEmptyMessage() {
        assertThrows(IllegalArgumentException.class,
                () -> Message.multipart((byte[][]) new byte[0][]));
    }

    @Test
    void bytesRejectMultipart() {
        Message message = Message.multipart("a".getBytes(), "b".getBytes());

        assertThrows(IllegalStateException.class, message::bytes);
    }
}
