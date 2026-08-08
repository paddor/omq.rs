package io.omq;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable OMQ message with one or more byte-array parts. */
public final class Message {
    private final byte[][] parts;

    private Message(byte[][] parts) {
        if (parts.length == 0) {
            throw new IllegalArgumentException("message must contain at least one part");
        }
        this.parts = copy(parts);
    }

    /** Creates a single-part binary message by copying {@code body}. */
    public static Message of(byte[] body) {
        Objects.requireNonNull(body, "body");
        return new Message(new byte[][] {body});
    }

    /** Creates a single-part message from the remaining bytes of {@code body}. */
    public static Message of(ByteBuffer body) {
        Objects.requireNonNull(body, "body");
        return of(readRemaining(body));
    }

    /** Creates a UTF-8 single-part text message. */
    public static Message text(String text) {
        return text(text, StandardCharsets.UTF_8);
    }

    /** Creates a single-part text message with the supplied charset. */
    public static Message text(String text, Charset charset) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return of(text.getBytes(charset));
    }

    /** Creates a multipart message by copying every part. */
    public static Message multipart(byte[]... parts) {
        Objects.requireNonNull(parts, "parts");
        return new Message(parts);
    }

    /** Creates a multipart message from the remaining bytes of every buffer. */
    public static Message multipart(ByteBuffer... parts) {
        Objects.requireNonNull(parts, "parts");
        byte[][] out = new byte[parts.length][];
        for (int i = 0; i < parts.length; i++) {
            out[i] = readRemaining(Objects.requireNonNull(parts[i], "part " + i));
        }
        return new Message(out);
    }

    static Message fromNative(byte[][] parts) {
        return new Message(parts);
    }

    byte[][] toNative() {
        return copy(parts);
    }

    /** Returns the number of message parts. */
    public int partCount() {
        return parts.length;
    }

    /** Returns whether this message has more than one part. */
    public boolean isMultipart() {
        return parts.length > 1;
    }

    /** Returns a copy of the part at {@code index}. */
    public byte[] part(int index) {
        return Arrays.copyOf(parts[index], parts[index].length);
    }

    /** Returns a read-only buffer containing a copy of the part at {@code index}. */
    public ByteBuffer partBuffer(int index) {
        return ByteBuffer.wrap(part(index)).asReadOnlyBuffer();
    }

    /** Returns copies of all message parts. */
    public List<byte[]> parts() {
        return Arrays.stream(parts)
                .map(part -> Arrays.copyOf(part, part.length))
                .toList();
    }

    /** Returns the body of a single-part message. */
    public byte[] bytes() {
        if (parts.length != 1) {
            throw new IllegalStateException("message has " + parts.length + " parts");
        }
        return part(0);
    }

    /** Decodes a single-part message as UTF-8 text. */
    public String text() {
        return text(StandardCharsets.UTF_8);
    }

    /** Decodes a single-part message as text with the supplied charset. */
    public String text(Charset charset) {
        Objects.requireNonNull(charset, "charset");
        return new String(bytes(), charset);
    }

    private static byte[][] copy(byte[][] input) {
        byte[][] out = new byte[input.length][];
        for (int i = 0; i < input.length; i++) {
            byte[] part = Objects.requireNonNull(input[i], "part " + i);
            out[i] = Arrays.copyOf(part, part.length);
        }
        return out;
    }

    private static byte[] readRemaining(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        byte[] out = new byte[copy.remaining()];
        copy.get(out);
        return out;
    }
}
