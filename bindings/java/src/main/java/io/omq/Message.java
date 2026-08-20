package io.omq;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/** Immutable OMQ message with one or more byte-array parts. */
public final class Message {
    private final byte[] body;
    private final byte[][] parts;
    private final int routingId;

    private Message(byte[][] parts) {
        this(parts, true, 0);
    }

    private Message(byte[][] parts, boolean copy) {
        this(parts, copy, 0);
    }

    private Message(byte[][] parts, boolean copy, int routingId) {
        if (parts.length == 0) {
            throw new IllegalArgumentException("message must contain at least one part");
        }
        this.body = null;
        this.parts = copy ? copy(parts) : requireParts(parts);
        this.routingId = requireRoutingId(routingId);
    }

    private Message(byte[] body, boolean copy) {
        this(body, copy, 0);
    }

    private Message(byte[] body, boolean copy, int routingId) {
        Objects.requireNonNull(body, "body");
        this.body = copy ? Arrays.copyOf(body, body.length) : body;
        this.parts = null;
        this.routingId = requireRoutingId(routingId);
    }

    /** Creates a single-part binary message by copying {@code body}. */
    public static Message of(byte[] body) {
        Objects.requireNonNull(body, "body");
        return new Message(body, true);
    }

    /** Creates a single-part message from remaining bytes without changing position. */
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

    /** Creates multipart message parts from remaining bytes without changing positions. */
    public static Message multipart(ByteBuffer... parts) {
        Objects.requireNonNull(parts, "parts");
        byte[][] out = new byte[parts.length][];
        for (int i = 0; i < parts.length; i++) {
            out[i] = readRemaining(Objects.requireNonNull(parts[i], "part " + i));
        }
        return new Message(out);
    }

    static Message fromNative(byte[][] parts) {
        Objects.requireNonNull(parts, "parts");
        return new Message(parts, false);
    }

    static Message fromNative(byte[] body) {
        return new Message(body, false);
    }

    static Message fromNative(byte[] body, int routingId) {
        return new Message(body, false, routingId);
    }

    static Message fromNative(byte[][] parts, int routingId) {
        return new Message(parts, false, routingId);
    }

    static Message fromNative(Object nativeMessage) {
        if (nativeMessage instanceof Message message) {
            return message;
        }
        if (nativeMessage instanceof byte[] body) {
            return fromNative(body);
        }
        if (nativeMessage instanceof byte[][] nativeParts) {
            return fromNative(nativeParts);
        }
        throw new IllegalArgumentException("native message must be byte[] or byte[][]");
    }

    static byte[] bytesFromNative(Object nativeMessage) {
        if (nativeMessage instanceof byte[] nativeBody) {
            return nativeBody;
        }
        if (nativeMessage instanceof byte[][] nativeParts && nativeParts.length == 1) {
            return Objects.requireNonNull(nativeParts[0], "part 0");
        }
        if (nativeMessage instanceof byte[][] nativeParts) {
            throw new IllegalStateException("message has " + nativeParts.length + " parts");
        }
        throw new IllegalArgumentException("native message must be byte[] or byte[][]");
    }

    byte[][] toNative() {
        if (body != null) {
            return new byte[][] {Arrays.copyOf(body, body.length)};
        }
        return copy(parts);
    }

    /** Returns the number of message parts. */
    public int partCount() {
        return body != null ? 1 : parts.length;
    }

    /** Returns whether this message has more than one part. */
    public boolean isMultipart() {
        return body == null && parts.length > 1;
    }

    /** Returns the native routing ID, if this message came from a routed socket. */
    public OptionalInt routingId() {
        return routingId == 0 ? OptionalInt.empty() : OptionalInt.of(routingId);
    }

    /** Returns a copy of this message carrying {@code routingId} metadata. */
    public Message withRoutingId(int routingId) {
        if (body != null) {
            return new Message(body, true, routingId);
        }
        return new Message(parts, true, routingId);
    }

    /** Returns a copy of the part at {@code index}. */
    public byte[] part(int index) {
        if (body != null) {
            if (index != 0) {
                throw new ArrayIndexOutOfBoundsException(index);
            }
            return Arrays.copyOf(body, body.length);
        }
        return Arrays.copyOf(parts[index], parts[index].length);
    }

    /** Returns a read-only buffer containing a copy of the part at {@code index}. */
    public ByteBuffer partBuffer(int index) {
        return ByteBuffer.wrap(part(index)).asReadOnlyBuffer();
    }

    /** Returns copies of all message parts. */
    public List<byte[]> parts() {
        if (body != null) {
            return List.of(Arrays.copyOf(body, body.length));
        }
        return Arrays.stream(parts)
                .map(part -> Arrays.copyOf(part, part.length))
                .toList();
    }

    /** Returns the body of a single-part message. */
    public byte[] bytes() {
        if (body != null) {
            return Arrays.copyOf(body, body.length);
        }
        if (parts.length == 1) {
            return Arrays.copyOf(parts[0], parts[0].length);
        }
        throw new IllegalStateException("message has " + parts.length + " parts");
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

    /** Returns whether this message has the same parts and bytes as another message. */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Message message) || partCount() != message.partCount()) {
            return false;
        }
        for (int i = 0; i < partCount(); i++) {
            if (!Arrays.equals(partView(i), message.partView(i))) {
                return false;
            }
        }
        return true;
    }

    /** Returns a hash code derived from message parts and bytes. */
    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = 0; i < partCount(); i++) {
            hash = 31 * hash + Arrays.hashCode(partView(i));
        }
        return hash;
    }

    /** Returns a compact description without copying or exposing message bytes. */
    @Override
    public String toString() {
        return "Message[parts=" + partCount() + ", bytes=" + totalBytes() + "]";
    }

    private static byte[][] copy(byte[][] input) {
        requireParts(input);
        byte[][] out = new byte[input.length][];
        for (int i = 0; i < input.length; i++) {
            out[i] = Arrays.copyOf(input[i], input[i].length);
        }
        return out;
    }

    private static byte[][] requireParts(byte[][] input) {
        Objects.requireNonNull(input, "parts");
        for (int i = 0; i < input.length; i++) {
            Objects.requireNonNull(input[i], "part " + i);
        }
        return input;
    }

    private static byte[] readRemaining(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        byte[] out = new byte[copy.remaining()];
        copy.get(out);
        return out;
    }

    private byte[] partView(int index) {
        if (body != null) {
            if (index != 0) {
                throw new ArrayIndexOutOfBoundsException(index);
            }
            return body;
        }
        return parts[index];
    }

    private long totalBytes() {
        long total = 0;
        for (int i = 0; i < partCount(); i++) {
            total += partView(i).length;
        }
        return total;
    }

    private static int requireRoutingId(int routingId) {
        if (routingId < 0) {
            throw new IllegalArgumentException("routing ID must be non-negative");
        }
        return routingId;
    }
}
