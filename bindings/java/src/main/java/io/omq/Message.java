package io.omq;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class Message {
    private final byte[][] parts;

    private Message(byte[][] parts) {
        if (parts.length == 0) {
            throw new IllegalArgumentException("message must contain at least one part");
        }
        this.parts = copy(parts);
    }

    public static Message of(byte[] body) {
        Objects.requireNonNull(body, "body");
        return new Message(new byte[][] {body});
    }

    public static Message of(ByteBuffer body) {
        Objects.requireNonNull(body, "body");
        return of(readRemaining(body));
    }

    public static Message text(String text) {
        return text(text, StandardCharsets.UTF_8);
    }

    public static Message text(String text, Charset charset) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return of(text.getBytes(charset));
    }

    public static Message multipart(byte[]... parts) {
        Objects.requireNonNull(parts, "parts");
        return new Message(parts);
    }

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

    public int partCount() {
        return parts.length;
    }

    public boolean isMultipart() {
        return parts.length > 1;
    }

    public byte[] part(int index) {
        return Arrays.copyOf(parts[index], parts[index].length);
    }

    public ByteBuffer partBuffer(int index) {
        return ByteBuffer.wrap(part(index)).asReadOnlyBuffer();
    }

    public List<byte[]> parts() {
        return Arrays.stream(parts)
                .map(part -> Arrays.copyOf(part, part.length))
                .toList();
    }

    public byte[] bytes() {
        if (parts.length != 1) {
            throw new IllegalStateException("message has " + parts.length + " parts");
        }
        return part(0);
    }

    public String text() {
        return text(StandardCharsets.UTF_8);
    }

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
