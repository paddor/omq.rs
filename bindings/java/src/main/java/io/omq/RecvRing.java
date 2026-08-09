package io.omq;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Optional;

final class RecvRing implements AutoCloseable {
    private static final int DEFAULT_DESC_CAPACITY = 1024;
    private static final long DEFAULT_PAYLOAD_CAPACITY = 4L * 1024L * 1024L;
    private static final int FILL_BATCH = 256;

    private static final long CONTROL_BYTES = 256;
    private static final long CONTROL_HEAD = 0;
    private static final long CONTROL_TAIL = 128;

    private static final long DESC_BYTES = 64;
    private static final long DESC_PAYLOAD = 0;
    private static final long DESC_PAYLOAD_LEN = 8;
    private static final long DESC_PART_COUNT = 24;
    private static final long DESC_FLAGS = 32;

    private static final long FLAG_MULTIPART = 1;
    private static final long FLAG_EXTERNAL = 2;

    private static final ValueLayout.OfInt INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.nativeOrder());
    private static final ValueLayout.OfLong LONG =
            ValueLayout.JAVA_LONG.withOrder(ByteOrder.nativeOrder());
    private static final VarHandle ATOMIC_LONG = LONG.varHandle();

    private long handle;
    private MemorySegment control;
    private MemorySegment descriptors;
    private MemorySegment payload;
    private long descMask;
    private long head;
    private long releasedHead;
    private long cachedTail;

    Message receive(long socketHandle, long timeoutMillis) {
        ensure(socketHandle);
        Desc desc = next(timeoutMillis);
        try {
            return readMessage(desc);
        } finally {
            advance();
        }
    }

    byte[] receiveBytes(long socketHandle, long timeoutMillis) {
        ensure(socketHandle);
        Desc desc = next(timeoutMillis);
        try {
            if (desc.partCount() != 1) {
                throw new IllegalStateException("message has " + desc.partCount() + " parts");
            }
            return readBytes(desc);
        } finally {
            advance();
        }
    }

    int receiveInto(long socketHandle, ByteBuffer destination, long timeoutMillis) {
        ensure(socketHandle);
        Desc desc = next(timeoutMillis);
        try {
            if (desc.partCount() != 1) {
                throw new IllegalStateException("message has " + desc.partCount() + " parts");
            }
            if (destination.isReadOnly()) {
                throw new ReadOnlyBufferException();
            }
            int len = checkedIntLength(desc.payloadLen());
            if (len > destination.remaining()) {
                throw new BufferOverflowException();
            }
            if (len > 0) {
                MemorySegment.copy(
                        source(desc), sourceOffset(desc),
                        MemorySegment.ofBuffer(destination), 0,
                        len);
            }
            destination.position(destination.position() + len);
            return len;
        } finally {
            advance();
        }
    }

    Optional<Message> tryReceiveCachedMessage() {
        if (handle == 0 || !hasCached()) {
            return Optional.empty();
        }
        Desc desc = current();
        try {
            return Optional.of(readMessage(desc));
        } finally {
            advance();
        }
    }

    private Desc next(long timeoutMillis) {
        if (!hasCached()) {
            releaseConsumed();
            NativeFfm.recvRingFill(handle, timeoutMillis, FILL_BATCH);
            cachedTail = tailAcquire();
        }
        if (!hasCached()) {
            throw new TimeoutException("operation timed out");
        }
        return current();
    }

    private boolean hasCached() {
        if (head != cachedTail) {
            return true;
        }
        cachedTail = tailAcquire();
        return head != cachedTail;
    }

    private Desc current() {
        long offset = (head & descMask) * DESC_BYTES;
        return new Desc(
                descriptors.get(LONG, offset + DESC_PAYLOAD),
                descriptors.get(LONG, offset + DESC_PAYLOAD_LEN),
                descriptors.get(LONG, offset + DESC_PART_COUNT),
                descriptors.get(LONG, offset + DESC_FLAGS));
    }

    private Message readMessage(Desc desc) {
        if (desc.partCount() == 1) {
            return Message.fromNative(readBytes(desc));
        }
        return Message.fromNative(readParts(desc));
    }

    private byte[] readBytes(Desc desc) {
        int len = checkedIntLength(desc.payloadLen());
        byte[] out = new byte[len];
        if (len > 0) {
            MemorySegment.copy(source(desc), sourceOffset(desc), MemorySegment.ofArray(out), 0, len);
        }
        return out;
    }

    private byte[][] readParts(Desc desc) {
        if ((desc.flags() & FLAG_MULTIPART) == 0) {
            return new byte[][] {readBytes(desc)};
        }
        MemorySegment source = source(desc);
        long offset = sourceOffset(desc);
        int partCount = checkedIntLength(desc.partCount());
        int encodedPartCount = source.get(INT, offset);
        if (encodedPartCount != partCount) {
            throw new OMQException("native receive ring metadata mismatch");
        }
        byte[][] parts = new byte[partCount][];
        long bodyOffset = offset + 4L + 4L * partCount;
        for (int i = 0; i < partCount; i++) {
            int len = source.get(INT, offset + 4L + 4L * i);
            if (len < 0) {
                throw new OMQException("native receive ring part length overflow");
            }
            byte[] part = new byte[len];
            if (len > 0) {
                MemorySegment.copy(source, bodyOffset, MemorySegment.ofArray(part), 0, len);
            }
            parts[i] = part;
            bodyOffset += len;
        }
        return parts;
    }

    private MemorySegment source(Desc desc) {
        if ((desc.flags() & FLAG_EXTERNAL) != 0) {
            return MemorySegment.ofAddress(desc.payload()).reinterpret(desc.payloadLen());
        }
        return payload;
    }

    private long sourceOffset(Desc desc) {
        return (desc.flags() & FLAG_EXTERNAL) != 0 ? 0 : desc.payload();
    }

    private void advance() {
        head++;
        if (head == cachedTail) {
            releaseConsumed();
        }
    }

    private long tailAcquire() {
        return (long) ATOMIC_LONG.getAcquire(control, CONTROL_TAIL);
    }

    private void releaseConsumed() {
        if (releasedHead != head) {
            ATOMIC_LONG.setRelease(control, CONTROL_HEAD, head);
            releasedHead = head;
        }
    }

    private void ensure(long socketHandle) {
        if (handle != 0) {
            return;
        }
        long created = NativeFfm.recvRingCreate(
                socketHandle, DEFAULT_DESC_CAPACITY, DEFAULT_PAYLOAD_CAPACITY);
        int descCapacity = NativeFfm.recvRingDescCapacity(created);
        long payloadCapacity = NativeFfm.recvRingPayloadCapacity(created);
        long controlAddress = NativeFfm.recvRingControlAddress(created);
        long descAddress = NativeFfm.recvRingDescAddress(created);
        long payloadAddress = NativeFfm.recvRingPayloadAddress(created);
        if (descCapacity <= 0
                || !isPowerOfTwo(descCapacity)
                || payloadCapacity <= 0
                || !isPowerOfTwo(payloadCapacity)
                || controlAddress == 0 || descAddress == 0 || payloadAddress == 0) {
            NativeFfm.recvRingClose(created);
            throw new OMQException("native receive ring returned invalid memory");
        }
        handle = created;
        control = MemorySegment.ofAddress(controlAddress).reinterpret(CONTROL_BYTES);
        descriptors = MemorySegment.ofAddress(descAddress)
                .reinterpret((long) descCapacity * DESC_BYTES);
        payload = MemorySegment.ofAddress(payloadAddress).reinterpret(payloadCapacity);
        descMask = descCapacity - 1L;
        head = 0;
        releasedHead = 0;
        cachedTail = 0;
    }

    @Override
    public void close() {
        long current = handle;
        if (current == 0) {
            return;
        }
        handle = 0;
        control = null;
        descriptors = null;
        payload = null;
        NativeFfm.recvRingClose(current);
    }

    private static int checkedIntLength(long len) {
        if (len < 0 || len > Integer.MAX_VALUE) {
            throw new OMQException("message is too large for a Java byte array");
        }
        return (int) len;
    }

    private static boolean isPowerOfTwo(long value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    private record Desc(
            long payload,
            long payloadLen,
            long partCount,
            long flags) {
    }
}
