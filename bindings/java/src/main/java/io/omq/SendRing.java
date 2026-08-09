package io.omq;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.concurrent.locks.LockSupport;

final class SendRing implements AutoCloseable {
    private static final int DEFAULT_DESC_CAPACITY = 4096;
    private static final long DEFAULT_PAYLOAD_CAPACITY = 16L * 1024L * 1024L;

    private static final long CONTROL_BYTES = 384;
    private static final long CONTROL_HEAD = 0;
    private static final long CONTROL_TAIL = 128;
    private static final long CONTROL_CLOSED = 256;

    private static final long DESC_BYTES = 64;
    private static final long DESC_PAYLOAD = 0;
    private static final long DESC_PAYLOAD_LEN = 8;
    private static final long DESC_PAYLOAD_END = 16;

    private static final ValueLayout.OfLong LONG =
            ValueLayout.JAVA_LONG.withOrder(ByteOrder.nativeOrder());
    private static final VarHandle ATOMIC_LONG = LONG.varHandle();

    private long handle;
    private MemorySegment control;
    private MemorySegment descriptors;
    private MemorySegment payload;
    private int descCapacity;
    private long descMask;
    private long payloadMask;
    private long payloadCapacity;
    private long tail;
    private long cachedHead;
    private long reclaimedHead;
    private long payloadTail;
    private long payloadHead;

    boolean send(long socketHandle, byte[] body) {
        ensure(socketHandle);
        if (body.length > payloadCapacity) {
            drainIfOpen(-1);
            return false;
        }

        int spins = 0;
        while (descIsFull()) {
            checkOpen();
            spins = backoff(spins);
        }

        Reservation reservation;
        spins = 0;
        while ((reservation = reservePayload(body.length)) == null) {
            checkOpen();
            reclaimConsumed();
            spins = backoff(spins);
        }

        if (body.length > 0) {
            MemorySegment.copy(MemorySegment.ofArray(body), 0, payload, reservation.offset(), body.length);
        }
        long descOffset = (tail & descMask) * DESC_BYTES;
        descriptors.set(LONG, descOffset + DESC_PAYLOAD, reservation.offset());
        descriptors.set(LONG, descOffset + DESC_PAYLOAD_LEN, body.length);
        descriptors.set(LONG, descOffset + DESC_PAYLOAD_END, reservation.end());
        tail++;
        ATOMIC_LONG.setRelease(control, CONTROL_TAIL, tail);
        return true;
    }

    private boolean descIsFull() {
        if (tail - cachedHead >= descCapacity) {
            cachedHead = headAcquire();
            return tail - cachedHead >= descCapacity;
        }
        return false;
    }

    private Reservation reservePayload(int len) {
        if (len == 0) {
            return new Reservation(0, payloadTail);
        }

        long cursor = payloadTail;
        long offset = cursor & payloadMask;
        long needed = len;
        if (offset + len > payloadCapacity) {
            long pad = payloadCapacity - offset;
            cursor += pad;
            needed += pad;
            offset = 0;
        }

        if (cursor + len - payloadHead > payloadCapacity) {
            return null;
        }

        payloadTail += needed;
        return new Reservation(offset, cursor + len);
    }

    private void reclaimConsumed() {
        long head = headAcquire();
        while (reclaimedHead != head) {
            long offset = (reclaimedHead & descMask) * DESC_BYTES;
            payloadHead = descriptors.get(LONG, offset + DESC_PAYLOAD_END);
            reclaimedHead++;
        }
        cachedHead = head;
    }

    boolean drainIfOpen(long timeoutMillis) {
        if (handle == 0) {
            return true;
        }
        long start = System.nanoTime();
        long timeoutNanos = saturatedNanos(timeoutMillis);
        int spins = 0;
        while (tail != headAcquire()) {
            checkOpen();
            if (timeoutMillis == 0) {
                return false;
            }
            if (timeoutMillis > 0 && System.nanoTime() - start >= timeoutNanos) {
                return false;
            }
            spins = backoff(spins);
        }
        reclaimConsumed();
        return true;
    }

    boolean isDrained() {
        if (handle == 0) {
            return true;
        }
        if (tail != headAcquire()) {
            return false;
        }
        reclaimConsumed();
        return true;
    }

    private long headAcquire() {
        return (long) ATOMIC_LONG.getAcquire(control, CONTROL_HEAD);
    }

    private boolean closedAcquire() {
        return (long) ATOMIC_LONG.getAcquire(control, CONTROL_CLOSED) != 0;
    }

    private void checkOpen() {
        if (!closedAcquire()) {
            return;
        }
        NativeFfm.throwSendRingError(handle);
        throw new ClosedException("native send ring closed");
    }

    @SuppressWarnings("restricted")
    private void ensure(long socketHandle) {
        if (handle != 0) {
            return;
        }
        long created = NativeFfm.sendRingCreate(
                socketHandle, DEFAULT_DESC_CAPACITY, DEFAULT_PAYLOAD_CAPACITY);
        int descCapacity = NativeFfm.sendRingDescCapacity(created);
        long nativePayloadCapacity = NativeFfm.sendRingPayloadCapacity(created);
        long controlAddress = NativeFfm.sendRingControlAddress(created);
        long descAddress = NativeFfm.sendRingDescAddress(created);
        long payloadAddress = NativeFfm.sendRingPayloadAddress(created);
        if (descCapacity != DEFAULT_DESC_CAPACITY
                || !isPowerOfTwo(descCapacity)
                || nativePayloadCapacity <= 0
                || !isPowerOfTwo(nativePayloadCapacity)
                || controlAddress == 0 || descAddress == 0 || payloadAddress == 0) {
            NativeFfm.sendRingClose(created);
            throw new OMQException("native send ring returned invalid memory");
        }
        handle = created;
        control = MemorySegment.ofAddress(controlAddress).reinterpret(CONTROL_BYTES);
        descriptors = MemorySegment.ofAddress(descAddress)
                .reinterpret((long) descCapacity * DESC_BYTES);
        payload = MemorySegment.ofAddress(payloadAddress).reinterpret(nativePayloadCapacity);
        this.descCapacity = descCapacity;
        descMask = descCapacity - 1L;
        payloadMask = nativePayloadCapacity - 1L;
        payloadCapacity = nativePayloadCapacity;
        tail = 0;
        cachedHead = 0;
        reclaimedHead = 0;
        payloadTail = 0;
        payloadHead = 0;
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
        descCapacity = 0;
        NativeFfm.sendRingClose(current);
    }

    private static int backoff(int spins) {
        if (spins < 256) {
            Thread.onSpinWait();
            return spins + 1;
        }
        if (spins < 512) {
            Thread.yield();
            return spins + 1;
        }
        LockSupport.parkNanos(50_000L);
        return spins;
    }

    private static long saturatedNanos(long timeoutMillis) {
        if (timeoutMillis <= 0) {
            return timeoutMillis;
        }
        long maxMillis = Long.MAX_VALUE / 1_000_000L;
        if (timeoutMillis >= maxMillis) {
            return Long.MAX_VALUE;
        }
        return timeoutMillis * 1_000_000L;
    }

    private static boolean isPowerOfTwo(long value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    private record Reservation(long offset, long end) {
    }
}
