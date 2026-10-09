package io.omq;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class SendRing implements AutoCloseable {
    private static final int DEFAULT_DESC_CAPACITY = 4096;
    private static final long DEFAULT_PAYLOAD_CAPACITY = 16L * 1024L * 1024L;

    private static final long CONTROL_BYTES = 640;
    private static final long CONTROL_HEAD = 0;
    private static final long CONTROL_TAIL = 128;
    private static final long CONTROL_CLOSED = 256;
    private static final long CONTROL_WORKER_PARKED = 384;
    private static final int SPIN_LIMIT = 512;

    private static final long DESC_BYTES = 64;
    private static final long DESC_PAYLOAD = 0;
    private static final long DESC_PAYLOAD_LEN = 8;
    private static final long DESC_PAYLOAD_END = 16;

    private static final ValueLayout.OfLong LONG =
            ValueLayout.JAVA_LONG.withOrder(ByteOrder.nativeOrder());
    private static final VarHandle ATOMIC_LONG = LONG.arrayElementVarHandle();

    private volatile long handle;
    private volatile boolean shutdownRequested;
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
            spins = waitForSpace(spins, cachedHead, -1);
        }

        Reservation reservation;
        spins = 0;
        while ((reservation = reservePayload(body.length)) == null) {
            checkOpen();
            long seen = cachedHead;
            reclaimConsumed();
            if (cachedHead == seen) {
                spins = waitForSpace(spins, seen, -1);
            }
        }

        if (body.length > 0) {
            MemorySegment.copy(MemorySegment.ofArray(body), 0, payload, reservation.offset(), body.length);
        }
        long descOffset = (tail & descMask) * DESC_BYTES;
        descriptors.set(LONG, descOffset + DESC_PAYLOAD, reservation.offset());
        descriptors.set(LONG, descOffset + DESC_PAYLOAD_LEN, body.length);
        descriptors.set(LONG, descOffset + DESC_PAYLOAD_END, reservation.end());
        tail++;
        ATOMIC_LONG.setRelease(control, CONTROL_TAIL / Long.BYTES, tail);
        // Pairs with the worker's parked-flag store and tail recheck.
        VarHandle.fullFence();
        if ((long) ATOMIC_LONG.getVolatile(control, CONTROL_WORKER_PARKED / Long.BYTES) != 0
                && ATOMIC_LONG.compareAndSet(control, CONTROL_WORKER_PARKED / Long.BYTES, 1L, 0L)) {
            NativeFfm.sendRingWake(handle);
        }
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
        long head;
        while (tail != (head = headAcquire())) {
            checkOpen();
            if (timeoutMillis == 0) {
                return false;
            }
            long remainingMillis = -1;
            if (timeoutMillis > 0) {
                long elapsed = System.nanoTime() - start;
                if (elapsed >= timeoutNanos) {
                    return false;
                }
                remainingMillis = Math.max(1, (timeoutNanos - elapsed) / 1_000_000L);
            }
            spins = waitForSpace(spins, head, remainingMillis);
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

    void shutdown() {
        shutdownRequested = true;
        long current = handle;
        if (current == 0) {
            return;
        }
        ATOMIC_LONG.setRelease(control, CONTROL_CLOSED / Long.BYTES, 1L);
        NativeFfm.sendRingWake(current);
    }

    private long headAcquire() {
        return (long) ATOMIC_LONG.getAcquire(control, CONTROL_HEAD / Long.BYTES);
    }

    private boolean closedAcquire() {
        return (long) ATOMIC_LONG.getAcquire(control, CONTROL_CLOSED / Long.BYTES) != 0;
    }

    private void checkOpen() {
        if (!shutdownRequested && !closedAcquire()) {
            return;
        }
        if (handle == 0) {
            throw new ClosedException("native send ring closed");
        }
        NativeFfm.throwSendRingError(handle);
        throw new ClosedException("native send ring closed");
    }

    @SuppressWarnings("restricted")
    private void ensure(long socketHandle) {
        if (shutdownRequested) {
            throw new ClosedException("native send ring closed");
        }
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
        handle = created;
        if (shutdownRequested) {
            ATOMIC_LONG.setRelease(control, CONTROL_CLOSED / Long.BYTES, 1L);
        }
    }

    @Override
    public void close() {
        long current = handle;
        if (current == 0) {
            return;
        }
        shutdown();
        handle = 0;
        control = null;
        descriptors = null;
        payload = null;
        descCapacity = 0;
        NativeFfm.sendRingClose(current);
    }

    /** Spins briefly, then parks natively until the head moves past {@code seenHead}. */
    private int waitForSpace(int spins, long seenHead, long timeoutMillis) {
        if (spins < 256) {
            Thread.onSpinWait();
            return spins + 1;
        }
        if (spins < SPIN_LIMIT) {
            Thread.yield();
            return spins + 1;
        }
        NativeFfm.sendRingWait(handle, seenHead, timeoutMillis);
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
