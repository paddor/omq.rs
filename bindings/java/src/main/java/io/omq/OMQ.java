package io.omq;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Static entry points for OMQ.java. */
public final class OMQ {
    private static final Object RECEIVE_ANY_SETUP_LOCK = new Object();

    private OMQ() {
    }

    /** Opens a context with one native I/O thread. */
    public static Context context() {
        return Context.open();
    }

    /** Opens a context with the requested native I/O thread count. */
    public static Context context(int ioThreads) {
        return Context.open(ioThreads);
    }

    /** Generates a CURVE Z85 public/secret key pair in native OMQ. */
    public static CurveKeypair curveKeypair() {
        String[] keypair = Native.curveKeypair();
        return new CurveKeypair(keypair[0], keypair[1]);
    }

    /** Derives the CURVE Z85 public key for a Z85 secret key. */
    public static String curvePublic(String secretKey) {
        return Native.curvePublic(secretKey);
    }

    /** Receives the next message from any distinct supplied socket on the native runtime. */
    public static CompletableFuture<ReceiveEvent> receiveAny(Socket... sockets) {
        Objects.requireNonNull(sockets, "sockets");
        if (sockets.length == 0) {
            throw new IllegalArgumentException("at least one socket is required");
        }
        Socket[] copy = sockets.clone();
        Set<Socket> unique = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < copy.length; i++) {
            Objects.requireNonNull(copy[i], "socket " + i);
            if (!unique.add(copy[i])) {
                throw new IllegalArgumentException("sockets must be distinct");
            }
        }
        NativeFuture<ReceiveEvent> future = new NativeFuture<>();
        try {
            long task = withReceiveAnySetupLocked(copy, () -> {
                long[] handles = new long[copy.length];
                for (int i = 0; i < copy.length; i++) {
                    handles[i] = copy[i].nativeHandle();
                }
                return Native.receiveAnyAsync(copy, handles, future);
            });
            future.setNativeTask(task);
        } catch (OMQException error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    private static long withReceiveAnySetupLocked(Socket[] sockets, NativeLongSupplier action) {
        synchronized (RECEIVE_ANY_SETUP_LOCK) {
            return withSocketLocks(sockets, 0, action);
        }
    }

    private static long withSocketLocks(Socket[] sockets, int index, NativeLongSupplier action) {
        if (index == sockets.length) {
            return action.getAsLong();
        }
        synchronized (sockets[index].nativeMonitor()) {
            return withSocketLocks(sockets, index + 1, action);
        }
    }

    @FunctionalInterface
    private interface NativeLongSupplier {
        long getAsLong();
    }
}
