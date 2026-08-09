package io.omq;

import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Closeable stream of diagnostic native socket monitor events. */
public final class Monitor implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final long FOREVER = -1;

    private final State state;
    private final Cleaner.Cleanable cleanable;

    Monitor(long handle) {
        this.state = new State(handle);
        this.cleanable = CLEANER.register(this, state);
    }

    /** Receives the next monitor event, blocking forever. */
    public synchronized MonitorEvent receive() {
        return withHandle(handle -> Native.monitorRecv(handle, FOREVER)).orElseThrow();
    }

    /** Receives the next monitor event before the timeout, or returns empty. */
    public synchronized Optional<MonitorEvent> receive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return withHandle(handle -> Native.monitorRecv(handle, Socket.millis(timeout)));
    }

    /** Receives the next monitor event if already available, or returns empty. */
    public synchronized Optional<MonitorEvent> tryReceive() {
        return withHandle(handle -> Native.monitorRecv(handle, 0));
    }

    /** Closes this monitor stream. */
    @Override
    public void close() {
        cleanable.clean();
    }

    private Optional<MonitorEvent> withHandle(MonitorAction action) {
        synchronized (state) {
            return Optional.ofNullable(action.apply(state.handle()));
        }
    }

    @FunctionalInterface
    private interface MonitorAction {
        MonitorEvent apply(long handle);
    }

    private static final class State implements Runnable {
        private final AtomicLong handle;

        private State(long handle) {
            this.handle = new AtomicLong(handle);
        }

        @Override
        public synchronized void run() {
            long handle = this.handle.getAndSet(0);
            if (handle != 0) {
                Native.monitorClose(handle);
            }
        }

        long handle() {
            long handle = this.handle.get();
            if (handle == 0) {
                throw new ClosedException("monitor closed");
            }
            return handle;
        }
    }
}
