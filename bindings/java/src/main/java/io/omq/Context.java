package io.omq;

import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Owns native OMQ I/O threads and creates sockets. */
public final class Context implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();

    private final Set<Socket.State> sockets = ConcurrentHashMap.newKeySet();
    private final State state;
    private final Cleaner.Cleanable cleanable;

    /** Creates a context with one native I/O thread. */
    public Context() {
        this(1);
    }

    /** Creates a context with the requested native I/O thread count. */
    public Context(int ioThreads) {
        if (ioThreads <= 0) {
            throw new IllegalArgumentException("ioThreads must be greater than zero");
        }
        this.state = new State(Native.contextCreate(ioThreads));
        this.cleanable = CLEANER.register(this, state);
    }

    private Context(long handle, boolean owner) {
        this.state = new State(handle, owner);
        this.cleanable = CLEANER.register(this, state);
    }

    /** Opens a context with one native I/O thread. */
    public static Context open() {
        return new Context();
    }

    /** Opens a context with the requested native I/O thread count. */
    public static Context open(int ioThreads) {
        return new Context(ioThreads);
    }

    /** Imports a context handle by an opaque process-local share key. */
    public static Optional<Context> fromShareKey(UUID key) {
        Objects.requireNonNull(key, "key");
        long handle = Native.contextFromShareKey(
                key.getMostSignificantBits(), key.getLeastSignificantBits());
        if (handle == 0) {
            return Optional.empty();
        }
        return Optional.of(new Context(handle, false));
    }

    /** Returns an opaque process-local key for importing this context. */
    public UUID shareKey() {
        synchronized (state) {
            long[] key = Native.contextShareKey(state.handle());
            return new UUID(key[0], key[1]);
        }
    }

    /** Creates a socket owned by this context. */
    public Socket socket(SocketType type) {
        Objects.requireNonNull(type, "type");
        synchronized (state) {
            return new Socket(this, state.handle(), type, sockets);
        }
    }

    void remove(Socket.State state) {
        sockets.remove(state);
    }

    /** Closes all owned sockets and terminates the native context. */
    @Override
    public void close() {
        synchronized (state) {
            for (Socket.State socket : sockets.toArray(Socket.State[]::new)) {
                socket.close();
            }
            cleanable.clean();
        }
    }

    private static final class State implements Runnable {
        private final AtomicLong handle;
        private final boolean owner;

        private State(long handle) {
            this(handle, true);
        }

        private State(long handle, boolean owner) {
            this.handle = new AtomicLong(handle);
            this.owner = owner;
        }

        @Override
        public synchronized void run() {
            long handle = this.handle.getAndSet(0);
            if (handle != 0) {
                Native.contextClose(handle, owner);
            }
        }

        synchronized long handle() {
            long handle = this.handle.get();
            if (handle == 0) {
                throw new ClosedException("context closed");
            }
            return handle;
        }
    }
}
