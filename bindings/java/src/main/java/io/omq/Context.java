package io.omq;

import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class Context implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();

    private final Set<Socket.State> sockets = ConcurrentHashMap.newKeySet();
    private final State state;
    private final Cleaner.Cleanable cleanable;

    public Context() {
        this(1);
    }

    public Context(int ioThreads) {
        if (ioThreads <= 0) {
            throw new IllegalArgumentException("ioThreads must be greater than zero");
        }
        this.state = new State(Native.contextCreate(ioThreads));
        this.cleanable = CLEANER.register(this, state);
    }

    public static Context open() {
        return new Context();
    }

    public static Context open(int ioThreads) {
        return new Context(ioThreads);
    }

    public Socket socket(SocketType type) {
        Objects.requireNonNull(type, "type");
        long contextHandle = handle();
        Socket socket = new Socket(this, contextHandle, type, sockets);
        return socket;
    }

    long handle() {
        long handle = state.handle.get();
        if (handle == 0) {
            throw new ClosedException("context closed");
        }
        return handle;
    }

    void remove(Socket.State state) {
        sockets.remove(state);
    }

    @Override
    public void close() {
        for (Socket.State socket : sockets.toArray(Socket.State[]::new)) {
            socket.close();
        }
        cleanable.clean();
    }

    private static final class State implements Runnable {
        private final AtomicLong handle;

        private State(long handle) {
            this.handle = new AtomicLong(handle);
        }

        @Override
        public void run() {
            long handle = this.handle.getAndSet(0);
            if (handle != 0) {
                Native.contextClose(handle);
            }
        }
    }
}
