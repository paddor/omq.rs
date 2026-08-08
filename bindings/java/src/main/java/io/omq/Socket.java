package io.omq;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

public final class Socket implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final long FOREVER = -1;
    private static final long NONE = -1;

    private final Context context;
    private final State state;
    private final Cleaner.Cleanable cleanable;

    Socket(Context context, long contextHandle, SocketType type, Set<State> owner) {
        this.context = context;
        this.state = new State(Native.socketCreate(contextHandle, type.code()), owner);
        owner.add(state);
        this.cleanable = CLEANER.register(this, state);
    }

    public synchronized String bind(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        return withHandle(handle -> Native.socketBind(handle, endpoint));
    }

    public synchronized Socket connect(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        withHandleVoid(handle -> Native.socketConnect(handle, endpoint));
        return this;
    }

    public synchronized Socket unbind(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        withHandleVoid(handle -> Native.socketUnbind(handle, endpoint));
        return this;
    }

    public synchronized Socket disconnect(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        withHandleVoid(handle -> Native.socketDisconnect(handle, endpoint));
        return this;
    }

    public synchronized Socket send(byte[] body) {
        Objects.requireNonNull(body, "body");
        withHandleVoid(handle -> Native.socketSend(handle, body));
        return this;
    }

    public synchronized Socket send(ByteBuffer body) {
        return send(Message.of(body));
    }

    public synchronized Socket send(String text) {
        return send(text, StandardCharsets.UTF_8);
    }

    public synchronized Socket send(String text, Charset charset) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return send(text.getBytes(charset));
    }

    public synchronized Socket send(Message message) {
        Objects.requireNonNull(message, "message");
        byte[][] parts = message.toNative();
        withHandleVoid(handle -> Native.socketSendMultipart(handle, parts));
        return this;
    }

    public synchronized Message receive() {
        return Message.fromNative(withHandle(handle -> Native.socketRecv(handle, FOREVER)));
    }

    public synchronized Optional<Message> receive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        try {
            return Optional.of(Message.fromNative(
                    withHandle(handle -> Native.socketRecv(handle, timeoutMillis))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    public synchronized Optional<Message> tryReceive() {
        try {
            return Optional.of(Message.fromNative(withHandle(handle -> Native.socketRecv(handle, 0))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    public synchronized Socket subscribe(byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix");
        withHandleVoid(handle -> Native.socketSubscribe(handle, prefix));
        return this;
    }

    public synchronized Socket subscribe(String prefix) {
        return subscribe(prefix, StandardCharsets.UTF_8);
    }

    public synchronized Socket subscribe(String prefix, Charset charset) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(charset, "charset");
        return subscribe(prefix.getBytes(charset));
    }

    public synchronized Socket unsubscribe(byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix");
        withHandleVoid(handle -> Native.socketUnsubscribe(handle, prefix));
        return this;
    }

    public synchronized Socket join(byte[] group) {
        Objects.requireNonNull(group, "group");
        withHandleVoid(handle -> Native.socketJoin(handle, group));
        return this;
    }

    public synchronized Socket leave(byte[] group) {
        Objects.requireNonNull(group, "group");
        withHandleVoid(handle -> Native.socketLeave(handle, group));
        return this;
    }

    public synchronized int waitConnected(int minPeers, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        return withHandle(handle -> Native.socketWaitConnected(handle, minPeers, timeoutMillis));
    }

    public synchronized long waitSubscribed(long minSubscriptions, Duration timeout) {
        if (minSubscriptions < 0) {
            throw new IllegalArgumentException("minSubscriptions must be non-negative");
        }
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        return withHandle(handle -> Native.socketWaitSubscribed(handle, minSubscriptions, timeoutMillis));
    }

    public synchronized Socket linger(Duration linger) {
        Objects.requireNonNull(linger, "linger");
        long lingerMillis = millis(linger);
        withHandleVoid(handle -> Native.socketSetLinger(handle, lingerMillis));
        return this;
    }

    public synchronized Socket lingerForever() {
        withHandleVoid(handle -> Native.socketSetLinger(handle, NONE));
        return this;
    }

    public synchronized Socket identity(byte[] identity) {
        Objects.requireNonNull(identity, "identity");
        withHandleVoid(handle -> Native.socketSetIdentity(handle, identity));
        return this;
    }

    public synchronized Socket sendHighWaterMark(int hwm) {
        if (hwm < 0) {
            throw new IllegalArgumentException("HWM must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetSendHighWaterMark(handle, hwm));
        return this;
    }

    public synchronized Socket receiveHighWaterMark(int hwm) {
        if (hwm < 0) {
            throw new IllegalArgumentException("HWM must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetReceiveHighWaterMark(handle, hwm));
        return this;
    }

    public synchronized Socket heartbeatInterval(Duration interval) {
        Objects.requireNonNull(interval, "interval");
        long intervalMillis = millis(interval);
        withHandleVoid(handle -> Native.socketSetHeartbeatInterval(handle, intervalMillis));
        return this;
    }

    public synchronized Socket heartbeatOff() {
        withHandleVoid(handle -> Native.socketSetHeartbeatInterval(handle, NONE));
        return this;
    }

    public synchronized Socket handshakeTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        withHandleVoid(handle -> Native.socketSetHandshakeTimeout(handle, timeoutMillis));
        return this;
    }

    public synchronized Socket maxMessageSize(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetMaxMessageSize(handle, size));
        return this;
    }

    public synchronized Socket noMaxMessageSize() {
        withHandleVoid(handle -> Native.socketSetMaxMessageSize(handle, NONE));
        return this;
    }

    public synchronized Socket compressionAutoTrain(boolean enabled) {
        withHandleVoid(handle -> Native.socketSetCompressionAutoTrain(handle, enabled ? 1 : 0));
        return this;
    }

    public synchronized Socket compressionThreshold(long threshold) {
        if (threshold < 0) {
            throw new IllegalArgumentException("threshold must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetCompressionThreshold(handle, threshold));
        return this;
    }

    public synchronized Socket compressionDefaultThreshold() {
        withHandleVoid(handle -> Native.socketSetCompressionThreshold(handle, NONE));
        return this;
    }

    public synchronized Socket compressionLevel(int level) {
        withHandleVoid(handle -> Native.socketSetCompressionLevel(handle, level));
        return this;
    }

    public synchronized Socket compressionDefaultLevel() {
        withHandleVoid(handle -> Native.socketSetCompressionLevel(handle, Integer.MIN_VALUE));
        return this;
    }

    public synchronized Socket plainServer(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        withHandleVoid(handle -> Native.socketSetPlainServer(handle, username, password));
        return this;
    }

    public synchronized Socket plainClient(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        withHandleVoid(handle -> Native.socketSetPlainClient(handle, username, password));
        return this;
    }

    public synchronized Socket curveServer(CurveKeypair keypair) {
        Objects.requireNonNull(keypair, "keypair");
        withHandleVoid(handle -> Native.socketSetCurveServer(
                handle, keypair.publicKey(), keypair.secretKey()));
        return this;
    }

    public synchronized Socket curveClient(CurveKeypair keypair, String serverPublicKey) {
        Objects.requireNonNull(keypair, "keypair");
        Objects.requireNonNull(serverPublicKey, "serverPublicKey");
        withHandleVoid(handle -> Native.socketSetCurveClient(
                handle, keypair.publicKey(), keypair.secretKey(), serverPublicKey));
        return this;
    }

    private <T> T withHandle(LongFunction<T> action) {
        synchronized (state) {
            return action.apply(state.handle());
        }
    }

    private void withHandleVoid(LongConsumer action) {
        synchronized (state) {
            action.accept(state.handle());
        }
    }

    @Override
    public void close() {
        cleanable.clean();
        context.remove(state);
    }

    private static long millis(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("duration must be non-negative");
        }
        return duration.toMillis();
    }

    static final class State implements Runnable {
        private final AtomicLong handle;
        private final Set<State> owner;

        private State(long handle, Set<State> owner) {
            this.handle = new AtomicLong(handle);
            this.owner = owner;
        }

        @Override
        public void run() {
            close();
        }

        long handle() {
            long handle = this.handle.get();
            if (handle == 0) {
                throw new ClosedException("socket closed");
            }
            return handle;
        }

        synchronized void close() {
            long handle = this.handle.getAndSet(0);
            if (handle != 0) {
                Native.socketClose(handle);
            }
            owner.remove(this);
        }
    }
}
