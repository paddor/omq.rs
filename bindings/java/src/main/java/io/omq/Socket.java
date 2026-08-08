package io.omq;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/** Synchronous OMQ socket backed by a native omq-tokio socket. */
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

    /** Binds to an endpoint and returns the actual bound endpoint. */
    public synchronized String bind(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        return withHandle(handle -> Native.socketBind(handle, endpoint));
    }

    /** Connects to an endpoint. */
    public synchronized Socket connect(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        withHandleVoid(handle -> Native.socketConnect(handle, endpoint));
        return this;
    }

    /** Unbinds a previously bound endpoint. */
    public synchronized Socket unbind(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        withHandleVoid(handle -> Native.socketUnbind(handle, endpoint));
        return this;
    }

    /** Disconnects a previously connected endpoint. */
    public synchronized Socket disconnect(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        withHandleVoid(handle -> Native.socketDisconnect(handle, endpoint));
        return this;
    }

    /** Sends a single-part binary message by copying {@code body}. */
    public synchronized Socket send(byte[] body) {
        Objects.requireNonNull(body, "body");
        withHandleVoid(handle -> Native.socketSend(handle, body));
        return this;
    }

    /** Sends the remaining bytes of {@code body} as one message part. */
    public synchronized Socket send(ByteBuffer body) {
        return send(Message.of(body));
    }

    /** Sends UTF-8 text as a single-part message. */
    public synchronized Socket send(String text) {
        return send(text, StandardCharsets.UTF_8);
    }

    /** Sends text encoded with the supplied charset as a single-part message. */
    public synchronized Socket send(String text, Charset charset) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return send(text.getBytes(charset));
    }

    /** Sends a single-part or multipart message. */
    public synchronized Socket send(Message message) {
        Objects.requireNonNull(message, "message");
        byte[][] parts = message.toNative();
        withHandleVoid(handle -> Native.socketSendMultipart(handle, parts));
        return this;
    }

    /** Attempts to send a single-part binary message without blocking. */
    public synchronized boolean trySend(byte[] body) {
        Objects.requireNonNull(body, "body");
        return trySend(Message.of(body));
    }

    /** Attempts to send remaining buffer bytes without blocking. */
    public synchronized boolean trySend(ByteBuffer body) {
        return trySend(Message.of(body));
    }

    /** Attempts to send UTF-8 text without blocking. */
    public synchronized boolean trySend(String text) {
        return trySend(text, StandardCharsets.UTF_8);
    }

    /** Attempts to send text encoded with the supplied charset without blocking. */
    public synchronized boolean trySend(String text, Charset charset) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return trySend(text.getBytes(charset));
    }

    /** Attempts to send a message without blocking. */
    public synchronized boolean trySend(Message message) {
        Objects.requireNonNull(message, "message");
        byte[][] parts = message.toNative();
        return withHandle(handle -> Native.socketTrySendMultipart(handle, parts)) != 0;
    }

    /** Sends a single-part binary message asynchronously on the native runtime. */
    public synchronized CompletableFuture<Void> sendAsync(byte[] body) {
        Objects.requireNonNull(body, "body");
        return sendAsync(Message.of(body));
    }

    /** Sends the remaining buffer bytes asynchronously on the native runtime. */
    public synchronized CompletableFuture<Void> sendAsync(ByteBuffer body) {
        return sendAsync(Message.of(body));
    }

    /** Sends UTF-8 text asynchronously on the native runtime. */
    public synchronized CompletableFuture<Void> sendAsync(String text) {
        return sendAsync(text, StandardCharsets.UTF_8);
    }

    /** Sends text asynchronously on the native runtime with the supplied charset. */
    public synchronized CompletableFuture<Void> sendAsync(String text, Charset charset) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return sendAsync(text.getBytes(charset));
    }

    /** Sends a message asynchronously on the native runtime; canceling aborts the native send. */
    public synchronized CompletableFuture<Void> sendAsync(Message message) {
        Objects.requireNonNull(message, "message");
        NativeFuture<Void> future = new NativeFuture<>();
        byte[][] parts = message.toNative();
        try {
            long task = withHandle(handle -> Native.socketSendAsync(handle, parts, future));
            future.setNativeTask(task);
        } catch (OMQException error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    /** Receives one message, blocking forever. */
    public synchronized Message receive() {
        return Message.fromNative(withHandle(handle -> Native.socketRecv(handle, FOREVER)));
    }

    /** Receives one message before the timeout, or returns empty. */
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

    /** Receives one message if already available, or returns empty. */
    public synchronized Optional<Message> tryReceive() {
        try {
            return Optional.of(Message.fromNative(withHandle(handle -> Native.socketRecv(handle, 0))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    /** Receives one message if already available, or returns empty. */
    public synchronized Optional<Message> tryRecv() {
        return tryReceive();
    }

    /** Receives one message asynchronously on the native runtime; canceling aborts the native receive. */
    public synchronized CompletableFuture<Message> receiveAsync() {
        NativeFuture<Message> future = new NativeFuture<>();
        try {
            long task = withHandle(handle -> Native.socketRecvAsync(handle, FOREVER, future));
            future.setNativeTask(task);
        } catch (OMQException error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    /** Receives one message asynchronously before the timeout; canceling aborts the native receive. */
    public synchronized CompletableFuture<Message> receiveAsync(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        NativeFuture<Message> future = new NativeFuture<>();
        long timeoutMillis = millis(timeout);
        try {
            long task = withHandle(handle -> Native.socketRecvAsync(handle, timeoutMillis, future));
            future.setNativeTask(task);
        } catch (OMQException error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    /** Subscribes this socket to a binary prefix. */
    public synchronized Socket subscribe(byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix");
        withHandleVoid(handle -> Native.socketSubscribe(handle, prefix));
        return this;
    }

    /** Subscribes this socket to a UTF-8 prefix. */
    public synchronized Socket subscribe(String prefix) {
        return subscribe(prefix, StandardCharsets.UTF_8);
    }

    /** Subscribes this socket to a text prefix encoded with the supplied charset. */
    public synchronized Socket subscribe(String prefix, Charset charset) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(charset, "charset");
        return subscribe(prefix.getBytes(charset));
    }

    /** Unsubscribes this socket from a binary prefix. */
    public synchronized Socket unsubscribe(byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix");
        withHandleVoid(handle -> Native.socketUnsubscribe(handle, prefix));
        return this;
    }

    /** Joins a RADIO/DISH group. */
    public synchronized Socket join(byte[] group) {
        Objects.requireNonNull(group, "group");
        withHandleVoid(handle -> Native.socketJoin(handle, group));
        return this;
    }

    /** Leaves a RADIO/DISH group. */
    public synchronized Socket leave(byte[] group) {
        Objects.requireNonNull(group, "group");
        withHandleVoid(handle -> Native.socketLeave(handle, group));
        return this;
    }

    /** Waits until at least {@code minPeers} peers are connected. */
    public synchronized int waitConnected(int minPeers, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        return withHandle(handle -> Native.socketWaitConnected(handle, minPeers, timeoutMillis));
    }

    /** Waits until at least {@code minSubscriptions} subscriptions are visible. */
    public synchronized long waitSubscribed(long minSubscriptions, Duration timeout) {
        if (minSubscriptions < 0) {
            throw new IllegalArgumentException("minSubscriptions must be non-negative");
        }
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        return withHandle(handle -> Native.socketWaitSubscribed(handle, minSubscriptions, timeoutMillis));
    }

    /** Sets linger duration for close. Must be set before first I/O. */
    public synchronized Socket linger(Duration linger) {
        Objects.requireNonNull(linger, "linger");
        long lingerMillis = millis(linger);
        withHandleVoid(handle -> Native.socketSetLinger(handle, lingerMillis));
        return this;
    }

    /** Sets infinite linger. Must be set before first I/O. */
    public synchronized Socket lingerForever() {
        withHandleVoid(handle -> Native.socketSetLinger(handle, NONE));
        return this;
    }

    /** Sets this socket identity. Must be set before first I/O. */
    public synchronized Socket identity(byte[] identity) {
        Objects.requireNonNull(identity, "identity");
        withHandleVoid(handle -> Native.socketSetIdentity(handle, identity));
        return this;
    }

    /** Sets send high-water mark in messages. Must be set before first I/O. */
    public synchronized Socket sendHighWaterMark(int hwm) {
        if (hwm < 0) {
            throw new IllegalArgumentException("HWM must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetSendHighWaterMark(handle, hwm));
        return this;
    }

    /** Sets receive high-water mark in messages. Must be set before first I/O. */
    public synchronized Socket receiveHighWaterMark(int hwm) {
        if (hwm < 0) {
            throw new IllegalArgumentException("HWM must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetReceiveHighWaterMark(handle, hwm));
        return this;
    }

    /** Sets heartbeat interval. Must be set before first I/O. */
    public synchronized Socket heartbeatInterval(Duration interval) {
        Objects.requireNonNull(interval, "interval");
        long intervalMillis = millis(interval);
        withHandleVoid(handle -> Native.socketSetHeartbeatInterval(handle, intervalMillis));
        return this;
    }

    /** Disables heartbeats. Must be set before first I/O. */
    public synchronized Socket heartbeatOff() {
        withHandleVoid(handle -> Native.socketSetHeartbeatInterval(handle, NONE));
        return this;
    }

    /** Sets ZMTP handshake timeout. Must be set before first I/O. */
    public synchronized Socket handshakeTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        withHandleVoid(handle -> Native.socketSetHandshakeTimeout(handle, timeoutMillis));
        return this;
    }

    /** Sets maximum message size in bytes. Must be set before first I/O. */
    public synchronized Socket maxMessageSize(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetMaxMessageSize(handle, size));
        return this;
    }

    /** Removes the maximum message size limit. Must be set before first I/O. */
    public synchronized Socket noMaxMessageSize() {
        withHandleVoid(handle -> Native.socketSetMaxMessageSize(handle, NONE));
        return this;
    }

    /** Enables or disables compression dictionary auto-training before first I/O. */
    public synchronized Socket compressionAutoTrain(boolean enabled) {
        withHandleVoid(handle -> Native.socketSetCompressionAutoTrain(handle, enabled ? 1 : 0));
        return this;
    }

    /** Sets compression threshold in bytes before first I/O. */
    public synchronized Socket compressionThreshold(long threshold) {
        if (threshold < 0) {
            throw new IllegalArgumentException("threshold must be non-negative");
        }
        withHandleVoid(handle -> Native.socketSetCompressionThreshold(handle, threshold));
        return this;
    }

    /** Restores default compression threshold before first I/O. */
    public synchronized Socket compressionDefaultThreshold() {
        withHandleVoid(handle -> Native.socketSetCompressionThreshold(handle, NONE));
        return this;
    }

    /** Sets compression level before first I/O. */
    public synchronized Socket compressionLevel(int level) {
        withHandleVoid(handle -> Native.socketSetCompressionLevel(handle, level));
        return this;
    }

    /** Restores default compression level before first I/O. */
    public synchronized Socket compressionDefaultLevel() {
        withHandleVoid(handle -> Native.socketSetCompressionLevel(handle, Integer.MIN_VALUE));
        return this;
    }

    /** Configures this socket as a PLAIN server before first I/O. */
    public synchronized Socket plainServer(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        withHandleVoid(handle -> Native.socketSetPlainServer(handle, username, password));
        return this;
    }

    /** Configures this socket as a PLAIN client before first I/O. */
    public synchronized Socket plainClient(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        withHandleVoid(handle -> Native.socketSetPlainClient(handle, username, password));
        return this;
    }

    /** Configures this socket as a CURVE server before first I/O. */
    public synchronized Socket curveServer(CurveKeypair keypair) {
        Objects.requireNonNull(keypair, "keypair");
        withHandleVoid(handle -> Native.socketSetCurveServer(
                handle, keypair.publicKey(), keypair.secretKey()));
        return this;
    }

    /** Configures this socket as a CURVE client before first I/O. */
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

    long nativeHandle() {
        synchronized (state) {
            return state.handle();
        }
    }

    Object nativeMonitor() {
        return state;
    }

    private void withHandleVoid(LongConsumer action) {
        synchronized (state) {
            action.accept(state.handle());
        }
    }

    /** Closes the socket and releases native resources. */
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
