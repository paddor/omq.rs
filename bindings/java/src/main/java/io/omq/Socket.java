package io.omq;

import java.lang.ref.Cleaner;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.Predicate;

/** Synchronous OMQ socket backed by a native omq-tokio socket. */
public final class Socket implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final long FOREVER = -1;
    private static final long NONE = -1;
    private static final int ZMTP_MAX_SHORT_STRING_BYTES = 255;
    private static final int COMPRESSION_DICT_MAX_BYTES = 8 * 1024;
    private static final int ZSTD_LEVEL_MIN = -8;
    private static final int ZSTD_LEVEL_MAX = 4;
    private static final long MAX_HEARTBEAT_TTL_MILLIS = 6_553_500;

    private final ReentrantLock operationLock = new ReentrantLock();
    private final Context context;
    private final SocketType type;
    private final State state;
    private final Cleaner.Cleanable cleanable;

    Socket(Context context, long contextHandle, SocketType type, Set<State> owner) {
        this.context = context;
        this.type = type;
        this.state = new State(
                Native.socketCreate(contextHandle, type.code()),
                owner,
                type == SocketType.PUSH || type == SocketType.SCATTER);
        owner.add(state);
        this.cleanable = CLEANER.register(this, state);
    }

    /** Binds to an endpoint and returns the actual bound endpoint. */
    public String bind(String endpoint) {
        operationLock.lock();
        try {
            Objects.requireNonNull(endpoint, "endpoint");
            return withHandle(handle -> Native.socketBind(handle, endpoint));
        } finally {
            operationLock.unlock();
        }
    }

    /** Connects to an endpoint. */
    public Socket connect(String endpoint) {
        operationLock.lock();
        try {
            Objects.requireNonNull(endpoint, "endpoint");
            withHandleVoid(handle -> Native.socketConnect(handle, endpoint));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Unbinds a previously bound endpoint. */
    public Socket unbind(String endpoint) {
        operationLock.lock();
        try {
            Objects.requireNonNull(endpoint, "endpoint");
            withHandleVoid(handle -> Native.socketUnbind(handle, endpoint));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disconnects a previously connected endpoint. */
    public Socket disconnect(String endpoint) {
        operationLock.lock();
        try {
            Objects.requireNonNull(endpoint, "endpoint");
            withHandleVoid(handle -> Native.socketDisconnect(handle, endpoint));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends a single-part binary message by copying {@code body}. */
    public Socket send(byte[] body) {
        Objects.requireNonNull(body, "body");
        synchronized (state) {
            long handle = state.handle();
            if (usesSendRing() && state.sendRing.send(handle, body)) {
                return this;
            }
            Native.socketSend(handle, body);
        }
        return this;
    }

    /** Sends the remaining bytes of {@code body} without changing its position. */
    public Socket send(ByteBuffer body) {
        operationLock.lock();
        try {
            return send(Message.of(body));
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends UTF-8 text as a single-part message. */
    public Socket send(String text) {
        operationLock.lock();
        try {
            return send(text, StandardCharsets.UTF_8);
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends text encoded with the supplied charset as a single-part message. */
    public Socket send(String text, Charset charset) {
        operationLock.lock();
        try {
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(charset, "charset");
            return send(text.getBytes(charset));
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends a single-part or multipart message. */
    public Socket send(Message message) {
        operationLock.lock();
        try {
            Objects.requireNonNull(message, "message");
            byte[][] parts = message.toNative();
            synchronized (state) {
                long handle = state.handle();
                drainSendRingOrThrow(FOREVER);
                Native.socketSendMultipart(handle, parts, message.routingId().orElse(0));
            }
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends a single-part binary message before the timeout, or returns false. */
    public boolean send(byte[] body, Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(body, "body");
            return send(Message.of(body), timeout);
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends the remaining bytes of {@code body} before the timeout without changing its position. */
    public boolean send(ByteBuffer body, Duration timeout) {
        operationLock.lock();
        try {
            return send(Message.of(body), timeout);
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends UTF-8 text before the timeout, or returns false. */
    public boolean send(String text, Duration timeout) {
        operationLock.lock();
        try {
            return send(text, StandardCharsets.UTF_8, timeout);
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends text encoded with the supplied charset before the timeout, or returns false. */
    public boolean send(String text, Charset charset, Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(charset, "charset");
            return send(text.getBytes(charset), timeout);
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends a message before the timeout, or returns false. */
    public boolean send(Message message, Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(timeout, "timeout");
            byte[][] parts = message.toNative();
            long timeoutMillis = millis(timeout);
            synchronized (state) {
                long handle = state.handle();
                if (!drainSendRing(timeoutMillis)) {
                    return false;
                }
                return Native.socketSendMultipartTimeout(
                        handle, parts, message.routingId().orElse(0), timeoutMillis) != 0;
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Attempts to send a single-part binary message without blocking. */
    public boolean trySend(byte[] body) {
        operationLock.lock();
        try {
            Objects.requireNonNull(body, "body");
            return trySend(Message.of(body));
        } finally {
            operationLock.unlock();
        }
    }

    /** Attempts to send remaining buffer bytes without blocking or changing its position. */
    public boolean trySend(ByteBuffer body) {
        operationLock.lock();
        try {
            return trySend(Message.of(body));
        } finally {
            operationLock.unlock();
        }
    }

    /** Attempts to send UTF-8 text without blocking. */
    public boolean trySend(String text) {
        operationLock.lock();
        try {
            return trySend(text, StandardCharsets.UTF_8);
        } finally {
            operationLock.unlock();
        }
    }

    /** Attempts to send text encoded with the supplied charset without blocking. */
    public boolean trySend(String text, Charset charset) {
        operationLock.lock();
        try {
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(charset, "charset");
            return trySend(text.getBytes(charset));
        } finally {
            operationLock.unlock();
        }
    }

    /** Attempts to send a message without blocking. */
    public boolean trySend(Message message) {
        operationLock.lock();
        try {
            Objects.requireNonNull(message, "message");
            byte[][] parts = message.toNative();
            synchronized (state) {
                long handle = state.handle();
                if (usesSendRing() && !state.sendRing.isDrained()) {
                    return false;
                }
                return Native.socketTrySendMultipart(handle, parts, message.routingId().orElse(0)) != 0;
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends a single-part binary message asynchronously on the native runtime. */
    public CompletableFuture<Void> sendAsync(byte[] body) {
        operationLock.lock();
        try {
            Objects.requireNonNull(body, "body");
            return sendAsync(Message.of(body));
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends the remaining buffer bytes asynchronously without changing its position. */
    public CompletableFuture<Void> sendAsync(ByteBuffer body) {
        operationLock.lock();
        try {
            return sendAsync(Message.of(body));
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends UTF-8 text asynchronously on the native runtime. */
    public CompletableFuture<Void> sendAsync(String text) {
        operationLock.lock();
        try {
            return sendAsync(text, StandardCharsets.UTF_8);
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends text asynchronously on the native runtime with the supplied charset. */
    public CompletableFuture<Void> sendAsync(String text, Charset charset) {
        operationLock.lock();
        try {
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(charset, "charset");
            return sendAsync(text.getBytes(charset));
        } finally {
            operationLock.unlock();
        }
    }

    /** Sends a message asynchronously on the native runtime; canceling aborts the native send. */
    public CompletableFuture<Void> sendAsync(Message message) {
        operationLock.lock();
        try {
            Objects.requireNonNull(message, "message");
            NativeFuture<Void> future = new NativeFuture<>();
            byte[][] parts = message.toNative();
            try {
                long task;
                synchronized (state) {
                    long handle = state.handle();
                    drainSendRingOrThrow(FOREVER);
                    task = Native.socketSendAsync(handle, parts, message.routingId().orElse(0), future);
                }
                future.setNativeTask(task);
            } catch (OMQException error) {
                future.completeExceptionally(error);
            }
            return future;
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one message, blocking forever. */
    public Message receive() {
        operationLock.lock();
        try {
            if (Thread.currentThread().isVirtual()) {
                return receiveVirtual(FOREVER);
            }
            return withRecvRing((ring, handle) -> ring.receive(handle, FOREVER));
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one single-part message body, blocking forever. */
    public byte[] receiveBytes() {
        operationLock.lock();
        try {
            if (Thread.currentThread().isVirtual()) {
                return receiveVirtual(FOREVER).bytes();
            }
            return withRecvRing((ring, handle) -> ring.receiveBytes(handle, FOREVER));
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one single-part message body into {@code destination}, blocking forever. */
    public int receiveInto(ByteBuffer destination) {
        operationLock.lock();
        try {
            Objects.requireNonNull(destination, "destination");
            if (Thread.currentThread().isVirtual()) {
                return writeInto(receiveVirtual(FOREVER), destination);
            }
            return withRecvRing((ring, handle) -> ring.receiveInto(handle, destination, FOREVER));
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one message before the timeout, or returns empty. */
    public Optional<Message> receive(Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            try {
                if (Thread.currentThread().isVirtual()) {
                    return Optional.of(receiveVirtual(timeoutMillis));
                }
                return Optional.of(receiveTimedDirect(timeoutMillis));
            } catch (TimeoutException timeoutError) {
                return Optional.empty();
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one single-part message body before the timeout, or returns empty. */
    public Optional<byte[]> receiveBytes(Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            try {
                if (Thread.currentThread().isVirtual()) {
                    return Optional.of(receiveVirtual(timeoutMillis).bytes());
                }
                return Optional.of(receiveTimedDirect(timeoutMillis).bytes());
            } catch (TimeoutException timeoutError) {
                return Optional.empty();
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one single-part body into {@code destination} before the timeout. */
    public OptionalInt receiveInto(ByteBuffer destination, Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(destination, "destination");
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            try {
                if (Thread.currentThread().isVirtual()) {
                    return OptionalInt.of(writeInto(receiveVirtual(timeoutMillis), destination));
                }
                return OptionalInt.of(writeInto(receiveTimedDirect(timeoutMillis), destination));
            } catch (TimeoutException timeoutError) {
                return OptionalInt.empty();
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one message if already available, or returns empty. */
    public Optional<Message> tryReceive() {
        operationLock.lock();
        try {
            try {
                return Optional.of(receiveTimedDirect(0));
            } catch (TimeoutException timeoutError) {
                return Optional.empty();
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one single-part message body if already available, or returns empty. */
    public Optional<byte[]> tryReceiveBytes() {
        operationLock.lock();
        try {
            try {
                return Optional.of(receiveTimedDirect(0).bytes());
            } catch (TimeoutException timeoutError) {
                return Optional.empty();
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one available single-part body into {@code destination} without blocking. */
    public OptionalInt tryReceiveInto(ByteBuffer destination) {
        operationLock.lock();
        try {
            Objects.requireNonNull(destination, "destination");
            try {
                return OptionalInt.of(writeInto(receiveTimedDirect(0), destination));
            } catch (TimeoutException timeoutError) {
                return OptionalInt.empty();
            }
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one message if already available, or returns empty. */
    public Optional<Message> tryRecv() {
        operationLock.lock();
        try {
            return tryReceive();
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one single-part message body if already available, or returns empty. */
    public Optional<byte[]> tryRecvBytes() {
        operationLock.lock();
        try {
            return tryReceiveBytes();
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one available single-part body into {@code destination} without blocking. */
    public OptionalInt tryRecvInto(ByteBuffer destination) {
        operationLock.lock();
        try {
            return tryReceiveInto(destination);
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one message asynchronously on the native runtime; canceling aborts the native receive. */
    public CompletableFuture<Message> receiveAsync() {
        operationLock.lock();
        try {
            NativeFuture<Message> future = new NativeFuture<>();
            try {
                Optional<Message> cached = tryReceiveCachedMessage();
                if (cached.isPresent()) {
                    future.complete(cached.orElseThrow());
                    return future;
                }
                long task = withHandle(handle -> Native.socketRecvAsync(handle, FOREVER, future));
                future.setNativeTask(task);
            } catch (OMQException error) {
                future.completeExceptionally(error);
            }
            return future;
        } finally {
            operationLock.unlock();
        }
    }

    /** Receives one message asynchronously before the timeout; canceling aborts the native receive. */
    public CompletableFuture<Message> receiveAsync(Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(timeout, "timeout");
            NativeFuture<Message> future = new NativeFuture<>();
            long timeoutMillis = millis(timeout);
            try {
                Optional<Message> cached = tryReceiveCachedMessage();
                if (cached.isPresent()) {
                    future.complete(cached.orElseThrow());
                    return future;
                }
                long task = withHandle(handle -> Native.socketRecvAsync(handle, timeoutMillis, future));
                future.setNativeTask(task);
            } catch (OMQException error) {
                future.completeExceptionally(error);
            }
            return future;
        } finally {
            operationLock.unlock();
        }
    }

    /** Subscribes this socket to a binary prefix. */
    public Socket subscribe(byte[] prefix) {
        operationLock.lock();
        try {
            Objects.requireNonNull(prefix, "prefix");
            withHandleVoid(handle -> Native.socketSubscribe(handle, prefix));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Subscribes this socket to a UTF-8 prefix. */
    public Socket subscribe(String prefix) {
        operationLock.lock();
        try {
            return subscribe(prefix, StandardCharsets.UTF_8);
        } finally {
            operationLock.unlock();
        }
    }

    /** Subscribes this socket to a text prefix encoded with the supplied charset. */
    public Socket subscribe(String prefix, Charset charset) {
        operationLock.lock();
        try {
            Objects.requireNonNull(prefix, "prefix");
            Objects.requireNonNull(charset, "charset");
            return subscribe(prefix.getBytes(charset));
        } finally {
            operationLock.unlock();
        }
    }

    /** Unsubscribes this socket from a binary prefix. */
    public Socket unsubscribe(byte[] prefix) {
        operationLock.lock();
        try {
            Objects.requireNonNull(prefix, "prefix");
            withHandleVoid(handle -> Native.socketUnsubscribe(handle, prefix));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Joins a RADIO/DISH group. */
    public Socket join(byte[] group) {
        operationLock.lock();
        try {
            Objects.requireNonNull(group, "group");
            withHandleVoid(handle -> Native.socketJoin(handle, group));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Joins a RADIO/DISH group encoded as UTF-8. */
    public Socket join(String group) {
        operationLock.lock();
        try {
            return join(group, StandardCharsets.UTF_8);
        } finally {
            operationLock.unlock();
        }
    }

    /** Joins a RADIO/DISH group encoded with the supplied charset. */
    public Socket join(String group, Charset charset) {
        operationLock.lock();
        try {
            Objects.requireNonNull(group, "group");
            Objects.requireNonNull(charset, "charset");
            return join(group.getBytes(charset));
        } finally {
            operationLock.unlock();
        }
    }

    /** Leaves a RADIO/DISH group. */
    public Socket leave(byte[] group) {
        operationLock.lock();
        try {
            Objects.requireNonNull(group, "group");
            withHandleVoid(handle -> Native.socketLeave(handle, group));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Leaves a RADIO/DISH group encoded as UTF-8. */
    public Socket leave(String group) {
        operationLock.lock();
        try {
            return leave(group, StandardCharsets.UTF_8);
        } finally {
            operationLock.unlock();
        }
    }

    /** Leaves a RADIO/DISH group encoded with the supplied charset. */
    public Socket leave(String group, Charset charset) {
        operationLock.lock();
        try {
            Objects.requireNonNull(group, "group");
            Objects.requireNonNull(charset, "charset");
            return leave(group.getBytes(charset));
        } finally {
            operationLock.unlock();
        }
    }

    /** Waits until at least {@code minPeers} peers are connected. */
    public int waitConnected(int minPeers, Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            return withHandle(handle -> Native.socketWaitConnected(handle, minPeers, timeoutMillis));
        } finally {
            operationLock.unlock();
        }
    }

    /** Waits until at least {@code minSubscriptions} subscriptions are visible. */
    public long waitSubscribed(long minSubscriptions, Duration timeout) {
        operationLock.lock();
        try {
            if (minSubscriptions < 0) {
                throw new IllegalArgumentException("minSubscriptions must be non-negative");
            }
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            return withHandle(handle -> Native.socketWaitSubscribed(handle, minSubscriptions, timeoutMillis));
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets linger duration for close. Must be set before first I/O. */
    public Socket linger(Duration linger) {
        operationLock.lock();
        try {
            Objects.requireNonNull(linger, "linger");
            long lingerMillis = millis(linger);
            withHandleVoid(handle -> Native.socketSetLinger(handle, lingerMillis));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets infinite linger. Must be set before first I/O. */
    public Socket lingerForever() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetLinger(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets this socket identity. Must be set before first I/O. */
    public Socket identity(byte[] identity) {
        operationLock.lock();
        try {
            Objects.requireNonNull(identity, "identity");
            requireMaxLength("identity", identity.length, ZMTP_MAX_SHORT_STRING_BYTES);
            withHandleVoid(handle -> Native.socketSetIdentity(handle, identity));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets send high-water mark in messages. Must be set before first I/O. */
    public Socket sendHighWaterMark(int hwm) {
        operationLock.lock();
        try {
            if (hwm < 0) {
                throw new IllegalArgumentException("HWM must be non-negative");
            }
            withHandleVoid(handle -> Native.socketSetSendHighWaterMark(handle, hwm));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets receive high-water mark in messages. Must be set before first I/O. */
    public Socket receiveHighWaterMark(int hwm) {
        operationLock.lock();
        try {
            if (hwm < 0) {
                throw new IllegalArgumentException("HWM must be non-negative");
            }
            withHandleVoid(handle -> Native.socketSetReceiveHighWaterMark(handle, hwm));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets heartbeat interval. Must be set before first I/O. */
    public Socket heartbeatInterval(Duration interval) {
        operationLock.lock();
        try {
            Objects.requireNonNull(interval, "interval");
            long intervalMillis = millis(interval);
            withHandleVoid(handle -> Native.socketSetHeartbeatInterval(handle, intervalMillis));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disables heartbeats. Must be set before first I/O. */
    public Socket heartbeatOff() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetHeartbeatInterval(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets connection setup timeout from DNS through READY (default 10s). Must be set before first I/O. */
    public Socket handshakeTimeout(Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            withHandleVoid(handle -> Native.socketSetHandshakeTimeout(handle, timeoutMillis));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets maximum message size in bytes. Must be set before first I/O. */
    public Socket maxMessageSize(long size) {
        operationLock.lock();
        try {
            if (size < 0) {
                throw new IllegalArgumentException("size must be non-negative");
            }
            withHandleVoid(handle -> Native.socketSetMaxMessageSize(handle, size));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Removes the maximum message size limit. Must be set before first I/O. */
    public Socket noMaxMessageSize() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetMaxMessageSize(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Enables or disables compression dictionary auto-training before first I/O. */
    public Socket compressionAutoTrain(boolean enabled) {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetCompressionAutoTrain(handle, enabled ? 1 : 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets compression threshold in bytes before first I/O. */
    public Socket compressionThreshold(long threshold) {
        operationLock.lock();
        try {
            if (threshold < 0) {
                throw new IllegalArgumentException("threshold must be non-negative");
            }
            withHandleVoid(handle -> Native.socketSetCompressionThreshold(handle, threshold));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default compression threshold before first I/O. */
    public Socket compressionDefaultThreshold() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetCompressionThreshold(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets compression level before first I/O. */
    public Socket compressionLevel(int level) {
        operationLock.lock();
        try {
            if (level < ZSTD_LEVEL_MIN || level > ZSTD_LEVEL_MAX) {
                throw new IllegalArgumentException(
                        "zstd compression level must be " + ZSTD_LEVEL_MIN + "..=" + ZSTD_LEVEL_MAX);
            }
            withHandleVoid(handle -> Native.socketSetCompressionLevel(handle, level));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default compression level before first I/O. */
    public Socket compressionDefaultLevel() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetCompressionLevel(handle, Integer.MIN_VALUE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /**
     * Configures this socket as a PLAIN server accepting one credential pair.
     * PLAIN authenticates clients but does not encrypt traffic.
     *
     * @param username accepted username
     * @param password accepted password
     * @return this socket
     * @throws NullPointerException if either value is null
     * @throws IllegalArgumentException if either value exceeds 255 bytes or contains
     *     bytes outside ASCII VCHAR
     */
    public Socket plainServer(String username, String password) {
        operationLock.lock();
        try {
            return plainServer(List.of(new PlainCredential(username, password)));
        } finally {
            operationLock.unlock();
        }
    }

    /**
     * Configures an exact, case-sensitive PLAIN credential allowlist before
     * bind, connect, or I/O. An empty list rejects every client. PLAIN
     * authenticates clients but does not encrypt traffic.
     *
     * @param credentials accepted credential pairs
     * @return this socket
     * @throws NullPointerException if the list or any credential is null
     * @throws IllegalArgumentException if a field exceeds 255 bytes or contains
     *     bytes outside ASCII VCHAR
     */
    public Socket plainServer(List<PlainCredential> credentials) {
        operationLock.lock();
        try {
            credentials = List.copyOf(Objects.requireNonNull(credentials, "credentials"));
            String[] usernames = new String[credentials.size()];
            String[] passwords = new String[credentials.size()];
            for (int i = 0; i < credentials.size(); i++) {
                PlainCredential credential = credentials.get(i);
                requireZmtpShortString("username", credential.username());
                requireZmtpShortString("password", credential.password());
                usernames[i] = credential.username();
                passwords[i] = credential.password();
            }
            withHandleVoid(handle -> Native.socketSetPlainServer(handle, usernames, passwords));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Configures this socket as a PLAIN server with an authenticator before first I/O. */
    public Socket plainServer(Predicate<PeerInfo> authenticator) {
        operationLock.lock();
        try {
            Objects.requireNonNull(authenticator, "authenticator");
            withHandleVoid(handle -> Native.socketSetPlainServerCallback(handle, authenticator));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Configures this socket as a PLAIN client before first I/O. */
    public Socket plainClient(String username, String password) {
        operationLock.lock();
        try {
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
            requireZmtpShortString("username", username);
            requireZmtpShortString("password", password);
            withHandleVoid(handle -> Native.socketSetPlainClient(handle, username, password));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Configures this socket as a CURVE server before first I/O. */
    public Socket curveServer(CurveKeypair keypair) {
        operationLock.lock();
        try {
            Objects.requireNonNull(keypair, "keypair");
            requireMatchingCurveKeypair(keypair);
            withHandleVoid(handle -> Native.socketSetCurveServer(
                    handle, keypair.publicKey(), keypair.secretKey()));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Configures this socket as a CURVE server with an authenticator before first I/O. */
    public Socket curveServer(
            CurveKeypair keypair, Predicate<PeerInfo> authenticator) {
        operationLock.lock();
        try {
            Objects.requireNonNull(keypair, "keypair");
            Objects.requireNonNull(authenticator, "authenticator");
            requireMatchingCurveKeypair(keypair);
            withHandleVoid(handle -> Native.socketSetCurveServerCallback(
                    handle, keypair.publicKey(), keypair.secretKey(), authenticator));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Configures this socket as a CURVE client before first I/O. */
    public Socket curveClient(CurveKeypair keypair, String serverPublicKey) {
        operationLock.lock();
        try {
            Objects.requireNonNull(keypair, "keypair");
            Objects.requireNonNull(serverPublicKey, "serverPublicKey");
            requireMatchingCurveKeypair(keypair);
            requireCurvePublicKey(serverPublicKey);
            withHandleVoid(handle -> Native.socketSetCurveClient(
                    handle, keypair.publicKey(), keypair.secretKey(), serverPublicKey));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Opens a diagnostic native monitor for this socket. */
    public Monitor monitor() {
        operationLock.lock();
        try {
            return new Monitor(withHandle(Native::socketMonitor));
        } finally {
            operationLock.unlock();
        }
    }

    /** Selects native socket-driver scheduling before first I/O. */
    public Socket workloadProfile(WorkloadProfile profile) {
        operationLock.lock();
        try {
            Objects.requireNonNull(profile, "profile");
            withHandleVoid(handle -> Native.socketSetWorkloadProfile(handle, profile.code()));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores native socket-type default scheduling before first I/O. */
    public Socket defaultWorkloadProfile() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetWorkloadProfile(handle, -1));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disables reconnect attempts before first I/O. */
    public Socket reconnectDisabled() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetReconnect(handle, 0, 0, 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Uses a fixed reconnect interval before first I/O. */
    public Socket reconnectInterval(Duration interval) {
        operationLock.lock();
        try {
            Objects.requireNonNull(interval, "interval");
            long intervalMillis = millis(interval);
            withHandleVoid(handle -> Native.socketSetReconnect(handle, 1, intervalMillis, 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Uses exponential reconnect backoff before first I/O. */
    public Socket reconnectExponential(Duration min, Duration max) {
        operationLock.lock();
        try {
            Objects.requireNonNull(min, "min");
            Objects.requireNonNull(max, "max");
            long minMillis = millis(min);
            long maxMillis = millis(max);
            if (maxMillis < minMillis) {
                throw new IllegalArgumentException("max must be greater than or equal to min");
            }
            withHandleVoid(handle -> Native.socketSetReconnect(handle, 2, minMillis, maxMillis));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Stops reconnecting after ECONNREFUSED before first I/O. */
    public Socket reconnectStopConnRefused(boolean enabled) {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetReconnectStopConnRefused(handle, enabled ? 1 : 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets heartbeat TTL advertised to peers before first I/O. */
    public Socket heartbeatTtl(Duration ttl) {
        operationLock.lock();
        try {
            Objects.requireNonNull(ttl, "ttl");
            long ttlMillis = millis(ttl);
            if (ttlMillis > MAX_HEARTBEAT_TTL_MILLIS) {
                throw new IllegalArgumentException("heartbeat TTL exceeds ZMTP maximum of 6553.5s");
            }
            withHandleVoid(handle -> Native.socketSetHeartbeatTtl(handle, ttlMillis));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Omits heartbeat TTL before first I/O. */
    public Socket noHeartbeatTtl() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetHeartbeatTtl(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets receive-idle heartbeat timeout before first I/O. */
    public Socket heartbeatTimeout(Duration timeout) {
        operationLock.lock();
        try {
            Objects.requireNonNull(timeout, "timeout");
            long timeoutMillis = millis(timeout);
            withHandleVoid(handle -> Native.socketSetHeartbeatTimeout(handle, timeoutMillis));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default heartbeat timeout before first I/O. */
    public Socket defaultHeartbeatTimeout() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetHeartbeatTimeout(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets maximum simultaneous pending handshakes before first I/O. */
    public Socket maxPendingHandshakes(int max) {
        operationLock.lock();
        try {
            if (max <= 0) {
                throw new IllegalArgumentException("max must be greater than zero");
            }
            withHandleVoid(handle -> Native.socketSetMaxPendingHandshakes(handle, max));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Enables or disables receive-side conflation before first I/O. */
    public Socket conflate(boolean enabled) {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetConflate(handle, enabled ? 1 : 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Enables ROUTER mandatory routing errors before first I/O. */
    public Socket routerMandatory(boolean enabled) {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetRouterMandatory(handle, enabled ? 1 : 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets outbound-full behavior before first I/O. */
    public Socket onMute(OnMute mode) {
        operationLock.lock();
        try {
            Objects.requireNonNull(mode, "mode");
            withHandleVoid(handle -> Native.socketSetOnMute(handle, mode.code()));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Leaves TCP keepalive policy at the operating-system default before first I/O. */
    public Socket tcpKeepaliveDefault() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetTcpKeepalive(handle, 0, 0, 0, 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disables TCP keepalive before first I/O. */
    public Socket tcpKeepaliveOff() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetTcpKeepalive(handle, 1, 0, 0, 0));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Enables TCP keepalive before first I/O. */
    public Socket tcpKeepalive(Duration idle, Duration interval, int count) {
        operationLock.lock();
        try {
            Objects.requireNonNull(idle, "idle");
            Objects.requireNonNull(interval, "interval");
            if (count <= 0) {
                throw new IllegalArgumentException("count must be greater than zero");
            }
            withHandleVoid(handle -> Native.socketSetTcpKeepalive(
                    handle, 2, millis(idle), millis(interval), count));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets OS send buffer size before first I/O. */
    public Socket sendBufferSize(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetSendBufferSize(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores OS default send buffer size before first I/O. */
    public Socket defaultSendBufferSize() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetSendBufferSize(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets OS receive buffer size before first I/O. */
    public Socket receiveBufferSize(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetReceiveBufferSize(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores OS default receive buffer size before first I/O. */
    public Socket defaultReceiveBufferSize() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetReceiveBufferSize(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets a compression dictionary before first I/O. */
    public Socket compressionDict(byte[] dict) {
        operationLock.lock();
        try {
            Objects.requireNonNull(dict, "dict");
            if (dict.length == 0) {
                throw new IllegalArgumentException("compression dict must not be empty");
            }
            requireMaxLength("compression dict", dict.length, COMPRESSION_DICT_MAX_BYTES);
            withHandleVoid(handle -> Native.socketSetCompressionDict(handle, dict));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disables the static compression dictionary before first I/O. */
    public Socket noCompressionDict() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetCompressionDict(handle, new byte[0]));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets compression auto-trained dictionary capacity before first I/O. */
    public Socket compressionDictCapacity(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetCompressionDictCapacity(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default compression auto-trained dictionary capacity before first I/O. */
    public Socket defaultCompressionDictCapacity() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetCompressionDictCapacity(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets maximum accepted peer compression dictionary size before first I/O. */
    public Socket maxReceiveDictSize(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetMaxReceiveDictSize(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default maximum accepted peer compression dictionary size before first I/O. */
    public Socket defaultMaxReceiveDictSize() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetMaxReceiveDictSize(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets minimum size for compression offload before first I/O. */
    public Socket compressionOffloadThreshold(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetCompressionOffloadThreshold(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disables compression offload before first I/O. */
    public Socket noCompressionOffload() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetCompressionOffloadThreshold(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets large-message receive threshold before first I/O. */
    public Socket largeMessageThreshold(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetLargeMessageThreshold(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Disables the large-message receive fast path before first I/O. */
    public Socket disableLargeMessagePath() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetLargeMessageThreshold(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets encoder arena threshold before first I/O. */
    public Socket arenaThreshold(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetArenaThreshold(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default encoder arena threshold before first I/O. */
    public Socket defaultArenaThreshold() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetArenaThreshold(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Sets per-peer transmit slot capacity before first I/O. */
    public Socket transmitSlotCapacity(long bytes) {
        operationLock.lock();
        try {
            requireNonNegative("bytes", bytes);
            withHandleVoid(handle -> Native.socketSetTransmitSlotCap(handle, bytes));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Restores default per-peer transmit slot capacity before first I/O. */
    public Socket defaultTransmitSlotCapacity() {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetTransmitSlotCap(handle, NONE));
            return this;
        } finally {
            operationLock.unlock();
        }
    }

    /** Enables or disables XPUB no-drop behavior before first I/O. */
    public Socket xpubNoDrop(boolean enabled) {
        operationLock.lock();
        try {
            withHandleVoid(handle -> Native.socketSetXpubNoDrop(handle, enabled ? 1 : 0));
            return this;
        } finally {
            operationLock.unlock();
        }
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

    private <T> T withRecvRing(RecvRingAction<T> action) {
        synchronized (state) {
            long handle = state.handle();
            return action.apply(state.recvRing, handle);
        }
    }

    private boolean drainSendRing(long timeoutMillis) {
        return !usesSendRing() || state.sendRing.drainIfOpen(timeoutMillis);
    }

    private void drainSendRingOrThrow(long timeoutMillis) {
        if (!drainSendRing(timeoutMillis)) {
            throw new TimeoutException("operation timed out");
        }
    }

    Optional<ReceiveEvent> tryReceiveCachedEvent() {
        synchronized (state) {
            state.handle();
            return state.recvRing.tryReceiveCachedMessage()
                    .map(message -> new ReceiveEvent(this, message));
        }
    }

    /** Closes the socket and releases native resources. */
    @Override
    public void close() {
        cleanable.clean();
        context.remove(state);
    }

    static long millis(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("duration must be non-negative");
        }
        if (duration.isZero()) {
            return 0;
        }
        try {
            long millis = Math.multiplyExact(duration.getSeconds(), 1_000L);
            int nanos = duration.getNano();
            millis = Math.addExact(millis, nanos / 1_000_000L);
            if (nanos % 1_000_000L != 0) {
                millis = Math.addExact(millis, 1L);
            }
            return millis;
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static void requireMaxLength(String name, int length, int max) {
        if (length > max) {
            throw new IllegalArgumentException(name + " length must be at most " + max + " bytes");
        }
    }

    private static void requireZmtpShortString(String name, String value) {
        requireMaxLength(
                name,
                value.getBytes(StandardCharsets.UTF_8).length,
                ZMTP_MAX_SHORT_STRING_BYTES);
        if (!value.chars().allMatch(character -> character >= 0x21 && character <= 0x7e)) {
            throw new IllegalArgumentException(name + " must contain only ASCII VCHAR bytes");
        }
    }

    private static void requireCurvePublicKey(String publicKey) {
        CurveKeys.requireZ85Key("CURVE public key", publicKey);
    }

    private static void requireMatchingCurveKeypair(CurveKeypair keypair) {
        String derivedPublicKey;
        try {
            derivedPublicKey = OMQ.curvePublic(keypair.secretKey());
        } catch (OMQException error) {
            throw new IllegalArgumentException("CURVE secret key must be valid Z85", error);
        }
        if (!keypair.publicKey().equals(derivedPublicKey)) {
            throw new IllegalArgumentException("CURVE public key does not match secret key");
        }
    }

    private Optional<Message> tryReceiveCachedMessage() {
        synchronized (state) {
            state.handle();
            return state.recvRing.tryReceiveCachedMessage();
        }
    }

    private Message receiveTimedDirect(long timeoutMillis) {
        synchronized (state) {
            long handle = state.handle();
            Optional<Message> cached = state.recvRing.tryReceiveCachedMessage();
            if (cached.isPresent()) {
                return cached.orElseThrow();
            }
            return (Message) Native.socketRecv(handle, timeoutMillis);
        }
    }

    private Message receiveVirtual(long timeoutMillis) {
        NativeFuture<Message> future;
        synchronized (state) {
            long handle = state.handle();
            Optional<Message> cached = state.recvRing.tryReceiveCachedMessage();
            if (cached.isPresent()) {
                return cached.orElseThrow();
            }
            future = new NativeFuture<>();
            long task = Native.socketRecvAsync(handle, timeoutMillis, future);
            future.setNativeTask(task);
        }
        return await(future);
    }

    private static int writeInto(Message message, ByteBuffer destination) {
        if (destination.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        byte[] bytes = message.bytes();
        if (bytes.length > destination.remaining()) {
            throw new BufferOverflowException();
        }
        destination.put(bytes);
        return bytes.length;
    }

    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw error;
        }
    }

    private boolean usesSendRing() {
        return type == SocketType.PUSH || type == SocketType.SCATTER;
    }

    static final class State implements Runnable {
        private final AtomicLong handle;
        private final Set<State> owner;
        private final boolean usesSendRing;
        private final SendRing sendRing = new SendRing();
        private final RecvRing recvRing = new RecvRing();

        private State(long handle, Set<State> owner, boolean usesSendRing) {
            this.handle = new AtomicLong(handle);
            this.owner = owner;
            this.usesSendRing = usesSendRing;
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

        void close() {
            long handle = this.handle.getAndSet(0);
            if (handle != 0) {
                if (usesSendRing) {
                    sendRing.shutdown();
                }
                Native.socketShutdown(handle);
                synchronized (this) {
                    sendRing.close();
                    recvRing.close();
                    Native.socketClose(handle);
                }
            }
            owner.remove(this);
        }
    }

    @FunctionalInterface
    private interface RecvRingAction<T> {
        T apply(RecvRing ring, long handle);
    }
}
