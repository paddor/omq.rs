package io.omq;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.Predicate;

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

    /** Sends a single-part binary message before the timeout, or returns false. */
    public synchronized boolean send(byte[] body, Duration timeout) {
        Objects.requireNonNull(body, "body");
        return send(Message.of(body), timeout);
    }

    /** Sends the remaining bytes of {@code body} before the timeout, or returns false. */
    public synchronized boolean send(ByteBuffer body, Duration timeout) {
        return send(Message.of(body), timeout);
    }

    /** Sends UTF-8 text before the timeout, or returns false. */
    public synchronized boolean send(String text, Duration timeout) {
        return send(text, StandardCharsets.UTF_8, timeout);
    }

    /** Sends text encoded with the supplied charset before the timeout, or returns false. */
    public synchronized boolean send(String text, Charset charset, Duration timeout) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(charset, "charset");
        return send(text.getBytes(charset), timeout);
    }

    /** Sends a message before the timeout, or returns false. */
    public synchronized boolean send(Message message, Duration timeout) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(timeout, "timeout");
        byte[][] parts = message.toNative();
        long timeoutMillis = millis(timeout);
        return withHandle(handle -> Native.socketSendMultipartTimeout(
                handle, parts, timeoutMillis)) != 0;
    }

    /** Sends each byte array as one single-part message. */
    public synchronized Socket sendManyBytes(byte[][] bodies) {
        Objects.requireNonNull(bodies, "bodies");
        byte[][] messages = requireBodies(bodies);
        withHandleVoid(handle -> Native.socketSendMany(handle, messages));
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
        return Message.fromNative(
                this.<Object>withHandle(handle -> Native.socketRecv(handle, FOREVER)));
    }

    /** Receives one single-part message body, blocking forever. */
    public synchronized byte[] receiveBytes() {
        return Message.bytesFromNative(
                this.<Object>withHandle(handle -> Native.socketRecv(handle, FOREVER)));
    }

    /** Receives one single-part message body into {@code destination}, blocking forever. */
    public synchronized int receiveInto(ByteBuffer destination) {
        Objects.requireNonNull(destination, "destination");
        return withHandle(handle -> Native.socketRecvInto(handle, destination, FOREVER));
    }

    /** Receives one message before the timeout, or returns empty. */
    public synchronized Optional<Message> receive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        try {
            return Optional.of(Message.fromNative(
                    this.<Object>withHandle(handle -> Native.socketRecv(handle, timeoutMillis))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    /** Receives one single-part message body before the timeout, or returns empty. */
    public synchronized Optional<byte[]> receiveBytes(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        try {
            return Optional.of(Message.bytesFromNative(
                    this.<Object>withHandle(handle -> Native.socketRecv(handle, timeoutMillis))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    /** Receives one single-part body into {@code destination} before the timeout. */
    public synchronized OptionalInt receiveInto(ByteBuffer destination, Duration timeout) {
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        try {
            return OptionalInt.of(withHandle(handle -> Native.socketRecvInto(
                    handle, destination, timeoutMillis)));
        } catch (TimeoutException timeoutError) {
            return OptionalInt.empty();
        }
    }

    /** Receives one message if already available, or returns empty. */
    public synchronized Optional<Message> tryReceive() {
        try {
            return Optional.of(Message.fromNative(
                    this.<Object>withHandle(handle -> Native.socketRecv(handle, 0))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    /** Receives one single-part message body if already available, or returns empty. */
    public synchronized Optional<byte[]> tryReceiveBytes() {
        try {
            return Optional.of(Message.bytesFromNative(
                    this.<Object>withHandle(handle -> Native.socketRecv(handle, 0))));
        } catch (TimeoutException timeoutError) {
            return Optional.empty();
        }
    }

    /** Receives one available single-part body into {@code destination} without blocking. */
    public synchronized OptionalInt tryReceiveInto(ByteBuffer destination) {
        Objects.requireNonNull(destination, "destination");
        try {
            return OptionalInt.of(withHandle(handle -> Native.socketRecvInto(handle, destination, 0)));
        } catch (TimeoutException timeoutError) {
            return OptionalInt.empty();
        }
    }

    /** Receives up to {@code maxMessages} messages, blocking until the first message. */
    public synchronized List<Message> receiveMany(int maxMessages) {
        requirePositive("maxMessages", maxMessages);
        return messagesFromNative(withHandle(handle -> Native.socketRecvMany(
                handle, maxMessages, FOREVER)));
    }

    /** Receives up to {@code maxMessages} single-part bodies, blocking until the first message. */
    public synchronized List<byte[]> receiveManyBytes(int maxMessages) {
        requirePositive("maxMessages", maxMessages);
        return bytesFromNative(withHandle(handle -> Native.socketRecvMany(
                handle, maxMessages, FOREVER)));
    }

    /** Receives up to {@code maxMessages} messages before the timeout, or returns an empty list. */
    public synchronized List<Message> receiveMany(int maxMessages, Duration timeout) {
        requirePositive("maxMessages", maxMessages);
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        try {
            return messagesFromNative(withHandle(handle -> Native.socketRecvMany(
                    handle, maxMessages, timeoutMillis)));
        } catch (TimeoutException timeoutError) {
            return List.of();
        }
    }

    /** Receives up to {@code maxMessages} single-part bodies before the timeout, or returns an empty list. */
    public synchronized List<byte[]> receiveManyBytes(int maxMessages, Duration timeout) {
        requirePositive("maxMessages", maxMessages);
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        try {
            return bytesFromNative(withHandle(handle -> Native.socketRecvMany(
                    handle, maxMessages, timeoutMillis)));
        } catch (TimeoutException timeoutError) {
            return List.of();
        }
    }

    /** Fills {@code output} with single-part bodies, blocking until the first message. */
    public synchronized int receiveManyBytesInto(byte[][] output) {
        return receiveManyBytesInto(output, 0, outputLength(output));
    }

    /** Fills {@code output} with single-part bodies, blocking until the first message. */
    public synchronized int receiveManyBytesInto(byte[][] output, int offset, int maxMessages) {
        int max = checkOutputRange(output, offset, maxMessages);
        if (max == 0) {
            return 0;
        }
        return withHandle(handle -> Native.socketRecvManyBytesInto(
                handle, output, offset, max, FOREVER));
    }

    /** Fills {@code output} with single-part bodies before the timeout, or returns zero. */
    public synchronized int receiveManyBytesInto(byte[][] output, Duration timeout) {
        return receiveManyBytesInto(output, 0, outputLength(output), timeout);
    }

    /** Fills {@code output} with single-part bodies before the timeout, or returns zero. */
    public synchronized int receiveManyBytesInto(
            byte[][] output, int offset, int maxMessages, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        int max = checkOutputRange(output, offset, maxMessages);
        if (max == 0) {
            return 0;
        }
        long timeoutMillis = millis(timeout);
        try {
            return withHandle(handle -> Native.socketRecvManyBytesInto(
                    handle, output, offset, max, timeoutMillis));
        } catch (TimeoutException timeoutError) {
            return 0;
        }
    }

    /** Receives available messages up to {@code maxMessages} without blocking. */
    public synchronized List<Message> tryReceiveMany(int maxMessages) {
        requirePositive("maxMessages", maxMessages);
        try {
            return messagesFromNative(withHandle(handle -> Native.socketRecvMany(
                    handle, maxMessages, 0)));
        } catch (TimeoutException timeoutError) {
            return List.of();
        }
    }

    /** Receives available single-part bodies up to {@code maxMessages} without blocking. */
    public synchronized List<byte[]> tryReceiveManyBytes(int maxMessages) {
        requirePositive("maxMessages", maxMessages);
        try {
            return bytesFromNative(withHandle(handle -> Native.socketRecvMany(
                    handle, maxMessages, 0)));
        } catch (TimeoutException timeoutError) {
            return List.of();
        }
    }

    /** Fills {@code output} with available single-part bodies without blocking. */
    public synchronized int tryReceiveManyBytesInto(byte[][] output) {
        return tryReceiveManyBytesInto(output, 0, outputLength(output));
    }

    /** Fills {@code output} with available single-part bodies without blocking. */
    public synchronized int tryReceiveManyBytesInto(byte[][] output, int offset, int maxMessages) {
        int max = checkOutputRange(output, offset, maxMessages);
        if (max == 0) {
            return 0;
        }
        try {
            return withHandle(handle -> Native.socketRecvManyBytesInto(
                    handle, output, offset, max, 0));
        } catch (TimeoutException timeoutError) {
            return 0;
        }
    }

    /** Receives one message if already available, or returns empty. */
    public synchronized Optional<Message> tryRecv() {
        return tryReceive();
    }

    /** Receives one single-part message body if already available, or returns empty. */
    public synchronized Optional<byte[]> tryRecvBytes() {
        return tryReceiveBytes();
    }

    /** Receives one available single-part body into {@code destination} without blocking. */
    public synchronized OptionalInt tryRecvInto(ByteBuffer destination) {
        return tryReceiveInto(destination);
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

    /** Configures this socket as a PLAIN server with an authenticator before first I/O. */
    public synchronized Socket plainServer(Predicate<PeerInfo> authenticator) {
        Objects.requireNonNull(authenticator, "authenticator");
        withHandleVoid(handle -> Native.socketSetPlainServerCallback(handle, authenticator));
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

    /** Configures this socket as a CURVE server with an authenticator before first I/O. */
    public synchronized Socket curveServer(
            CurveKeypair keypair, Predicate<PeerInfo> authenticator) {
        Objects.requireNonNull(keypair, "keypair");
        Objects.requireNonNull(authenticator, "authenticator");
        withHandleVoid(handle -> Native.socketSetCurveServerCallback(
                handle, keypair.publicKey(), keypair.secretKey(), authenticator));
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

    /** Opens a diagnostic native monitor for this socket. */
    public synchronized Monitor monitor() {
        return new Monitor(withHandle(Native::socketMonitor));
    }

    /** Selects native socket-driver scheduling before first I/O. */
    public synchronized Socket workloadProfile(WorkloadProfile profile) {
        Objects.requireNonNull(profile, "profile");
        withHandleVoid(handle -> Native.socketSetWorkloadProfile(handle, profile.code()));
        return this;
    }

    /** Restores native socket-type default scheduling before first I/O. */
    public synchronized Socket defaultWorkloadProfile() {
        withHandleVoid(handle -> Native.socketSetWorkloadProfile(handle, -1));
        return this;
    }

    /** Disables reconnect attempts before first I/O. */
    public synchronized Socket reconnectDisabled() {
        withHandleVoid(handle -> Native.socketSetReconnect(handle, 0, 0, 0));
        return this;
    }

    /** Uses a fixed reconnect interval before first I/O. */
    public synchronized Socket reconnectInterval(Duration interval) {
        Objects.requireNonNull(interval, "interval");
        long intervalMillis = millis(interval);
        withHandleVoid(handle -> Native.socketSetReconnect(handle, 1, intervalMillis, 0));
        return this;
    }

    /** Uses exponential reconnect backoff before first I/O. */
    public synchronized Socket reconnectExponential(Duration min, Duration max) {
        Objects.requireNonNull(min, "min");
        Objects.requireNonNull(max, "max");
        long minMillis = millis(min);
        long maxMillis = millis(max);
        if (maxMillis < minMillis) {
            throw new IllegalArgumentException("max must be greater than or equal to min");
        }
        withHandleVoid(handle -> Native.socketSetReconnect(handle, 2, minMillis, maxMillis));
        return this;
    }

    /** Stops reconnecting after ECONNREFUSED before first I/O. */
    public synchronized Socket reconnectStopConnRefused(boolean enabled) {
        withHandleVoid(handle -> Native.socketSetReconnectStopConnRefused(handle, enabled ? 1 : 0));
        return this;
    }

    /** Sets heartbeat TTL advertised to peers before first I/O. */
    public synchronized Socket heartbeatTtl(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        long ttlMillis = millis(ttl);
        withHandleVoid(handle -> Native.socketSetHeartbeatTtl(handle, ttlMillis));
        return this;
    }

    /** Omits heartbeat TTL before first I/O. */
    public synchronized Socket noHeartbeatTtl() {
        withHandleVoid(handle -> Native.socketSetHeartbeatTtl(handle, NONE));
        return this;
    }

    /** Sets receive-idle heartbeat timeout before first I/O. */
    public synchronized Socket heartbeatTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = millis(timeout);
        withHandleVoid(handle -> Native.socketSetHeartbeatTimeout(handle, timeoutMillis));
        return this;
    }

    /** Restores default heartbeat timeout before first I/O. */
    public synchronized Socket defaultHeartbeatTimeout() {
        withHandleVoid(handle -> Native.socketSetHeartbeatTimeout(handle, NONE));
        return this;
    }

    /** Sets maximum simultaneous pending handshakes before first I/O. */
    public synchronized Socket maxPendingHandshakes(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("max must be greater than zero");
        }
        withHandleVoid(handle -> Native.socketSetMaxPendingHandshakes(handle, max));
        return this;
    }

    /** Enables or disables receive-side conflation before first I/O. */
    public synchronized Socket conflate(boolean enabled) {
        withHandleVoid(handle -> Native.socketSetConflate(handle, enabled ? 1 : 0));
        return this;
    }

    /** Enables ROUTER mandatory routing errors before first I/O. */
    public synchronized Socket routerMandatory(boolean enabled) {
        withHandleVoid(handle -> Native.socketSetRouterMandatory(handle, enabled ? 1 : 0));
        return this;
    }

    /** Sets outbound-full behavior before first I/O. */
    public synchronized Socket onMute(OnMute mode) {
        Objects.requireNonNull(mode, "mode");
        withHandleVoid(handle -> Native.socketSetOnMute(handle, mode.code()));
        return this;
    }

    /** Leaves TCP keepalive policy at the operating-system default before first I/O. */
    public synchronized Socket tcpKeepaliveDefault() {
        withHandleVoid(handle -> Native.socketSetTcpKeepalive(handle, 0, 0, 0, 0));
        return this;
    }

    /** Disables TCP keepalive before first I/O. */
    public synchronized Socket tcpKeepaliveOff() {
        withHandleVoid(handle -> Native.socketSetTcpKeepalive(handle, 1, 0, 0, 0));
        return this;
    }

    /** Enables TCP keepalive before first I/O. */
    public synchronized Socket tcpKeepalive(Duration idle, Duration interval, int count) {
        Objects.requireNonNull(idle, "idle");
        Objects.requireNonNull(interval, "interval");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be greater than zero");
        }
        withHandleVoid(handle -> Native.socketSetTcpKeepalive(
                handle, 2, millis(idle), millis(interval), count));
        return this;
    }

    /** Sets OS send buffer size before first I/O. */
    public synchronized Socket sendBufferSize(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetSendBufferSize(handle, bytes));
        return this;
    }

    /** Restores OS default send buffer size before first I/O. */
    public synchronized Socket defaultSendBufferSize() {
        withHandleVoid(handle -> Native.socketSetSendBufferSize(handle, NONE));
        return this;
    }

    /** Sets OS receive buffer size before first I/O. */
    public synchronized Socket receiveBufferSize(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetReceiveBufferSize(handle, bytes));
        return this;
    }

    /** Restores OS default receive buffer size before first I/O. */
    public synchronized Socket defaultReceiveBufferSize() {
        withHandleVoid(handle -> Native.socketSetReceiveBufferSize(handle, NONE));
        return this;
    }

    /** Sets a compression dictionary before first I/O. */
    public synchronized Socket compressionDict(byte[] dict) {
        Objects.requireNonNull(dict, "dict");
        withHandleVoid(handle -> Native.socketSetCompressionDict(handle, dict));
        return this;
    }

    /** Disables the static compression dictionary before first I/O. */
    public synchronized Socket noCompressionDict() {
        withHandleVoid(handle -> Native.socketSetCompressionDict(handle, new byte[0]));
        return this;
    }

    /** Sets compression auto-trained dictionary capacity before first I/O. */
    public synchronized Socket compressionDictCapacity(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetCompressionDictCapacity(handle, bytes));
        return this;
    }

    /** Restores default compression auto-trained dictionary capacity before first I/O. */
    public synchronized Socket defaultCompressionDictCapacity() {
        withHandleVoid(handle -> Native.socketSetCompressionDictCapacity(handle, NONE));
        return this;
    }

    /** Sets maximum accepted peer compression dictionary size before first I/O. */
    public synchronized Socket maxReceiveDictSize(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetMaxReceiveDictSize(handle, bytes));
        return this;
    }

    /** Restores default maximum accepted peer compression dictionary size before first I/O. */
    public synchronized Socket defaultMaxReceiveDictSize() {
        withHandleVoid(handle -> Native.socketSetMaxReceiveDictSize(handle, NONE));
        return this;
    }

    /** Sets minimum size for compression offload before first I/O. */
    public synchronized Socket compressionOffloadThreshold(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetCompressionOffloadThreshold(handle, bytes));
        return this;
    }

    /** Disables compression offload before first I/O. */
    public synchronized Socket noCompressionOffload() {
        withHandleVoid(handle -> Native.socketSetCompressionOffloadThreshold(handle, NONE));
        return this;
    }

    /** Sets large-message receive threshold before first I/O. */
    public synchronized Socket largeMessageThreshold(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetLargeMessageThreshold(handle, bytes));
        return this;
    }

    /** Disables the large-message receive fast path before first I/O. */
    public synchronized Socket disableLargeMessagePath() {
        withHandleVoid(handle -> Native.socketSetLargeMessageThreshold(handle, NONE));
        return this;
    }

    /** Sets encoder arena threshold before first I/O. */
    public synchronized Socket arenaThreshold(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetArenaThreshold(handle, bytes));
        return this;
    }

    /** Restores default encoder arena threshold before first I/O. */
    public synchronized Socket defaultArenaThreshold() {
        withHandleVoid(handle -> Native.socketSetArenaThreshold(handle, NONE));
        return this;
    }

    /** Sets per-peer transmit slot capacity before first I/O. */
    public synchronized Socket transmitSlotCapacity(long bytes) {
        requireNonNegative("bytes", bytes);
        withHandleVoid(handle -> Native.socketSetTransmitSlotCap(handle, bytes));
        return this;
    }

    /** Restores default per-peer transmit slot capacity before first I/O. */
    public synchronized Socket defaultTransmitSlotCapacity() {
        withHandleVoid(handle -> Native.socketSetTransmitSlotCap(handle, NONE));
        return this;
    }

    /** Enables or disables XPUB no-drop behavior before first I/O. */
    public synchronized Socket xpubNoDrop(boolean enabled) {
        withHandleVoid(handle -> Native.socketSetXpubNoDrop(handle, enabled ? 1 : 0));
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

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static List<Message> messagesFromNative(Object[] nativeMessages) {
        ArrayList<Message> out = new ArrayList<>(nativeMessages.length);
        for (Object nativeMessage : nativeMessages) {
            out.add(Message.fromNative(nativeMessage));
        }
        return out;
    }

    private static List<byte[]> bytesFromNative(Object[] nativeMessages) {
        ArrayList<byte[]> out = new ArrayList<>(nativeMessages.length);
        for (Object nativeMessage : nativeMessages) {
            out.add(Message.bytesFromNative(nativeMessage));
        }
        return out;
    }

    private static byte[][] requireBodies(byte[][] bodies) {
        for (int i = 0; i < bodies.length; i++) {
            Objects.requireNonNull(bodies[i], "body " + i);
        }
        return bodies;
    }

    private static int outputLength(byte[][] output) {
        return Objects.requireNonNull(output, "output").length;
    }

    private static int checkOutputRange(byte[][] output, int offset, int maxMessages) {
        Objects.requireNonNull(output, "output");
        if (offset < 0) {
            throw new IndexOutOfBoundsException("offset must be non-negative");
        }
        if (maxMessages < 0) {
            throw new IllegalArgumentException("maxMessages must be non-negative");
        }
        if (offset > output.length || maxMessages > output.length - offset) {
            throw new IndexOutOfBoundsException("output range exceeds array length");
        }
        return maxMessages;
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
