package io.omq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/** Immutable reusable socket option set. */
public final class SocketOptions {
    private static final int ZMTP_MAX_SHORT_STRING_BYTES = 255;
    private static final int COMPRESSION_DICT_MAX_BYTES = 8 * 1024;
    private static final int ZSTD_LEVEL_MIN = -8;
    private static final int ZSTD_LEVEL_MAX = 4;
    private static final long MAX_HEARTBEAT_TTL_MILLIS = 6_553_500;

    private final List<OptionAction> actions;

    private SocketOptions(List<OptionAction> actions) {
        this.actions = List.copyOf(actions);
    }

    /** Returns a new socket option builder. */
    public static Builder builder() {
        return new Builder();
    }

    void apply(Socket socket) {
        for (OptionAction action : actions) {
            action.apply(socket);
        }
    }

    @FunctionalInterface
    private interface OptionAction {
        void apply(Socket socket);
    }

    /** Builder for immutable socket option sets. */
    public static final class Builder {
        private final ArrayList<OptionAction> actions = new ArrayList<>();

        private Builder() {
        }

        /** Builds an immutable option set. */
        public SocketOptions build() {
            return new SocketOptions(actions);
        }

        /** Sets linger duration for close. */
        public Builder linger(Duration linger) {
            Socket.millis(Objects.requireNonNull(linger, "linger"));
            return add(socket -> socket.linger(linger));
        }

        /** Sets infinite linger. */
        public Builder lingerForever() {
            return add(Socket::lingerForever);
        }

        /** Sets the socket identity. */
        public Builder identity(byte[] identity) {
            byte[] value = copy(Objects.requireNonNull(identity, "identity"));
            requireMaxLength("identity", value.length, ZMTP_MAX_SHORT_STRING_BYTES);
            return add(socket -> socket.identity(copy(value)));
        }

        /** Sets send high-water mark in messages. */
        public Builder sendHighWaterMark(int hwm) {
            requireNonNegative("HWM", hwm);
            return add(socket -> socket.sendHighWaterMark(hwm));
        }

        /** Sets receive high-water mark in messages. */
        public Builder receiveHighWaterMark(int hwm) {
            requireNonNegative("HWM", hwm);
            return add(socket -> socket.receiveHighWaterMark(hwm));
        }

        /** Sets heartbeat interval. */
        public Builder heartbeatInterval(Duration interval) {
            Socket.millis(Objects.requireNonNull(interval, "interval"));
            return add(socket -> socket.heartbeatInterval(interval));
        }

        /** Disables heartbeats. */
        public Builder heartbeatOff() {
            return add(Socket::heartbeatOff);
        }

        /** Sets ZMTP handshake timeout. */
        public Builder handshakeTimeout(Duration timeout) {
            Socket.millis(Objects.requireNonNull(timeout, "timeout"));
            return add(socket -> socket.handshakeTimeout(timeout));
        }

        /** Sets maximum message size in bytes. */
        public Builder maxMessageSize(long size) {
            requireNonNegative("size", size);
            return add(socket -> socket.maxMessageSize(size));
        }

        /** Removes the maximum message size limit. */
        public Builder noMaxMessageSize() {
            return add(Socket::noMaxMessageSize);
        }

        /** Enables or disables compression dictionary auto-training. */
        public Builder compressionAutoTrain(boolean enabled) {
            return add(socket -> socket.compressionAutoTrain(enabled));
        }

        /** Sets compression threshold in bytes. */
        public Builder compressionThreshold(long threshold) {
            requireNonNegative("threshold", threshold);
            return add(socket -> socket.compressionThreshold(threshold));
        }

        /** Restores default compression threshold. */
        public Builder compressionDefaultThreshold() {
            return add(Socket::compressionDefaultThreshold);
        }

        /** Sets compression level. */
        public Builder compressionLevel(int level) {
            if (level < ZSTD_LEVEL_MIN || level > ZSTD_LEVEL_MAX) {
                throw new IllegalArgumentException(
                        "zstd compression level must be " + ZSTD_LEVEL_MIN + "..=" + ZSTD_LEVEL_MAX);
            }
            return add(socket -> socket.compressionLevel(level));
        }

        /** Restores default compression level. */
        public Builder compressionDefaultLevel() {
            return add(Socket::compressionDefaultLevel);
        }

        /**
         * Configures a PLAIN server accepting one credential pair.
         * PLAIN authenticates clients but does not encrypt traffic.
         *
         * @param username accepted username
         * @param password accepted password
         * @return this builder
         * @throws NullPointerException if either value is null
         * @throws IllegalArgumentException if either value exceeds 255 bytes or contains
         *     bytes outside ASCII VCHAR
         */
        public Builder plainServer(String username, String password) {
            return plainServer(List.of(new PlainCredential(username, password)));
        }

        /**
         * Configures an exact, case-sensitive PLAIN server credential
         * allowlist. An empty list rejects every client. Each field must
         * contain at most 255 ASCII VCHAR bytes.
         *
         * @param credentials accepted credential pairs
         * @return this builder
         * @throws NullPointerException if the list or any credential is null
         * @throws IllegalArgumentException if a field is invalid
         */
        public Builder plainServer(List<PlainCredential> credentials) {
            credentials = List.copyOf(Objects.requireNonNull(credentials, "credentials"));
            for (PlainCredential credential : credentials) {
                requireZmtpShortString("username", credential.username());
                requireZmtpShortString("password", credential.password());
            }
            List<PlainCredential> copied = credentials;
            return add(socket -> socket.plainServer(copied));
        }

        /** Configures a PLAIN server with an authenticator. */
        public Builder plainServer(Predicate<PeerInfo> authenticator) {
            Objects.requireNonNull(authenticator, "authenticator");
            return add(socket -> socket.plainServer(authenticator));
        }

        /** Configures a PLAIN client. */
        public Builder plainClient(String username, String password) {
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
            requireZmtpShortString("username", username);
            requireZmtpShortString("password", password);
            return add(socket -> socket.plainClient(username, password));
        }

        /** Configures a CURVE server. */
        public Builder curveServer(CurveKeypair keypair) {
            Objects.requireNonNull(keypair, "keypair");
            requireMatchingCurveKeypair(keypair);
            return add(socket -> socket.curveServer(keypair));
        }

        /** Configures a CURVE server with an authenticator. */
        public Builder curveServer(CurveKeypair keypair, Predicate<PeerInfo> authenticator) {
            Objects.requireNonNull(keypair, "keypair");
            Objects.requireNonNull(authenticator, "authenticator");
            requireMatchingCurveKeypair(keypair);
            return add(socket -> socket.curveServer(keypair, authenticator));
        }

        /** Configures a CURVE client. */
        public Builder curveClient(CurveKeypair keypair, String serverPublicKey) {
            Objects.requireNonNull(keypair, "keypair");
            Objects.requireNonNull(serverPublicKey, "serverPublicKey");
            requireMatchingCurveKeypair(keypair);
            CurveKeys.requireZ85Key("CURVE public key", serverPublicKey);
            return add(socket -> socket.curveClient(keypair, serverPublicKey));
        }

        /** Selects native socket-driver scheduling. */
        public Builder workloadProfile(WorkloadProfile profile) {
            Objects.requireNonNull(profile, "profile");
            return add(socket -> socket.workloadProfile(profile));
        }

        /** Restores native socket-type default scheduling. */
        public Builder defaultWorkloadProfile() {
            return add(Socket::defaultWorkloadProfile);
        }

        /** Disables reconnect attempts. */
        public Builder reconnectDisabled() {
            return add(Socket::reconnectDisabled);
        }

        /** Uses a fixed reconnect interval. */
        public Builder reconnectInterval(Duration interval) {
            Socket.millis(Objects.requireNonNull(interval, "interval"));
            return add(socket -> socket.reconnectInterval(interval));
        }

        /** Uses exponential reconnect backoff. */
        public Builder reconnectExponential(Duration min, Duration max) {
            long minMillis = Socket.millis(Objects.requireNonNull(min, "min"));
            long maxMillis = Socket.millis(Objects.requireNonNull(max, "max"));
            if (maxMillis < minMillis) {
                throw new IllegalArgumentException("max must be greater than or equal to min");
            }
            return add(socket -> socket.reconnectExponential(min, max));
        }

        /** Stops reconnecting after ECONNREFUSED. */
        public Builder reconnectStopConnRefused(boolean enabled) {
            return add(socket -> socket.reconnectStopConnRefused(enabled));
        }

        /** Sets heartbeat TTL advertised to peers. */
        public Builder heartbeatTtl(Duration ttl) {
            long ttlMillis = Socket.millis(Objects.requireNonNull(ttl, "ttl"));
            if (ttlMillis > MAX_HEARTBEAT_TTL_MILLIS) {
                throw new IllegalArgumentException("heartbeat TTL exceeds ZMTP maximum of 6553.5s");
            }
            return add(socket -> socket.heartbeatTtl(ttl));
        }

        /** Omits heartbeat TTL. */
        public Builder noHeartbeatTtl() {
            return add(Socket::noHeartbeatTtl);
        }

        /** Sets receive-idle heartbeat timeout. */
        public Builder heartbeatTimeout(Duration timeout) {
            Socket.millis(Objects.requireNonNull(timeout, "timeout"));
            return add(socket -> socket.heartbeatTimeout(timeout));
        }

        /** Restores default heartbeat timeout. */
        public Builder defaultHeartbeatTimeout() {
            return add(Socket::defaultHeartbeatTimeout);
        }

        /** Sets maximum simultaneous pending handshakes. */
        public Builder maxPendingHandshakes(int max) {
            if (max <= 0) {
                throw new IllegalArgumentException("max must be greater than zero");
            }
            return add(socket -> socket.maxPendingHandshakes(max));
        }

        /** Enables or disables receive-side conflation. */
        public Builder conflate(boolean enabled) {
            return add(socket -> socket.conflate(enabled));
        }

        /** Enables or disables ROUTER mandatory routing errors. */
        public Builder routerMandatory(boolean enabled) {
            return add(socket -> socket.routerMandatory(enabled));
        }

        /** Sets outbound-full behavior. */
        public Builder onMute(OnMute mode) {
            Objects.requireNonNull(mode, "mode");
            return add(socket -> socket.onMute(mode));
        }

        /** Leaves TCP keepalive policy at the operating-system default. */
        public Builder tcpKeepaliveDefault() {
            return add(Socket::tcpKeepaliveDefault);
        }

        /** Disables TCP keepalive. */
        public Builder tcpKeepaliveOff() {
            return add(Socket::tcpKeepaliveOff);
        }

        /** Enables TCP keepalive. */
        public Builder tcpKeepalive(Duration idle, Duration interval, int count) {
            Socket.millis(Objects.requireNonNull(idle, "idle"));
            Socket.millis(Objects.requireNonNull(interval, "interval"));
            if (count <= 0) {
                throw new IllegalArgumentException("count must be greater than zero");
            }
            return add(socket -> socket.tcpKeepalive(idle, interval, count));
        }

        /** Sets OS send buffer size. */
        public Builder sendBufferSize(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.sendBufferSize(bytes));
        }

        /** Restores OS default send buffer size. */
        public Builder defaultSendBufferSize() {
            return add(Socket::defaultSendBufferSize);
        }

        /** Sets OS receive buffer size. */
        public Builder receiveBufferSize(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.receiveBufferSize(bytes));
        }

        /** Restores OS default receive buffer size. */
        public Builder defaultReceiveBufferSize() {
            return add(Socket::defaultReceiveBufferSize);
        }

        /** Sets a compression dictionary. */
        public Builder compressionDict(byte[] dict) {
            byte[] value = copy(Objects.requireNonNull(dict, "dict"));
            if (value.length == 0) {
                throw new IllegalArgumentException("compression dict must not be empty");
            }
            requireMaxLength("compression dict", value.length, COMPRESSION_DICT_MAX_BYTES);
            return add(socket -> socket.compressionDict(copy(value)));
        }

        /** Disables the static compression dictionary. */
        public Builder noCompressionDict() {
            return add(Socket::noCompressionDict);
        }

        /** Sets compression auto-trained dictionary capacity. */
        public Builder compressionDictCapacity(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.compressionDictCapacity(bytes));
        }

        /** Restores default compression auto-trained dictionary capacity. */
        public Builder defaultCompressionDictCapacity() {
            return add(Socket::defaultCompressionDictCapacity);
        }

        /** Sets maximum accepted peer compression dictionary size. */
        public Builder maxReceiveDictSize(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.maxReceiveDictSize(bytes));
        }

        /** Restores default maximum accepted peer compression dictionary size. */
        public Builder defaultMaxReceiveDictSize() {
            return add(Socket::defaultMaxReceiveDictSize);
        }

        /** Sets minimum size for compression offload. */
        public Builder compressionOffloadThreshold(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.compressionOffloadThreshold(bytes));
        }

        /** Disables compression offload. */
        public Builder noCompressionOffload() {
            return add(Socket::noCompressionOffload);
        }

        /** Sets large-message receive threshold. */
        public Builder largeMessageThreshold(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.largeMessageThreshold(bytes));
        }

        /** Disables the large-message receive fast path. */
        public Builder disableLargeMessagePath() {
            return add(Socket::disableLargeMessagePath);
        }

        /** Sets encoder arena threshold. */
        public Builder arenaThreshold(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.arenaThreshold(bytes));
        }

        /** Restores default encoder arena threshold. */
        public Builder defaultArenaThreshold() {
            return add(Socket::defaultArenaThreshold);
        }

        /** Sets per-peer transmit slot capacity. */
        public Builder transmitSlotCapacity(long bytes) {
            requireNonNegative("bytes", bytes);
            return add(socket -> socket.transmitSlotCapacity(bytes));
        }

        /** Restores default per-peer transmit slot capacity. */
        public Builder defaultTransmitSlotCapacity() {
            return add(Socket::defaultTransmitSlotCapacity);
        }

        /** Enables or disables XPUB no-drop behavior. */
        public Builder xpubNoDrop(boolean enabled) {
            return add(socket -> socket.xpubNoDrop(enabled));
        }

        private Builder add(OptionAction action) {
            actions.add(action);
            return this;
        }
    }

    private static byte[] copy(byte[] value) {
        return Arrays.copyOf(value, value.length);
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
}
