package io.omq;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Locally verified authentication facts, separate from peer-advertised metadata. */
public final class PeerAuthentication {
    private final String mechanism;
    private final String publicKeyType;
    private final byte[] publicKey;
    private final byte[] userId;
    private final Map<String, byte[]> metadata;

    PeerAuthentication(String mechanism, String publicKeyType, byte[] publicKey,
            byte[] userId, Map<String, byte[]> metadata) {
        this.mechanism = mechanism;
        this.publicKeyType = publicKeyType;
        this.publicKey = copy(publicKey);
        this.userId = copy(userId);
        this.metadata = copyMetadata(metadata);
    }

    /** Returns the verified ZMTP mechanism, when present. */
    public Optional<String> mechanism() {
        return Optional.ofNullable(mechanism);
    }

    /** Returns the public key scheme, currently CURVE, when verified. */
    public Optional<String> publicKeyType() {
        return Optional.ofNullable(publicKeyType);
    }

    /** Returns raw verified public key bytes (32 bytes for CURVE), not Z85 text. */
    public Optional<byte[]> publicKey() {
        return Optional.ofNullable(copy(publicKey));
    }

    /** Returns the locally assigned authenticated user ID, when present. */
    public Optional<byte[]> userId() {
        return Optional.ofNullable(copy(userId));
    }

    /** Returns a defensive copy of locally assigned authentication metadata. */
    public Map<String, byte[]> metadata() {
        return copyMetadata(metadata);
    }

    private static byte[] copy(byte[] value) {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    private static Map<String, byte[]> copyMetadata(Map<String, byte[]> values) {
        Map<String, byte[]> result = new LinkedHashMap<>();
        values.forEach((key, value) -> result.put(key, copy(value)));
        return Collections.unmodifiableMap(result);
    }
}
