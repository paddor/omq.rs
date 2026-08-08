package io.omq;

import java.util.Arrays;
import java.util.Optional;

/** Peer metadata supplied to authentication callbacks and monitor events. */
public final class PeerInfo {
    private final String mechanism;
    private final String publicKey;
    private final byte[] identity;
    private final String username;
    private final String password;

    PeerInfo(String mechanism, String publicKey, byte[] identity, String username, String password) {
        this.mechanism = mechanism;
        this.publicKey = publicKey;
        this.identity = identity == null ? null : Arrays.copyOf(identity, identity.length);
        this.username = username;
        this.password = password;
    }

    /** Returns the ZMTP mechanism name, such as {@code PLAIN} or {@code CURVE}. */
    public Optional<String> mechanism() {
        return Optional.ofNullable(mechanism);
    }

    /** Returns the CURVE peer public key as Z85 text, when present. */
    public Optional<String> publicKey() {
        return Optional.ofNullable(publicKey);
    }

    /** Returns a copy of the READY identity, when the peer sent one. */
    public Optional<byte[]> identity() {
        return identity == null ? Optional.empty() : Optional.of(Arrays.copyOf(identity, identity.length));
    }

    /** Returns the PLAIN username, when present. */
    public Optional<String> username() {
        return Optional.ofNullable(username);
    }

    /** Returns the PLAIN password, when present. */
    public Optional<String> password() {
        return Optional.ofNullable(password);
    }
}
