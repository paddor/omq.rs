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
    private final long connectionId;
    private final String peerAddress;
    private final String socketType;
    private final int zmtpMajor;
    private final int zmtpMinor;

    PeerInfo(String mechanism, String publicKey, byte[] identity, String username, String password, String peerAddress) {
        this(mechanism, publicKey, identity, username, password, -1, peerAddress, null, -1, -1);
    }

    PeerInfo(
            String mechanism,
            String publicKey,
            byte[] identity,
            String username,
            String password,
            long connectionId,
            String peerAddress,
            String socketType,
            int zmtpMajor,
            int zmtpMinor) {
        this.mechanism = mechanism;
        this.publicKey = publicKey;
        this.identity = identity == null ? null : Arrays.copyOf(identity, identity.length);
        this.username = username;
        this.password = password;
        this.connectionId = connectionId;
        this.peerAddress = peerAddress;
        this.socketType = socketType;
        this.zmtpMajor = zmtpMajor;
        this.zmtpMinor = zmtpMinor;
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

    /** Returns native connection id, when present. */
    public java.util.OptionalLong connectionId() {
        return connectionId < 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(connectionId);
    }

    /** Returns TCP peer address, when present. */
    public Optional<String> peerAddress() {
        return Optional.ofNullable(peerAddress);
    }

    /** Returns peer READY socket type, when present. */
    public Optional<String> socketType() {
        return Optional.ofNullable(socketType);
    }

    /** Returns negotiated ZMTP version text, when present. */
    public Optional<String> zmtpVersion() {
        return zmtpMajor < 0 ? Optional.empty() : Optional.of(zmtpMajor + "." + zmtpMinor);
    }
}
