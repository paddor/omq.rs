package io.omq;

import java.util.Objects;

/** CURVE Z85 public and secret key pair. */
public record CurveKeypair(String publicKey, String secretKey) {
    /** Validates CURVE key length and nullness. */
    public CurveKeypair {
        Objects.requireNonNull(publicKey, "publicKey");
        Objects.requireNonNull(secretKey, "secretKey");
        if (publicKey.length() != 40) {
            throw new IllegalArgumentException("CURVE public key must be 40 Z85 characters");
        }
        if (secretKey.length() != 40) {
            throw new IllegalArgumentException("CURVE secret key must be 40 Z85 characters");
        }
    }

    /** Returns the CURVE Z85 public key. */
    @Override
    public String publicKey() {
        return publicKey;
    }

    /** Returns the CURVE Z85 secret key. */
    @Override
    public String secretKey() {
        return secretKey;
    }
}
