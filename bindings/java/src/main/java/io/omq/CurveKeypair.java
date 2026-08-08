package io.omq;

import java.util.Objects;

public record CurveKeypair(String publicKey, String secretKey) {
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
}
