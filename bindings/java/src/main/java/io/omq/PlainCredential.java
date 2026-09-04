package io.omq;

import java.util.Objects;

/**
 * One exact username/password pair accepted by a PLAIN server.
 *
 * <p>Each value must contain at most 255 ASCII VCHAR bytes. The value is
 * validated when it is applied to a socket.
 *
 * @param username PLAIN username
 * @param password PLAIN password
 */
public record PlainCredential(String username, String password) {
    /**
     * Creates a non-null credential pair.
     *
     * @throws NullPointerException if either value is null
     */
    public PlainCredential {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
    }
}
