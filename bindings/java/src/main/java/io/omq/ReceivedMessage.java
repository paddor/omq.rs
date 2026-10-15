package io.omq;

import java.util.Objects;

/** Complete ROUTER/PEER body and its owning sender identity, without the routing prefix. */
public record ReceivedMessage(Identity identity, Message message) {
    /** Creates a received message. */
    public ReceivedMessage {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(message, "message");
    }
}
