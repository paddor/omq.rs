package io.omq;

import java.util.Objects;

/** Message received by an async multi-socket receive. */
public record ReceiveEvent(Socket socket, Message message) {
    /** Creates a receive event. */
    public ReceiveEvent {
        Objects.requireNonNull(socket, "socket");
        Objects.requireNonNull(message, "message");
    }

    /** Returns the socket that received the message. */
    @Override
    public Socket socket() {
        return socket;
    }

    /** Returns the received message. */
    @Override
    public Message message() {
        return message;
    }
}
