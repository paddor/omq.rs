package io.omq;

/** Raised for invalid endpoints or unsupported endpoint schemes. */
public final class InvalidEndpointException extends OMQException {
    /** Creates an invalid-endpoint exception. */
    public InvalidEndpointException(String message) {
        super(message);
    }
}
