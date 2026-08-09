package io.omq;

/** Raised for invalid endpoints or unsupported endpoint schemes. */
public final class InvalidEndpointException extends OMQException {
    private static final long serialVersionUID = 1L;

    /** Creates an invalid-endpoint exception. */
    public InvalidEndpointException(String message) {
        super(message);
    }
}
