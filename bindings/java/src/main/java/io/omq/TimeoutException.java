package io.omq;

/** Raised when an operation times out or would block. */
public final class TimeoutException extends OMQException {
    private static final long serialVersionUID = 1L;

    /** Creates a timeout exception. */
    public TimeoutException(String message) {
        super(message);
    }
}
