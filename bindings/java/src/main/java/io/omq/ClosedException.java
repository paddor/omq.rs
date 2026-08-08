package io.omq;

/** Raised when a context or socket has already been closed. */
public final class ClosedException extends OMQException {
    /** Creates a closed-resource exception. */
    public ClosedException(String message) {
        super(message);
    }
}
