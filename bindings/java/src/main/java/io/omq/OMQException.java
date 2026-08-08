package io.omq;

/** Base unchecked exception for native OMQ failures. */
public class OMQException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** Creates an OMQ exception with a message. */
    public OMQException(String message) {
        super(message);
    }

    /** Creates an OMQ exception with a message and cause. */
    public OMQException(String message, Throwable cause) {
        super(message, cause);
    }
}
