package io.omq;

public class OMQException extends RuntimeException {
    public OMQException(String message) {
        super(message);
    }

    public OMQException(String message, Throwable cause) {
        super(message, cause);
    }
}
