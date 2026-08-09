package io.omq;

/** Raised when native OMQ cannot bind an endpoint. */
public final class BindException extends TransportException {
    private static final long serialVersionUID = 1L;

    /** Creates a bind exception for one endpoint. */
    public BindException(String endpoint, String detail) {
        this("bind", endpoint, detail);
    }

    /** Creates a bind exception for one endpoint. */
    public BindException(String operation, String endpoint, String detail) {
        super(operation, endpoint, detail);
    }
}
