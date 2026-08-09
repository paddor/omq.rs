package io.omq;

/** Raised when native OMQ cannot connect an endpoint during preflight. */
public final class ConnectException extends TransportException {
    private static final long serialVersionUID = 1L;

    /** Creates a connect exception for one endpoint. */
    public ConnectException(String endpoint, String detail) {
        this("connect", endpoint, detail);
    }

    /** Creates a connect exception for one endpoint. */
    public ConnectException(String operation, String endpoint, String detail) {
        super(operation, endpoint, detail);
    }
}
