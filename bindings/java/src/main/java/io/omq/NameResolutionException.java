package io.omq;

/** Raised when endpoint host name resolution fails. */
public final class NameResolutionException extends TransportException {
    /** Creates a name-resolution exception for one endpoint. */
    public NameResolutionException(String operation, String endpoint, String detail) {
        super(operation, endpoint, detail);
    }
}
