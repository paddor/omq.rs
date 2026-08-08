package io.omq;

import java.util.Objects;

/** Base unchecked exception for endpoint transport I/O failures. */
public class TransportException extends OMQException {
    private final String operation;
    private final String endpoint;
    private final String detail;

    /** Creates a transport exception for one endpoint operation. */
    public TransportException(String operation, String endpoint, String detail) {
        super(operation + " failed for " + endpoint + ": " + detail);
        this.operation = Objects.requireNonNull(operation, "operation");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.detail = Objects.requireNonNull(detail, "detail");
    }

    /** Returns the operation that failed, such as {@code bind} or {@code connect}. */
    public String operation() {
        return operation;
    }

    /** Returns the endpoint used by the failed operation. */
    public String endpoint() {
        return endpoint;
    }

    /** Returns the native transport error detail. */
    public String detail() {
        return detail;
    }
}
