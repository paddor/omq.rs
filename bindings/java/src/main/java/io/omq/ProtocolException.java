package io.omq;

/** Raised for ZMTP protocol, version, or handshake failures. */
public final class ProtocolException extends OMQException {
    private static final long serialVersionUID = 1L;

    /** Creates a protocol exception. */
    public ProtocolException(String message) {
        super(message);
    }
}
