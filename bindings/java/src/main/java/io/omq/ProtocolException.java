package io.omq;

/** Raised for ZMTP protocol, version, or handshake failures. */
public final class ProtocolException extends OMQException {
    /** Creates a protocol exception. */
    public ProtocolException(String message) {
        super(message);
    }
}
