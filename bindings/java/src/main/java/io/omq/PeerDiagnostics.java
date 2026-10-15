package io.omq;

import java.util.Optional;

/** Admission-time transport details for diagnostics, not authorization or application identity. */
public final class PeerDiagnostics {
    private final String endpoint;
    private final String localAddress;
    private final String remoteAddress;

    PeerDiagnostics(String endpoint, String localAddress, String remoteAddress) {
        this.endpoint = endpoint;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
    }

    /** Returns the configured endpoint URI, including an IPC or inproc path when applicable. */
    public String endpoint() {
        return endpoint;
    }

    /** Returns the local socket address or path when available. */
    public Optional<String> localAddress() {
        return Optional.ofNullable(localAddress);
    }

    /** Returns the remote socket address or path when available; this may change after admission. */
    public Optional<String> remoteAddress() {
        return Optional.ofNullable(remoteAddress);
    }
}
