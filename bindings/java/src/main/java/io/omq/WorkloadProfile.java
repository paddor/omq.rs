package io.omq;

/** Native socket-driver scheduling preference. */
public enum WorkloadProfile {
    /** Prefer batching and throughput. */
    THROUGHPUT(0),
    /** Prefer promptly handing messages to the application. */
    LATENCY(1);

    private final int code;

    WorkloadProfile(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }
}
