package io.omq;

/** Send behavior when native outbound queues are full. */
public enum OnMute {
    /** Wait for space before accepting the message. */
    BLOCK(0),
    /** Drop the new message when no queue has room. */
    DROP_NEWEST(1),
    /** Drop the oldest queued message, then enqueue the new message. */
    DROP_OLDEST(2);

    private final int code;

    OnMute(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }
}
