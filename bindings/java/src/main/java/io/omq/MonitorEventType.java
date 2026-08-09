package io.omq;

/** Type of native socket monitor event. */
public enum MonitorEventType {
    /** Bind listener became active. */
    LISTENING,
    /** Incoming transport connection accepted. */
    ACCEPTED,
    /** Outbound transport connection established. */
    CONNECTED,
    /** ZMTP handshake completed. */
    HANDSHAKE_SUCCEEDED,
    /** ZMTP handshake failed. */
    HANDSHAKE_FAILED,
    /** Connect attempt delayed before reconnect. */
    CONNECT_DELAYED,
    /** Peer connection disconnected. */
    DISCONNECTED,
    /** Peer subscription received. */
    SUBSCRIBE_RECEIVED,
    /** Peer unsubscription received. */
    UNSUBSCRIBE_RECEIVED,
    /** Peer group join received. */
    JOIN_RECEIVED,
    /** Peer group leave received. */
    LEAVE_RECEIVED,
    /** Peer command surfaced by native OMQ. */
    PEER_COMMAND,
    /** Socket driver closed. */
    CLOSED
}
