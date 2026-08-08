package io.omq;

/** OMQ socket type. */
public enum SocketType {
    /** Strict request socket. */
    REQ(0),
    /** Strict reply socket. */
    REP(1),
    /** Publish socket. */
    PUB(2),
    /** Subscribe socket. */
    SUB(3),
    /** Extended publish socket. */
    XPUB(4),
    /** Extended subscribe socket. */
    XSUB(5),
    /** Pipeline push socket. */
    PUSH(6),
    /** Pipeline pull socket. */
    PULL(7),
    /** Async request socket. */
    DEALER(8),
    /** Async reply routing socket. */
    ROUTER(9),
    /** One-to-one bidirectional socket. */
    PAIR(10),
    /** Client socket. */
    CLIENT(11),
    /** Server socket. */
    SERVER(12),
    /** Radio group publish socket. */
    RADIO(13),
    /** Dish group subscribe socket. */
    DISH(14),
    /** Single-part pipeline push socket. */
    SCATTER(15),
    /** Single-part pipeline pull socket. */
    GATHER(16),
    /** Single-part one-to-one socket. */
    CHANNEL(17),
    /** Peer socket. */
    PEER(18),
    /** Raw TCP stream socket. */
    STREAM(19);

    private final int code;

    SocketType(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }
}
