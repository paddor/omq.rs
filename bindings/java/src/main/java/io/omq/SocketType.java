package io.omq;

public enum SocketType {
    REQ(0),
    REP(1),
    PUB(2),
    SUB(3),
    XPUB(4),
    XSUB(5),
    PUSH(6),
    PULL(7),
    DEALER(8),
    ROUTER(9),
    PAIR(10),
    CLIENT(11),
    SERVER(12),
    RADIO(13),
    DISH(14),
    SCATTER(15),
    GATHER(16),
    CHANNEL(17),
    PEER(18),
    STREAM(19);

    private final int code;

    SocketType(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }
}
