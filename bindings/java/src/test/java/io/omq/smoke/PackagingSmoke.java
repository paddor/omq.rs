package io.omq.smoke;

import io.omq.Context;
import io.omq.OMQ;
import io.omq.SocketType;

/** Smoke entrypoint for packaged OMQ.java runtime jars. */
public final class PackagingSmoke {
    private PackagingSmoke() {
    }

    public static void main(String[] args) {
        try (Context context = OMQ.context()) {
            context.socket(SocketType.PAIR).close();
            OMQ.curveKeypair();
        }
    }
}
