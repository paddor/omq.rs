package io.omq;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicLong;

final class TestSupport {
    private static final AtomicLong IDS = new AtomicLong();

    private TestSupport() {
    }

    static String inprocEndpoint(String name) {
        return "inproc://" + name + "-" + IDS.incrementAndGet();
    }

    static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        } catch (IOException error) {
            throw new RuntimeException(error);
        }
    }

    static String unboundTcpEndpoint() {
        return "tcp://127.0.0.1:" + freePort();
    }
}
