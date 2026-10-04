package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

final class JeroMQInteropTest {
    @Test
    void omqPushTalksToJeroMqPull() {
        try (ZContext zcontext = new ZContext();
             Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            ZMQ.Socket pull = zcontext.createSocket(org.zeromq.SocketType.PULL);
            try {
                String endpoint = "tcp://127.0.0.1:" + pull.bindToRandomPort("tcp://127.0.0.1");
                pull.setReceiveTimeOut(5_000);

                push.connect(endpoint);
                push.waitConnected(1, Duration.ofSeconds(5));
                push.send("hello-jeromq");

                byte[] received = pull.recv(0);
                assertNotNull(received);
                assertEquals("hello-jeromq", new String(received, StandardCharsets.UTF_8));
            } finally {
                pull.close();
            }
        }
    }

    @Test
    void jeroMqPushTalksToOmqPull() {
        try (ZContext zcontext = new ZContext();
             Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Monitor monitor = pull.monitor()) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            ZMQ.Socket push = zcontext.createSocket(org.zeromq.SocketType.PUSH);
            try {
                push.connect(endpoint);
                try {
                    pull.waitConnected(1, Duration.ofSeconds(5));
                } catch (TimeoutException timeout) {
                    dumpHandshakeState(monitor, endpoint);
                    throw timeout;
                }
                assertEquals(true, push.send("hello-omq".getBytes(StandardCharsets.UTF_8), 0));

                Message received = pull.receive(Duration.ofSeconds(5)).orElseThrow();
                assertArrayEquals("hello-omq".getBytes(StandardCharsets.UTF_8), received.bytes());
            } finally {
                push.close();
            }
        }
    }

    private static void dumpHandshakeState(Monitor monitor, String endpoint) {
        System.err.println("[DEBUG-jero] Timeout waiting for " + endpoint);
        for (int i = 0; i < 64; i++) {
            MonitorEvent event = monitor.tryReceive().orElse(null);
            if (event == null) {
                break;
            }
            System.err.println("[DEBUG-jero] OMQ " + event.type() + " "
                    + event.endpoint().orElse("") + " " + event.reason().orElse(""));
        }
        Thread.getAllStackTraces().forEach((thread, stack) -> {
            if (thread.getName().contains("iothread")) {
                System.err.println("[DEBUG-jero] " + thread.getName() + " " + thread.getState());
                for (StackTraceElement frame : stack) {
                    System.err.println("[DEBUG-jero] " + frame);
                }
            }
        });
    }
}
