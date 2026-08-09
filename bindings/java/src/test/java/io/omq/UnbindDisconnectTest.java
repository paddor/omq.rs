package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class UnbindDisconnectTest {
    @Test
    void disconnectThenReconnect() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.disconnect(endpoint);
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("again");

            assertEquals("again", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void unbindThenBindAgain() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            pull.unbind(endpoint);
            pull.bind(endpoint);
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("after-rebind");

            assertEquals("after-rebind", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }
}
