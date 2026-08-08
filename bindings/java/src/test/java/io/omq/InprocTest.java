package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class InprocTest {
    @Test
    void inprocPushPull() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind(TestSupport.inprocEndpoint("push-pull"));
            push.connect(endpoint);
            push.send("hello-inproc");

            assertEquals("hello-inproc", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void inprocWorksAcrossContexts() {
        String endpoint = TestSupport.inprocEndpoint("isolated");
        try (Context a = OMQ.context();
             Context b = OMQ.context();
             Socket pull = a.socket(SocketType.PULL);
             Socket push = b.socket(SocketType.PUSH)) {
            pull.bind(endpoint);
            push.connect(endpoint);
            push.send("hidden");

            assertEquals("hidden", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void pairOverInproc() {
        try (Context context = OMQ.context();
             Socket a = context.socket(SocketType.PAIR);
             Socket b = context.socket(SocketType.PAIR)) {
            String endpoint = a.bind(TestSupport.inprocEndpoint("pair"));
            b.connect(endpoint);
            a.send("one");
            assertEquals("one", b.receive(Duration.ofSeconds(5)).orElseThrow().text());
            b.send("two");
            assertEquals("two", a.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }
}
