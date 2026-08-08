package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class PubSubTest {
    @Test
    void prefixFilter() {
        try (Context context = OMQ.context();
             Socket pub = context.socket(SocketType.PUB);
             Socket sub = context.socket(SocketType.SUB)) {
            String endpoint = pub.bind("tcp://127.0.0.1:0");
            sub.connect(endpoint);
            sub.subscribe("weather/");
            pub.waitSubscribed(1, Duration.ofSeconds(5));

            pub.send("sports/score-12");
            pub.send("weather/sunny");
            pub.send("weather/rain");

            assertEquals("weather/sunny", sub.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertEquals("weather/rain", sub.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertTrue(sub.tryReceive().isEmpty());
        }
    }

    @Test
    void unsubscribeDropsTopic() throws Exception {
        try (Context context = OMQ.context();
             Socket pub = context.socket(SocketType.PUB);
             Socket sub = context.socket(SocketType.SUB)) {
            String endpoint = pub.bind("tcp://127.0.0.1:0");
            sub.connect(endpoint);
            sub.subscribe("a");
            sub.subscribe("b");
            pub.waitSubscribed(2, Duration.ofSeconds(5));
            sub.unsubscribe("a".getBytes());
            Thread.sleep(100);

            pub.send("a-one");
            pub.send("b-two");

            assertEquals("b-two", sub.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertTrue(sub.tryReceive().isEmpty());
        }
    }

    @Test
    void xpubReceivesSubscriptionFrame() {
        try (Context context = OMQ.context();
             Socket xpub = context.socket(SocketType.XPUB);
             Socket xsub = context.socket(SocketType.XSUB)) {
            String endpoint = xpub.bind("tcp://127.0.0.1:0");
            xsub.connect(endpoint);
            xsub.subscribe("");

            Message subscription = xpub.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(1, subscription.partCount());
            assertEquals(1, subscription.bytes()[0]);
        }
    }
}
