package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class ConnectBeforeBindTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 50, 250})
    void pushPullTcp(int delayMillis) throws Exception {
        String endpoint = TestSupport.unboundTcpEndpoint();
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH);
             Socket pull = context.socket(SocketType.PULL)) {
            push.connect(endpoint);
            Thread.sleep(delayMillis);
            pull.bind(endpoint);
            push.send("late");

            assertEquals("late", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 50, 250})
    void reqRepTcp(int delayMillis) throws Exception {
        String endpoint = TestSupport.unboundTcpEndpoint();
        try (Context context = OMQ.context();
             Socket req = context.socket(SocketType.REQ);
             Socket rep = context.socket(SocketType.REP)) {
            req.connect(endpoint);
            Thread.sleep(delayMillis);
            rep.bind(endpoint);
            req.send("q");
            assertEquals("q", rep.receive(Duration.ofSeconds(5)).orElseThrow().text());
            rep.send("a");
            assertEquals("a", req.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 50, 250})
    void pairInproc(int delayMillis) throws Exception {
        String endpoint = TestSupport.inprocEndpoint("cbb-pair");
        try (Context context = OMQ.context();
             Socket a = context.socket(SocketType.PAIR);
             Socket b = context.socket(SocketType.PAIR)) {
            a.connect(endpoint);
            Thread.sleep(delayMillis);
            b.bind(endpoint);
            a.send("from-a");
            assertEquals("from-a", b.receive(Duration.ofSeconds(5)).orElseThrow().text());
            b.send("from-b");
            assertEquals("from-b", a.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }
}
