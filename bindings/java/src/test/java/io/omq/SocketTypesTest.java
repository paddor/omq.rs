package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;

final class SocketTypesTest {
    @ParameterizedTest
    @EnumSource(SocketType.class)
    void constructsEachSocketType(SocketType type) {
        try (Context context = OMQ.context();
             Socket socket = context.socket(type)) {
            assertEquals(Socket.class, socket.getClass());
        }
    }

    @Test
    void dealerRouterRoundTrip() {
        try (Context context = OMQ.context();
             Socket router = context.socket(SocketType.ROUTER);
             Socket dealer = context.socket(SocketType.DEALER).identity("dealer-1".getBytes())) {
            String endpoint = router.bind(TestSupport.inprocEndpoint("dealer-router"));
            dealer.connect(endpoint);
            dealer.send("hello");

            Message request = router.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(2, request.partCount());
            assertEquals("dealer-1", new String(request.part(0)));
            assertEquals("hello", new String(request.part(1)));

            router.send(Message.multipart(request.part(0), "world".getBytes()));
            assertEquals("world", dealer.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void clientServerRoundTrip() {
        try (Context context = OMQ.context();
             Socket server = context.socket(SocketType.SERVER);
             Socket client = context.socket(SocketType.CLIENT).identity("client-1".getBytes())) {
            String endpoint = server.bind(TestSupport.inprocEndpoint("client-server"));
            client.connect(endpoint);
            client.send("ping");

            Message request = server.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals(2, request.partCount());
            assertEquals("client-1", new String(request.part(0)));
            assertEquals("ping", new String(request.part(1)));

            server.send(Message.multipart(request.part(0), "pong".getBytes()));
            assertEquals("pong", client.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void scatterGatherRoundTrip() {
        try (Context context = OMQ.context();
             Socket gather = context.socket(SocketType.GATHER);
             Socket scatter = context.socket(SocketType.SCATTER)) {
            String endpoint = gather.bind(TestSupport.inprocEndpoint("scatter-gather"));
            scatter.connect(endpoint);
            scatter.send("work");

            assertEquals("work", gather.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void channelRoundTrip() {
        try (Context context = OMQ.context();
             Socket a = context.socket(SocketType.CHANNEL);
             Socket b = context.socket(SocketType.CHANNEL)) {
            String endpoint = a.bind(TestSupport.inprocEndpoint("channel"));
            b.connect(endpoint);
            a.send("one");
            assertEquals("one", b.receive(Duration.ofSeconds(5)).orElseThrow().text());
            b.send("two");
            assertEquals("two", a.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }
}
