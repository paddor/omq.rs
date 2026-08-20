package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
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
            assertEquals(1, request.partCount());
            assertEquals("ping", request.text());
            int routingId = request.routingId().orElseThrow();

            server.send(Message.text("pong").withRoutingId(routingId));
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

    @Test
    void pairRoundTrip() {
        try (Context context = OMQ.context();
             Socket a = context.socket(SocketType.PAIR);
             Socket b = context.socket(SocketType.PAIR)) {
            String endpoint = a.bind(TestSupport.inprocEndpoint("pair"));
            b.connect(endpoint);
            a.send("x");
            assertEquals("x", b.receive(Duration.ofSeconds(5)).orElseThrow().text());
            b.send("y");
            assertEquals("y", a.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void draftSinglePartSocketsRejectMultipart() {
        try (Context context = OMQ.context();
             Socket client = context.socket(SocketType.CLIENT);
             Socket server = context.socket(SocketType.SERVER);
             Socket scatter = context.socket(SocketType.SCATTER);
             Socket channel = context.socket(SocketType.CHANNEL);
             Socket radio = context.socket(SocketType.RADIO)) {
            client.connect(TestSupport.inprocEndpoint("client-rejects-multipart"));
            server.bind(TestSupport.inprocEndpoint("server-requires-routing"));
            scatter.bind(TestSupport.inprocEndpoint("scatter-rejects-multipart"));
            channel.bind(TestSupport.inprocEndpoint("channel-rejects-multipart"));
            radio.bind(TestSupport.inprocEndpoint("radio-requires-group"));

            assertThrows(ProtocolException.class,
                    () -> client.send(Message.multipart(utf8("a"), utf8("b"))));
            assertThrows(ProtocolException.class, () -> server.send("missing-routing-id"));
            assertThrows(ProtocolException.class,
                    () -> scatter.send(Message.multipart(utf8("a"), utf8("b"))));
            assertThrows(ProtocolException.class,
                    () -> channel.send(Message.multipart(utf8("a"), utf8("b"))));
            assertThrows(ProtocolException.class, () -> radio.send("missing-group"));
        }
    }

    @Test
    void radioDishFiltersGroupsAndStringHelpers() throws Exception {
        try (Context context = OMQ.context();
             Socket radio = context.socket(SocketType.RADIO);
             Socket dish = context.socket(SocketType.DISH)) {
            String endpoint = radio.bind(TestSupport.inprocEndpoint("radio-dish"));
            dish.join("weather");
            dish.connect(endpoint);
            radio.waitConnected(1, Duration.ofSeconds(5));

            radio.send(Message.multipart(utf8("news"), utf8("ignored")));
            radio.send(Message.multipart(utf8("weather"), utf8("sunny")));

            Message received = dish.receive(Duration.ofSeconds(5)).orElseThrow();
            assertEquals("weather", new String(received.part(0), StandardCharsets.UTF_8));
            assertEquals("sunny", new String(received.part(1), StandardCharsets.UTF_8));

            dish.leave("weather");
            Thread.sleep(50);
            radio.send(Message.multipart(utf8("weather"), utf8("rain")));
            assertTrue(dish.receive(Duration.ofMillis(150)).isEmpty());
        }
    }

    @Test
    void joinOnWrongSocketTypeIsProtocolError() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind(TestSupport.inprocEndpoint("join-wrong-type"));

            assertThrows(ProtocolException.class, () -> pull.join("g"));
        }
    }

    @Test
    void clientServerMultipleClients() {
        try (Context context = OMQ.context();
             Socket server = context.socket(SocketType.SERVER);
             Socket client0 = context.socket(SocketType.CLIENT).identity(utf8("c0"));
             Socket client1 = context.socket(SocketType.CLIENT).identity(utf8("c1"));
             Socket client2 = context.socket(SocketType.CLIENT).identity(utf8("c2"))) {
            String endpoint = server.bind(TestSupport.inprocEndpoint("client-server-many"));
            client0.connect(endpoint);
            client1.connect(endpoint);
            client2.connect(endpoint);
            client0.send("from-0");
            client1.send("from-1");
            client2.send("from-2");

            for (int i = 0; i < 3; i++) {
                Message request = server.receive(Duration.ofSeconds(5)).orElseThrow();
                assertEquals(1, request.partCount());
                server.send(Message.text("re:" + request.text())
                        .withRoutingId(request.routingId().orElseThrow()));
            }

            assertTrue(client0.receive(Duration.ofSeconds(5)).orElseThrow().text().startsWith("re:from-"));
            assertTrue(client1.receive(Duration.ofSeconds(5)).orElseThrow().text().startsWith("re:from-"));
            assertTrue(client2.receive(Duration.ofSeconds(5)).orElseThrow().text().startsWith("re:from-"));
        }
    }

    @Test
    void streamRawTcpRoundTrip() throws Exception {
        try (Context context = OMQ.context();
             Socket stream = context.socket(SocketType.STREAM)) {
            String endpoint = stream.bind("tcp://127.0.0.1:0");
            String[] hostPort = endpoint.substring("tcp://".length()).split(":");
            try (java.net.Socket raw = new java.net.Socket(hostPort[0], Integer.parseInt(hostPort[1]))) {
                raw.setSoTimeout(5_000);
                raw.getOutputStream().write(utf8("hello"));
                raw.getOutputStream().flush();

                Message connected = stream.receive(Duration.ofSeconds(5)).orElseThrow();
                assertEquals(2, connected.partCount());
                byte[] identity = connected.part(0);
                assertTrue(identity.length > 0);
                assertEquals(0, connected.part(1).length);

                Message data = stream.receive(Duration.ofSeconds(5)).orElseThrow();
                assertArrayEquals(identity, data.part(0));
                assertArrayEquals(utf8("hello"), data.part(1));

                stream.send(Message.multipart(identity, utf8("world")));
                byte[] reply = raw.getInputStream().readNBytes(5);
                assertArrayEquals(utf8("world"), reply);
            }
        }
    }

    @Test
    void streamRejectsNonTcpTransports() {
        try (Context context = OMQ.context();
             Socket stream = context.socket(SocketType.STREAM)) {
            assertThrows(ProtocolException.class,
                    () -> stream.bind(TestSupport.inprocEndpoint("stream-inproc")));
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
