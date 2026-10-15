package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class RecvFromTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void ringAndDirectReceivesKeepIdentityAcrossSocketClose() {
        for (boolean peer : new boolean[] {false, true}) {
            for (String endpoint : List.of("tcp://127.0.0.1:0", "inproc://received")) {
                Identity retained;
                try (Context context = OMQ.context();
                     Socket server = context.socket(peer ? SocketType.PEER : SocketType.ROUTER).identity(bytes("server"));
                     Socket client = context.socket(peer ? SocketType.PEER : SocketType.DEALER).identity(bytes("client"))) {
                    String bound = server.bind(endpoint);
                    client.connect(bound);
                    client.waitConnected(1, TIMEOUT);
                    for (int i = 0; i < 128; i++) {
                        byte[][] body = peer
                                ? new byte[][] {bytes("server"), new byte[0], bytes("body-" + i)}
                                : new byte[][] {new byte[0], bytes("body-" + i)};
                        client.send(Message.multipart(body));
                    }
                    // Ordinary receive may cache subsequent messages in the FFM ring.
                    Message first = server.receive();
                    assertArrayEquals(bytes("body-0"), first.part(2), peer + " " + endpoint);
                    ReceivedMessage next = server.recvFrom();
                    assertArrayEquals(bytes("body-1"), next.message().part(1), peer + " " + endpoint);
                    retained = next.identity();
                    for (int i = 2; i < 128; i++) {
                        ReceivedMessage received = server.recvFrom(TIMEOUT).orElseThrow();
                        assertEquals(retained, received.identity());
                        assertEquals(retained.hashCode(), received.identity().hashCode());
                        assertEquals(2, received.message().partCount());
                        assertArrayEquals(new byte[0], received.message().part(0));
                        assertArrayEquals(bytes("body-" + i), received.message().part(1), peer + " " + endpoint + " " + i);
                    }
                    assertTrue(server.tryRecvFrom().isEmpty());
                    server.sendTo(retained, Message.text("reply"));
                    if (peer) {
                        ReceivedMessage reply = client.recvFrom(TIMEOUT).orElseThrow();
                        assertArrayEquals(bytes("server"), reply.identity().bytes());
                        assertEquals("reply", reply.message().text());
                    } else {
                        assertEquals("reply", client.receive(TIMEOUT).orElseThrow().text());
                    }
                }
                assertArrayEquals(bytes("client"), retained.bytes());
                PeerInfo info = retained.peerInfo(); // First metadata access after socket close.
                assertFalse(info.authentication().orElseThrow().publicKey().isPresent());
                assertTrue(info.diagnostics().orElseThrow().endpoint().startsWith(endpoint.split(":")[0]));
                byte[] route = retained.bytes();
                route[0] = 0;
                assertArrayEquals(bytes("client"), retained.bytes());
            }
        }
    }

    @Test
    void asyncAndVirtualReceivesPreserveSnapshotAndCancellation() throws Exception {
        try (Context context = OMQ.context();
             Socket server = context.socket(SocketType.ROUTER);
             Socket client = context.socket(SocketType.DEALER).identity(bytes("async"));
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            String endpoint = server.bind("inproc://async-from");
            client.connect(endpoint);
            CompletableFuture<ReceivedMessage> pending = server.recvFromAsync(TIMEOUT);
            client.send("async");
            ReceivedMessage received = pending.get(5, TimeUnit.SECONDS);
            assertEquals("async", received.message().text());
            assertArrayEquals(bytes("async"), received.identity().bytes());
            CompletableFuture<ReceivedMessage> cancelled = server.recvFromAsync();
            assertTrue(cancelled.cancel(false));
            var virtual = executor.submit(() -> server.recvFrom(TIMEOUT).orElseThrow());
            client.send("virtual");
            ReceivedMessage second = virtual.get(5, TimeUnit.SECONDS);
            assertEquals("virtual", second.message().text());
            assertEquals(received.identity(), second.identity());
            assertThrows(IllegalStateException.class, client::tryRecvFrom);
        }
    }

    @Test
    void curveSnapshotsRetainDistinctKeysForSameRoute() {
        CurveKeypair serverKeys = OMQ.curveKeypair();
        List<Identity> identities = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        try (Context context = OMQ.context();
             Socket server = context.socket(SocketType.ROUTER).curveServer(serverKeys)) {
            String endpoint = server.bind("tcp://127.0.0.1:0");
            for (int i = 0; i < 2; i++) {
                CurveKeypair clientKeys = OMQ.curveKeypair();
                keys.add(clientKeys.publicKey());
                try (Socket client = context.socket(SocketType.DEALER).identity(bytes("same"))
                        .curveClient(clientKeys, serverKeys.publicKey())) {
                    client.connect(endpoint);
                    client.waitConnected(1, TIMEOUT);
                    client.send("hello");
                    identities.add(server.recvFrom(TIMEOUT).orElseThrow().identity());
                }
            }
        }
        assertNotEquals(identities.get(0), identities.get(1));
        assertArrayEquals(identities.get(0).bytes(), identities.get(1).bytes());
        for (int i = 0; i < identities.size(); i++) {
            PeerInfo info = identities.get(i).peerInfo();
            assertEquals(keys.get(i), info.publicKey().orElseThrow());
            PeerAuthentication auth = info.authentication().orElseThrow();
            assertEquals("CURVE", auth.mechanism().orElseThrow());
            assertEquals("CURVE", auth.publicKeyType().orElseThrow());
            assertEquals(32, auth.publicKey().orElseThrow().length);
            assertTrue(info.password().isEmpty());
            byte[] key = auth.publicKey().orElseThrow();
            key[0] ^= 1;
            assertNotEquals(key[0], auth.publicKey().orElseThrow()[0]);
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
