package io.omq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class TransportTest {
    @Test
    void dartRoundTrip() {
        assumeLinux();
        roundTrip("dart://127.0.0.1:0", SocketType.CHANNEL, SocketType.CHANNEL);
    }

    @Test
    void shmRoundTrip(@TempDir Path tempDir) {
        assumeLinux();
        Path socket = tempDir.resolve("endpoint.sock");
        roundTrip("shm://" + socket, SocketType.CHANNEL, SocketType.CHANNEL);
    }

    @Test
    void quicRoundTrip(@TempDir Path tempDir) throws Exception {
        Path root = repositoryRoot();
        Path target = targetDirectory(root);
        Process build = new ProcessBuilder(
                        "cargo", "build", "-p", "omq-tokio", "--features", "quic",
                        "--example", "quic_interop_peer")
                .directory(root.toFile())
                .inheritIO()
                .start();
        assertTrue(build.waitFor(3, TimeUnit.MINUTES), "QUIC peer build timed out");
        assertEquals(0, build.exitValue(), "QUIC peer build failed");

        String suffix = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")
                ? ".exe"
                : "";
        Path peer = target.resolve("debug/examples/quic_interop_peer" + suffix);
        Path certificates = Files.createDirectory(tempDir.resolve("certificates"));
        run(peer.toString(), "certs", certificates.toString());
        byte[] certificate = Files.readAllBytes(certificates.resolve("server.pem"));
        byte[] key = Files.readAllBytes(certificates.resolve("server.key"));

        try (Context context = OMQ.context();
             Socket rep = context.socket(
                     SocketType.REP,
                     SocketOptions.builder()
                             .quicServerCertificate(certificate, key)
                             .quicTrustSystem(false)
                             .build())) {
            String endpoint = rep.bind("quic://127.0.0.1:0");
            Process client = new ProcessBuilder(
                            peer.toString(), "connect-req", endpoint, certificates.toString())
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader output = new BufferedReader(
                    new InputStreamReader(client.getInputStream()))) {
                assertEquals("CONNECTED", output.readLine());
                for (Message expected : quicMessages()) {
                    Message received = rep.receive(Duration.ofSeconds(10)).orElseThrow();
                    assertMessageEquals(expected, received);
                    rep.send(received);
                }
                assertEquals("OK", output.readLine());
                assertTrue(client.waitFor(10, TimeUnit.SECONDS), "QUIC peer did not exit");
                assertEquals(0, client.exitValue());
            } finally {
                if (client.isAlive()) {
                    client.destroyForcibly();
                }
            }
        }
    }

    private static void roundTrip(
            String bindEndpoint, SocketType receiverType, SocketType senderType) {
        try (Context context = OMQ.context();
             Socket receiver = context.socket(receiverType);
             Socket sender = context.socket(senderType)) {
            String endpoint = receiver.bind(bindEndpoint);
            sender.connect(endpoint);
            assertEquals(1, sender.waitConnected(1, Duration.ofSeconds(10)));
            sender.send(Message.text("java transport smoke"));
            assertEquals("java transport smoke",
                    receiver.receive(Duration.ofSeconds(10)).orElseThrow().text());
        }
    }

    private static List<Message> quicMessages() {
        byte[] large = new byte[256 * 1024];
        for (int index = 0; index < large.length; index++) {
            large[index] = (byte) (index % 251);
        }
        byte[] mixed = new byte[4097];
        for (int index = 0; index < mixed.length; index++) {
            mixed[index] = (byte) (index % 251);
        }
        return List.of(
                Message.of(new byte[0]),
                Message.of(new byte[] {0, (byte) 0xff, (byte) 0x80}),
                Message.multipart("head".getBytes(), new byte[0], "tail".getBytes()),
                Message.of(large),
                Message.multipart(large, new byte[0], mixed));
    }

    private static void assertMessageEquals(Message expected, Message actual) {
        assertEquals(expected.partCount(), actual.partCount());
        for (int part = 0; part < expected.partCount(); part++) {
            assertArrayEquals(expected.part(part), actual.part(part));
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null && !Files.isDirectory(current.resolve("omq-tokio"))) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IllegalStateException("cannot find workspace root");
        }
        return current;
    }

    private static Path targetDirectory(Path root) {
        String configured = System.getenv("CARGO_TARGET_DIR");
        Path target = configured == null ? root.resolve("target") : Path.of(configured);
        return target.isAbsolute() ? target : root.resolve(target);
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).inheritIO().start();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "command timed out");
        assertEquals(0, process.exitValue());
    }

    private static void assumeLinux() {
        assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux"));
    }
}
