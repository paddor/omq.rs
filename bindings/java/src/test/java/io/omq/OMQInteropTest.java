package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class OMQInteropTest {
    @Test
    void javaBindingTalksToRustOmqPeer() throws Exception {
        Process process = startPeer("pull", "tcp://127.0.0.1:0");
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
             Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = output.readLine();
            assertNotNull(endpoint);

            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("hello-rust");

            assertEquals("hello-rust", output.readLine());
            assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            assertEquals(0, process.exitValue());
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    private static Process startPeer(String... args) throws IOException {
        String configured = System.getProperty("omq.java.peer");
        Path peer = Path.of(configured == null || configured.isBlank()
                ? "native/target/debug/omq-java-peer"
                : configured);
        if (System.getProperty("os.name").toLowerCase().contains("win")
                && !peer.toString().endsWith(".exe")) {
            peer = Path.of(peer + ".exe");
        }
        if (!Files.isExecutable(peer)) {
            throw new IOException("Rust peer is not executable: " + peer.toAbsolutePath());
        }

        String[] command = new String[args.length + 1];
        command[0] = peer.toAbsolutePath().toString();
        System.arraycopy(args, 0, command, 1, args.length);
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
    }
}
