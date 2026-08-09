package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class OMQInteropTest {
    private static final String PYZMQ_CURVE_PULL = """
            import os, zmq

            ctx = zmq.Context()
            sock = ctx.socket(zmq.PULL)
            sock.curve_server = True
            sock.curve_publickey = os.environ["SRV_PUB"].encode()
            sock.curve_secretkey = os.environ["SRV_SEC"].encode()
            sock.bind("tcp://127.0.0.1:*")
            endpoint = sock.getsockopt(zmq.LAST_ENDPOINT)
            print(endpoint.decode() if isinstance(endpoint, bytes) else endpoint, flush=True)
            msg = sock.recv()
            print(msg.decode(), flush=True)
            sock.close(0)
            ctx.term()
            """;
    private static final String PYZMQ_CURVE_PUSH = """
            import os, time, zmq

            ctx = zmq.Context()
            sock = ctx.socket(zmq.PUSH)
            sock.curve_publickey = os.environ["CLI_PUB"].encode()
            sock.curve_secretkey = os.environ["CLI_SEC"].encode()
            sock.curve_serverkey = os.environ["SRV_PUB"].encode()
            sock.connect(os.environ["ENDPOINT"])
            sock.send(os.environ["PAYLOAD"].encode())
            time.sleep(0.1)
            sock.close(1000)
            ctx.term()
            """;

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

    @Test
    void javaCurveClientTalksToRustOmqPeer() throws Exception {
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        Process process = startPeer(
                Map.of(
                        "OMQ_CURVE_SERVER_PUBLIC", serverKeypair.publicKey(),
                        "OMQ_CURVE_SERVER_SECRET", serverKeypair.secretKey()),
                "curve-pull",
                "tcp://127.0.0.1:0");
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
             Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)
                     .curveClient(clientKeypair, serverKeypair.publicKey())) {
            String endpoint = output.readLine();
            assertNotNull(endpoint);

            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("hello-rust-curve");

            assertEquals("hello-rust-curve", output.readLine());
            assertProcessSuccess(process);
        } finally {
            destroy(process);
        }
    }

    @Test
    void rustOmqCurveClientTalksToJavaServer() throws Exception {
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        Process process = null;
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).curveServer(serverKeypair)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            process = startPeer(
                    Map.of(
                            "OMQ_CURVE_CLIENT_PUBLIC", clientKeypair.publicKey(),
                            "OMQ_CURVE_CLIENT_SECRET", clientKeypair.secretKey(),
                            "OMQ_CURVE_SERVER_PUBLIC", serverKeypair.publicKey()),
                    "curve-push",
                    endpoint,
                    "hello-java-curve");

            assertEquals("hello-java-curve", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertProcessSuccess(process);
        } finally {
            destroy(process);
        }
    }

    @Test
    void javaCurveClientTalksToPyzmqPull() throws Exception {
        assumePyzmqCurve();
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        Process process = startPython(
                Map.of(
                        "SRV_PUB", serverKeypair.publicKey(),
                        "SRV_SEC", serverKeypair.secretKey()),
                PYZMQ_CURVE_PULL);
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
             Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)
                     .curveClient(clientKeypair, serverKeypair.publicKey())) {
            String endpoint = output.readLine();
            assertNotNull(endpoint);

            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("hello-pyzmq-curve");

            assertEquals("hello-pyzmq-curve", output.readLine());
            assertProcessSuccess(process);
        } finally {
            destroy(process);
        }
    }

    @Test
    void pyzmqCurvePushTalksToJavaServer() throws Exception {
        assumePyzmqCurve();
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        Process process = null;
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).curveServer(serverKeypair)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            process = startPython(
                    Map.of(
                            "ENDPOINT", endpoint,
                            "PAYLOAD", "hello-java-from-pyzmq-curve",
                            "CLI_PUB", clientKeypair.publicKey(),
                            "CLI_SEC", clientKeypair.secretKey(),
                            "SRV_PUB", serverKeypair.publicKey()),
                    PYZMQ_CURVE_PUSH);

            assertEquals("hello-java-from-pyzmq-curve", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
            assertProcessSuccess(process);
        } finally {
            destroy(process);
        }
    }

    private static Process startPeer(String... args) throws IOException {
        return startPeer(Map.of(), args);
    }

    private static Process startPeer(Map<String, String> environment, String... args) throws IOException {
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
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().putAll(environment);
        return builder.start();
    }

    private static Process startPython(Map<String, String> environment, String script) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(python(), "-c", script)
                .redirectErrorStream(true);
        builder.environment().putAll(environment);
        return builder.start();
    }

    private static void assumePyzmqCurve() throws IOException, InterruptedException {
        Process process = startPython(Map.of(), "import sys, zmq; sys.exit(0 if zmq.has('curve') else 1)");
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        if (!exited) {
            destroy(process);
            requireOrSkip(false, "python3 + pyzmq CURVE probe timed out");
        }
        requireOrSkip(process.exitValue() == 0, "python3 + pyzmq with CURVE is not available");
    }

    private static void requireOrSkip(boolean condition, String message) {
        if (condition) {
            return;
        }
        if ("1".equals(System.getenv("OMQ_INTEROP_REQUIRED"))) {
            fail(message);
        }
        assumeTrue(false, message);
    }

    private static String python() {
        String configured = System.getenv("OMQ_PYTHON3");
        return configured == null || configured.isBlank() ? "python3" : configured;
    }

    private static void assertProcessSuccess(Process process) throws InterruptedException {
        assertNotNull(process);
        assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue());
    }

    private static void destroy(Process process) {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
        }
    }
}
