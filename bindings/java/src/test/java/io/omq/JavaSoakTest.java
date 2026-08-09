package io.omq;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

final class JavaSoakTest {
    private static final Duration RECV_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final byte[] ZSTD_DICT = hex(
            "37a430ecbeaadd5c811120841042664644444444244902002114c418638c21841042"
                    + "082184104208214444444444444444240900005110638c31c618630c21c418636666"
                    + "864692040080000000c000000000010000");

    @Test
    void mixedWorkloadsKeepMemoryBounded() throws Exception {
        assumeTrue(soakEnabled(), "set OMQ_JAVA_SOAK=1 to run Java soak");

        long durationSeconds = longConfig("omq.java.soak.durationSeconds",
                "OMQ_JAVA_SOAK_DURATION_SECS", 60);
        int workers = (int) longConfig("omq.java.soak.workers",
                "OMQ_JAVA_SOAK_WORKERS", Runtime.getRuntime().availableProcessors());
        long heapGrowthLimit = mb(longConfig("omq.java.soak.maxHeapGrowthMb",
                "OMQ_JAVA_SOAK_MAX_HEAP_GROWTH_MB", 384));
        long rssGrowthLimit = mb(longConfig("omq.java.soak.maxRssGrowthMb",
                "OMQ_JAVA_SOAK_MAX_RSS_GROWTH_MB", 768));

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        LongAdder tcpMessages = new LongAdder();
        LongAdder curveMessages = new LongAdder();
        LongAdder compressionMessages = new LongAdder();
        LongAdder inprocMessages = new LongAdder();
        CompletableFuture<String> tcpEndpoint = new CompletableFuture<>();
        CompletableFuture<String> curveEndpoint = new CompletableFuture<>();
        CurveKeypair curveServer = OMQ.curveKeypair();
        CurveKeypair curveClient = OMQ.curveKeypair();

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            tasks.add(pool.submit(() -> tcpPull(stop, failure, tcpEndpoint, tcpMessages)));
            tasks.add(pool.submit(() -> curvePull(
                    stop, failure, curveEndpoint, curveServer, curveClient, curveMessages)));
            tasks.add(pool.submit(() -> compressionPair(
                    stop, failure, "lz4+tcp://127.0.0.1:0", null, compressionMessages)));
            tasks.add(pool.submit(() -> compressionPair(
                    stop, failure, "zstd+tcp://127.0.0.1:0", ZSTD_DICT, compressionMessages)));
            tasks.add(pool.submit(() -> inprocReqRep(stop, failure, inprocMessages)));

            String tcp = tcpEndpoint.get(5, TimeUnit.SECONDS);
            String curve = curveEndpoint.get(5, TimeUnit.SECONDS);
            int churnWorkers = Math.max(1, workers - tasks.size());
            for (int i = 0; i < churnWorkers; i++) {
                int workerId = i;
                tasks.add(pool.submit(() -> tcpChurnPush(stop, failure, tcp, workerId, tcpMessages)));
                tasks.add(pool.submit(() -> curveChurnPush(
                        stop, failure, curve, curveClient, curveServer.publicKey(), workerId)));
            }

            long start = System.nanoTime();
            long deadline = start + Duration.ofSeconds(durationSeconds).toNanos();
            long baselineHeap = usedHeapBytes();
            long baselineRss = rssBytes().orElse(usedHeapBytes());
            long nextReport = start;
            while (System.nanoTime() < deadline && failure.get() == null) {
                Thread.sleep(1_000);
                long now = System.nanoTime();
                if (now >= nextReport) {
                    long heap = usedHeapBytes();
                    long rss = rssBytes().orElse(heap);
                    System.out.printf(
                            "[java-soak] %.0fs tcp=%d curve=%d compression=%d inproc=%d heap=%dMB rss=%dMB%n",
                            (now - start) / 1_000_000_000.0,
                            tcpMessages.sum(),
                            curveMessages.sum(),
                            compressionMessages.sum(),
                            inprocMessages.sum(),
                            heap / 1_048_576,
                            rss / 1_048_576);
                    if (now - start > Duration.ofSeconds(20).toNanos()) {
                        assertTrue(heap <= baselineHeap + heapGrowthLimit,
                                "heap growth exceeded limit");
                        assertTrue(rss <= baselineRss + rssGrowthLimit,
                                "RSS growth exceeded limit");
                    }
                    nextReport = now + Duration.ofSeconds(10).toNanos();
                }
            }
        } finally {
            stop.set(true);
            pool.shutdown();
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        }

        for (Future<?> task : tasks) {
            task.get(1, TimeUnit.SECONDS);
        }
        if (failure.get() != null) {
            fail(failure.get());
        }
        assertTrue(tcpMessages.sum() > 0, "TCP churn made no progress");
        assertTrue(curveMessages.sum() > 0, "CURVE churn made no progress");
        assertTrue(compressionMessages.sum() > 0, "compression loop made no progress");
        assertTrue(inprocMessages.sum() > 0, "inproc loop made no progress");
    }

    private static void tcpPull(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            CompletableFuture<String> endpoint,
            LongAdder received) {
        runWorker(stop, failure, () -> {
            try (Context context = OMQ.context(1);
                 Socket pull = context.socket(SocketType.PULL)) {
                endpoint.complete(pull.bind("tcp://127.0.0.1:0"));
                while (!stop.get()) {
                    if (pull.receiveBytes(RECV_TIMEOUT).isPresent()) {
                        received.increment();
                    }
                }
            }
        });
    }

    private static void tcpChurnPush(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            String endpoint,
            int workerId,
            LongAdder sent) {
        runWorker(stop, failure, () -> {
            byte[] payload = payload("tcp", workerId, 256);
            try (Context context = OMQ.context(1)) {
                while (!stop.get()) {
                    try (Socket push = context.socket(SocketType.PUSH)) {
                        push.connect(endpoint);
                        if (!waitConnectedOrRetry(push, stop)) {
                            continue;
                        }
                        for (int i = 0; i < 32 && !stop.get(); i++) {
                            payload[0] = (byte) i;
                            push.send(payload);
                            sent.increment();
                        }
                    }
                }
            }
        });
    }

    private static void curvePull(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            CompletableFuture<String> endpoint,
            CurveKeypair serverKeypair,
            CurveKeypair allowedClient,
            LongAdder received) {
        runWorker(stop, failure, () -> {
            try (Context context = OMQ.context(1);
                 Socket pull = context.socket(SocketType.PULL).curveServer(
                         serverKeypair,
                         peer -> allowedClient.publicKey().equals(peer.publicKey().orElse("")))) {
                endpoint.complete(pull.bind("tcp://127.0.0.1:0"));
                while (!stop.get()) {
                    if (pull.receiveBytes(RECV_TIMEOUT).isPresent()) {
                        received.increment();
                    }
                }
            }
        });
    }

    private static void curveChurnPush(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            String endpoint,
            CurveKeypair clientKeypair,
            String serverPublicKey,
            int workerId) {
        runWorker(stop, failure, () -> {
            byte[] payload = payload("curve", workerId, 192);
            try (Context context = OMQ.context(1)) {
                while (!stop.get()) {
                    try (Socket push = context.socket(SocketType.PUSH)
                            .curveClient(clientKeypair, serverPublicKey)) {
                        push.connect(endpoint);
                        if (!waitConnectedOrRetry(push, stop)) {
                            continue;
                        }
                        for (int i = 0; i < 8 && !stop.get(); i++) {
                            payload[0] = (byte) i;
                            push.send(payload);
                        }
                    }
                }
            }
        });
    }

    private static void compressionPair(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            String bindEndpoint,
            byte[] dict,
            LongAdder received) {
        runWorker(stop, failure, () -> {
            try (Context context = OMQ.context(1);
                 Socket pull = compressionSocket(context.socket(SocketType.PULL), dict);
                 Socket push = compressionSocket(context.socket(SocketType.PUSH), dict)) {
                String endpoint = pull.bind(bindEndpoint);
                push.connect(endpoint);
                push.waitConnected(1, CONNECT_TIMEOUT);
                int seq = 0;
                while (!stop.get()) {
                    byte[] payload = payload("compression-" + bindEndpoint, seq++, 1_024);
                    push.send(payload);
                    byte[] got = pull.receiveBytes(Duration.ofSeconds(5)).orElseThrow();
                    if (got.length != payload.length || got[0] != payload[0]) {
                        throw new AssertionError("compression payload mismatch");
                    }
                    received.increment();
                }
            }
        });
    }

    private static Socket compressionSocket(Socket socket, byte[] dict) {
        socket.compressionAutoTrain(true)
                .compressionDictCapacity(4_096)
                .compressionThreshold(32)
                .compressionLevel(1);
        if (dict != null) {
            socket.compressionDict(dict);
        }
        return socket;
    }

    private static void inprocReqRep(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            LongAdder cycles) {
        runWorker(stop, failure, () -> {
            try (Context context = OMQ.context(1);
                 Socket rep = context.socket(SocketType.REP);
                 Socket req = context.socket(SocketType.REQ)) {
                String endpoint = rep.bind(TestSupport.inprocEndpoint("java-soak-req-rep"));
                req.connect(endpoint);
                int seq = 0;
                while (!stop.get()) {
                    String payload = "req-" + seq++;
                    req.send(payload);
                    if (!payload.equals(rep.receive(Duration.ofSeconds(5)).orElseThrow().text())) {
                        throw new AssertionError("inproc request mismatch");
                    }
                    rep.send("ok");
                    if (!"ok".equals(req.receive(Duration.ofSeconds(5)).orElseThrow().text())) {
                        throw new AssertionError("inproc reply mismatch");
                    }
                    cycles.increment();
                }
            }
        });
    }

    private static void runWorker(
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            ThrowingRunnable body) {
        try {
            body.run();
        } catch (Throwable error) {
            failure.compareAndSet(null, error);
            stop.set(true);
        }
    }

    private static boolean waitConnectedOrRetry(Socket socket, AtomicBoolean stop) {
        try {
            socket.waitConnected(1, CONNECT_TIMEOUT);
            return true;
        } catch (TimeoutException error) {
            return false;
        } catch (ClosedException error) {
            if (stop.get()) {
                return false;
            }
            throw error;
        }
    }

    private static boolean soakEnabled() {
        return Boolean.getBoolean("omq.java.soak")
                || "1".equals(System.getenv("OMQ_JAVA_SOAK"));
    }

    private static long longConfig(String property, String env, long fallback) {
        String propertyValue = System.getProperty(property);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return Long.parseLong(propertyValue);
        }
        String envValue = System.getenv(env);
        if (envValue != null && !envValue.isBlank()) {
            return Long.parseLong(envValue);
        }
        return fallback;
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static Optional<Long> rssBytes() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    String digits = line.replaceAll("[^0-9]", "");
                    return Optional.of(Long.parseLong(digits) * 1024);
                }
            }
        } catch (IOException | NumberFormatException ignored) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private static byte[] payload(String kind, int seq, int size) {
        String head = "{\"kind\":\"" + kind + "\",\"seq\":" + seq + ",\"pad\":\"";
        String tail = "\"}";
        return (head + "x".repeat(size - head.length() - tail.length()) + tail)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static long mb(long value) {
        return value * 1_048_576L;
    }

    private static byte[] hex(String input) {
        byte[] out = new byte[input.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(input.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
