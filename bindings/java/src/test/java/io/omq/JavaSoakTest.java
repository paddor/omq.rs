package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class JavaSoakTest {
    private static final Duration RECV_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REPORT_INTERVAL = Duration.ofSeconds(10);
    private static final Duration RESOURCE_CHECK_DELAY = Duration.ofSeconds(20);
    private static final Duration RESOURCE_WARMUP = Duration.ofMinutes(10);
    private static final Duration RESOURCE_WINDOW = Duration.ofMinutes(5);
    private static final int RESOURCE_MIN_SAMPLES = 12;
    private static final long BYTES_PER_KIB = 1024L;
    private static final long BYTES_PER_MIB = 1_048_576L;
    private static final byte[] ZSTD_DICT = hex(
            "37a430ecbeaadd5c811120841042664644444444244902002114c418638c21841042"
                    + "082184104208214444444444444444240900005110638c31c618630c21c418636666"
                    + "864692040080000000c000000000010000");

    @Test
    void mixedWorkloadsKeepMemoryBounded() throws Exception {
        assumeTrue(soakEnabled(), "set OMQ_JAVA_SOAK=1 to run Java soak");

        long durationSeconds = longConfig("omq.java.soak.durationSeconds",
                "OMQ_JAVA_SOAK_DURATION_SECS", 60);
        durationSeconds = Math.max(5, durationSeconds);
        int workers = Math.max(1, (int) longConfig("omq.java.soak.workers",
                "OMQ_JAVA_SOAK_WORKERS", Runtime.getRuntime().availableProcessors()));

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
        SoakResources baseline = readSoakResources();
        SoakResourceLimits limits = readSoakResourceLimits();
        Instant started = Instant.now();
        long start = System.nanoTime();
        SoakResourceTracker resources = new SoakResourceTracker(started, baseline, limits);
        int baseWorkloads = 5;
        int churnWorkers = Math.max(1, workers - baseWorkloads);
        int poolSize = Math.max(workers, baseWorkloads + churnWorkers * 2);

        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
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
            for (int i = 0; i < churnWorkers; i++) {
                int workerId = i;
                tasks.add(pool.submit(() -> tcpChurnPush(stop, failure, tcp, workerId, tcpMessages)));
                tasks.add(pool.submit(() -> curveChurnPush(
                        stop, failure, curve, curveClient, curveServer.publicKey(), workerId)));
            }

            long deadline = start + Duration.ofSeconds(durationSeconds).toNanos();
            long nextReport = start;
            while (System.nanoTime() < deadline && failure.get() == null) {
                Thread.sleep(1_000);
                long now = System.nanoTime();
                if (now >= nextReport) {
                    Duration elapsed = Duration.ofNanos(now - start);
                    SoakResources current = resources.sample(elapsed);
                    System.out.printf(
                            "[java-soak] %.0fs tcp=%d curve=%d compression=%d inproc=%d heap=%dMB rss=%dMB fds=%d%n",
                            elapsed.toMillis() / 1_000.0,
                            tcpMessages.sum(),
                            curveMessages.sum(),
                            compressionMessages.sum(),
                            inprocMessages.sum(),
                            current.heapBytes() / BYTES_PER_MIB,
                            current.rssBytes() / BYTES_PER_MIB,
                            current.fdCount());
                    nextReport = now + REPORT_INTERVAL.toNanos();
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
        System.gc();
        Thread.sleep(200);
        System.gc();
        resources.assertFinal(Duration.ofNanos(System.nanoTime() - start));
        if (failure.get() != null) {
            fail(failure.get());
        }
        assertTrue(tcpMessages.sum() > 0, "TCP churn made no progress");
        assertTrue(curveMessages.sum() > 0, "CURVE churn made no progress");
        assertTrue(compressionMessages.sum() > 0, "compression loop made no progress");
        assertTrue(inprocMessages.sum() > 0, "inproc loop made no progress");
    }

    @Test
    void resourceSlopePerSecond() {
        Instant start = Instant.EPOCH;
        OptionalDouble slope = slopePerSecond(List.of(
                new SoakResourceSample(start, 10),
                new SoakResourceSample(start.plusSeconds(1), 20),
                new SoakResourceSample(start.plusSeconds(2), 30)));

        assertTrue(slope.isPresent());
        assertEquals(10.0, slope.getAsDouble(), 0.001);
    }

    @Test
    void resourceLiveGrowthDetectsSustainedRssGrowth() {
        Instant start = Instant.EPOCH;
        List<SoakResourceSample> samples = new ArrayList<>();
        for (int seconds = 0; seconds <= 1_200; seconds += 20) {
            samples.add(new SoakResourceSample(start.plusSeconds(seconds),
                    seconds * BYTES_PER_MIB));
        }

        Optional<String> error = liveGrowthError("RSS", start, samples, 128, 8 * BYTES_PER_MIB);

        assertTrue(error.isPresent());
    }

    @Test
    void resourceLiveGrowthIgnoresPlateau() {
        Instant start = Instant.EPOCH;
        List<SoakResourceSample> samples = new ArrayList<>();
        for (int seconds = 0; seconds <= 1_200; seconds += 20) {
            long value = Math.min(seconds, 600) * BYTES_PER_MIB;
            samples.add(new SoakResourceSample(start.plusSeconds(seconds), value));
        }

        Optional<String> error = liveGrowthError("RSS", start, samples, 128, 8 * BYTES_PER_MIB);

        assertTrue(error.isEmpty(), error.orElse(""));
    }

    @Test
    void resourceLiveFdGrowthDetectsSustainedGrowth() {
        Instant start = Instant.EPOCH;
        List<SoakResourceSample> samples = new ArrayList<>();
        for (int seconds = 0; seconds <= 1_200; seconds += 20) {
            samples.add(new SoakResourceSample(start.plusSeconds(seconds),
                    10 + seconds / 2L));
        }

        Optional<String> error = liveFdGrowthError(start, samples, 0.05, 32);

        assertTrue(error.isPresent());
    }

    @Test
    void resourceLimitBaselineStartsAfterCheckDelay() {
        SoakResources warm = new SoakResources(64 * BYTES_PER_MIB, 700 * BYTES_PER_MIB, 120);
        SoakResources later = new SoakResources(65 * BYTES_PER_MIB, 701 * BYTES_PER_MIB, 120);

        assertEquals(null, checkedResourceBaseline(Duration.ofSeconds(10), warm, null));
        assertEquals(warm, checkedResourceBaseline(RESOURCE_CHECK_DELAY, warm, null));
        assertEquals(warm, checkedResourceBaseline(Duration.ofSeconds(40), later, warm));
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
            try {
                return Long.parseLong(propertyValue);
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        String envValue = System.getenv(env);
        if (envValue != null && !envValue.isBlank()) {
            try {
                return Long.parseLong(envValue);
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private static long nonNegativeLongConfig(String property, String env, long fallback) {
        long value = longConfig(property, env, fallback);
        if (value < 0) {
            return fallback;
        }
        return value;
    }

    private static long mibConfig(String property, String env, long fallback) {
        return nonNegativeLongConfig(property, env, fallback) * BYTES_PER_MIB;
    }

    private static double doubleConfig(String property, String env, double fallback) {
        String propertyValue = System.getProperty(property);
        if (propertyValue != null && !propertyValue.isBlank()) {
            try {
                double parsed = Double.parseDouble(propertyValue);
                return parsed > 0 ? parsed : fallback;
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        String envValue = System.getenv(env);
        if (envValue != null && !envValue.isBlank()) {
            try {
                double parsed = Double.parseDouble(envValue);
                return parsed > 0 ? parsed : fallback;
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private static SoakResourceLimits readSoakResourceLimits() {
        // Java heap/RSS samples are pre-GC and include allocator high-water
        // marks. Keep absolute caps wide enough for normal cycles and use the
        // slope gates for sustained growth.
        return new SoakResourceLimits(
                mibConfig("omq.java.soak.maxHeapGrowthMb",
                        "OMQ_JAVA_SOAK_MAX_HEAP_GROWTH_MB", 768),
                mibConfig("omq.java.soak.maxRssGrowthMb",
                        "OMQ_JAVA_SOAK_MAX_RSS_GROWTH_MB", 768),
                nonNegativeLongConfig("omq.java.soak.maxFdGrowth",
                        "OMQ_JAVA_SOAK_MAX_FD_GROWTH", 128),
                mibConfig("omq.java.soak.maxFinalHeapGrowthMb",
                        "OMQ_JAVA_SOAK_MAX_FINAL_HEAP_GROWTH_MB", 128),
                nonNegativeLongConfig("omq.java.soak.maxFinalFdGrowth",
                        "OMQ_JAVA_SOAK_MAX_FINAL_FD_GROWTH", 16),
                doubleConfig("omq.java.soak.rssSlopeLimitKibPerSecond",
                        "OMQ_JAVA_SOAK_RSS_SLOPE_LIMIT_KIB_S", 1_024),
                doubleConfig("omq.java.soak.fdSlopeLimitPerSecond",
                        "OMQ_JAVA_SOAK_FD_SLOPE_LIMIT_PER_SEC", 0.05),
                mibConfig("omq.java.soak.rssSlopeMinGrowthMb",
                        "OMQ_JAVA_SOAK_RSS_SLOPE_MIN_GROWTH_MB", 128),
                nonNegativeLongConfig("omq.java.soak.fdSlopeMinGrowth",
                        "OMQ_JAVA_SOAK_FD_SLOPE_MIN_GROWTH", 32));
    }

    private static SoakResources readSoakResources() {
        long heap = usedHeapBytes();
        return new SoakResources(heap, rssBytes().orElse(heap), fdCount());
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

    private static long fdCount() {
        try (Stream<Path> entries = Files.list(Path.of("/proc/self/fd"))) {
            return entries.count();
        } catch (IOException | SecurityException ignored) {
            return 0;
        }
    }

    private static Optional<String> liveGrowthError(
            String metric,
            Instant started,
            List<SoakResourceSample> samples,
            double slopeLimitKibPerSecond,
            long minGrowthBytes) {
        Optional<List<SoakResourceSample>> window = liveGrowthWindow(started, samples);
        if (window.isEmpty()) {
            return Optional.empty();
        }
        List<SoakResourceSample> values = window.orElseThrow();
        long current = values.getLast().value();
        long growth = saturatingSub(current, values.getFirst().value());
        if (growth < minGrowthBytes) {
            return Optional.empty();
        }
        OptionalDouble slope = slopePerSecond(values);
        if (slope.isEmpty()) {
            return Optional.empty();
        }
        double slopeKibPerSecond = slope.getAsDouble() / BYTES_PER_KIB;
        if (slopeKibPerSecond <= slopeLimitKibPerSecond) {
            return Optional.empty();
        }
        return Optional.of(String.format(
                "live %s growth detected: slope %.1f KiB/s over %.0fs, growth %.1f MiB, current %.1f MiB, limit %.1f KiB/s",
                metric,
                slopeKibPerSecond,
                RESOURCE_WINDOW.toSeconds() * 1.0,
                growth / (double) BYTES_PER_MIB,
                current / (double) BYTES_PER_MIB,
                slopeLimitKibPerSecond));
    }

    private static Optional<String> liveFdGrowthError(
            Instant started,
            List<SoakResourceSample> samples,
            double slopeLimitPerSecond,
            long minGrowth) {
        Optional<List<SoakResourceSample>> window = liveGrowthWindow(started, samples);
        if (window.isEmpty()) {
            return Optional.empty();
        }
        List<SoakResourceSample> values = window.orElseThrow();
        long growth = saturatingSub(values.getLast().value(), values.getFirst().value());
        if (growth < minGrowth) {
            return Optional.empty();
        }
        OptionalDouble slope = slopePerSecond(values);
        if (slope.isEmpty() || slope.getAsDouble() <= slopeLimitPerSecond) {
            return Optional.empty();
        }
        SampleRange range = sampleRange(values);
        return Optional.of(String.format(
                "live FD growth detected: slope %.4f FDs/s over %.0fs, range %d..%d, limit %.4f FDs/s",
                slope.getAsDouble(),
                RESOURCE_WINDOW.toSeconds() * 1.0,
                range.min(),
                range.max(),
                slopeLimitPerSecond));
    }

    private static Optional<List<SoakResourceSample>> liveGrowthWindow(
            Instant started,
            List<SoakResourceSample> samples) {
        if (samples.size() < RESOURCE_MIN_SAMPLES) {
            return Optional.empty();
        }
        Instant now = samples.getLast().at();
        if (Duration.between(started, now).compareTo(RESOURCE_WARMUP.plus(RESOURCE_WINDOW)) < 0) {
            return Optional.empty();
        }
        Instant windowStart = now.minus(RESOURCE_WINDOW);
        int first = 0;
        for (int i = 0; i < samples.size(); i++) {
            if (!samples.get(i).at().isBefore(windowStart)) {
                first = i;
                break;
            }
        }
        List<SoakResourceSample> window = samples.subList(first, samples.size());
        if (window.size() < RESOURCE_MIN_SAMPLES) {
            return Optional.empty();
        }
        return Optional.of(window);
    }

    private static OptionalDouble slopePerSecond(List<SoakResourceSample> samples) {
        if (samples.size() < 2) {
            return OptionalDouble.empty();
        }
        Instant first = samples.getFirst().at();
        double elapsed = Duration.between(first, samples.getLast().at()).toNanos()
                / 1_000_000_000.0;
        if (elapsed < 1) {
            return OptionalDouble.empty();
        }
        double n = samples.size();
        double sumX = 0;
        double sumY = 0;
        double sumXy = 0;
        double sumXx = 0;
        for (SoakResourceSample sample : samples) {
            double x = Duration.between(first, sample.at()).toNanos() / 1_000_000_000.0;
            double y = sample.value();
            sumX += x;
            sumY += y;
            sumXy += x * y;
            sumXx += x * x;
        }
        double denominator = n * sumXx - sumX * sumX;
        if (denominator == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of((n * sumXy - sumX * sumY) / denominator);
    }

    private static SampleRange sampleRange(List<SoakResourceSample> samples) {
        if (samples.isEmpty()) {
            return new SampleRange(0, 0);
        }
        long min = samples.getFirst().value();
        long max = samples.getFirst().value();
        for (SoakResourceSample sample : samples) {
            min = Math.min(min, sample.value());
            max = Math.max(max, sample.value());
        }
        return new SampleRange(min, max);
    }

    private static long saturatingSub(long value, long other) {
        if (value < other) {
            return 0;
        }
        return value - other;
    }

    private static SoakResources checkedResourceBaseline(
            Duration elapsed,
            SoakResources current,
            SoakResources baseline) {
        if (baseline != null || elapsed.compareTo(RESOURCE_CHECK_DELAY) < 0) {
            return baseline;
        }
        return current;
    }

    private static void assertSoakResources(
            SoakResources baseline,
            SoakResources current,
            SoakResourceLimits limits) {
        if (current.heapBytes() > baseline.heapBytes() + limits.heapGrowthBytes()) {
            fail(String.format("heap growth exceeded limit: baseline=%d current=%d limit=%d",
                    baseline.heapBytes(), current.heapBytes(), limits.heapGrowthBytes()));
        }
        if (current.rssBytes() > baseline.rssBytes() + limits.rssGrowthBytes()) {
            fail(String.format("RSS growth exceeded limit: baseline=%d current=%d limit=%d",
                    baseline.rssBytes(), current.rssBytes(), limits.rssGrowthBytes()));
        }
        if (current.fdCount() > baseline.fdCount() + limits.fdGrowth()) {
            fail(String.format("FD growth exceeded limit: baseline=%d current=%d limit=%d",
                    baseline.fdCount(), current.fdCount(), limits.fdGrowth()));
        }
    }

    private static byte[] payload(String kind, int seq, int size) {
        String head = "{\"kind\":\"" + kind + "\",\"seq\":" + seq + ",\"pad\":\"";
        String tail = "\"}";
        return (head + "x".repeat(size - head.length() - tail.length()) + tail)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] hex(String input) {
        byte[] out = new byte[input.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(input.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private record SoakResourceLimits(
            long heapGrowthBytes,
            long rssGrowthBytes,
            long fdGrowth,
            long finalHeapGrowthBytes,
            long finalFdGrowth,
            double rssSlopeKibPerSecond,
            double fdSlopePerSecond,
            long rssSlopeMinGrowth,
            long fdSlopeMinGrowth) {
    }

    private record SoakResources(long heapBytes, long rssBytes, long fdCount) {
    }

    private record SoakResourceSample(Instant at, long value) {
    }

    private record SampleRange(long min, long max) {
    }

    private static final class SoakResourceTracker {
        private final Instant started;
        private final SoakResources startupBaseline;
        private final SoakResourceLimits limits;
        private final List<SoakResourceSample> heap = new ArrayList<>();
        private final List<SoakResourceSample> rss = new ArrayList<>();
        private final List<SoakResourceSample> fds = new ArrayList<>();
        private SoakResources checkedBaseline;

        private SoakResourceTracker(
                Instant started,
                SoakResources baseline,
                SoakResourceLimits limits) {
            this.started = started;
            this.startupBaseline = baseline;
            this.limits = limits;
            appendSample(started, baseline);
        }

        private SoakResources sample(Duration elapsed) {
            SoakResources current = readSoakResources();
            appendSample(Instant.now(), current);
            checkedBaseline = checkedResourceBaseline(elapsed, current, checkedBaseline);
            if (checkedBaseline != null) {
                assertSoakResources(checkedBaseline, current, limits);
            }
            liveGrowthError("RSS", started, rss,
                            limits.rssSlopeKibPerSecond(), limits.rssSlopeMinGrowth())
                    .ifPresent(message -> fail(message));
            liveFdGrowthError(started, fds,
                            limits.fdSlopePerSecond(), limits.fdSlopeMinGrowth())
                    .ifPresent(message -> fail(message));
            return current;
        }

        private void assertFinal(Duration elapsed) {
            SoakResources current = sample(elapsed);
            System.out.printf(
                    "[java-soak] final resources heap=%dMB rss=%dMB fds=%d%n",
                    current.heapBytes() / BYTES_PER_MIB,
                    current.rssBytes() / BYTES_PER_MIB,
                    current.fdCount());
            logSlope("heap", heap, BYTES_PER_KIB, "KiB");
            logSlope("RSS", rss, BYTES_PER_KIB, "KiB");
            logSlope("FD", fds, 1, "FDs");
            if (current.heapBytes() > startupBaseline.heapBytes()
                    + limits.finalHeapGrowthBytes()) {
                fail(String.format(
                        "final heap growth exceeded limit: baseline=%d current=%d limit=%d",
                        startupBaseline.heapBytes(),
                        current.heapBytes(),
                        limits.finalHeapGrowthBytes()));
            }
            if (current.fdCount() > startupBaseline.fdCount() + limits.finalFdGrowth()) {
                fail(String.format(
                        "final FD growth exceeded limit: baseline=%d current=%d limit=%d",
                        startupBaseline.fdCount(), current.fdCount(), limits.finalFdGrowth()));
            }
        }

        private void appendSample(Instant at, SoakResources resources) {
            heap.add(new SoakResourceSample(at, resources.heapBytes()));
            rss.add(new SoakResourceSample(at, resources.rssBytes()));
            fds.add(new SoakResourceSample(at, resources.fdCount()));
        }

        private static void logSlope(
                String metric,
                List<SoakResourceSample> samples,
                double scale,
                String unit) {
            if (samples.size() < 2) {
                return;
            }
            int warmup = samples.size() / 5;
            List<SoakResourceSample> postWarmup = samples.subList(warmup, samples.size());
            OptionalDouble slope = slopePerSecond(postWarmup);
            if (slope.isEmpty()) {
                return;
            }
            SampleRange range = sampleRange(postWarmup);
            System.out.printf(
                    "[java-soak] %s range %.1f..%.1f %s slope %.3f %s/s%n",
                    metric,
                    range.min() / scale,
                    range.max() / scale,
                    unit,
                    slope.getAsDouble() / scale,
                    unit);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
