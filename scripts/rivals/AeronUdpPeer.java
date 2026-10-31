import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** One Aeron client and one embedded Media Driver per process. */
public final class AeronUdpPeer {
    private static final long TIMEOUT_NS = 180_000_000_000L;
    // Long enough for the JIT to finish compiling the round-trip path.
    private static final int WARMUP_RTT = 200_000;
    private static final int SAMPLES = 10_000;
    private static final long WARMUP_NS = 3_000_000_000L;
    private static final long MEASURE_NS = 3_000_000_000L;

    private static void check(long started, String phase) {
        if (System.nanoTime() - started > TIMEOUT_NS) {
            throw new IllegalStateException("timeout " + phase);
        }
    }

    private static void awaitConnected(Publication outgoing, Subscription incoming) {
        long started = System.nanoTime();
        while (!outgoing.isConnected() || !incoming.isConnected()) {
            check(started, "connection");
            Thread.onSpinWait();
        }
    }

    private static void offer(Publication publication, UnsafeBuffer buffer, int size, String phase) {
        long started = System.nanoTime();
        while (publication.offer(buffer, 0, size) < 0) {
            check(started, phase);
            Thread.onSpinWait();
        }
    }

    private static void receiveThroughput(Subscription incoming, Publication outgoing, int size) {
        UnsafeBuffer ack = new UnsafeBuffer(ByteBuffer.allocateDirect(3 * Long.BYTES));
        for (int round = -1; round < 3; round++) {
            final int currentRound = round;
            long duration = round == -1 ? WARMUP_NS : MEASURE_NS;
            long[] first = {0};
            long[] received = {0};
            long[] measured = {0};
            boolean[] ended = {false};
            FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
                if (length == Long.BYTES) {
                    if (buffer.getLong(offset) != currentRound) {
                        throw new IllegalStateException("wrong end marker");
                    }
                    ended[0] = true;
                    return;
                }
                if (length != size || ended[0]) {
                    throw new IllegalStateException("wrong message length " + length);
                }
                long now = System.nanoTime();
                if (first[0] == 0) {
                    first[0] = now;
                }
                received[0]++;
                if (now - first[0] < duration) {
                    measured[0]++;
                }
            });
            long started = System.nanoTime();
            while (!ended[0]) {
                if (incoming.poll(assembler, 32) == 0) {
                    Thread.onSpinWait();
                }
                check(started, "receive round " + round);
            }
            if (received[0] == 0) {
                throw new IllegalStateException("empty round");
            }
            ack.putLong(0, round);
            ack.putLong(Long.BYTES, measured[0]);
            ack.putLong(2 * Long.BYTES, received[0]);
            offer(outgoing, ack, 3 * Long.BYTES, "ack round " + round);
        }
    }

    private static void sendThroughput(Publication outgoing, Subscription incoming, int size) {
        UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
        UnsafeBuffer marker = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));
        long[] lastAck = {-2};
        long[] measured = {0};
        long[] received = {0};
        for (int round = -1; round < 3; round++) {
            long started = System.nanoTime();
            long duration = round == -1 ? WARMUP_NS : MEASURE_NS;
            long sent = 0;
            while (System.nanoTime() - started < duration) {
                while (outgoing.offer(payload, 0, size) < 0) {
                    check(started, "send round " + round);
                    Thread.onSpinWait();
                }
                sent++;
            }
            marker.putLong(0, round);
            offer(outgoing, marker, Long.BYTES, "end round " + round);
            while (lastAck[0] != round) {
                incoming.poll((buffer, offset, length, header) -> {
                    if (length != 3 * Long.BYTES) {
                        throw new IllegalStateException("wrong ack length " + length);
                    }
                    lastAck[0] = buffer.getLong(offset);
                    measured[0] = buffer.getLong(offset + Long.BYTES);
                    received[0] = buffer.getLong(offset + 2 * Long.BYTES);
                }, 10);
                check(started, "ack round " + round);
                Thread.onSpinWait();
            }
            if (received[0] != sent || measured[0] > received[0]) {
                throw new IllegalStateException("wrong receiver count");
            }
            if (round >= 0) {
                double elapsed = MEASURE_NS / 1e9;
                double msgsPerSecond = measured[0] / elapsed;
                System.out.printf("impl=aeron-udp-2proc kind=throughput size=%d round=%d msgs_s=%.6f mbps=%.6f elapsed=%.6f%n",
                    size, round + 1, msgsPerSecond, msgsPerSecond * size / 1e6, elapsed);
            }
        }
    }

    private static void echo(Subscription incoming, Publication outgoing, int size) {
        long[] received = {0};
        long started = System.nanoTime();
        long total = WARMUP_RTT + SAMPLES;
        FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
            if (length != size) {
                throw new IllegalStateException("wrong request length " + length);
            }
            long offerStarted = System.nanoTime();
            while (outgoing.offer(buffer, offset, length) < 0) {
                check(offerStarted, "echo offer");
                Thread.onSpinWait();
            }
            received[0]++;
        });
        while (received[0] < total) {
            int n = incoming.poll(assembler, 10);
            if (n == 0) {
                Thread.onSpinWait();
            }
            check(started, "echo");
        }
    }

    private static void measureRtt(Publication outgoing, Subscription incoming, int size) {
        UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
        long[] lastReply = {-1};
        FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
            if (length != size) {
                throw new IllegalStateException("wrong response length " + length);
            }
            lastReply[0] = buffer.getLong(offset);
        });
        long[] samples = new long[SAMPLES];
        for (long sequence = 0; sequence < WARMUP_RTT + SAMPLES; sequence++) {
            payload.putLong(0, sequence);
            long started = System.nanoTime();
            offer(outgoing, payload, size, "request");
            while (lastReply[0] != sequence) {
                incoming.poll(assembler, 10);
                check(started, "response");
                Thread.onSpinWait();
            }
            if (sequence >= WARMUP_RTT) {
                samples[(int) sequence - WARMUP_RTT] = System.nanoTime() - started;
            }
        }
        Arrays.sort(samples);
        System.out.printf("impl=aeron-udp-2proc kind=latency size=%d round=1 p50_us=%.6f p99_us=%.6f p999_us=%.6f%n",
            size, samples[SAMPLES / 2] / 1_000.0,
            samples[SAMPLES * 99 / 100] / 1_000.0,
            samples[SAMPLES * 999 / 1000] / 1_000.0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("usage: AeronUdpPeer throughput|latency server|client size base_port directory");
        }
        String mode = args[0];
        String role = args[1];
        int size = Integer.parseInt(args[2]);
        int basePort = Integer.parseInt(args[3]);
        Path directory = Path.of(args[4], role);
        Files.createDirectories(directory.getParent());
        String data = "aeron:udp?endpoint=127.0.0.1:" + basePort + "|term-length=67108864";
        String reply = "aeron:udp?endpoint=127.0.0.1:" + (basePort + 1) + "|term-length=67108864";
        boolean server = switch (role) {
            case "server" -> true;
            case "client" -> false;
            default -> throw new IllegalArgumentException("unknown role " + role);
        };
        if (!mode.equals("throughput") && !mode.equals("latency")) {
            throw new IllegalArgumentException("unknown mode " + mode);
        }
        if (mode.equals("latency") && !Arrays.asList(16, 32, 64, 256, 1024, 4096).contains(size)) {
            throw new IllegalArgumentException("unsupported latency size");
        }
        if (mode.equals("throughput") && !Arrays.asList(16, 32, 64, 128, 256, 512,
                1024, 2048, 4096, 8192, 16384, 32768, 262144, 4194304, 8388608).contains(size)) {
            throw new IllegalArgumentException("unsupported throughput size");
        }
        MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(directory.toString())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED);
        try (MediaDriver driver = MediaDriver.launchEmbedded(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             Subscription incoming = aeron.addSubscription(server ? data : reply, server ? 201 : 202);
             Publication outgoing = aeron.addExclusivePublication(server ? reply : data, server ? 202 : 201)) {
            awaitConnected(outgoing, incoming);
            if (size > outgoing.maxMessageLength()) {
                throw new IllegalArgumentException("message size " + size + " exceeds Aeron max " + outgoing.maxMessageLength());
            }
            if (mode.equals("latency")) {
                if (server) {
                    echo(incoming, outgoing, size);
                } else {
                    measureRtt(outgoing, incoming, size);
                }
            } else if (server) {
                receiveThroughput(incoming, outgoing, size);
            } else {
                sendThroughput(outgoing, incoming, size);
            }
        }
    }
}
