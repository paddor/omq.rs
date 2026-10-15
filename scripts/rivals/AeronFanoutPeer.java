import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Small fan-outs share a receiver; 32 readers use separate processes. */
public final class AeronFanoutPeer {
    private static final long DEADLINE = 120_000_000_000L;
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    private static void check(long started) {
        if (System.nanoTime() - started >= DEADLINE) {
            throw new IllegalStateException("peer timeout");
        }
    }

    private static void write(SocketChannel stream, ByteBuffer bytes) throws Exception {
        long started = System.nanoTime();
        bytes.flip();
        while (bytes.hasRemaining()) {
            stream.write(bytes);
            check(started);
        }
    }

    private static ByteBuffer read(SocketChannel stream, int length) throws Exception {
        ByteBuffer bytes = ByteBuffer.allocate(length).order(ORDER);
        long started = System.nanoTime();
        while (bytes.hasRemaining()) {
            if (stream.read(bytes) < 0) {
                throw new IllegalStateException("control closed");
            }
            check(started);
            Thread.yield();
        }
        return bytes.flip();
    }

    private static void offer(Publication port, UnsafeBuffer bytes, int length) {
        long started = System.nanoTime();
        while (port.offer(bytes, 0, length) < 0) {
            check(started);
            Thread.yield();
        }
    }

    private static void subscribe(Subscription port, int size, Path directory, Aeron separateClient) throws Exception {
        try (SocketChannel control = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            control.connect(UnixDomainSocketAddress.of(directory.resolve("control.sock")));
            control.configureBlocking(false);
            boolean[] ready = {false};
            for (int round = 0; round < 4; round++) {
                int currentRound = round;
                long[] count = {0};
                long[] first = {0};
                boolean[] ended = {false};
                FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
                    try {
                        if (length == 8) {
                            long marker = buffer.getLong(offset, ORDER);
                            if (marker == -1) {
                                if (!ready[0]) {
                                    write(control, ByteBuffer.allocate(1).put((byte) 1));
                                    ready[0] = true;
                                }
                            } else if (marker == -2L - currentRound) {
                                ended[0] = true;
                            } else {
                                throw new IllegalStateException("unexpected marker");
                            }
                        } else {
                            if (!ready[0] || ended[0] || length != size
                                || buffer.getLong(offset, ORDER) != count[0]
                                || buffer.getByte(offset + size - 1) != (byte) 0xa5) {
                                throw new IllegalStateException("wrong size/sequence/sentinel " + count[0]);
                            }
                            if (first[0] == 0) { first[0] = System.nanoTime(); }
                            count[0]++;
                        }
                    } catch (Exception error) {
                        throw new IllegalStateException(error);
                    }
                });
                long started = System.nanoTime();
                while (!ended[0]) {
                    if (port.poll(assembler, Math.min(256, 1048576 / size)) == 0) { Thread.yield(); }
                    check(started);
                }
                if (count[0] == 0) { throw new IllegalStateException("empty round"); }
                double elapsed = (System.nanoTime() - first[0]) / 1e9;
                write(control, ByteBuffer.allocate(16).order(ORDER).putLong(count[0]).putDouble(elapsed));
            }
            read(control, 1);
            port.close();
            if (separateClient != null) { separateClient.close(); }
            write(control, ByteBuffer.allocate(1).put((byte) 1));
        }
    }

    private static void publish(Publication port, int size, int readers, String implementation, Path directory) throws Exception {
        List<SocketChannel> controls = new ArrayList<>();
        try (ServerSocketChannel listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            listener.bind(UnixDomainSocketAddress.of(directory.resolve("control.sock")));
            listener.configureBlocking(false);
            Files.writeString(directory.resolve("ready"), "ready");
            long started = System.nanoTime();
            while (controls.size() < readers) {
                SocketChannel stream = listener.accept();
                if (stream != null) {
                    stream.configureBlocking(false);
                    controls.add(stream);
                }
                check(started);
                Thread.sleep(1);
            }
            UnsafeBuffer marker = new UnsafeBuffer(ByteBuffer.allocateDirect(8));
            marker.putLong(0, -1, ORDER);
            boolean[] ready = new boolean[readers];
            int readyCount = 0;
            while (readyCount < readers) {
                offer(port, marker, 8);
                for (int i = 0; i < readers; i++) {
                    if (!ready[i] && controls.get(i).read(ByteBuffer.allocate(1)) == 1) {
                        ready[i] = true;
                        readyCount++;
                    }
                }
                check(started);
                Thread.sleep(1);
            }
            UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
            payload.setMemory(0, size, (byte) 0xa5);
            for (int round = 0; round < 4; round++) {
                long duration = round == 0 ? 1_000_000_000L : 3_000_000_000L;
                started = System.nanoTime();
                long sent = 0;
                while (System.nanoTime() - started < duration) {
                    for (int n = 0; n < Math.min(256, 1048576 / size); n++) {
                        payload.putLong(0, sent, ORDER);
                        offer(port, payload, size);
                        sent++;
                    }
                }
                double senderElapsed = (System.nanoTime() - started) / 1e9;
                marker.putLong(0, -2L - round, ORDER);
                offer(port, marker, 8);
                double elapsed = senderElapsed;
                List<Long> counts = new ArrayList<>();
                List<Double> times = new ArrayList<>();
                for (SocketChannel stream : controls) {
                    ByteBuffer ack = read(stream, 16);
                    long count = ack.getLong();
                    double seconds = ack.getDouble();
                    if (count != sent || !Double.isFinite(seconds) || seconds <= 0) {
                        throw new IllegalStateException("delivery mismatch " + sent + " / " + count);
                    }
                    counts.add(count);
                    times.add(seconds);
                    elapsed = Math.max(elapsed, seconds);
                }
                if (round > 0) {
                    double rate = sent / elapsed;
                    System.out.printf("{\"event\":\"result\",\"implementation\":\"%s\",\"msg_size\":%d,\"readers\":%d,\"round\":%d,\"offered\":%d,\"reader_counts\":%s,\"reader_elapsed\":%s,\"sender_elapsed\":%.9f,\"elapsed\":%.9f,\"msgs_s\":%.9f,\"aggregate_msgs_s\":%.9f,\"mbps\":%.9f,\"missing\":0,\"duplicates\":0,\"gaps\":0,\"corrupt\":0}%n",
                        implementation, size, readers, round, sent, counts, times, senderElapsed, elapsed, rate, rate * readers, rate * size / 1e6);
                }
            }
            for (SocketChannel stream : controls) { write(stream, ByteBuffer.allocate(1).put((byte) 1)); }
            for (SocketChannel stream : controls) { read(stream, 1); }
        } finally {
            for (SocketChannel stream : controls) { stream.close(); }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 7) { throw new IllegalArgumentException("impl pub|sub size readers id port directory"); }
        String implementation = args[0];
        boolean publisher = args[1].equals("pub");
        int size = Integer.parseInt(args[2]);
        int readers = Integer.parseInt(args[3]);
        int readerId = Integer.parseInt(args[4]);
        boolean separateReaders = readers == 32;
        if (!args[1].equals("pub") && !args[1].equals("sub")) { throw new IllegalArgumentException("unknown role"); }
        int base = Integer.parseInt(args[5]);
        Path directory = Path.of(args[6]);
        boolean ipc = implementation.equals("aeron-ipc");
        if (!ipc && !implementation.equals("aeron-udp")) { throw new IllegalArgumentException("unknown implementation"); }
        Path driverDirectory = Path.of("/dev/shm", directory.getFileName().toString()
            + (ipc ? "-driver" : (publisher ? "-driver-pub"
                : separateReaders ? "-driver-sub-" + readerId : "-driver-sub")));
        MediaDriver driver = null;
        if (publisher || !ipc) {
            driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED));
        }
        String channel;
        if (ipc) {
            channel = "aeron:ipc?term-length=16777216";
        } else if (separateReaders && publisher) {
            channel = "aeron:udp?control=127.0.0.1:" + (base + 1)
                + "|control-mode=dynamic|fc=min,g:/32|term-length=16777216";
        } else if (separateReaders) {
            channel = "aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:" + (base + 1)
                + "|control-mode=dynamic|term-length=16777216";
        } else {
            channel = "aeron:udp?endpoint=127.0.0.1:" + (base + 1)
                + "|term-length=16777216";
        }
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDirectory.toString()))) {
            if (publisher) {
                try (Publication port = aeron.addExclusivePublication(channel, 201)) {
                    publish(port, size, readers, implementation, directory);
                }
            } else if (separateReaders) {
                try (Subscription port = aeron.addSubscription(channel, 201)) {
                    subscribe(port, size, directory, aeron);
                }
            } else {
                List<Subscription> subscriptions = new ArrayList<>();
                List<Future<?>> workers = new ArrayList<>();
                ExecutorService executor = Executors.newFixedThreadPool(readers);
                try {
                    for (int i = 0; i < readers; i++) { subscriptions.add(aeron.addSubscription(channel, 201)); }
                    for (Subscription port : subscriptions) {
                        workers.add(executor.submit(() -> {
                            try { subscribe(port, size, directory, null); }
                            catch (Exception error) { error.printStackTrace(); System.exit(1); }
                        }));
                    }
                    for (Future<?> worker : workers) { worker.get(); }
                } finally {
                    executor.shutdownNow();
                    for (Subscription port : subscriptions) { port.close(); }
                }
            }
        } finally {
            if (driver != null) { driver.close(); }
        }
    }
}
