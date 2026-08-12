package io.omq.perf;

import io.omq.Context;
import io.omq.OMQ;
import io.omq.Socket;
import io.omq.SocketType;
import io.omq.WorkloadProfile;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;

/** Two-process REQ/REP TCP latency benchmark peer for OMQ.java and JeroMQ. */
public final class ReqRepTcpPeer {
    private static final int HWM = 1_000;
    private static final Duration LINGER = Duration.ZERO;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private ReqRepTcpPeer() {
    }

    /** Runs one benchmark peer process. */
    public static void main(String[] args) {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: <omq|omq-into|jeromq|jeromq-into> "
                            + "<req|rep> <endpoint> <size> <duration_secs> <warmup_secs>");
        }

        Impl impl = Impl.parse(args[0]);
        Role role = Role.parse(args[1]);
        String endpoint = args[2];
        int size = Integer.parseInt(args[3]);
        long durationNanos = parsePositiveSeconds(args[4], "duration_secs");
        long warmupNanos = parseNonNegativeSeconds(args[5], "warmup_secs");
        if (size < 0) {
            throw new IllegalArgumentException("invalid size");
        }

        if (role == Role.REP) {
            runRep(impl, endpoint, size);
        } else {
            runReq(impl, endpoint, size, durationNanos, warmupNanos);
        }
    }

    private static void runRep(Impl impl, String endpoint, int size) {
        switch (impl) {
            case OMQ -> runOmqRep(endpoint, size, false);
            case OMQ_INTO -> runOmqRep(endpoint, size, true);
            case JEROMQ -> runJeroRep(endpoint, size, false);
            case JEROMQ_INTO -> runJeroRep(endpoint, size, true);
        }
    }

    private static void runReq(
            Impl impl, String endpoint, int size, long durationNanos, long warmupNanos) {
        switch (impl) {
            case OMQ -> runOmqReq(endpoint, size, durationNanos, warmupNanos, false);
            case OMQ_INTO -> runOmqReq(endpoint, size, durationNanos, warmupNanos, true);
            case JEROMQ -> runJeroReq(endpoint, size, durationNanos, warmupNanos, false);
            case JEROMQ_INTO -> runJeroReq(endpoint, size, durationNanos, warmupNanos, true);
        }
    }

    private static void runOmqRep(
            String endpoint, int size, boolean receiveInto) {
        byte[] payload = payload(size);
        try (Context context = OMQ.context();
             Socket rep = context.socket(SocketType.REP)
                     .workloadProfile(WorkloadProfile.LATENCY)
                     .receiveHighWaterMark(HWM)
                     .sendHighWaterMark(HWM)
                     .linger(LINGER)) {
            rep.bind(endpoint);
            ready(endpoint);
            if (receiveInto) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                while (true) {
                    receiveOmqInto(rep, buffer, size);
                    rep.send(payload);
                }
            } else {
                while (true) {
                    receiveOmq(rep, size);
                    rep.send(payload);
                }
            }
        }
    }

    private static void runOmqReq(
            String endpoint, int size, long durationNanos, long warmupNanos, boolean receiveInto) {
        byte[] payload = payload(size);
        try (Context context = OMQ.context();
             Socket req = context.socket(SocketType.REQ)
                     .workloadProfile(WorkloadProfile.LATENCY)
                     .receiveHighWaterMark(HWM)
                     .sendHighWaterMark(HWM)
                     .linger(LINGER)) {
            req.connect(endpoint);
            req.waitConnected(1, CONNECT_TIMEOUT);
            if (receiveInto) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                long warmupDeadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < warmupDeadline) {
                    req.send(payload);
                    receiveOmqInto(req, buffer, size);
                }
                LongArray rtts = new LongArray();
                long deadline = System.nanoTime() + durationNanos;
                do {
                    long started = System.nanoTime();
                    req.send(payload);
                    receiveOmqInto(req, buffer, size);
                    rtts.add(System.nanoTime() - started);
                } while (System.nanoTime() < deadline);
                result(Impl.OMQ_INTO.name, endpoint, size, rtts.toArray());
            } else {
                long warmupDeadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < warmupDeadline) {
                    req.send(payload);
                    receiveOmq(req, size);
                }
                LongArray rtts = new LongArray();
                long deadline = System.nanoTime() + durationNanos;
                do {
                    long started = System.nanoTime();
                    req.send(payload);
                    receiveOmq(req, size);
                    rtts.add(System.nanoTime() - started);
                } while (System.nanoTime() < deadline);
                result(Impl.OMQ.name, endpoint, size, rtts.toArray());
            }
        }
    }

    private static void runJeroRep(
            String endpoint, int size, boolean receiveInto) {
        byte[] payload = payload(size);
        try (org.zeromq.ZContext context = new org.zeromq.ZContext();
             org.zeromq.ZMQ.Socket rep = context.createSocket(org.zeromq.SocketType.REP)) {
            rep.setRcvHWM(HWM);
            rep.setSndHWM(HWM);
            rep.setLinger((int) LINGER.toMillis());
            rep.bind(endpoint);
            ready(endpoint);
            if (receiveInto) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                while (true) {
                    receiveJeroInto(rep, buffer, size);
                    sendJero(rep, payload);
                }
            } else {
                while (true) {
                    receiveJero(rep, size);
                    sendJero(rep, payload);
                }
            }
        }
    }

    private static void runJeroReq(
            String endpoint, int size, long durationNanos, long warmupNanos, boolean receiveInto) {
        byte[] payload = payload(size);
        try (org.zeromq.ZContext context = new org.zeromq.ZContext();
             org.zeromq.ZMQ.Socket req = context.createSocket(org.zeromq.SocketType.REQ)) {
            req.setRcvHWM(HWM);
            req.setSndHWM(HWM);
            req.setLinger((int) LINGER.toMillis());
            req.connect(endpoint);
            sleep(100);
            if (receiveInto) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                long warmupDeadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < warmupDeadline) {
                    sendJero(req, payload);
                    receiveJeroInto(req, buffer, size);
                }
                LongArray rtts = new LongArray();
                long deadline = System.nanoTime() + durationNanos;
                do {
                    long started = System.nanoTime();
                    sendJero(req, payload);
                    receiveJeroInto(req, buffer, size);
                    rtts.add(System.nanoTime() - started);
                } while (System.nanoTime() < deadline);
                result(Impl.JEROMQ_INTO.name, endpoint, size, rtts.toArray());
            } else {
                long warmupDeadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < warmupDeadline) {
                    sendJero(req, payload);
                    receiveJero(req, size);
                }
                LongArray rtts = new LongArray();
                long deadline = System.nanoTime() + durationNanos;
                do {
                    long started = System.nanoTime();
                    sendJero(req, payload);
                    receiveJero(req, size);
                    rtts.add(System.nanoTime() - started);
                } while (System.nanoTime() < deadline);
                result(Impl.JEROMQ.name, endpoint, size, rtts.toArray());
            }
        }
    }

    private static void receiveOmq(Socket socket, int size) {
        byte[] body = socket.receiveBytes();
        if (body.length != size) {
            throw new IllegalStateException("expected " + size + " bytes, got " + body.length);
        }
    }

    private static void receiveOmqInto(Socket socket, ByteBuffer buffer, int size) {
        buffer.clear();
        int received = socket.receiveInto(buffer);
        if (received != size) {
            throw new IllegalStateException("expected " + size + " bytes, got " + received);
        }
    }

    private static void receiveJero(org.zeromq.ZMQ.Socket socket, int size) {
        byte[] body = socket.recv(0);
        if (body.length != size) {
            throw new IllegalStateException("expected " + size + " bytes, got " + body.length);
        }
    }

    private static void receiveJeroInto(
            org.zeromq.ZMQ.Socket socket, ByteBuffer buffer, int size) {
        buffer.clear();
        int received = socket.recvByteBuffer(buffer, 0);
        if (received != size) {
            throw new IllegalStateException("expected " + size + " bytes, got " + received);
        }
    }

    private static void sendJero(org.zeromq.ZMQ.Socket socket, byte[] payload) {
        if (!socket.send(payload, 0)) {
            throw new IllegalStateException("JeroMQ send failed");
        }
    }

    private static byte[] payload(int size) {
        byte[] payload = new byte[size];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        return payload;
    }

    private static void ready(String endpoint) {
        System.out.println("READY " + endpoint);
        System.out.flush();
    }

    private static void result(
            String impl, String endpoint, int size, long[] rtts) {
        Arrays.sort(rtts);
        int iterations = rtts.length;
        double p50 = rtts[iterations * 50 / 100] / 1_000.0;
        double p99 = rtts[Math.min(iterations - 1, iterations * 99 / 100)] / 1_000.0;
        System.out.printf(
                Locale.ROOT,
                "RESULT {\"impl\":\"%s\",\"endpoint\":\"%s\",\"msg_size\":%d,"
                        + "\"iterations\":%d,\"p50_us\":%.3f,\"p99_us\":%.3f}%n",
                impl, endpoint, size, iterations, p50, p99);
        System.out.flush();
    }

    private static long parsePositiveSeconds(String value, String name) {
        long nanos = parseNonNegativeSeconds(value, name);
        if (nanos <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return nanos;
    }

    private static long parseNonNegativeSeconds(String value, String name) {
        double seconds = Double.parseDouble(value);
        if (seconds < 0.0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return (long) (seconds * 1_000_000_000.0);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    private enum Impl {
        OMQ("omq"),
        OMQ_INTO("omq-into"),
        JEROMQ("jeromq"),
        JEROMQ_INTO("jeromq-into");

        private final String name;

        Impl(String name) {
            this.name = name;
        }

        private static Impl parse(String value) {
            for (Impl impl : values()) {
                if (impl.name.equals(value)) {
                    return impl;
                }
            }
            throw new IllegalArgumentException("unknown impl: " + value);
        }
    }

    private enum Role {
        REQ,
        REP;

        private static Role parse(String value) {
            return switch (value) {
                case "req" -> REQ;
                case "rep" -> REP;
                default -> throw new IllegalArgumentException("unknown role: " + value);
            };
        }
    }

    private static final class LongArray {
        private long[] values = new long[16_384];
        private int len;

        void add(long value) {
            if (len == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[len] = value;
            len++;
        }

        long[] toArray() {
            return Arrays.copyOf(values, len);
        }
    }
}
