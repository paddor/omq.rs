package io.omq.perf;

import io.omq.Context;
import io.omq.OMQ;
import io.omq.Socket;
import io.omq.SocketType;
import io.omq.WorkloadProfile;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;

/** Two-process PUSH/PULL TCP benchmark peer for OMQ.java and JeroMQ. */
public final class PushPullTcpPeer {
    private static final int HWM = 1_000_000;
    private static final Duration LINGER = Duration.ofSeconds(5);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private PushPullTcpPeer() {
    }

    /** Runs one benchmark peer process. */
    public static void main(String[] args) {
        if (args.length != 6 && args.length != 7) {
            throw new IllegalArgumentException(
                    "usage: <omq|omq-into|jeromq|jeromq-into> "
                            + "<push|pull> <endpoint> <size> "
                            + "<duration_secs> <warmup_secs> [batch]");
        }

        Impl impl = Impl.parse(args[0]);
        Role role = Role.parse(args[1]);
        String endpoint = args[2];
        int size = Integer.parseInt(args[3]);
        long durationNanos = parsePositiveSeconds(args[4], "duration_secs");
        long warmupNanos = parseNonNegativeSeconds(args[5], "warmup_secs");
        int batch = args.length == 7 ? Integer.parseInt(args[6]) : 64;
        if (size < 0 || batch <= 0) {
            throw new IllegalArgumentException("invalid size/batch");
        }

        if (role == Role.PULL) {
            runPull(impl, endpoint, size, durationNanos, warmupNanos, batch);
        } else {
            runPush(impl, endpoint, size);
        }
    }

    private static void runPull(
            Impl impl, String endpoint, int size, long durationNanos, long warmupNanos, int batch) {
        switch (impl) {
            case OMQ -> runOmqPull(endpoint, size, durationNanos, warmupNanos, false);
            case OMQ_INTO -> runOmqPull(endpoint, size, durationNanos, warmupNanos, true);
            case JEROMQ -> runJeroPull(endpoint, size, durationNanos, warmupNanos, false);
            case JEROMQ_INTO -> runJeroPull(endpoint, size, durationNanos, warmupNanos, true);
        }
    }

    private static void runPush(Impl impl, String endpoint, int size) {
        switch (impl) {
            case OMQ, OMQ_INTO -> runOmqPush(endpoint, size);
            case JEROMQ, JEROMQ_INTO -> runJeroPush(endpoint, size);
        }
    }

    private static void runOmqPull(
            String endpoint, int size, long durationNanos, long warmupNanos, boolean receiveInto) {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)
                     .workloadProfile(WorkloadProfile.THROUGHPUT)
                     .receiveHighWaterMark(HWM)
                     .linger(LINGER)) {
            pull.bind(endpoint);
            ready(endpoint);
            if (receiveInto) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                receiveIntoUntil(pull, buffer, size, System.nanoTime() + warmupNanos);
                long started = System.nanoTime();
                long count = receiveIntoUntil(pull, buffer, size, started + durationNanos);
                result(Impl.OMQ_INTO.name, endpoint, size, count, started, System.nanoTime());
            } else {
                receiveBytesUntil(pull, size, System.nanoTime() + warmupNanos);
                long started = System.nanoTime();
                long count = receiveBytesUntil(pull, size, started + durationNanos);
                result(Impl.OMQ.name, endpoint, size, count, started, System.nanoTime());
            }
        }
    }

    private static void runOmqPush(String endpoint, int size) {
        byte[] payload = payload(size);
        try (Context context = OMQ.context();
             Socket push = context.socket(SocketType.PUSH)
                     .workloadProfile(WorkloadProfile.THROUGHPUT)
                     .sendHighWaterMark(HWM)
                     .linger(LINGER)) {
            push.connect(endpoint);
            push.waitConnected(1, CONNECT_TIMEOUT);
            sendOmqForever(push, payload);
        }
    }

    private static void runJeroPull(
            String endpoint, int size, long durationNanos, long warmupNanos, boolean receiveInto) {
        try (org.zeromq.ZContext context = new org.zeromq.ZContext();
             org.zeromq.ZMQ.Socket pull = context.createSocket(org.zeromq.SocketType.PULL)) {
            pull.setRcvHWM(HWM);
            pull.setLinger((int) LINGER.toMillis());
            pull.bind(endpoint);
            ready(endpoint);
            if (receiveInto) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                receiveJeroIntoUntil(pull, buffer, size, System.nanoTime() + warmupNanos);
                long started = System.nanoTime();
                long count = receiveJeroIntoUntil(pull, buffer, size, started + durationNanos);
                result(Impl.JEROMQ_INTO.name, endpoint, size, count, started, System.nanoTime());
            } else {
                receiveJeroUntil(pull, size, System.nanoTime() + warmupNanos);
                long started = System.nanoTime();
                long count = receiveJeroUntil(pull, size, started + durationNanos);
                result(Impl.JEROMQ.name, endpoint, size, count, started, System.nanoTime());
            }
        }
    }

    private static void runJeroPush(String endpoint, int size) {
        byte[] payload = payload(size);
        try (org.zeromq.ZContext context = new org.zeromq.ZContext();
             org.zeromq.ZMQ.Socket push = context.createSocket(org.zeromq.SocketType.PUSH)) {
            push.setSndHWM(HWM);
            push.setLinger((int) LINGER.toMillis());
            push.connect(endpoint);
            sleep(250);
            sendJeroForever(push, payload);
        }
    }

    private static void sendOmqForever(Socket push, byte[] payload) {
        while (true) {
            push.send(payload);
        }
    }

    private static void sendJeroForever(org.zeromq.ZMQ.Socket push, byte[] payload) {
        while (true) {
            if (!push.send(payload, 0)) {
                throw new IllegalStateException("JeroMQ send failed");
            }
        }
    }

    private static long receiveBytesUntil(Socket pull, int size, long deadlineNanos) {
        long count = 0;
        while (System.nanoTime() < deadlineNanos) {
            byte[] body = pull.receiveBytes();
            if (body.length != size) {
                throw new IllegalStateException("expected " + size + " bytes, got " + body.length);
            }
            count++;
        }
        return count;
    }

    private static long receiveIntoUntil(
            Socket pull, ByteBuffer buffer, int size, long deadlineNanos) {
        long count = 0;
        while (System.nanoTime() < deadlineNanos) {
            buffer.clear();
            int received = pull.receiveInto(buffer);
            if (received != size) {
                throw new IllegalStateException("expected " + size + " bytes, got " + received);
            }
            count++;
        }
        return count;
    }

    private static long receiveJeroUntil(org.zeromq.ZMQ.Socket pull, int size, long deadlineNanos) {
        long count = 0;
        while (System.nanoTime() < deadlineNanos) {
            byte[] body = pull.recv(0);
            if (body.length != size) {
                throw new IllegalStateException("expected " + size + " bytes, got " + body.length);
            }
            count++;
        }
        return count;
    }

    private static long receiveJeroIntoUntil(
            org.zeromq.ZMQ.Socket pull, ByteBuffer buffer, int size, long deadlineNanos) {
        long count = 0;
        while (System.nanoTime() < deadlineNanos) {
            buffer.clear();
            int received = pull.recvByteBuffer(buffer, 0);
            if (received != size) {
                throw new IllegalStateException("expected " + size + " bytes, got " + received);
            }
            count++;
        }
        return count;
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
            String impl, String endpoint, int size, long messages, long started, long ended) {
        double seconds = (ended - started) / 1_000_000_000.0;
        double messagesPerSecond = messages / seconds;
        double gbPerSecond = messagesPerSecond * size / 1_000_000_000.0;
        System.out.printf(
                Locale.ROOT,
                "RESULT {\"impl\":\"%s\",\"endpoint\":\"%s\",\"msg_size\":%d,"
                        + "\"messages\":%d,\"seconds\":%.9f,\"msgs_s\":%.3f,\"gb_s\":%.6f}%n",
                impl, endpoint, size, messages, seconds, messagesPerSecond, gbPerSecond);
        System.out.flush();
    }

    private static long parsePositiveSeconds(String value, String label) {
        long nanos = parseSeconds(value, label);
        if (nanos <= 0) {
            throw new IllegalArgumentException(label + " must be greater than zero");
        }
        return nanos;
    }

    private static long parseNonNegativeSeconds(String value, String label) {
        long nanos = parseSeconds(value, label);
        if (nanos < 0) {
            throw new IllegalArgumentException(label + " must be non-negative");
        }
        return nanos;
    }

    private static long parseSeconds(String value, String label) {
        double seconds = Double.parseDouble(value);
        if (!Double.isFinite(seconds)) {
            throw new IllegalArgumentException(label + " must be finite");
        }
        double nanos = seconds * 1_000_000_000.0;
        if (nanos > Long.MAX_VALUE) {
            throw new IllegalArgumentException(label + " is too large");
        }
        return Math.round(nanos);
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
        PUSH,
        PULL;

        private static Role parse(String value) {
            return switch (value) {
                case "push" -> PUSH;
                case "pull" -> PULL;
                default -> throw new IllegalArgumentException("unknown role: " + value);
            };
        }
    }
}
