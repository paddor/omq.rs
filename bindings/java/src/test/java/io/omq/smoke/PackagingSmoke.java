package io.omq.smoke;

import io.omq.Context;
import io.omq.OMQ;
import io.omq.Socket;
import io.omq.SocketType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Smoke entrypoint for packaged OMQ.java runtime jars. */
public final class PackagingSmoke {
    private PackagingSmoke() {
    }

    public static void main(String[] args) throws Exception {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            context.socket(SocketType.PAIR).close();
            OMQ.curveKeypair();

            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("ring".getBytes(StandardCharsets.UTF_8));
            String body = new String(pull.receiveBytes(), StandardCharsets.UTF_8);
            if (!"ring".equals(body)) {
                throw new AssertionError("ring roundtrip failed: " + body);
            }

            CountDownLatch receiving = new CountDownLatch(1);
            CompletableFuture<String> received = new CompletableFuture<>();
            Thread receiver = Thread.ofVirtual().start(() -> {
                receiving.countDown();
                try {
                    received.complete(pull.receive().text());
                } catch (Throwable error) {
                    received.completeExceptionally(error);
                }
            });
            if (!receiving.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("virtual receiver did not start");
            }
            Thread sender = Thread.ofVirtual().start(() -> {
                try {
                    push.send("virtual");
                } catch (Throwable error) {
                    received.completeExceptionally(error);
                }
            });
            if (!"virtual".equals(received.get(5, TimeUnit.SECONDS))) {
                throw new AssertionError("virtual roundtrip failed");
            }
            receiver.join(5_000);
            sender.join(5_000);
        }
    }
}
