package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class AsyncTest {
    @Test
    void receiveAsyncCompletesWhenMessageArrives() throws Exception {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            CompletableFuture<Message> received = pull.receiveAsync();
            assertFalse(received.isDone());
            push.send("async");

            assertEquals("async", received.get(5, TimeUnit.SECONDS).text());
        }
    }

    @Test
    void sendAsyncCompletesOnNativeRuntime() throws Exception {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            push.sendAsync("async-send").get(5, TimeUnit.SECONDS);

            assertEquals("async-send", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void receiveAsyncTimeoutCompletesExceptionally() throws Exception {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            pull.bind("tcp://127.0.0.1:0");

            CompletableFuture<Message> received = pull.receiveAsync(Duration.ofMillis(20));
            ExecutionException error = assertThrowsExecution(received);

            assertInstanceOf(TimeoutException.class, error.getCause());
        }
    }

    @Test
    void canceledReceiveAsyncDoesNotConsumeNextMessage() throws Exception {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));

            CompletableFuture<Message> received = pull.receiveAsync();
            assertTrue(received.cancel(true));
            Thread.sleep(50);

            push.send("after-cancel");

            assertEquals(
                    "after-cancel", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void receiveAnyCanReadFromMultipleSockets() throws Exception {
        try (Context context = OMQ.context();
             Socket pull1 = context.socket(SocketType.PULL);
             Socket pull2 = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            pull1.bind("tcp://127.0.0.1:0");
            String endpoint2 = pull2.bind("tcp://127.0.0.1:0");
            push.connect(endpoint2);
            push.waitConnected(1, Duration.ofSeconds(5));

            CompletableFuture<ReceiveEvent> either = OMQ.receiveAny(pull1, pull2);
            push.send("second");

            ReceiveEvent event = either.get(5, TimeUnit.SECONDS);
            assertEquals(pull2, event.socket());
            assertEquals("second", event.message().text());
        }
    }

    @Test
    void canceledReceiveAnyDoesNotConsumeNextMessage() throws Exception {
        try (Context context = OMQ.context();
             Socket pull1 = context.socket(SocketType.PULL);
             Socket pull2 = context.socket(SocketType.PULL);
             Socket push = context.socket(SocketType.PUSH)) {
            pull1.bind("tcp://127.0.0.1:0");
            String endpoint2 = pull2.bind("tcp://127.0.0.1:0");
            push.connect(endpoint2);
            push.waitConnected(1, Duration.ofSeconds(5));

            CompletableFuture<ReceiveEvent> either = OMQ.receiveAny(pull1, pull2);
            assertTrue(either.cancel(true));
            Thread.sleep(50);

            push.send("after-cancel");

            assertEquals(
                    "after-cancel", pull2.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void receiveAnyRejectsDuplicateSockets() {
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(IllegalArgumentException.class, () -> OMQ.receiveAny(pull, pull));
        }
    }

    private static ExecutionException assertThrowsExecution(CompletableFuture<?> future)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException error) {
            return error;
        }
        throw new AssertionError("future completed successfully");
    }
}
