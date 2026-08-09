package io.omq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class ContextTest {
    @Test
    void rejectsNonPositiveIoThreads() {
        assertThrows(IllegalArgumentException.class, () -> OMQ.context(0));
        assertThrows(IllegalArgumentException.class, () -> OMQ.context(-1));
    }

    @Test
    void acceptsMultipleIoThreads() {
        assertDoesNotThrow(() -> {
            try (Context context = OMQ.context(2)) {
                context.socket(SocketType.PULL).close();
            }
        });
    }

    @Test
    void closeIsIdempotent() {
        Context context = OMQ.context();
        context.close();
        context.close();
        assertThrows(ClosedException.class, () -> context.socket(SocketType.PULL));
    }

    @Test
    void sharedContextCloseDoesNotTerminateOwner() {
        try (Context owner = OMQ.context()) {
            Context shared = OMQ.contextFromShareKey(owner.shareKey()).orElseThrow();
            shared.close();
            assertDoesNotThrow(() -> {
                try (Socket socket = owner.socket(SocketType.PULL)) {
                    assertNotNull(socket);
                }
            });
        }
    }

    @Test
    void shareKeyExpiresAfterOwnerClose() {
        Context owner = OMQ.context();
        UUID key = owner.shareKey();
        owner.close();
        assertTrue(Context.fromShareKey(key).isEmpty());
    }

    @Test
    void socketCreationAndCloseCanRace() throws Exception {
        Context context = OMQ.context();
        int workers = 8;
        CyclicBarrier start = new CyclicBarrier(workers + 1);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (int i = 0; i < workers; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int attempt = 0; attempt < 100; attempt++) {
                        try (Socket socket = context.socket(SocketType.PULL)) {
                            assertNotNull(socket);
                            // Close racing context may close this socket first.
                        } catch (ClosedException closed) {
                            return null;
                        }
                    }
                    return null;
                }));
            }

            start.await(5, TimeUnit.SECONDS);
            context.close();

            for (Future<Void> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            context.close();
            executor.shutdownNow();
        }
    }
}
