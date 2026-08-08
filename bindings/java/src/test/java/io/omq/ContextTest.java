package io.omq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
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
        try (Context owner = OMQ.context();
             Context shared = OMQ.contextFromShareKey(owner.shareKey()).orElseThrow()) {
            shared.close();
            assertDoesNotThrow(() -> {
                try (Socket socket = owner.socket(SocketType.PULL)) {
                    socket.close();
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
}
