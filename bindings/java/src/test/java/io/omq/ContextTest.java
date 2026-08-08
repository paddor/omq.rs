package io.omq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
