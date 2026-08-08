package io.omq;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

final class NativeFuture<T> extends CompletableFuture<T> {
    private final AtomicLong taskHandle = new AtomicLong();

    void setNativeTask(long handle) {
        if (handle == 0) {
            return;
        }
        if (!taskHandle.compareAndSet(0, handle)) {
            Native.asyncTaskCancel(handle);
            return;
        }
        if (isDone()) {
            clearNativeTask();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        try {
            return super.cancel(mayInterruptIfRunning);
        } finally {
            clearNativeTask();
        }
    }

    @Override
    public boolean complete(T value) {
        try {
            return super.complete(value);
        } finally {
            clearNativeTask();
        }
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        try {
            return super.completeExceptionally(ex);
        } finally {
            clearNativeTask();
        }
    }

    private void clearNativeTask() {
        long handle = taskHandle.getAndSet(0);
        if (handle != 0) {
            Native.asyncTaskCancel(handle);
        }
    }
}
