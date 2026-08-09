package io.omq;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;

final class NativeFfm {
    static final int STATUS_OK = 0;
    static final int STATUS_TIMEOUT = 1;
    static final int STATUS_CLOSED = 2;
    static final int STATUS_INVALID_ENDPOINT = 3;
    static final int STATUS_PROTOCOL = 4;
    static final int STATUS_ERROR = 5;

    private static final int MAX_ERROR_STRING = 4096;
    private static final Linker LINKER;
    private static final SymbolLookup LOOKUP;

    private static final MethodHandle LAST_ERROR_CODE;
    private static final MethodHandle LAST_ERROR_MESSAGE;
    private static final MethodHandle RECV_RING_CREATE;
    private static final MethodHandle RECV_RING_CLOSE;
    private static final MethodHandle RECV_RING_CONTROL_ADDR;
    private static final MethodHandle RECV_RING_DESC_ADDR;
    private static final MethodHandle RECV_RING_PAYLOAD_ADDR;
    private static final MethodHandle RECV_RING_DESC_CAPACITY;
    private static final MethodHandle RECV_RING_PAYLOAD_CAPACITY;
    private static final MethodHandle RECV_RING_ERROR_MESSAGE;
    private static final MethodHandle RECV_RING_FILL;

    static {
        Native.ensureLoaded();
        LINKER = Linker.nativeLinker();
        LOOKUP = SymbolLookup.loaderLookup();
        LAST_ERROR_CODE = downcall("omq_java_last_error_code", FunctionDescriptor.of(JAVA_INT));
        LAST_ERROR_MESSAGE = downcall("omq_java_last_error_message", FunctionDescriptor.of(JAVA_LONG));
        RECV_RING_CREATE = downcall(
                "omq_java_recv_ring_create",
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, JAVA_INT, JAVA_LONG));
        RECV_RING_CLOSE = downcall(
                "omq_java_recv_ring_close",
                FunctionDescriptor.ofVoid(JAVA_LONG));
        RECV_RING_CONTROL_ADDR = downcall(
                "omq_java_recv_ring_control_addr",
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
        RECV_RING_DESC_ADDR = downcall(
                "omq_java_recv_ring_desc_addr",
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
        RECV_RING_PAYLOAD_ADDR = downcall(
                "omq_java_recv_ring_payload_addr",
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
        RECV_RING_DESC_CAPACITY = downcall(
                "omq_java_recv_ring_desc_capacity",
                FunctionDescriptor.of(JAVA_INT, JAVA_LONG));
        RECV_RING_PAYLOAD_CAPACITY = downcall(
                "omq_java_recv_ring_payload_capacity",
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
        RECV_RING_ERROR_MESSAGE = downcall(
                "omq_java_recv_ring_error_message",
                FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
        RECV_RING_FILL = downcall(
                "omq_java_recv_ring_fill",
                FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT));
    }

    private NativeFfm() {
    }

    static long recvRingCreate(long socketHandle, int descCapacity, long payloadCapacity) {
        try {
            long handle = (long) RECV_RING_CREATE.invokeExact(socketHandle, descCapacity, payloadCapacity);
            if (handle == 0) {
                throwStatus(lastErrorCode(), 0);
                throw new OMQException("failed to create native receive ring");
            }
            return handle;
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
    }

    static void recvRingClose(long handle) {
        if (handle == 0) {
            return;
        }
        try {
            RECV_RING_CLOSE.invokeExact(handle);
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
    }

    static long recvRingControlAddress(long handle) {
        return callLong(RECV_RING_CONTROL_ADDR, handle);
    }

    static long recvRingDescAddress(long handle) {
        return callLong(RECV_RING_DESC_ADDR, handle);
    }

    static long recvRingPayloadAddress(long handle) {
        return callLong(RECV_RING_PAYLOAD_ADDR, handle);
    }

    static int recvRingDescCapacity(long handle) {
        return callInt(RECV_RING_DESC_CAPACITY, handle);
    }

    static long recvRingPayloadCapacity(long handle) {
        return callLong(RECV_RING_PAYLOAD_CAPACITY, handle);
    }

    static int recvRingFill(long handle, long timeoutMillis, int maxMessages) {
        try {
            int status = (int) RECV_RING_FILL.invokeExact(handle, timeoutMillis, maxMessages);
            throwStatus(status, handle);
            return status;
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
    }

    static void throwStatus(int status, long ringHandle) {
        if (status == STATUS_OK) {
            return;
        }
        String message = errorMessage(ringHandle);
        if (message.isEmpty()) {
            message = switch (status) {
                case STATUS_TIMEOUT -> "operation timed out";
                case STATUS_CLOSED -> "socket closed";
                case STATUS_INVALID_ENDPOINT -> "invalid endpoint";
                case STATUS_PROTOCOL -> "protocol violation";
                default -> "native OMQ error";
            };
        }
        throw switch (status) {
            case STATUS_TIMEOUT -> new TimeoutException(message);
            case STATUS_CLOSED -> new ClosedException(message);
            case STATUS_INVALID_ENDPOINT -> new InvalidEndpointException(message);
            case STATUS_PROTOCOL -> new ProtocolException(message);
            default -> new OMQException(message);
        };
    }

    private static MethodHandle downcall(String name, FunctionDescriptor descriptor) {
        MemorySegment symbol = LOOKUP.findOrThrow(name);
        return LINKER.downcallHandle(symbol, descriptor);
    }

    private static int lastErrorCode() {
        try {
            return (int) LAST_ERROR_CODE.invokeExact();
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
    }

    private static String errorMessage(long ringHandle) {
        long address;
        try {
            address = ringHandle == 0
                    ? (long) LAST_ERROR_MESSAGE.invokeExact()
                    : (long) RECV_RING_ERROR_MESSAGE.invokeExact(ringHandle);
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
        if (address == 0) {
            return "";
        }
        return MemorySegment.ofAddress(address)
                .reinterpret(MAX_ERROR_STRING)
                .getString(0);
    }

    private static long callLong(MethodHandle handle, long argument) {
        try {
            return (long) handle.invokeExact(argument);
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
    }

    private static int callInt(MethodHandle handle, long argument) {
        try {
            return (int) handle.invokeExact(argument);
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new OMQException("native FFM call failed", error);
        }
    }
}
