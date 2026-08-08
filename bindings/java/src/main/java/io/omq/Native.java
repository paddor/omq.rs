package io.omq;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

final class Native {
    static {
        load();
    }

    private Native() {
    }

    static native void asyncTaskCancel(long handle);

    static native long contextCreate(int ioThreads);

    static native void contextClose(long handle);

    static native String[] curveKeypair();

    static native String curvePublic(String secretKey);

    static native long receiveAnyAsync(Socket[] sockets, long[] handles, Object future);

    static native long socketCreate(long contextHandle, int socketType);

    static native void socketClose(long handle);

    static native String socketBind(long handle, String endpoint);

    static native void socketConnect(long handle, String endpoint);

    static native void socketUnbind(long handle, String endpoint);

    static native void socketDisconnect(long handle, String endpoint);

    static native void socketSend(long handle, byte[] data);

    static native void socketSendMultipart(long handle, byte[][] parts);

    static native void socketSendMany(long handle, byte[][] messages);

    static native int socketTrySendMultipart(long handle, byte[][] parts);

    static native long socketSendAsync(long handle, byte[][] parts, Object future);

    static native Object socketRecv(long handle, long timeoutMillis);

    static native Object[] socketRecvMany(long handle, int maxMessages, long timeoutMillis);

    static native int socketRecvManyBytesInto(
            long handle, byte[][] out, int offset, int maxMessages, long timeoutMillis);

    static native long socketRecvAsync(long handle, long timeoutMillis, Object future);

    static native void socketSubscribe(long handle, byte[] prefix);

    static native void socketUnsubscribe(long handle, byte[] prefix);

    static native void socketJoin(long handle, byte[] group);

    static native void socketLeave(long handle, byte[] group);

    static native int socketWaitConnected(long handle, int minPeers, long timeoutMillis);

    static native long socketWaitSubscribed(long handle, long minSubscriptions, long timeoutMillis);

    static native void socketSetLinger(long handle, long millis);

    static native void socketSetIdentity(long handle, byte[] identity);

    static native void socketSetSendHighWaterMark(long handle, int hwm);

    static native void socketSetReceiveHighWaterMark(long handle, int hwm);

    static native void socketSetHeartbeatInterval(long handle, long millis);

    static native void socketSetHandshakeTimeout(long handle, long millis);

    static native void socketSetMaxMessageSize(long handle, long size);

    static native void socketSetCompressionAutoTrain(long handle, int enabled);

    static native void socketSetCompressionThreshold(long handle, long threshold);

    static native void socketSetCompressionLevel(long handle, int level);

    static native void socketSetPlainServer(long handle, String username, String password);

    static native void socketSetPlainClient(long handle, String username, String password);

    static native void socketSetCurveServer(long handle, String publicKey, String secretKey);

    static native void socketSetCurveClient(
            long handle, String publicKey, String secretKey, String serverPublicKey);

    private static void load() {
        try {
            System.loadLibrary("omq_java");
            return;
        } catch (UnsatisfiedLinkError first) {
            try {
                loadFromResource();
                return;
            } catch (IOException | UnsatisfiedLinkError ignored) {
                throw first;
            }
        }
    }

    private static void loadFromResource() throws IOException {
        String library = System.mapLibraryName("omq_java");
        String resource = "/io/omq/native/" + platform() + "/" + library;
        try (InputStream input = Native.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new UnsatisfiedLinkError("native library resource not found: " + resource);
            }
            Path temp = Files.createTempFile("omq-java-", "-" + library);
            temp.toFile().deleteOnExit();
            Files.copy(input, temp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            System.load(temp.toAbsolutePath().toString());
        }
    }

    private static String platform() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            os = "windows";
        } else if (os.contains("mac") || os.contains("darwin")) {
            os = "macos";
        } else if (os.contains("linux")) {
            os = "linux";
        }
        if (arch.equals("x86_64") || arch.equals("amd64")) {
            arch = "x86_64";
        } else if (arch.equals("aarch64") || arch.equals("arm64")) {
            arch = "aarch64";
        }
        return os + "-" + arch;
    }
}
