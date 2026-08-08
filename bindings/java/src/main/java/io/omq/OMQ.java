package io.omq;

/** Static entry points for OMQ.java. */
public final class OMQ {
    private OMQ() {
    }

    /** Opens a context with one native I/O thread. */
    public static Context context() {
        return Context.open();
    }

    /** Opens a context with the requested native I/O thread count. */
    public static Context context(int ioThreads) {
        return Context.open(ioThreads);
    }

    /** Generates a CURVE Z85 public/secret key pair in native OMQ. */
    public static CurveKeypair curveKeypair() {
        String[] keypair = Native.curveKeypair();
        return new CurveKeypair(keypair[0], keypair[1]);
    }

    /** Derives the CURVE Z85 public key for a Z85 secret key. */
    public static String curvePublic(String secretKey) {
        return Native.curvePublic(secretKey);
    }
}
