package io.omq;

public final class OMQ {
    private OMQ() {
    }

    public static Context context() {
        return Context.open();
    }

    public static Context context(int ioThreads) {
        return Context.open(ioThreads);
    }

    public static CurveKeypair curveKeypair() {
        String[] keypair = Native.curveKeypair();
        return new CurveKeypair(keypair[0], keypair[1]);
    }

    public static String curvePublic(String secretKey) {
        return Native.curvePublic(secretKey);
    }
}
