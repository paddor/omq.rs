package io.omq;

import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.Arrays;

/** Owning sender identity. Equality distinguishes separate admissions of the same routing bytes. */
public final class Identity {
    private static final Cleaner CLEANER = Cleaner.create();
    private final long handle;
    private final byte[] bytes;
    private volatile PeerInfo peerInfo;

    private Identity(long handle, byte[] bytes) {
        this.handle = handle;
        this.bytes = bytes;
        CLEANER.register(this, new Release(handle));
    }

    /** Returns routing bytes. Sending to them may reach a replacement peer. */
    public byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /** Returns the immutable admission snapshot, even after the socket closes. */
    public PeerInfo peerInfo() {
        PeerInfo info = peerInfo;
        if (info == null) {
            try {
                info = Native.identityPeerInfo(handle);
                peerInfo = info;
            } finally {
                Reference.reachabilityFence(this);
            }
        }
        return info;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Identity identity)) {
            return false;
        }
        try {
            return Native.identityEquals(handle, identity.handle);
        } finally {
            Reference.reachabilityFence(this);
            Reference.reachabilityFence(identity);
        }
    }

    @Override
    public int hashCode() {
        try {
            return Native.identityHash(handle);
        } finally {
            Reference.reachabilityFence(this);
        }
    }

    private record Release(long handle) implements Runnable {
        @Override
        public void run() {
            Native.identityRelease(handle);
        }
    }
}
