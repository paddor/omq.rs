package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class CurveTest {
    @Test
    void keypairShapeAndUniqueness() {
        CurveKeypair first = OMQ.curveKeypair();
        CurveKeypair second = OMQ.curveKeypair();

        assertEquals(40, first.publicKey().length());
        assertEquals(40, first.secretKey().length());
        assertNotEquals(first.publicKey(), second.publicKey());
        assertNotEquals(first.secretKey(), second.secretKey());
    }

    @Test
    void publicKeyDerivesFromSecret() {
        CurveKeypair keypair = OMQ.curveKeypair();

        assertEquals(keypair.publicKey(), OMQ.curvePublic(keypair.secretKey()));
    }

    @Test
    void badSecretKeyIsRejected() {
        assertThrows(OMQException.class, () -> OMQ.curvePublic("not-valid-z85-key"));
    }

    @Test
    void pushPullOverCurve() {
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).curveServer(serverKeypair);
             Socket push = context.socket(SocketType.PUSH)
                     .curveClient(clientKeypair, serverKeypair.publicKey())) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.waitConnected(1, Duration.ofSeconds(5));
            push.send("hello over curve");

            assertEquals("hello over curve", pull.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void reqRepOverCurve() {
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        try (Context context = OMQ.context();
             Socket rep = context.socket(SocketType.REP).curveServer(serverKeypair);
             Socket req = context.socket(SocketType.REQ)
                     .curveClient(clientKeypair, serverKeypair.publicKey())) {
            String endpoint = rep.bind("tcp://127.0.0.1:0");
            req.connect(endpoint);
            req.waitConnected(1, Duration.ofSeconds(5));
            req.send("ping");
            assertEquals("ping", rep.receive(Duration.ofSeconds(5)).orElseThrow().text());
            rep.send("pong");
            assertEquals("pong", req.receive(Duration.ofSeconds(5)).orElseThrow().text());
        }
    }

    @Test
    void curveRejectsWrongServerKey() {
        CurveKeypair serverKeypair = OMQ.curveKeypair();
        CurveKeypair wrongServer = OMQ.curveKeypair();
        CurveKeypair clientKeypair = OMQ.curveKeypair();
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL).curveServer(serverKeypair);
             Socket push = context.socket(SocketType.PUSH)
                     .curveClient(clientKeypair, wrongServer.publicKey())) {
            String endpoint = pull.bind("tcp://127.0.0.1:0");
            push.connect(endpoint);
            push.send("blocked");

            assertTrue(pull.receive(Duration.ofMillis(500)).isEmpty());
        }
    }

    @Test
    void curveRejectsMismatchedKeypair() {
        CurveKeypair first = OMQ.curveKeypair();
        CurveKeypair second = OMQ.curveKeypair();
        CurveKeypair mismatched = new CurveKeypair(first.publicKey(), second.secretKey());
        try (Context context = OMQ.context();
             Socket pull = context.socket(SocketType.PULL)) {
            assertThrows(OMQException.class, () -> pull.curveServer(mismatched));
        }
    }
}
