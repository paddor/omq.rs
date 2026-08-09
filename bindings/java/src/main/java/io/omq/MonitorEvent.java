package io.omq;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** Immutable event emitted by a native socket monitor. */
public final class MonitorEvent {
    private final MonitorEventType type;
    private final String endpoint;
    private final PeerInfo peer;
    private final String peerIdent;
    private final long connectionId;
    private final String reason;
    private final long retryMillis;
    private final int attempt;
    private final byte[] data;
    private final String commandName;
    private final byte[] commandBody;

    MonitorEvent(
            String type,
            String endpoint,
            PeerInfo peer,
            String peerIdent,
            long connectionId,
            String reason,
            long retryMillis,
            int attempt,
            byte[] data,
            String commandName,
            byte[] commandBody) {
        this.type = MonitorEventType.valueOf(type);
        this.endpoint = endpoint;
        this.peer = peer;
        this.peerIdent = peerIdent;
        this.connectionId = connectionId;
        this.reason = reason;
        this.retryMillis = retryMillis;
        this.attempt = attempt;
        this.data = data == null ? null : Arrays.copyOf(data, data.length);
        this.commandName = commandName;
        this.commandBody = commandBody == null ? null : Arrays.copyOf(commandBody, commandBody.length);
    }

    /** Returns the event type. */
    public MonitorEventType type() {
        return type;
    }

    /** Returns the endpoint related to this event, when present. */
    public Optional<String> endpoint() {
        return Optional.ofNullable(endpoint);
    }

    /** Returns peer metadata, when this event carries it. */
    public Optional<PeerInfo> peer() {
        return Optional.ofNullable(peer);
    }

    /** Returns transport peer identity before handshake, when present. */
    public Optional<String> peerIdent() {
        return Optional.ofNullable(peerIdent);
    }

    /** Returns native connection id, when present. */
    public OptionalLong connectionId() {
        return connectionId < 0 ? OptionalLong.empty() : OptionalLong.of(connectionId);
    }

    /** Returns failure or disconnect reason, when present. */
    public Optional<String> reason() {
        return Optional.ofNullable(reason);
    }

    /** Returns reconnect delay, when present. */
    public Optional<Duration> retryDelay() {
        return retryMillis < 0 ? Optional.empty() : Optional.of(Duration.ofMillis(retryMillis));
    }

    /** Returns reconnect attempt number, when present. */
    public OptionalInt attempt() {
        return attempt < 0 ? OptionalInt.empty() : OptionalInt.of(attempt);
    }

    /** Returns prefix, group, or command payload bytes, when present. */
    public Optional<byte[]> data() {
        return data == null ? Optional.empty() : Optional.of(Arrays.copyOf(data, data.length));
    }

    /** Returns peer command name, when present. */
    public Optional<String> commandName() {
        return Optional.ofNullable(commandName);
    }

    /** Returns peer command body bytes, when present. */
    public Optional<byte[]> commandBody() {
        return commandBody == null ? Optional.empty() : Optional.of(Arrays.copyOf(commandBody, commandBody.length));
    }
}
