import type { PlainAuthOptions } from "./auth.ts";
import {
  decodeErrorReason,
  decodeReadyProperties,
  encodePlainHello,
  encodePlainInitiate,
  encodePong,
  encodeReady,
  isCompatibleSocketType,
  isSocketTypeName,
  type PeerProperties,
  type SocketTypeName,
} from "./command.ts";
import type { Message } from "./message.ts";

/** Lifecycle states shared by WebSocket and WebTransport connections. */
export type ConnectionState = "connecting" | "handshaking" | "ready" | "closed";

/**
 * One peer connection as seen by socket classes. `Connection` (ZWS over
 * WebSocket) and `WebTransportConnection` (ZMTP over WebTransport) both
 * implement it; routing, auth, and reconnect logic stay transport-neutral.
 */
export interface PeerConnection {
  /** Original endpoint URL passed to the constructor. */
  readonly url: string;
  /** Current connection lifecycle state. */
  readonly connectionState: ConnectionState;
  /** Whether the ZMTP handshake has completed. */
  readonly isReady: boolean;
  /** Peer READY properties after the ZMTP handshake completes. */
  peerProperties: PeerProperties | null;
  /** Send one application message over this ready connection. */
  send(msg: Message): Promise<void>;
  /** Send a raw ZMTP command payload (name length, name, data). */
  sendCommand(payload: Uint8Array): void;
  /** Close the connection and release its resources. */
  close(): void;
}

// The pieces below are shared by both carriers. They throw on protocol
// violations; carriers turn a throw into an error close.

/** A decoded ZMTP command. */
export interface Command {
  name: string;
  body: Uint8Array;
}

/** Local settings the client handshake advertises. */
export interface HandshakeOptions {
  socketType: SocketTypeName;
  identity: Uint8Array;
  plain?: PlainAuthOptions;
}

type HandshakeState = "idle" | "awaiting-welcome" | "awaiting-ready";

/** Client side of the ZMTP NULL or PLAIN mechanism handshake. */
export class ZmtpHandshake {
  private state: HandshakeState = "idle";

  constructor(
    private readonly opts: HandshakeOptions,
    private readonly sendCommand: (payload: Uint8Array) => void,
  ) {}

  /** Send HELLO for PLAIN or READY for NULL. */
  start(): void {
    const { plain, socketType, identity } = this.opts;
    if (plain) {
      this.state = "awaiting-welcome";
      this.sendCommand(encodePlainHello(plain.username, plain.password));
    } else {
      this.state = "awaiting-ready";
      this.sendCommand(encodeReady(socketType, identity));
    }
  }

  /** Feed one handshake command; returns the peer's READY properties. */
  handle(cmd: Command): PeerProperties | null {
    if (cmd.name === "ERROR") {
      throw new Error(`Peer sent ERROR: ${decodeErrorReason(cmd.body)}`);
    }
    if (this.state === "awaiting-welcome") {
      if (cmd.name !== "WELCOME") {
        throw new Error(`Expected WELCOME, got ${cmd.name}`);
      }
      if (cmd.body.byteLength !== 0) {
        throw new Error("WELCOME body must be empty");
      }
      this.state = "awaiting-ready";
      this.sendCommand(
        encodePlainInitiate(this.opts.socketType, this.opts.identity),
      );
      return null;
    }
    if (this.state === "idle") throw new Error("Handshake not started");
    if (cmd.name !== "READY") {
      throw new Error(`Expected READY, got ${cmd.name}`);
    }
    this.state = "idle";
    return this.acceptReady(cmd.body);
  }

  private acceptReady(body: Uint8Array): PeerProperties {
    const props = decodeReadyProperties(body);
    const peerType = props.socketType;
    if (!peerType) throw new Error("READY missing Socket-Type");
    if (!isSocketTypeName(peerType)) {
      throw new Error(`Unknown peer socket type: ${peerType}`);
    }
    if (!isCompatibleSocketType(this.opts.socketType, peerType)) {
      throw new Error(
        `Incompatible socket types: ours=${this.opts.socketType} peer=${peerType}`,
      );
    }
    return props;
  }
}

/**
 * Answer a ZMTP PING and validate a PONG. Returns false for other commands,
 * which belong to the socket.
 */
export function handleHeartbeat(
  cmd: Command,
  sendCommand: (payload: Uint8Array) => void,
): boolean {
  if (cmd.name === "PING") {
    if (cmd.body.byteLength < 2) throw new Error("PING body missing TTL");
    const context = cmd.body.subarray(2);
    if (context.byteLength > 16) throw new Error("PING context too long");
    sendCommand(encodePong(context));
    return true;
  }
  if (cmd.name === "PONG") {
    if (cmd.body.byteLength > 16) throw new Error("PONG context too long");
    return true;
  }
  return false;
}

interface QueuedSend {
  msg: Message;
  resolve: () => void;
  reject: (error: Error) => void;
}

/**
 * Writes queued messages in order. `batchLength` picks how many leading
 * queued messages one `write` carries; it never waits for more to arrive.
 * A send resolves when the write carrying it resolves, that is, once the
 * carrier accepted the whole message.
 */
export class SendQueue {
  private items: QueuedSend[] = [];
  private sending = false;
  /** Queued plus in-flight sends, bounded by the high water mark. */
  private pending = 0;

  constructor(
    private readonly highWaterMark: number | undefined,
    private readonly write: (msgs: Message[]) => Promise<void>,
    private readonly batchLength?: (queued: Iterable<Message>) => number,
  ) {}

  push(msg: Message): Promise<void> {
    if (
      this.highWaterMark !== undefined && this.pending >= this.highWaterMark
    ) {
      return Promise.reject(new Error("Send high water mark reached"));
    }
    return new Promise((resolve, reject) => {
      this.pending++;
      this.items.push({ msg, resolve, reject });
      this.drain();
    });
  }

  /** Reject sends that have not started writing. */
  rejectQueued(error: Error): void {
    const items = this.items.splice(0);
    this.pending -= items.length;
    for (const item of items) item.reject(error);
  }

  private drain(): void {
    if (this.sending || this.items.length === 0) return;
    const count = Math.max(1, this.batchLength?.(this.queuedMessages()) ?? 1);
    const batch = this.items.splice(0, count);
    this.sending = true;
    this.write(batch.map((item) => item.msg))
      .then(
        () => batch.forEach((item) => item.resolve()),
        (error) => batch.forEach((item) => item.reject(error)),
      )
      .finally(() => {
        this.pending -= batch.length;
        this.sending = false;
        this.drain();
      });
  }

  private *queuedMessages(): Iterable<Message> {
    for (const item of this.items) yield item.msg;
  }
}
