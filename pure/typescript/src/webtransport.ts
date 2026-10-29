import {
  decodeCommand,
  decodeErrorReason,
  decodeReadyProperties,
  encodePlainHello,
  encodePlainInitiate,
  encodePong,
  encodeReady,
  isCompatibleSocketType,
  isSocketTypeName,
  type PeerProperties,
} from "./command.ts";
import type { ConnectionOptions } from "./connection.ts";
import { Message } from "./message.ts";
import type { ConnectionState, PeerConnection } from "./peer_connection.ts";
import {
  encodeCommandFrame,
  encodeGreeting,
  encodeMessage,
  type PeerGreeting,
  ZmtpDecoder,
  type ZmtpEvent,
} from "./zmtp.ts";

/** Role preface of the ZMTP data stream. */
export const DATA_PREFACE = new Uint8Array([
  0x4f,
  0x4d,
  0x51,
  0x44,
  1,
  0,
  0,
  0,
]);
/** Role preface of the carrier liveness stream. */
export const LIVENESS_PREFACE = new Uint8Array([
  0x4f,
  0x4d,
  0x51,
  0x4c,
  1,
  0,
  0,
  0,
]);
const RECORD_LENGTH = 9;
const PING = 0x01;
const PONG = 0x02;

/** Require the browser's WebTransport URL scheme. */
export function webTransportUrl(url: string): string {
  if (url.startsWith("https://")) return url;
  throw new Error(`Unsupported WebTransport URL: ${url}`);
}

/** Whether `url` selects the WebTransport carrier. */
export function isWebTransportUrl(url: string): boolean {
  return url.startsWith("https://");
}

function errorFromUnknown(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a, 0);
  out.set(b, a.byteLength);
  return out;
}

function encodeRecord(kind: number, value: bigint): Uint8Array<ArrayBuffer> {
  const record = new Uint8Array(RECORD_LENGTH);
  record[0] = kind;
  new DataView(record.buffer).setBigUint64(1, value, false);
  return record;
}

interface QueuedSend {
  msg: Message;
  resolve: () => void;
  reject: (error: Error) => void;
}

type PlainState = "awaiting-welcome" | "awaiting-ready";

/**
 * ZMTP 3.1 over one WebTransport session: an ordered data stream carrying
 * unchanged ZMTP frames and a liveness stream carrying PING/PONG records.
 * Both streams start with an 8-byte role preface. See the omq.rs QUIC RFC.
 */
export class WebTransportConnection implements PeerConnection {
  readonly url: string;
  peerProperties: PeerProperties | null = null;
  private state: ConnectionState = "connecting";
  private transport: WebTransport | null = null;
  private dataWriter: WritableStreamDefaultWriter<Uint8Array> | null = null;
  private livenessWriter: WritableStreamDefaultWriter<Uint8Array> | null = null;
  private readers: ReadableStreamDefaultReader<Uint8Array>[] = [];
  private decoder: ZmtpDecoder;
  private plainState: PlainState | null = null;
  private sendQueue: QueuedSend[] = [];
  private sending = false;
  private queuedSends = 0;
  private closeEmitted = false;
  private timers: ReturnType<typeof setTimeout>[] = [];
  private pingNonce = 0n;
  private lastRecordAt = 0;
  private pongPending: bigint | null = null;
  private pongWriting = false;

  constructor(url: string, private readonly opts: ConnectionOptions) {
    this.url = url;
    this.decoder = new ZmtpDecoder({ maxMessageSize: opts.maxMessageSize });
    if (typeof WebTransport === "undefined") {
      throw new Error("WebTransport is not available in this runtime");
    }
    void this.open(webTransportUrl(url)).catch((error) =>
      this.closeWithError(errorFromUnknown(error).message)
    );
    this.startHandshakeTimer();
  }

  get connectionState(): ConnectionState {
    return this.state;
  }

  get isReady(): boolean {
    return this.state === "ready";
  }

  /** Re-read after awaits; the state can change while suspended. */
  private get isClosed(): boolean {
    return this.state === "closed";
  }

  private async open(url: string): Promise<void> {
    const transport = new WebTransport(url, {
      allowPooling: false,
      requireUnreliable: true,
      serverCertificateHashes: this.opts.webTransport?.serverCertificateHashes,
    });
    this.transport = transport;
    transport.closed.then(
      () => this.onTransportClosed(),
      (error) => this.closeWithError(errorFromUnknown(error).message),
    );
    await transport.ready;
    if (this.isClosed) return;
    const data = await transport.createBidirectionalStream();
    const liveness = await transport.createBidirectionalStream();
    if (this.isClosed) return;
    this.dataWriter = data.writable.getWriter();
    this.livenessWriter = liveness.writable.getWriter();
    const mechanism = this.opts.plain ? "PLAIN" : "NULL";
    // Write both role prefaces at once; never wait for the peer first.
    void this.dataWriter
      .write(concat(DATA_PREFACE, encodeGreeting(mechanism)))
      .catch(() => {});
    void this.livenessWriter.write(LIVENESS_PREFACE).catch(() => {});
    this.state = "handshaking";
    this.lastRecordAt = Date.now();
    void this.readData(data.readable.getReader());
    void this.readLiveness(liveness.readable.getReader());
    this.startHeartbeat();
  }

  private async readData(reader: ReadableStreamDefaultReader<Uint8Array>) {
    this.readers.push(reader);
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          // FIN ends this generation; an incomplete message is dropped.
          this.closeWithError("Peer finished the data stream");
          return;
        }
        for (const event of this.decoder.push(value)) this.handleEvent(event);
        if (this.isClosed) return;
      }
    } catch (error) {
      this.closeWithError(errorFromUnknown(error).message);
    }
  }

  private handleEvent(event: ZmtpEvent): void {
    if (event.type === "greeting") {
      this.onGreeting(event.greeting);
    } else if (event.type === "command") {
      const cmd = decodeCommand(event.payload);
      if (this.state === "handshaking") this.handleHandshakeCommand(cmd);
      else this.handleCommand(cmd);
    } else if (this.state === "ready") {
      this.opts.onMessage?.(this, Message.fromParts(event.parts));
    } else {
      throw new Error("Message before ZMTP handshake completed");
    }
  }

  private onGreeting(greeting: PeerGreeting): void {
    const expected = this.opts.plain ? "PLAIN" : "NULL";
    if (greeting.mechanism !== expected) {
      throw new Error(
        `Peer mechanism ${greeting.mechanism}, expected ${expected}`,
      );
    }
    if (this.opts.plain) {
      if (!greeting.asServer) throw new Error("PLAIN peer is not a server");
      this.plainState = "awaiting-welcome";
      this.sendCommand(
        encodePlainHello(this.opts.plain.username, this.opts.plain.password),
      );
    } else {
      this.sendCommand(encodeReady(this.opts.socketType, this.opts.identity));
    }
  }

  private handleHandshakeCommand(cmd: { name: string; body: Uint8Array }) {
    if (cmd.name === "ERROR") {
      throw new Error(`Peer sent ERROR: ${decodeErrorReason(cmd.body)}`);
    }
    if (this.plainState === "awaiting-welcome") {
      if (cmd.name !== "WELCOME" || cmd.body.byteLength !== 0) {
        throw new Error(`Expected empty WELCOME, got ${cmd.name}`);
      }
      this.plainState = "awaiting-ready";
      this.sendCommand(
        encodePlainInitiate(this.opts.socketType, this.opts.identity),
      );
      return;
    }
    if (cmd.name !== "READY") {
      throw new Error(`Expected READY, got ${cmd.name}`);
    }
    this.completeHandshake(cmd.body);
  }

  private completeHandshake(body: Uint8Array): void {
    this.peerProperties = decodeReadyProperties(body);
    const peerType = this.peerProperties.socketType;
    if (!peerType || !isSocketTypeName(peerType)) {
      throw new Error(`Invalid peer socket type: ${peerType}`);
    }
    if (!isCompatibleSocketType(this.opts.socketType, peerType)) {
      throw new Error(
        `Incompatible socket types: ours=${this.opts.socketType} peer=${peerType}`,
      );
    }
    this.plainState = null;
    this.state = "ready";
    this.clearHandshakeTimer();
    this.opts.onReady?.(this);
  }

  private handleCommand(cmd: { name: string; body: Uint8Array }): void {
    if (cmd.name === "PING") {
      // ZMTP PING stays valid on the data stream; answer it in order.
      if (cmd.body.byteLength < 2) throw new Error("PING body missing TTL");
      const context = cmd.body.subarray(2);
      if (context.byteLength > 16) throw new Error("PING context too long");
      this.sendCommand(encodePong(context));
      return;
    }
    if (cmd.name === "PONG") return;
    this.opts.onCommand?.(this, cmd.name, cmd.body);
  }

  private async readLiveness(reader: ReadableStreamDefaultReader<Uint8Array>) {
    this.readers.push(reader);
    let buffer = new Uint8Array(0);
    let prefaceDone = false;
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          this.closeWithError("Peer finished the liveness stream");
          return;
        }
        buffer = concat(buffer, value);
        for (;;) {
          const need = prefaceDone ? RECORD_LENGTH : LIVENESS_PREFACE.length;
          if (buffer.byteLength < need) break;
          const unit = buffer.subarray(0, need);
          buffer = buffer.slice(need);
          this.lastRecordAt = Date.now();
          if (!prefaceDone) {
            if (!unit.every((byte, i) => byte === LIVENESS_PREFACE[i])) {
              throw new Error("Invalid liveness preface");
            }
            prefaceDone = true;
            continue;
          }
          const value64 = new DataView(unit.buffer, unit.byteOffset)
            .getBigUint64(1, false);
          if (unit[0] === PING) this.queuePong(value64);
          else if (unit[0] !== PONG) throw new Error("Invalid liveness record");
        }
      }
    } catch (error) {
      this.closeWithError(errorFromUnknown(error).message);
    }
  }

  /** Answer only the newest unanswered PING; at most one write in flight. */
  private queuePong(nonce: bigint): void {
    this.pongPending = nonce;
    if (this.pongWriting) return;
    void this.flushPongs();
  }

  private async flushPongs(): Promise<void> {
    this.pongWriting = true;
    try {
      while (this.pongPending !== null && this.livenessWriter) {
        const nonce = this.pongPending;
        this.pongPending = null;
        await this.livenessWriter.write(encodeRecord(PONG, nonce));
      }
    } catch {
      // Transport failure surfaces through the closed promise.
    } finally {
      this.pongWriting = false;
    }
  }

  private startHeartbeat(): void {
    const interval = this.opts.heartbeatIntervalMs;
    if (!interval || interval <= 0) return;
    const timeout = this.opts.heartbeatTimeoutMs ?? interval;
    const tick = () => {
      if (this.state === "closed") return;
      if (Date.now() - this.lastRecordAt > timeout) {
        this.closeWithError(`Liveness timeout on ${this.url}`);
        return;
      }
      const writer = this.livenessWriter;
      // Skip a PING while the previous record is still being written.
      if (writer && (writer.desiredSize ?? 0) > 0) {
        this.pingNonce += 1n;
        void writer.write(encodeRecord(PING, this.pingNonce)).catch(() => {});
      }
      this.timers.push(setTimeout(tick, interval));
    };
    this.timers.push(setTimeout(tick, interval));
  }

  send(msg: Message): Promise<void> {
    if (this.state !== "ready" || !this.dataWriter) {
      throw new Error("Connection not ready");
    }
    if (
      this.opts.sendHighWaterMark !== undefined &&
      this.queuedSends >= this.opts.sendHighWaterMark
    ) {
      return Promise.reject(new Error("Send high water mark reached"));
    }
    return new Promise((resolve, reject) => {
      this.queuedSends++;
      this.sendQueue.push({ msg, resolve, reject });
      this.drainSendQueue();
    });
  }

  private drainSendQueue(): void {
    if (this.sending) return;
    const item = this.sendQueue.shift();
    if (!item) return;
    this.sending = true;
    this.sendNow(item.msg)
      .then(item.resolve, item.reject)
      .finally(() => {
        this.queuedSends--;
        this.sending = false;
        this.drainSendQueue();
      });
  }

  /** One contiguous write per message; waits for stream backpressure. */
  private async sendNow(msg: Message): Promise<void> {
    const writer = this.dataWriter;
    if (this.state !== "ready" || !writer) {
      throw new Error("Connection not ready");
    }
    await writer.ready;
    void writer.write(encodeMessage(msg.parts)).catch(() => {});
  }

  sendCommand(payload: Uint8Array): void {
    if (!this.dataWriter) throw new Error("Connection closed");
    void this.dataWriter.write(encodeCommandFrame(payload)).catch(() => {});
  }

  close(): void {
    const queued = this.sendQueue.splice(0);
    this.queuedSends -= queued.length;
    const error = new Error("Connection closed");
    for (const item of queued) item.reject(error);
    for (const timer of this.timers) clearTimeout(timer);
    this.timers = [];
    this.clearHandshakeTimer();
    for (const reader of this.readers) void reader.cancel().catch(() => {});
    this.readers = [];
    this.dataWriter = null;
    this.livenessWriter = null;
    const transport = this.transport;
    this.transport = null;
    this.state = "closed";
    try {
      transport?.close({ closeCode: 0, reason: "" });
    } catch {
      // Already closed.
    }
    this.onTransportClosed();
  }

  private onTransportClosed(): void {
    if (this.closeEmitted) return;
    this.closeEmitted = true;
    if (this.state !== "closed") {
      this.state = "closed";
      for (const timer of this.timers) clearTimeout(timer);
      this.timers = [];
      this.clearHandshakeTimer();
    }
    this.opts.onClose?.(this);
  }

  private closeWithError(reason: string): void {
    if (this.state === "closed") return;
    this.opts.onError?.(this, new Error(reason));
    this.close();
  }

  private handshakeTimer: ReturnType<typeof setTimeout> | null = null;

  private startHandshakeTimer(): void {
    const timeout = this.opts.handshakeTimeoutMs ?? 0;
    if (timeout <= 0) return;
    this.handshakeTimer = setTimeout(() => {
      this.closeWithError(`Handshake timeout on ${this.url}`);
    }, timeout);
  }

  private clearHandshakeTimer(): void {
    if (this.handshakeTimer === null) return;
    clearTimeout(this.handshakeTimer);
    this.handshakeTimer = null;
  }
}
