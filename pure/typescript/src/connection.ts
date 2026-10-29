import {
  decodeCommand,
  type PeerProperties,
  type SocketTypeName,
} from "./command.ts";
import type { PlainAuthOptions } from "./auth.ts";
import {
  initLz4,
  isLz4DictionaryShipment,
  Lz4Decoder,
  Lz4Encoder,
} from "./lz4.ts";
import { Message } from "./message.ts";
import {
  type ConnectionState,
  handleHeartbeat,
  type PeerConnection,
  SendQueue,
  ZmtpHandshake,
} from "./peer_connection.ts";
import {
  decodeZwsFrame,
  encodeCommandFrame,
  encodeDataFrames,
  FLAG_COMMAND,
  FLAG_FINAL,
  FLAG_MORE,
} from "./zws.ts";

export type { ConnectionState } from "./peer_connection.ts";

/** WebTransport-specific connection settings. */
export interface WebTransportOptions {
  /**
   * Accept a server certificate by SHA-256 hash instead of the browser's trust
   * store (W3C `serverCertificateHashes`: ECDSA, at most 14 days validity).
   */
  serverCertificateHashes?: WebTransportHash[];
}

/** Low-level connection options used by `Socket`. */
export interface ConnectionOptions {
  /** Local ZMQ socket type advertised in READY. */
  socketType: SocketTypeName;
  /** Local ZMQ identity bytes advertised in READY. */
  identity: Uint8Array;
  /** Pre-shared LZ4 dictionary sent before compressed messages. */
  lz4Dict?: Uint8Array;
  /** Maximum decompressed message size accepted from the peer. */
  maxMessageSize?: number;
  /** Maximum queued sends before new sends reject. */
  sendHighWaterMark?: number;
  /** WebSocket buffered bytes above which sends wait. */
  sendBufferHighWaterMark?: number;
  /** WebSocket buffered bytes at or below which sends resume. */
  sendBufferLowWaterMark?: number;
  /** Milliseconds between WebSocket bufferedAmount checks. */
  sendBufferPollMs?: number;
  /** Close if WebSocket/ZMTP handshake does not complete in time. */
  handshakeTimeoutMs?: number;
  /** Optional ZMTP PLAIN credentials. */
  plain?: PlainAuthOptions;
  /** WebTransport settings for `https://` endpoints. */
  webTransport?: WebTransportOptions;
  /** WebTransport liveness PING interval. Disabled by default. */
  heartbeatIntervalMs?: number;
  /** WebTransport liveness timeout; defaults to the interval. */
  heartbeatTimeoutMs?: number;
  /** WebTransport close: wait for sent messages to reach the peer. */
  lingerMs?: number;
  /** Called after ZMTP READY/PLAIN handshake succeeds. */
  onReady?: (conn: PeerConnection) => void;
  /** Called for each decoded application message. */
  onMessage?: (conn: PeerConnection, msg: Message) => void;
  /** Called for incoming ZMTP command frames. */
  onCommand?: (conn: PeerConnection, name: string, body: Uint8Array) => void;
  /** Called when the connection closes. */
  onClose?: (conn: PeerConnection) => void;
  /** Called when protocol or transport errors close the connection. */
  onError?: (conn: PeerConnection, error: Error) => void;
}

function errorFromUnknown(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

const DEFAULT_SEND_BUFFER_HIGH_WATER_MARK = 1024 * 1024;
const DEFAULT_SEND_BUFFER_LOW_WATER_MARK = 512 * 1024;
const DEFAULT_SEND_BUFFER_POLL_MS = 4;
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 0;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function eventDataToArrayBuffer(data: unknown): ArrayBuffer {
  if (data instanceof ArrayBuffer) return data;
  if (ArrayBuffer.isView(data)) {
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength,
    ) as ArrayBuffer;
  }
  throw new Error("Expected binary WebSocket message");
}

/** Low-level ZWS/ZMTP connection used by socket classes. */
export class Connection implements PeerConnection {
  /** Original endpoint URL passed to the constructor. */
  readonly url: string;
  private ws: WebSocket | null = null;
  private state: ConnectionState = "connecting";
  private opts: ConnectionOptions;
  private pendingParts: Uint8Array[] = [];
  private lz4Decoder: Lz4Decoder | null = null;
  private lz4Encoder: Lz4Encoder | null = null;
  private useLz4: boolean;
  private closeEmitted = false;
  private handshake: ZmtpHandshake;
  private sends: SendQueue;
  private handshakeTimer: ReturnType<typeof setTimeout> | null = null;
  /** Peer READY properties after the ZMTP handshake completes. */
  peerProperties: PeerProperties | null = null;

  /** Create and start opening a WebSocket connection. */
  constructor(url: string, opts: ConnectionOptions) {
    this.url = url;
    this.opts = opts;
    this.handshake = new ZmtpHandshake(opts, (p) => this.sendCommand(p));
    this.sends = new SendQueue(
      opts.sendHighWaterMark,
      (msg) => this.sendNow(msg),
    );
    this.useLz4 = url.startsWith("lz4+ws://");
    const wsUrl = this.useLz4 ? url.replace(/^lz4\+/, "") : url;
    if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://")) {
      throw new Error(`Unsupported WebSocket URL: ${url}`);
    }

    if (this.useLz4) {
      void this.openLz4WebSocket(wsUrl);
    } else {
      this.openWebSocket(wsUrl);
    }
  }

  private async openLz4WebSocket(wsUrl: string): Promise<void> {
    try {
      await initLz4();
      if (this.state === "closed") return;
      this.lz4Decoder = new Lz4Decoder(this.opts.maxMessageSize);
      this.lz4Encoder = new Lz4Encoder(this.opts.lz4Dict);
      this.openWebSocket(wsUrl);
    } catch (error) {
      this.closeWithError(errorFromUnknown(error).message);
    }
  }

  private openWebSocket(wsUrl: string): void {
    if (this.useLz4) {
      if (!this.lz4Decoder || !this.lz4Encoder) {
        throw new Error("LZ4 not initialized.");
      }
    }

    const ws = new WebSocket(wsUrl, ["ZWS2.0"]);
    ws.binaryType = "arraybuffer";
    ws.onopen = () => this.onOpen();
    ws.onmessage = (ev) => this.onWsMessage(ev);
    ws.onclose = () => this.onWsClose();
    ws.onerror = () => this.onWsError();
    this.ws = ws;
    this.startHandshakeTimer();
  }

  /** Current connection lifecycle state. */
  get connectionState(): ConnectionState {
    return this.state;
  }

  /** Whether the ZMTP handshake has completed. */
  get isReady(): boolean {
    return this.state === "ready";
  }

  private get sendBufferHighWaterMark(): number {
    return this.opts.sendBufferHighWaterMark ??
      DEFAULT_SEND_BUFFER_HIGH_WATER_MARK;
  }

  private get sendBufferLowWaterMark(): number {
    const low = this.opts.sendBufferLowWaterMark ??
      DEFAULT_SEND_BUFFER_LOW_WATER_MARK;
    return Math.min(low, this.sendBufferHighWaterMark);
  }

  private get sendBufferPollMs(): number {
    return this.opts.sendBufferPollMs ?? DEFAULT_SEND_BUFFER_POLL_MS;
  }

  /** Send one application message over this ready connection. */
  send(msg: Message): Promise<void> {
    if (!this.ws || this.state !== "ready") {
      throw new Error("Connection not ready");
    }
    return this.sends.push(msg);
  }

  private async sendNow(msg: Message): Promise<void> {
    if (!this.ws || this.state !== "ready") {
      throw new Error("Connection not ready");
    }

    if (this.useLz4 && this.lz4Encoder) {
      const encoded = this.lz4Encoder.encodeMessage(
        msg.parts.map((p) => p),
      );
      for (const wireMsg of encoded) {
        const frames = encodeDataFrames({ parts: wireMsg });
        for (const frame of frames) {
          const wait = this.waitForWritable();
          if (wait) await wait;
          this.ws.send(frame);
        }
      }
    } else {
      const frames = encodeDataFrames(msg);
      for (const frame of frames) {
        const wait = this.waitForWritable();
        if (wait) await wait;
        this.ws.send(frame);
      }
    }
  }

  private waitForWritable(): Promise<void> | null {
    if (
      !this.ws ||
      this.state !== "ready" ||
      this.ws.bufferedAmount <= this.sendBufferHighWaterMark
    ) {
      return null;
    }

    return this.waitForLowSendBuffer();
  }

  private async waitForLowSendBuffer(): Promise<void> {
    do {
      await delay(this.sendBufferPollMs);
      if (!this.ws || this.state !== "ready") {
        throw new Error("Connection not ready");
      }
    } while (this.ws.bufferedAmount > this.sendBufferLowWaterMark);
  }

  /** Send a raw ZMTP command payload. */
  sendCommand(payload: Uint8Array): void {
    if (!this.ws) throw new Error("Connection closed");
    this.ws.send(encodeCommandFrame(payload));
  }

  /** Close the WebSocket and release compression state. */
  close(): void {
    this.pendingParts = [];
    this.clearHandshakeTimer();
    this.lz4Decoder?.free();
    this.lz4Decoder = null;
    this.lz4Encoder?.free();
    this.lz4Encoder = null;
    this.sends.rejectQueued(new Error("Connection closed"));
    const ws = this.ws;
    this.state = "closed";
    this.ws = null;
    if (ws) ws.close();
    this.onWsClose();
  }

  private onOpen(): void {
    this.state = "handshaking";
    this.handshake.start();
  }

  private onWsMessage(ev: MessageEvent): void {
    try {
      this.handleWsMessage(ev);
    } catch (error) {
      this.closeWithError(errorFromUnknown(error).message);
    }
  }

  private handleWsMessage(ev: MessageEvent): void {
    if (this.state === "closed") return;

    const frame = decodeZwsFrame(eventDataToArrayBuffer(ev.data));

    if (this.state === "handshaking") {
      if (frame.flag !== FLAG_COMMAND) {
        this.closeWithError("Expected command during handshake");
        return;
      }
      const props = this.handshake.handle(decodeCommand(frame.payload));
      if (props) this.completeHandshake(props);
      return;
    }

    if (frame.flag === FLAG_COMMAND) {
      this.handleCommand(frame.payload);
      return;
    }

    if (
      this.useLz4 && isLz4DictionaryShipment(frame.payload) &&
      frame.flag === FLAG_MORE
    ) {
      this.closeWithError("LZ4D dictionary shipment must be single-part");
      return;
    }

    if (frame.flag === FLAG_MORE) {
      this.pendingParts.push(frame.payload);
      return;
    }

    if (frame.flag === FLAG_FINAL) {
      this.pendingParts.push(frame.payload);
      const rawParts = this.pendingParts;
      this.pendingParts = [];

      let parts: Uint8Array[];
      if (this.useLz4 && this.lz4Decoder) {
        const decoded = this.lz4Decoder.decodeMessage(rawParts);
        if (decoded === null) return;
        parts = decoded;
      } else {
        if (this.opts.maxMessageSize !== undefined) {
          let total = 0;
          for (const part of rawParts) {
            total += part.byteLength;
            if (total > this.opts.maxMessageSize) {
              this.closeWithError(
                `Message size ${total} exceeds max ${this.opts.maxMessageSize}`,
              );
              return;
            }
          }
        }
        parts = rawParts;
      }

      this.opts.onMessage?.(this, Message.fromParts(parts));
    }
  }

  private onWsClose(): void {
    if (this.closeEmitted) return;
    this.closeEmitted = true;
    this.state = "closed";
    this.ws = null;
    this.pendingParts = [];
    this.clearHandshakeTimer();
    this.opts.onClose?.(this);
  }

  private onWsError(): void {
    this.closeWithError(`WebSocket error on ${this.url}`);
  }

  private closeWithError(reason: string): void {
    if (this.state === "closed") return;
    this.opts.onError?.(this, new Error(reason));
    this.close();
  }

  private handleCommand(payload: Uint8Array): void {
    const cmd = decodeCommand(payload);
    if (handleHeartbeat(cmd, (p) => this.sendCommand(p))) return;
    this.opts.onCommand?.(this, cmd.name, cmd.body);
  }

  private completeHandshake(props: PeerProperties): void {
    this.peerProperties = props;
    this.state = "ready";
    this.clearHandshakeTimer();
    this.opts.onReady?.(this);
  }

  private startHandshakeTimer(): void {
    const timeout = this.opts.handshakeTimeoutMs ??
      DEFAULT_HANDSHAKE_TIMEOUT_MS;
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
