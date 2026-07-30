import {
  decodeCommand,
  decodeReadyProperties,
  encodePong,
  encodeReady,
  isCompatibleSocketType,
  isSocketTypeName,
  type PeerProperties,
  type SocketTypeName,
} from "./command.ts";
import { isLz4DictionaryShipment, Lz4Decoder, Lz4Encoder } from "./lz4.ts";
import { Message } from "./message.ts";
import {
  decodeZwsFrame,
  encodeCommandFrame,
  encodeDataFrames,
  FLAG_COMMAND,
  FLAG_FINAL,
  FLAG_MORE,
} from "./zws.ts";

export type ConnectionState = "connecting" | "handshaking" | "ready" | "closed";

export interface ConnectionOptions {
  socketType: SocketTypeName;
  identity: Uint8Array;
  lz4Dict?: Uint8Array;
  maxMessageSize?: number;
  onReady?: (conn: Connection) => void;
  onMessage?: (conn: Connection, msg: Message) => void;
  onCommand?: (conn: Connection, name: string, body: Uint8Array) => void;
  onClose?: (conn: Connection) => void;
  onError?: (conn: Connection, error: Error) => void;
}

function errorFromUnknown(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
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

export class Connection {
  readonly url: string;
  private ws: WebSocket | null = null;
  private state: ConnectionState = "connecting";
  private opts: ConnectionOptions;
  private pendingParts: Uint8Array[] = [];
  private lz4Decoder: Lz4Decoder | null = null;
  private lz4Encoder: Lz4Encoder | null = null;
  private useLz4: boolean;
  private closeEmitted = false;
  peerProperties: PeerProperties | null = null;

  constructor(url: string, opts: ConnectionOptions) {
    this.url = url;
    this.opts = opts;
    this.useLz4 = url.startsWith("lz4+");
    const wsUrl = this.useLz4 ? url.replace(/^lz4\+/, "") : url;
    if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://")) {
      throw new Error(`Unsupported WebSocket URL: ${url}`);
    }

    if (this.useLz4) {
      this.lz4Decoder = new Lz4Decoder(opts.maxMessageSize);
      this.lz4Encoder = new Lz4Encoder(opts.lz4Dict);
    }

    const ws = new WebSocket(wsUrl, ["ZWS2.0/NULL"]);
    ws.binaryType = "arraybuffer";
    ws.onopen = () => this.onOpen();
    ws.onmessage = (ev) => this.onWsMessage(ev);
    ws.onclose = () => this.onWsClose();
    ws.onerror = () => this.onWsError();
    this.ws = ws;
  }

  get connectionState(): ConnectionState {
    return this.state;
  }

  get isReady(): boolean {
    return this.state === "ready";
  }

  send(msg: Message): void {
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
          this.ws.send(frame);
        }
      }
    } else {
      const frames = encodeDataFrames(msg);
      for (const frame of frames) {
        this.ws.send(frame);
      }
    }
  }

  sendCommand(payload: Uint8Array): void {
    if (!this.ws) throw new Error("Connection closed");
    this.ws.send(encodeCommandFrame(payload));
  }

  close(): void {
    this.pendingParts = [];
    if (this.ws) {
      this.state = "closed";
      this.ws.close();
      this.ws = null;
    } else {
      this.state = "closed";
    }
  }

  private onOpen(): void {
    this.state = "handshaking";
    const ready = encodeReady(this.opts.socketType, this.opts.identity);
    this.sendCommand(ready);
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
        this.closeWithError("Expected READY command during handshake");
        return;
      }
      const cmd = decodeCommand(frame.payload);
      if (cmd.name !== "READY") {
        this.closeWithError(`Expected READY, got ${cmd.name}`);
        return;
      }
      this.peerProperties = decodeReadyProperties(cmd.body);
      const peerType = this.peerProperties.socketType;
      if (!peerType) {
        this.closeWithError("READY missing Socket-Type");
        return;
      }
      if (!isSocketTypeName(peerType)) {
        this.closeWithError(`Unknown peer socket type: ${peerType}`);
        return;
      }
      if (!isCompatibleSocketType(this.opts.socketType, peerType)) {
        this.closeWithError(
          `Incompatible socket types: ours=${this.opts.socketType} peer=${peerType}`,
        );
        return;
      }
      this.state = "ready";
      this.opts.onReady?.(this);
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
    if (cmd.name === "PING") {
      this.handlePing(cmd.body);
      return;
    }
    if (cmd.name === "PONG") {
      if (cmd.body.byteLength > 16) throw new Error("PONG context too long");
      return;
    }
    this.opts.onCommand?.(this, cmd.name, cmd.body);
  }

  private handlePing(body: Uint8Array): void {
    if (body.byteLength < 2) throw new Error("PING body missing TTL");
    const context = body.subarray(2);
    if (context.byteLength > 16) throw new Error("PING context too long");
    this.sendCommand(encodePong(context));
  }
}
