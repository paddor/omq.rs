import {
  type SocketTypeName,
  decodeCommand,
  decodeReadyProperties,
  encodeReady,
  type PeerProperties,
} from "./command.ts"
import { Lz4Decoder, Lz4Encoder } from "./lz4.ts"
import { Message } from "./message.ts"
import {
  FLAG_COMMAND,
  FLAG_FINAL,
  FLAG_MORE,
  decodeZwsFrame,
  encodeCommandFrame,
  encodeDataFrames,
} from "./zws.ts"

export type ConnectionState = "connecting" | "handshaking" | "ready" | "closed"

export interface ConnectionOptions {
  socketType: SocketTypeName
  identity: Uint8Array
  lz4Dict?: Uint8Array
  maxMessageSize?: number
  onReady?: (conn: Connection) => void
  onMessage?: (conn: Connection, msg: Message) => void
  onClose?: (conn: Connection) => void
  onError?: (conn: Connection, error: Error) => void
}

export class Connection {
  readonly url: string
  private ws: WebSocket | null = null
  private state: ConnectionState = "connecting"
  private opts: ConnectionOptions
  private pendingParts: Uint8Array[] = []
  private lz4Decoder: Lz4Decoder | null = null
  private lz4Encoder: Lz4Encoder | null = null
  private useLz4: boolean
  peerProperties: PeerProperties | null = null

  constructor(url: string, opts: ConnectionOptions) {
    this.url = url
    this.opts = opts
    this.useLz4 = url.startsWith("lz4+")
    const wsUrl = this.useLz4
      ? url.replace(/^lz4\+/, "")
      : url

    if (this.useLz4) {
      this.lz4Decoder = new Lz4Decoder(opts.maxMessageSize)
      this.lz4Encoder = new Lz4Encoder(opts.lz4Dict)
    }

    const ws = new WebSocket(wsUrl, ["ZWS2.0/NULL"])
    ws.binaryType = "arraybuffer"
    ws.onopen = () => this.onOpen()
    ws.onmessage = (ev) => this.onWsMessage(ev)
    ws.onclose = () => this.onWsClose()
    ws.onerror = () => this.onWsError()
    this.ws = ws
  }

  get connectionState(): ConnectionState {
    return this.state
  }

  get isReady(): boolean {
    return this.state === "ready"
  }

  send(msg: Message): void {
    if (!this.ws || this.state !== "ready") {
      throw new Error("Connection not ready")
    }

    if (this.useLz4 && this.lz4Encoder) {
      const encoded = this.lz4Encoder.encodeMessage(
        msg.parts.map((p) => p),
      )
      for (const wireMsg of encoded) {
        const frames = encodeDataFrames({ parts: wireMsg })
        for (const frame of frames) {
          this.ws.send(frame)
        }
      }
    } else {
      const frames = encodeDataFrames(msg)
      for (const frame of frames) {
        this.ws.send(frame)
      }
    }
  }

  sendCommand(payload: Uint8Array): void {
    if (!this.ws) throw new Error("Connection closed")
    this.ws.send(encodeCommandFrame(payload))
  }

  close(): void {
    if (this.ws) {
      this.state = "closed"
      this.ws.close()
      this.ws = null
    }
  }

  private onOpen(): void {
    this.state = "handshaking"
    const ready = encodeReady(this.opts.socketType, this.opts.identity)
    this.sendCommand(ready)
  }

  private onWsMessage(ev: MessageEvent): void {
    const frame = decodeZwsFrame(ev.data as ArrayBuffer)

    if (this.state === "handshaking") {
      if (frame.flag !== FLAG_COMMAND) {
        this.closeWithError("Expected READY command during handshake")
        return
      }
      const cmd = decodeCommand(frame.payload)
      if (cmd.name !== "READY") {
        this.closeWithError(`Expected READY, got ${cmd.name}`)
        return
      }
      this.peerProperties = decodeReadyProperties(cmd.body)
      this.state = "ready"
      this.opts.onReady?.(this)
      return
    }

    if (frame.flag === FLAG_COMMAND) {
      return
    }

    if (frame.flag === FLAG_MORE) {
      this.pendingParts.push(frame.payload)
      return
    }

    if (frame.flag === FLAG_FINAL) {
      this.pendingParts.push(frame.payload)
      const rawParts = this.pendingParts
      this.pendingParts = []

      let parts: Uint8Array[]
      if (this.useLz4 && this.lz4Decoder) {
        const decoded = this.lz4Decoder.decodeMessage(rawParts)
        if (decoded === null) return
        parts = decoded
      } else {
        if (this.opts.maxMessageSize !== undefined) {
          let total = 0
          for (const part of rawParts) {
            total += part.byteLength
            if (total > this.opts.maxMessageSize) {
              this.closeWithError(`Message size ${total} exceeds max ${this.opts.maxMessageSize}`)
              return
            }
          }
        }
        parts = rawParts
      }

      this.opts.onMessage?.(this, Message.fromParts(parts))
    }
  }

  private onWsClose(): void {
    this.state = "closed"
    this.ws = null
    this.opts.onClose?.(this)
  }

  private onWsError(): void {
    this.opts.onError?.(this, new Error(`WebSocket error on ${this.url}`))
  }

  private closeWithError(reason: string): void {
    this.opts.onError?.(this, new Error(reason))
    this.close()
  }
}
