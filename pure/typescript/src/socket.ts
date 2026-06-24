import { Connection, type ConnectionOptions } from "./connection.ts"
import type { SocketTypeName } from "./command.ts"
import type { Message } from "./message.ts"

/** Options for creating a socket. */
export interface SocketOptions {
  /** ZMQ identity string. Sent to the peer during handshake. */
  identity?: string
  /** Pre-shared LZ4 dictionary for `lz4+wss://` connections. */
  lz4Dict?: Uint8Array
  /** Maximum decompressed message size in bytes. Exceeding this closes the connection. */
  maxMessageSize?: number
}

const encoder = new TextEncoder()

/**
 * Abstract base class for ZMQ socket types. Manages connections, handshakes,
 * and round-robin load balancing.
 */
export abstract class Socket {
  /** @ignore */
  protected abstract readonly socketType: SocketTypeName
  /** @ignore */
  protected connections: Map<string, Connection> = new Map()
  /** @ignore */
  protected readyConnections: Connection[] = []
  /** @ignore */
  protected opts: SocketOptions
  /** @ignore */
  protected identity: Uint8Array

  private readyWaiters: Array<(conn: Connection) => void> = []
  private pendingSends: Array<{ msg: Message; resolve: () => void }> = []

  /** @ignore */
  constructor(opts: SocketOptions = {}) {
    this.opts = opts
    this.identity = opts.identity ? encoder.encode(opts.identity) : new Uint8Array(0)
  }

  /** Connect to a ZMQ endpoint. Use `lz4+wss://` for LZ4-compressed connections. */
  connect(url: string): void {
    if (this.connections.has(url)) return

    const connOpts: ConnectionOptions = {
      socketType: this.socketType,
      identity: this.identity,
      lz4Dict: this.opts.lz4Dict,
      maxMessageSize: this.opts.maxMessageSize,
      onReady: (conn) => this.onConnectionReady(conn),
      onMessage: (conn, msg) => this.onConnectionMessage(conn, msg),
      onClose: (conn) => this.onConnectionClosed(conn),
      onError: (_conn, err) => this.onConnectionError(err),
    }

    const conn = new Connection(url, connOpts)
    this.connections.set(url, conn)
  }

  /** Disconnect from a previously connected endpoint. */
  disconnect(url: string): void {
    const conn = this.connections.get(url)
    if (conn) {
      conn.close()
      this.connections.delete(url)
      const idx = this.readyConnections.indexOf(conn)
      if (idx >= 0) this.readyConnections.splice(idx, 1)
    }
  }

  /** Close all connections. */
  close(): void {
    for (const conn of this.connections.values()) {
      conn.close()
    }
    this.connections.clear()
    this.readyConnections.length = 0
  }

  /** Number of active connections (including those still handshaking). */
  get connectionCount(): number {
    return this.connections.size
  }

  /** Number of connections that have completed the ZMTP handshake. */
  get readyCount(): number {
    return this.readyConnections.length
  }

  /** @ignore */
  protected onConnectionReady(conn: Connection): void {
    this.readyConnections.push(conn)

    const waiter = this.readyWaiters.shift()
    if (waiter) waiter(conn)
  }

  /** @ignore */
  protected abstract onConnectionMessage(conn: Connection, msg: Message): void

  /** @ignore */
  protected onConnectionClosed(conn: Connection): void {
    const idx = this.readyConnections.indexOf(conn)
    if (idx >= 0) this.readyConnections.splice(idx, 1)
    for (const [url, c] of this.connections) {
      if (c === conn) {
        this.connections.delete(url)
        break
      }
    }
  }

  /** @ignore */
  protected onConnectionError(_err: Error): void {
    // Subclasses can override
  }

  /** @ignore */
  protected waitForReady(): Promise<Connection> {
    if (this.readyConnections.length > 0) {
      return Promise.resolve(this.readyConnections[0]!)
    }
    return new Promise((resolve) => {
      this.readyWaiters.push(resolve)
    })
  }

  /** @ignore */
  protected pickRoundRobin(): Connection | null {
    if (this.readyConnections.length === 0) return null
    const conn = this.readyConnections.shift()!
    this.readyConnections.push(conn)
    return conn
  }
}
