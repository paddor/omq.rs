import { Connection, type ConnectionOptions, type ConnectionState } from "./connection.ts"
import type { SocketTypeName } from "./command.ts"
import type { Message } from "./message.ts"

export interface SocketOptions {
  identity?: string
  lz4Dict?: Uint8Array
  maxMessageSize?: number
}

const encoder = new TextEncoder()

export abstract class Socket {
  protected abstract readonly socketType: SocketTypeName
  protected connections: Map<string, Connection> = new Map()
  protected readyConnections: Connection[] = []
  protected opts: SocketOptions
  protected identity: Uint8Array

  private readyWaiters: Array<(conn: Connection) => void> = []
  private pendingSends: Array<{ msg: Message; resolve: () => void }> = []

  constructor(opts: SocketOptions = {}) {
    this.opts = opts
    this.identity = opts.identity ? encoder.encode(opts.identity) : new Uint8Array(0)
  }

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

  disconnect(url: string): void {
    const conn = this.connections.get(url)
    if (conn) {
      conn.close()
      this.connections.delete(url)
      const idx = this.readyConnections.indexOf(conn)
      if (idx >= 0) this.readyConnections.splice(idx, 1)
    }
  }

  close(): void {
    for (const conn of this.connections.values()) {
      conn.close()
    }
    this.connections.clear()
    this.readyConnections.length = 0
  }

  get connectionCount(): number {
    return this.connections.size
  }

  get readyCount(): number {
    return this.readyConnections.length
  }

  protected onConnectionReady(conn: Connection): void {
    this.readyConnections.push(conn)

    const waiter = this.readyWaiters.shift()
    if (waiter) waiter(conn)
  }

  protected abstract onConnectionMessage(conn: Connection, msg: Message): void

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

  protected onConnectionError(_err: Error): void {
    // Subclasses can override
  }

  protected waitForReady(): Promise<Connection> {
    if (this.readyConnections.length > 0) {
      return Promise.resolve(this.readyConnections[0]!)
    }
    return new Promise((resolve) => {
      this.readyWaiters.push(resolve)
    })
  }

  protected pickRoundRobin(): Connection | null {
    if (this.readyConnections.length === 0) return null
    const conn = this.readyConnections.shift()!
    this.readyConnections.push(conn)
    return conn
  }
}
