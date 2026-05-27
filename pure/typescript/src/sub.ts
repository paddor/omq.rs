import type { SocketTypeName } from "./command.ts"
import { encodeCancel, encodeSubscribe } from "./command.ts"
import type { Connection } from "./connection.ts"
import { Message } from "./message.ts"
import { Socket, type SocketOptions } from "./socket.ts"

const encoder = new TextEncoder()

export class Sub extends Socket {
  protected readonly socketType: SocketTypeName = "SUB"
  private subscriptions: Set<string> = new Set()
  private messageQueue: Message[] = []
  private messageWaiters: Array<(msg: Message) => void> = []

  constructor(opts?: SocketOptions) {
    super(opts)
  }

  subscribe(prefix: string): void {
    if (this.subscriptions.has(prefix)) return
    this.subscriptions.add(prefix)
    const cmd = encodeSubscribe(encoder.encode(prefix))
    for (const conn of this.readyConnections) {
      conn.sendCommand(cmd)
    }
  }

  unsubscribe(prefix: string): void {
    if (!this.subscriptions.delete(prefix)) return
    const cmd = encodeCancel(encoder.encode(prefix))
    for (const conn of this.readyConnections) {
      conn.sendCommand(cmd)
    }
  }

  async recv(): Promise<Message> {
    const queued = this.messageQueue.shift()
    if (queued) return queued
    return new Promise((resolve) => {
      this.messageWaiters.push(resolve)
    })
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (this.connections.size > 0) {
      yield await this.recv()
    }
  }

  protected override onConnectionReady(conn: Connection): void {
    super.onConnectionReady(conn)
    for (const prefix of this.subscriptions) {
      conn.sendCommand(encodeSubscribe(encoder.encode(prefix)))
    }
  }

  protected override onConnectionMessage(_conn: Connection, msg: Message): void {
    const waiter = this.messageWaiters.shift()
    if (waiter) {
      waiter(msg)
    } else {
      this.messageQueue.push(msg)
    }
  }
}
