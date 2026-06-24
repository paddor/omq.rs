import type { SocketTypeName } from "./command.ts"
import type { Connection } from "./connection.ts"
import type { Message } from "./message.ts"
import { Socket, type SocketOptions } from "./socket.ts"

/**
 * PAIR socket. Exclusive one-to-one bidirectional communication.
 * Only one connection is allowed at a time.
 */
export class Pair extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "PAIR"

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }

  /** @ignore */
  override connect(url: string): void {
    if (this.connections.size > 0) {
      throw new Error("PAIR socket allows only one connection")
    }
    super.connect(url)
  }

  /** Send a message to the connected peer. */
  async send(msg: Message): Promise<void> {
    let conn = this.readyConnections[0]
    if (!conn) {
      conn = await this.waitForReady()
    }
    conn.send(msg)
  }

  /** Wait for the next message. Resolves immediately if one is queued. */
  async recv(): Promise<Message> {
    return this.dequeueMessage()
  }

  /** Async iterator that yields messages until the connection closes. */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (this.connections.size > 0) {
      yield await this.recv()
    }
  }

  /** @ignore */
  protected override onConnectionMessage(
    _conn: Connection,
    msg: Message,
  ): void {
    this.enqueueMessage(msg)
  }
}
