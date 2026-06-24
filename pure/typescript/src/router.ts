import type { SocketTypeName } from "./command.ts"
import type { Connection } from "./connection.ts"
import { Message } from "./message.ts"
import { Socket, type SocketOptions } from "./socket.ts"

function identityKey(id: Uint8Array): string {
  let s = ""
  for (let i = 0; i < id.byteLength; i++) s += String.fromCharCode(id[i]!)
  return s
}

/**
 * ROUTER socket. Received messages are prepended with the peer's
 * identity frame. When sending, the first frame is stripped and used to
 * look up the destination peer.
 */
export class Router extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "ROUTER"
  /** @ignore */
  protected peerIdentities: Map<Connection, Uint8Array> = new Map()
  /** @ignore */
  protected connectionsByIdentity: Map<string, Connection> = new Map()
  private idCounter = 0

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }

  /** Wait for the next message. The first frame is the sender's identity. */
  async recv(): Promise<Message> {
    return this.dequeueMessage()
  }

  /**
   * Send a message. The first frame must be the destination peer's
   * identity. It is stripped before sending. If the peer is not found,
   * the message is silently dropped.
   */
  async send(msg: Message): Promise<void> {
    if (msg.parts.length < 2) return
    const id = msg.parts[0]!
    const conn = this.connectionsByIdentity.get(identityKey(id))
    if (!conn) return
    conn.send(Message.fromParts(msg.parts.slice(1)))
  }

  /** Async iterator that yields messages until all connections close. */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (this.connections.size > 0) {
      yield await this.recv()
    }
  }

  /** @ignore */
  protected override onConnectionReady(conn: Connection): void {
    super.onConnectionReady(conn)

    const peerIdentity = conn.peerProperties?.identity
    const identity =
      peerIdentity && peerIdentity.byteLength > 0
        ? peerIdentity
        : this.generateIdentity()

    this.peerIdentities.set(conn, identity)
    this.connectionsByIdentity.set(identityKey(identity), conn)
  }

  /** @ignore */
  protected override onConnectionClosed(conn: Connection): void {
    const identity = this.peerIdentities.get(conn)
    if (identity) {
      this.connectionsByIdentity.delete(identityKey(identity))
      this.peerIdentities.delete(conn)
    }
    super.onConnectionClosed(conn)
  }

  /** @ignore */
  protected override onConnectionMessage(
    conn: Connection,
    msg: Message,
  ): void {
    const identity = this.peerIdentities.get(conn)
    if (!identity) return
    const withIdentity = Message.fromParts([identity, ...msg.parts])
    this.enqueueMessage(withIdentity)
  }

  private generateIdentity(): Uint8Array {
    const id = new Uint8Array(5)
    id[0] = 0
    const n = ++this.idCounter
    id[1] = (n >>> 24) & 0xff
    id[2] = (n >>> 16) & 0xff
    id[3] = (n >>> 8) & 0xff
    id[4] = n & 0xff
    return id
  }
}
