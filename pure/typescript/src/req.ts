import type { SocketTypeName } from "./command.ts"
import type { Connection } from "./connection.ts"
import { Message } from "./message.ts"
import { Socket, type SocketOptions } from "./socket.ts"

/**
 * REQ (request) socket. Sends a message and awaits exactly one reply
 * before the next send is allowed (strict request-reply alternation).
 */
export class Req extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "REQ"
  private pendingReply: ((msg: Message) => void) | null = null
  private replyConnection: Connection | null = null
  private rrIndex = 0

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }

  /**
   * Send a request and wait for the reply. Throws if a previous request
   * is still pending.
   */
  async send(msg: Message): Promise<Message> {
    if (this.pendingReply) {
      throw new Error("REQ socket must receive a reply before sending again")
    }

    let conn = this.pickRoundRobin()
    if (!conn) {
      conn = await this.waitForReady()
    }

    const withDelimiter = new Message(
      new Uint8Array(0),
      ...msg.parts,
    )

    conn.send(withDelimiter)
    this.replyConnection = conn

    return new Promise((resolve) => {
      this.pendingReply = resolve
    })
  }

  /** @ignore */
  protected override onConnectionMessage(_conn: Connection, msg: Message): void {
    if (!this.pendingReply) return

    // Strip empty delimiter
    if (msg.parts.length > 0 && msg.parts[0]!.byteLength === 0) {
      const reply = Message.fromParts(msg.parts.slice(1))
      const resolve = this.pendingReply
      this.pendingReply = null
      this.replyConnection = null
      resolve(reply)
    }
  }
}
