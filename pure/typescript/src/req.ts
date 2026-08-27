import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * REQ (request) socket. Sends a message and awaits exactly one reply
 * before the next send is allowed (strict request-reply alternation).
 */
export class Req extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "REQ";
  private pendingReply: {
    resolve: (msg: Message) => void;
    reject: (error: Error) => void;
  } | null = null;
  private replyConnection: Connection | null = null;

  /** Create a REQ socket. */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /**
   * Send a request and wait for the reply. Throws if a previous request
   * is still pending.
   */
  async send(msg: Message): Promise<Message> {
    if (this.pendingReply) {
      throw new Error("REQ socket must receive a reply before sending again");
    }

    let conn = this.pickRoundRobin();
    if (!conn) {
      conn = await this.waitForReady();
    }

    const withDelimiter = new Message(
      new Uint8Array(0),
      ...msg.parts,
    );

    this.replyConnection = conn;

    let rejectReply: (error: Error) => void = () => {};
    const reply = new Promise<Message>((resolve, reject) => {
      this.pendingReply = { resolve, reject };
      rejectReply = reject;
    });

    try {
      await this.sendOnConnection(conn, withDelimiter);
    } catch (error) {
      this.pendingReply = null;
      this.replyConnection = null;
      rejectReply(error instanceof Error ? error : new Error(String(error)));
    }

    return reply;
  }

  /** @ignore */
  protected override onConnectionMessage(
    conn: Connection,
    msg: Message,
  ): void {
    if (!this.pendingReply) return;
    if (conn !== this.replyConnection) return;

    // Strip empty delimiter
    if (msg.parts.length > 0 && msg.parts[0]!.byteLength === 0) {
      const reply = Message.fromParts(msg.parts.slice(1));
      const pending = this.pendingReply;
      this.pendingReply = null;
      this.replyConnection = null;
      pending.resolve(reply);
    }
  }

  /** @ignore */
  protected override onConnectionClosed(conn: Connection): void {
    if (this.replyConnection === conn && this.pendingReply) {
      const pending = this.pendingReply;
      this.pendingReply = null;
      this.replyConnection = null;
      pending.reject(new Error("Connection closed before REQ reply"));
    }
    super.onConnectionClosed(conn);
  }

  /** @ignore */
  protected override onSocketClosed(error: Error): void {
    if (!this.pendingReply) return;
    const pending = this.pendingReply;
    this.pendingReply = null;
    this.replyConnection = null;
    pending.reject(error);
  }
}
