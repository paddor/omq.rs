import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * REP (reply) socket. Receives a request and sends exactly one reply
 * before the next receive is allowed (strict request-reply alternation).
 */
export class Rep extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "REP";
  private pendingRequests: Array<{ conn: Connection; msg: Message }> = [];
  private requestWaiters: Array<
    {
      resolve: (entry: { conn: Connection; msg: Message }) => void;
      reject: (error: Error) => void;
    }
  > = [];
  private routingEnvelope: Uint8Array[] | null = null;
  private replyConnection: Connection | null = null;

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /**
   * Receive the next request. Strips the routing envelope (all frames up
   * to and including the empty delimiter). Throws if a reply to the
   * previous request has not been sent yet.
   */
  async recv(): Promise<Message> {
    if (this.routingEnvelope) {
      throw new Error("REP socket must send a reply before receiving again");
    }

    let entry: { conn: Connection; msg: Message };
    const queued = this.pendingRequests.shift();
    if (queued) {
      entry = queued;
    } else {
      entry = await new Promise((resolve, reject) =>
        this.requestWaiters.push({ resolve, reject })
      );
    }

    this.replyConnection = entry.conn;

    const delimIdx = entry.msg.parts.findIndex((p) => p.byteLength === 0);
    if (delimIdx >= 0) {
      this.routingEnvelope = entry.msg.parts.slice(0, delimIdx + 1);
      return Message.fromParts(entry.msg.parts.slice(delimIdx + 1));
    }
    this.routingEnvelope = [new Uint8Array(0)];
    return entry.msg;
  }

  /**
   * Send a reply to the last received request. The saved routing
   * envelope is prepended automatically. Throws if no request has been
   * received yet.
   */
  async send(msg: Message): Promise<void> {
    if (!this.routingEnvelope || !this.replyConnection) {
      return Promise.reject(
        new Error("REP socket must receive a request before sending"),
      );
    }
    const withEnvelope = Message.fromParts([
      ...this.routingEnvelope,
      ...msg.parts,
    ]);
    const conn = this.replyConnection;
    await this.sendOnConnection(conn, withEnvelope);
    this.routingEnvelope = null;
    this.replyConnection = null;
  }

  /** Async iterator that yields requests until all connections close. */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (this.hasOpenEndpoints()) {
      yield await this.recv();
    }
  }

  /** @ignore */
  protected override onConnectionMessage(conn: Connection, msg: Message): void {
    const entry = { conn, msg };
    const waiter = this.requestWaiters.shift();
    if (waiter) {
      waiter.resolve(entry);
    } else {
      if (
        this.opts.receiveHighWaterMark !== undefined &&
        this.pendingRequests.length >= this.opts.receiveHighWaterMark
      ) {
        this.closeForReceiveHighWaterMark(conn);
        return;
      }
      this.pendingRequests.push(entry);
    }
  }

  /** @ignore */
  protected override onConnectionClosed(conn: Connection): void {
    this.pendingRequests = this.pendingRequests.filter((entry) =>
      entry.conn !== conn
    );
    if (this.replyConnection === conn) {
      this.routingEnvelope = null;
      this.replyConnection = null;
    }
    super.onConnectionClosed(conn);
  }

  /** @ignore */
  protected override onSocketClosed(error: Error): void {
    const waiters = this.requestWaiters;
    this.requestWaiters = [];
    for (const waiter of waiters) waiter.reject(error);
    this.routingEnvelope = null;
    this.replyConnection = null;
  }
}
