import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * DEALER socket. Asynchronous send and receive without request-reply
 * state constraints. Sends are round-robined, receives are
 * fair-queued.
 */
export class Dealer extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "DEALER";

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Send a message. Round-robins across connected peers. */
  async send(msg: Message): Promise<void> {
    let conn = this.pickRoundRobin();
    if (!conn) {
      conn = await this.waitForReady();
    }
    await this.sendOnConnection(conn, msg);
  }

  /** Wait for the next message. Resolves immediately if one is queued. */
  recv(): Promise<Message> {
    return this.dequeueMessage();
  }

  /** Async iterator that yields messages until all connections close. */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (this.hasOpenEndpoints()) {
      yield await this.recv();
    }
  }

  /** @ignore */
  protected override onConnectionMessage(
    _conn: Connection,
    msg: Message,
  ): void {
    this.enqueueMessage(msg);
  }
}
