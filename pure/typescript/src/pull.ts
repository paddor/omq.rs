import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * PULL socket. Receives messages fair-queued from connected peers. Does
 * not send messages.
 */
export class Pull extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "PULL";

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
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
