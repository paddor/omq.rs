import type { SocketTypeName } from "./command.ts";
import { encodeJoin, encodeLeave } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

const encoder = new TextEncoder();

/**
 * DISH socket (draft). Group-based subscribe. Joins groups and receives
 * messages from connected RADIO peers matching those groups. Group
 * membership is managed via ZMTP JOIN/LEAVE commands.
 */
export class Dish extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "DISH";
  private groups: Set<string> = new Set();

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Join a group. Messages with this group will be received. */
  join(group: string): void {
    if (this.groups.has(group)) return;
    this.groups.add(group);
    const cmd = encodeJoin(encoder.encode(group));
    for (const conn of this.readyConnections) {
      conn.sendCommand(cmd);
    }
  }

  /** Leave a previously joined group. */
  leave(group: string): void {
    if (!this.groups.delete(group)) return;
    const cmd = encodeLeave(encoder.encode(group));
    for (const conn of this.readyConnections) {
      conn.sendCommand(cmd);
    }
  }

  /** Wait for the next message. */
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
  protected override onConnectionReady(conn: Connection): void {
    super.onConnectionReady(conn);
    for (const group of this.groups) {
      conn.sendCommand(encodeJoin(encoder.encode(group)));
    }
  }

  /** @ignore */
  protected override onConnectionMessage(
    conn: Connection,
    msg: Message,
  ): void {
    if (msg.parts.length !== 2) return;
    this.enqueueMessage(conn, msg);
  }
}
