import type { SocketTypeName } from "./command.ts";
import { encodeCancel, encodeSubscribe } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

const encoder = new TextEncoder();

/**
 * SUB (subscribe) socket. Receives messages matching subscribed topic
 * prefixes. Supports async iteration.
 */
export class Sub extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "SUB";
  private subscriptions: Set<string> = new Set();

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Subscribe to messages matching `prefix`. Sent to all connected peers. */
  subscribe(prefix: string): void {
    if (this.subscriptions.has(prefix)) return;
    this.subscriptions.add(prefix);
    const cmd = encodeSubscribe(encoder.encode(prefix));
    for (const conn of this.readyConnections) {
      conn.sendCommand(cmd);
    }
  }

  /** Unsubscribe from a previously subscribed prefix. */
  unsubscribe(prefix: string): void {
    if (!this.subscriptions.delete(prefix)) return;
    const cmd = encodeCancel(encoder.encode(prefix));
    for (const conn of this.readyConnections) {
      conn.sendCommand(cmd);
    }
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
  protected override onConnectionReady(conn: Connection): void {
    super.onConnectionReady(conn);
    for (const prefix of this.subscriptions) {
      conn.sendCommand(encodeSubscribe(encoder.encode(prefix)));
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
