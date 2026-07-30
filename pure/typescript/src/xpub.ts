import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import { Message } from "./message.ts";
import { Pub } from "./pub.ts";
import type { SocketOptions } from "./socket.ts";

/**
 * XPUB socket. Like PUB but surfaces subscription changes as messages.
 * Received messages have a first byte of `0x01` (subscribe) or `0x00`
 * (unsubscribe), followed by the subscription prefix.
 */
export class XPub extends Pub {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "XPUB";

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Wait for the next subscription change message. */
  recv(): Promise<Message> {
    return this.dequeueMessage();
  }

  /** Async iterator that yields subscription change messages. */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (this.hasOpenEndpoints()) {
      yield await this.recv();
    }
  }

  /** @ignore */
  protected override onCommand(
    conn: Connection,
    name: string,
    body: Uint8Array,
  ): void {
    super.onCommand(conn, name, body);

    if (name === "SUBSCRIBE") {
      const frame = new Uint8Array(1 + body.byteLength);
      frame[0] = 0x01;
      frame.set(body, 1);
      this.enqueueMessage(Message.fromParts([frame]));
    } else if (name === "CANCEL") {
      const frame = new Uint8Array(1 + body.byteLength);
      frame[0] = 0x00;
      frame.set(body, 1);
      this.enqueueMessage(Message.fromParts([frame]));
    }
  }
}
