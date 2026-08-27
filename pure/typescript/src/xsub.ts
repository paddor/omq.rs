import { bytesKey } from "./bytes.ts";
import type { SocketTypeName } from "./command.ts";
import { encodeCancel, encodeSubscribe } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * XSUB socket. Like SUB but subscriptions are managed by sending
 * messages: first byte `0x01` to subscribe, `0x00` to unsubscribe,
 * followed by the prefix. Useful in proxy chains.
 */
export class XSub extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "XSUB";
  private subscriptions: Map<string, Uint8Array> = new Map();

  /** Create an XSUB socket. */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /**
   * Send a subscription control message. The first byte must be `0x01`
   * (subscribe) or `0x00` (unsubscribe), followed by the prefix bytes.
   * The control message is translated to a ZMTP SUBSCRIBE/CANCEL
   * command on the wire.
   */
  send(msg: Message): Promise<void> {
    return this.runSynchronously(() => {
      const frame = msg.parts[0];
      if (!frame || frame.byteLength < 1) return;

      const type = frame[0];
      const prefix = frame.subarray(1);
      const key = bytesKey(prefix);

      if (type === 0x01) {
        if (this.subscriptions.has(key)) return;
        this.subscriptions.set(key, prefix.slice());
        const cmd = encodeSubscribe(prefix);
        for (const conn of this.readyConnections) {
          conn.sendCommand(cmd);
        }
      } else if (type === 0x00) {
        if (!this.subscriptions.delete(key)) return;
        const cmd = encodeCancel(prefix);
        for (const conn of this.readyConnections) {
          conn.sendCommand(cmd);
        }
      }
    });
  }

  /** Wait for the next published message. */
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
    for (const prefix of this.subscriptions.values()) {
      conn.sendCommand(encodeSubscribe(prefix));
    }
  }

  /** @ignore */
  protected override onConnectionMessage(
    conn: Connection,
    msg: Message,
  ): void {
    this.enqueueMessage(conn, msg);
  }
}
