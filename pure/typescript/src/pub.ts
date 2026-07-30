import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

function prefixMatches(data: Uint8Array, prefix: Uint8Array): boolean {
  if (data.byteLength < prefix.byteLength) return false;
  for (let i = 0; i < prefix.byteLength; i++) {
    if (data[i] !== prefix[i]) return false;
  }
  return true;
}

/**
 * PUB (publish) socket. Sends messages to all connected peers whose
 * subscriptions match the message topic (first frame). Peers advertise
 * subscriptions via ZMTP SUBSCRIBE/CANCEL commands.
 */
export class Pub extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "PUB";
  /** @ignore */
  protected peerSubscriptions: Map<Connection, Uint8Array[]> = new Map();

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Send a message to all peers whose subscriptions match the first frame. */
  send(msg: Message): Promise<void> {
    return this.runSynchronously(() => {
      const topic = msg.parts[0] || new Uint8Array(0);
      for (const conn of this.readyConnections) {
        const subs = this.peerSubscriptions.get(conn);
        if (subs && subs.some((prefix) => prefixMatches(topic, prefix))) {
          conn.send(msg);
        }
      }
    });
  }

  /** @ignore */
  protected override onCommand(
    conn: Connection,
    name: string,
    body: Uint8Array,
  ): void {
    if (name === "SUBSCRIBE") {
      let subs = this.peerSubscriptions.get(conn);
      if (!subs) {
        subs = [];
        this.peerSubscriptions.set(conn, subs);
      }
      subs.push(body.slice());
    } else if (name === "CANCEL") {
      const subs = this.peerSubscriptions.get(conn);
      if (subs) {
        const idx = subs.findIndex(
          (s) =>
            s.byteLength === body.byteLength &&
            prefixMatches(s, body) &&
            prefixMatches(body, s),
        );
        if (idx >= 0) subs.splice(idx, 1);
      }
    }
  }

  /** @ignore */
  protected override onConnectionClosed(conn: Connection): void {
    this.peerSubscriptions.delete(conn);
    super.onConnectionClosed(conn);
  }

  /** @ignore */
  protected override onConnectionMessage(
    _conn: Connection,
    _msg: Message,
  ): void {
    // PUB never receives application messages
  }
}
