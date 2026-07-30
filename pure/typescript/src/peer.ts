import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * PEER socket (draft). Bidirectional socket with uint32 routing IDs,
 * like a ROUTER but for the draft async model. Received messages are
 * prepended with a 4-byte routing ID frame. When sending, the first
 * frame is stripped and used to route to the destination peer.
 */
export class Peer extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "PEER";
  private routingIds: Map<Connection, Uint8Array> = new Map();
  private connectionsByRoutingId: Map<number, Connection> = new Map();
  private nextRoutingId = 1;

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Wait for the next message. The first frame is the 4-byte routing ID. */
  recv(): Promise<Message> {
    return this.dequeueMessage();
  }

  /**
   * Send a message. The first frame must be the 4-byte routing ID of
   * the destination peer. It is stripped before sending.
   */
  send(msg: Message): Promise<void> {
    if (msg.parts.length < 2) return Promise.resolve();
    const idFrame = msg.parts[0]!;
    if (idFrame.byteLength !== 4) return Promise.resolve();
    const id = new DataView(
      idFrame.buffer,
      idFrame.byteOffset,
      4,
    ).getUint32(0, false);
    const conn = this.connectionsByRoutingId.get(id);
    if (!conn) return Promise.resolve();
    return this.sendOnConnection(conn, Message.fromParts(msg.parts.slice(1)));
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
    const id = this.nextRoutingId++;
    const frame = new Uint8Array(4);
    new DataView(frame.buffer).setUint32(0, id, false);
    this.routingIds.set(conn, frame);
    this.connectionsByRoutingId.set(id, conn);
  }

  /** @ignore */
  protected override onConnectionClosed(conn: Connection): void {
    const frame = this.routingIds.get(conn);
    if (frame) {
      const id = new DataView(frame.buffer).getUint32(0, false);
      this.connectionsByRoutingId.delete(id);
      this.routingIds.delete(conn);
    }
    super.onConnectionClosed(conn);
  }

  /** @ignore */
  protected override onConnectionMessage(
    conn: Connection,
    msg: Message,
  ): void {
    const idFrame = this.routingIds.get(conn);
    if (!idFrame) return;
    this.enqueueMessage(Message.fromParts([idFrame, ...msg.parts]));
  }
}
