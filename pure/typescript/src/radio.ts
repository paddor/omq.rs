import { bytesKey } from "./bytes.ts";
import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * RADIO socket (draft). Group-based publish. Sends messages to
 * connected DISH peers that have joined a matching group. Peers
 * advertise group membership via ZMTP JOIN/LEAVE commands. The first
 * frame of each sent message is the group name.
 */
export class Radio extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "RADIO";
  private peerGroups: Map<Connection, Set<string>> = new Map();

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /**
   * Send a message. The first frame is the group name. Only peers
   * that have joined this group receive the message.
   */
  send(msg: Message): Promise<void> {
    return this.runSynchronously(() => {
      if (msg.parts.length !== 2) {
        throw new Error("RADIO socket requires [group, body]");
      }
      const group = bytesKey(msg.parts[0]!);
      for (const conn of this.readyConnections) {
        const groups = this.peerGroups.get(conn);
        if (groups && groups.has(group)) {
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
    const group = bytesKey(body);
    if (name === "JOIN") {
      let groups = this.peerGroups.get(conn);
      if (!groups) {
        groups = new Set();
        this.peerGroups.set(conn, groups);
      }
      groups.add(group);
    } else if (name === "LEAVE") {
      const groups = this.peerGroups.get(conn);
      if (groups) groups.delete(group);
    }
  }

  /** @ignore */
  protected override onConnectionClosed(conn: Connection): void {
    this.peerGroups.delete(conn);
    super.onConnectionClosed(conn);
  }

  /** @ignore */
  protected override onConnectionMessage(
    _conn: Connection,
    _msg: Message,
  ): void {
    // RADIO never receives application messages
  }
}
