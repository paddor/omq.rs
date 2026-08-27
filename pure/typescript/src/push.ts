import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Socket, type SocketOptions } from "./socket.ts";

/**
 * PUSH socket. Sends messages in a round-robin fashion across connected
 * peers. Does not receive messages.
 */
export class Push extends Socket {
  /** @ignore */
  protected readonly socketType: SocketTypeName = "PUSH";

  /** Create a PUSH socket. */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Send a message. Waits for at least one ready connection if none exist yet. */
  async send(msg: Message): Promise<void> {
    let conn = this.pickRoundRobin();
    if (!conn) {
      conn = await this.waitForReady();
    }
    await this.sendOnConnection(conn, msg);
  }

  /** @ignore */
  protected override onConnectionMessage(
    _conn: Connection,
    _msg: Message,
  ): void {
    // PUSH never receives
  }
}
