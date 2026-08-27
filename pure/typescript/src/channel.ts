import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Pair } from "./pair.ts";
import type { SocketOptions } from "./socket.ts";

/**
 * CHANNEL socket (draft). Exclusive one-to-one bidirectional
 * communication, like PAIR but for the draft async model.
 */
export class Channel extends Pair {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "CHANNEL";

  /** Create a CHANNEL socket. */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Send a single-frame message. */
  override send(msg: Message): Promise<void> {
    if (msg.parts.length !== 1) {
      return Promise.reject(
        new Error("CHANNEL socket requires single-part messages"),
      );
    }
    return super.send(msg);
  }

  /** @ignore */
  protected override onConnectionMessage(conn: Connection, msg: Message): void {
    if (msg.parts.length !== 1) return;
    super.onConnectionMessage(conn, msg);
  }
}
