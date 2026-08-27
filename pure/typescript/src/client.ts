import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import { Dealer } from "./dealer.ts";
import type { Message } from "./message.ts";
import type { SocketOptions } from "./socket.ts";

/**
 * CLIENT socket (draft). Asynchronous request-reply without state
 * constraints. Functionally identical to DEALER but uses the CLIENT
 * socket type and can only peer with SERVER.
 */
export class Client extends Dealer {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "CLIENT";

  /** Create a CLIENT socket. */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Send a single-frame request. */
  override send(msg: Message): Promise<void> {
    if (msg.parts.length !== 1) {
      return Promise.reject(
        new Error("CLIENT socket requires single-part messages"),
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
