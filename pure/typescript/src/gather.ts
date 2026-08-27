import type { SocketTypeName } from "./command.ts";
import type { Connection } from "./connection.ts";
import type { Message } from "./message.ts";
import { Pull } from "./pull.ts";
import type { SocketOptions } from "./socket.ts";

/**
 * GATHER socket (draft). Fair-queued receive from connected SCATTER
 * peers. Functionally identical to PULL but uses the GATHER socket type.
 */
export class Gather extends Pull {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "GATHER";

  /** Create a GATHER socket. */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** @ignore */
  protected override onConnectionMessage(conn: Connection, msg: Message): void {
    if (msg.parts.length !== 1) return;
    super.onConnectionMessage(conn, msg);
  }
}
