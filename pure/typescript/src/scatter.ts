import type { SocketTypeName } from "./command.ts";
import type { Message } from "./message.ts";
import { Push } from "./push.ts";
import type { SocketOptions } from "./socket.ts";

/**
 * SCATTER socket (draft). Round-robin send to connected GATHER peers.
 * Functionally identical to PUSH but uses the SCATTER socket type.
 */
export class Scatter extends Push {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "SCATTER";

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts);
  }

  /** Send a single-frame message. */
  override send(msg: Message): Promise<void> {
    if (msg.parts.length !== 1) {
      return Promise.reject(
        new Error("SCATTER socket requires single-part messages"),
      );
    }
    return super.send(msg);
  }
}
