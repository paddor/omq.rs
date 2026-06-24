import type { SocketTypeName } from "./command.ts"
import { Push } from "./push.ts"
import type { SocketOptions } from "./socket.ts"

/**
 * SCATTER socket (draft). Round-robin send to connected GATHER peers.
 * Functionally identical to PUSH but uses the SCATTER socket type.
 */
export class Scatter extends Push {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "SCATTER"

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }
}
