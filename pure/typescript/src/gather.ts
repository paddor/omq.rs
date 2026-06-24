import type { SocketTypeName } from "./command.ts"
import { Pull } from "./pull.ts"
import type { SocketOptions } from "./socket.ts"

/**
 * GATHER socket (draft). Fair-queued receive from connected SCATTER
 * peers. Functionally identical to PULL but uses the GATHER socket type.
 */
export class Gather extends Pull {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "GATHER"

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }
}
