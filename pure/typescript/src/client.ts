import type { SocketTypeName } from "./command.ts"
import { Dealer } from "./dealer.ts"
import type { SocketOptions } from "./socket.ts"

/**
 * CLIENT socket (draft). Asynchronous request-reply without state
 * constraints. Functionally identical to DEALER but uses the CLIENT
 * socket type and can only peer with SERVER.
 */
export class Client extends Dealer {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "CLIENT"

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }
}
