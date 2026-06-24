import type { SocketTypeName } from "./command.ts"
import { Pair } from "./pair.ts"
import type { SocketOptions } from "./socket.ts"

/**
 * CHANNEL socket (draft). Exclusive one-to-one bidirectional
 * communication, like PAIR but for the draft async model.
 */
export class Channel extends Pair {
  /** @ignore */
  protected override readonly socketType: SocketTypeName = "CHANNEL"

  /** @ignore */
  constructor(opts?: SocketOptions) {
    super(opts)
  }
}
