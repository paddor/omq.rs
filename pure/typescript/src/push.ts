import type { SocketTypeName } from "./command.ts"
import type { Connection } from "./connection.ts"
import type { Message } from "./message.ts"
import { Socket, type SocketOptions } from "./socket.ts"

export class Push extends Socket {
  protected readonly socketType: SocketTypeName = "PUSH"

  constructor(opts?: SocketOptions) {
    super(opts)
  }

  async send(msg: Message): Promise<void> {
    let conn = this.pickRoundRobin()
    if (!conn) {
      conn = await this.waitForReady()
    }
    conn.send(msg)
  }

  protected override onConnectionMessage(_conn: Connection, _msg: Message): void {
    // PUSH never receives
  }
}
