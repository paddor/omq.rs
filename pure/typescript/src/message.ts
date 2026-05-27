const encoder = new TextEncoder()
const decoder = new TextDecoder()

export class Message {
  readonly parts: Uint8Array[]

  constructor(...parts: (string | Uint8Array)[]) {
    this.parts = parts.map((p) =>
      typeof p === "string" ? encoder.encode(p) : p
    )
  }

  static from(s: string): Message {
    return new Message(s)
  }

  static fromParts(parts: Uint8Array[]): Message {
    const msg = new Message()
    msg.parts.length = 0
    msg.parts.push(...parts)
    return msg
  }

  string(index: number): string {
    const part = this.parts[index]
    if (!part) throw new RangeError(`No part at index ${index}`)
    return decoder.decode(part)
  }

  get size(): number {
    let total = 0
    for (const part of this.parts) {
      total += part.byteLength
    }
    return total
  }
}
