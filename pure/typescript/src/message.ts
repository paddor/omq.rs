const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A multipart ZMQ message. Each part is a `Uint8Array`. */
export class Message {
  /** The raw binary parts of this message. */
  readonly parts: Uint8Array[];

  /** Create a message from string or binary parts. */
  constructor(...parts: (string | Uint8Array)[]) {
    this.parts = parts.map((p) =>
      typeof p === "string" ? encoder.encode(p) : p
    );
  }

  /** Create a single-part message from a string. */
  static from(s: string): Message {
    return new Message(s);
  }

  /** Wrap pre-existing binary parts into a message. */
  static fromParts(parts: Uint8Array[]): Message {
    const msg = new Message();
    msg.parts.length = 0;
    msg.parts.push(...parts);
    return msg;
  }

  /** Decode part at `index` as a UTF-8 string. Throws `RangeError` if out of bounds. */
  string(index: number): string {
    const part = this.parts[index];
    if (!part) throw new RangeError(`No part at index ${index}`);
    return decoder.decode(part);
  }

  /** Total byte size across all parts. */
  get size(): number {
    let total = 0;
    for (const part of this.parts) {
      total += part.byteLength;
    }
    return total;
  }
}
