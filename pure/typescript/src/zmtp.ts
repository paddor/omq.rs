/**
 * ZMTP 3.1 byte-stream framing (RFC 37) for ordered stream carriers such as
 * WebTransport. ZWS keeps its own message mapping in `zws.ts`.
 */

export const ZMTP_FLAG_MORE = 0x01;
export const ZMTP_FLAG_LONG = 0x02;
export const ZMTP_FLAG_COMMAND = 0x04;

export const GREETING_LENGTH = 64;
/** Maximum parts in one message, matching the ZWS receive bound. */
export const MAX_MESSAGE_PARTS = 65_536;
/** Maximum command frame body; READY/HELLO/INITIATE are far smaller. */
export const MAX_COMMAND_BYTES = 64 * 1024;

export type MechanismName = "NULL" | "PLAIN";

/** Encode our 64-byte greeting. `asServer` is set only for mechanism servers. */
export function encodeGreeting(
  mechanism: MechanismName,
  asServer = false,
): Uint8Array<ArrayBuffer> {
  const greeting = new Uint8Array(GREETING_LENGTH);
  greeting[0] = 0xff;
  greeting[8] = 0x01;
  greeting[9] = 0x7f;
  greeting[10] = 3;
  greeting[11] = 1;
  for (let i = 0; i < mechanism.length; i++) {
    greeting[12 + i] = mechanism.charCodeAt(i);
  }
  greeting[32] = asServer ? 1 : 0;
  return greeting;
}

export interface PeerGreeting {
  major: number;
  minor: number;
  mechanism: string;
  asServer: boolean;
}

/** Validate a peer greeting. ZMTP 3.0 and later are accepted. */
export function decodeGreeting(bytes: Uint8Array): PeerGreeting {
  if (bytes.byteLength !== GREETING_LENGTH) {
    throw new Error("ZMTP greeting must be 64 bytes");
  }
  if (bytes[0] !== 0xff || bytes[9] !== 0x7f) {
    throw new Error("Invalid ZMTP greeting signature");
  }
  const major = bytes[10]!;
  if (major < 3) throw new Error(`Unsupported ZMTP version ${major}`);
  let end = 12;
  while (end < 32 && bytes[end] !== 0) end++;
  for (let i = end; i < 32; i++) {
    if (bytes[i] !== 0) throw new Error("Invalid ZMTP mechanism padding");
  }
  const mechanism = String.fromCharCode(...bytes.subarray(12, end));
  return { major, minor: bytes[11]!, mechanism, asServer: bytes[32] === 1 };
}

function frameHeader(flags: number, size: number): Uint8Array<ArrayBuffer> {
  if (size <= 255) {
    return new Uint8Array([flags, size]);
  }
  const header = new Uint8Array(9);
  header[0] = flags | ZMTP_FLAG_LONG;
  new DataView(header.buffer).setBigUint64(1, BigInt(size), false);
  return header;
}

/** Encode one ZMTP frame (header plus body) into a single buffer. */
export function encodeFrame(
  flags: number,
  body: Uint8Array,
): Uint8Array<ArrayBuffer> {
  const header = frameHeader(flags, body.byteLength);
  const frame = new Uint8Array(header.byteLength + body.byteLength);
  frame.set(header, 0);
  frame.set(body, header.byteLength);
  return frame;
}

/** Encode a command frame from a command payload (name length, name, data). */
export function encodeCommandFrame(
  payload: Uint8Array,
): Uint8Array<ArrayBuffer> {
  return encodeFrame(ZMTP_FLAG_COMMAND, payload);
}

/**
 * Encode all parts of one message into one contiguous write. Batching
 * headers and small bodies avoids one stream write per part.
 */
export function encodeMessage(parts: Uint8Array[]): Uint8Array<ArrayBuffer> {
  if (parts.length === 0) throw new Error("Message has no parts");
  const out = new Uint8Array(messageWireSize(parts));
  writeMessage(parts, out, 0);
  return out;
}

/** Frame headers plus bodies of one message. */
export function messageWireSize(parts: Uint8Array[]): number {
  let size = 0;
  for (const part of parts) {
    size += (part.byteLength <= 255 ? 2 : 9) + part.byteLength;
  }
  return size;
}

/** Write one message's data frames at `offset`; returns the end offset. */
export function writeMessage(
  parts: Uint8Array[],
  out: Uint8Array,
  offset: number,
): number {
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]!;
    const more = i < parts.length - 1 ? ZMTP_FLAG_MORE : 0;
    if (part.byteLength <= 255) {
      out[offset++] = more;
      out[offset++] = part.byteLength;
    } else {
      out[offset++] = more | ZMTP_FLAG_LONG;
      view.setBigUint64(offset, BigInt(part.byteLength), false);
      offset += 8;
    }
    out.set(part, offset);
    offset += part.byteLength;
  }
  return offset;
}

/**
 * FIFO of received chunks. Copies only when a unit spans chunks. A head
 * index instead of `Array.shift` keeps many tiny chunks linear.
 */
class ChunkQueue {
  private chunks: Uint8Array[] = [];
  private head = 0;
  private offset = 0;
  length = 0;

  push(chunk: Uint8Array): void {
    if (chunk.byteLength === 0) return;
    this.chunks.push(chunk);
    this.length += chunk.byteLength;
  }

  peekByte(index: number): number {
    let i = index + this.offset;
    for (let c = this.head; c < this.chunks.length; c++) {
      const chunk = this.chunks[c]!;
      if (i < chunk.byteLength) return chunk[i]!;
      i -= chunk.byteLength;
    }
    throw new RangeError("peek beyond queued bytes");
  }

  take(n: number): Uint8Array {
    if (n > this.length) throw new RangeError("take beyond queued bytes");
    this.length -= n;
    const first = this.chunks[this.head];
    if (first && first.byteLength - this.offset >= n) {
      const out = first.subarray(this.offset, this.offset + n);
      this.advance(first, n);
      return out;
    }
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const chunk = this.chunks[this.head]!;
      const count = Math.min(chunk.byteLength - this.offset, n - filled);
      out.set(chunk.subarray(this.offset, this.offset + count), filled);
      filled += count;
      this.advance(chunk, count);
    }
    return out;
  }

  private advance(chunk: Uint8Array, count: number): void {
    this.offset += count;
    if (this.offset < chunk.byteLength) return;
    this.offset = 0;
    this.head++;
    if (this.head === this.chunks.length) {
      this.chunks = [];
      this.head = 0;
    } else if (this.head >= 1024 && this.head * 2 >= this.chunks.length) {
      this.chunks = this.chunks.slice(this.head);
      this.head = 0;
    }
  }
}

export type ZmtpEvent =
  | { type: "greeting"; greeting: PeerGreeting }
  | { type: "command"; payload: Uint8Array }
  | { type: "message"; parts: Uint8Array[] };

export interface ZmtpDecoderOptions {
  /** Maximum total message bytes; checked before buffering a frame body. */
  maxMessageSize?: number;
}

/**
 * Incremental ZMTP decoder. Transport chunks are neither frames nor
 * messages. Incomplete messages are never emitted.
 */
export class ZmtpDecoder {
  private queue = new ChunkQueue();
  private greeted = false;
  private parts: Uint8Array[] = [];
  private messageBytes = 0;

  constructor(private readonly opts: ZmtpDecoderOptions = {}) {}

  /** Whether a partially received message is pending. */
  get hasPartialMessage(): boolean {
    return this.parts.length > 0;
  }

  /** Feed received bytes; returns every complete unit now available. */
  push(chunk: Uint8Array): ZmtpEvent[] {
    this.queue.push(chunk);
    const events: ZmtpEvent[] = [];
    for (;;) {
      const event = this.next();
      if (!event) return events;
      events.push(event);
    }
  }

  private next(): ZmtpEvent | null {
    if (!this.greeted) {
      if (this.queue.length < GREETING_LENGTH) return null;
      this.greeted = true;
      return {
        type: "greeting",
        greeting: decodeGreeting(this.queue.take(GREETING_LENGTH)),
      };
    }
    for (;;) {
      if (this.queue.length < 2) return null;
      const flags = this.queue.peekByte(0);
      if (flags & ~(ZMTP_FLAG_MORE | ZMTP_FLAG_LONG | ZMTP_FLAG_COMMAND)) {
        throw new Error(`Invalid ZMTP frame flags 0x${flags.toString(16)}`);
      }
      const long = (flags & ZMTP_FLAG_LONG) !== 0;
      const headerLength = long ? 9 : 2;
      if (this.queue.length < headerLength) return null;
      const size = long ? this.longSize() : this.queue.peekByte(1);
      const command = (flags & ZMTP_FLAG_COMMAND) !== 0;
      this.checkSize(command, flags, size);
      if (this.queue.length < headerLength + size) return null;
      this.queue.take(headerLength);
      const body = this.queue.take(size);
      if (command) return { type: "command", payload: body };
      this.parts.push(body);
      this.messageBytes += size;
      if (flags & ZMTP_FLAG_MORE) continue;
      const parts = this.parts;
      this.parts = [];
      this.messageBytes = 0;
      return { type: "message", parts };
    }
  }

  private longSize(): number {
    let size = 0n;
    for (let i = 1; i < 9; i++) {
      size = (size << 8n) | BigInt(this.queue.peekByte(i));
    }
    if (size > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error("ZMTP frame length exceeds this runtime's range");
    }
    return Number(size);
  }

  private checkSize(command: boolean, flags: number, size: number): void {
    if (command) {
      if (flags & ZMTP_FLAG_MORE) {
        throw new Error("ZMTP command with MORE flag");
      }
      if (this.parts.length > 0) {
        throw new Error("ZMTP command inside a message");
      }
      if (size > MAX_COMMAND_BYTES) throw new Error("ZMTP command too large");
      return;
    }
    if (this.parts.length + 1 > MAX_MESSAGE_PARTS) {
      throw new Error("ZMTP message has too many parts");
    }
    const max = this.opts.maxMessageSize;
    // Checked from the header, before the body is buffered.
    if (max !== undefined && this.messageBytes + size > max) {
      throw new Error(`Message size exceeds max ${max}`);
    }
  }
}
