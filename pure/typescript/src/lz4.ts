import type {
  Compressor as CompressorInstance,
  Decompressor as DecompressorInstance,
} from "@paddor/lz4rip";

import {
  messageWireSize,
  writeMessage,
  ZMTP_FLAG_LONG,
  ZMTP_FLAG_MORE,
} from "./zmtp.ts";

type Lz4Module = typeof import("@paddor/lz4rip");

const SENTINEL_PLAIN = 0x00000000;
const SENTINEL_LZ4B = 0x4c5a3442; // "LZ4B"
const SENTINEL_LZ4M = 0x4c5a344d; // "LZ4M"
const SENTINEL_LZ4D = 0x4c5a3444; // "LZ4D"
const SENTINEL_LZ4A = 0x4c5a3441; // "LZ4A"

const MAX_DICT_BYTES = 8192;
const LZ4M_BLOCK_SIZE = 0x40000000;
const LZ4_MAX_EXPANSION = 255;
const MAX_TS_SEND_PART_SIZE = LZ4M_BLOCK_SIZE - 1;
const MIN_COMPRESS_NO_DICT = 512;
const MIN_COMPRESS_WITH_DICT = 128;

// Arena: sentinel, mode u8, reserved[3], decoded_size u32 LE,
// messages u32 LE, then raw or LZ4-compressed ZMTP data frames.
const ARENA_HEADER = 16;
const ARENA_MODE_LZ4 = 1;
const MAX_ARENA_BYTES = 64 * 1024;
const MAX_ARENA_MESSAGES = 256;
const MAX_ARENA_PARTS = 4096;
// Sender policy, as in omq-proto: small messages, 16 KiB arenas.
const MAX_ARENA_MESSAGE_BYTES = 8 * 1024;
const ARENA_TARGET_BYTES = 16 * 1024;

let lz4Module: Lz4Module | null = null;
let lz4Init: Promise<void> | null = null;

// ENVELOPE_PLAIN = 4 bytes (sentinel only)
// ENVELOPE_LZ4B = 12 bytes (sentinel + u64 LE decompressed_size)
const ENVELOPE_PLAIN = 4;
const ENVELOPE_LZ4B = 12;

/** Initialize the LZ4 WASM module. Useful for prewarming `lz4+` sockets. */
export async function initLz4(): Promise<void> {
  if (lz4Module) {
    return;
  }
  if (!lz4Init) {
    lz4Init = (async () => {
      const mod = await import("@paddor/lz4rip");
      if (isNodeRuntime()) {
        await initLz4FromNodePackageBytes(mod);
      } else {
        await mod.init();
      }
      lz4Module = mod;
    })();
  }
  await lz4Init;
}

/** Initialize LZ4 from bytes in runtimes where fetch cannot load file URLs. */
export async function initLz4FromBytes(bytes: BufferSource): Promise<void> {
  const mod = await import("@paddor/lz4rip");
  mod.initSyncFromBytes(bytes);
  lz4Module = mod;
}

function requireLz4(): Lz4Module {
  if (!lz4Module) {
    throw new Error("LZ4 not initialized.");
  }
  return lz4Module;
}

async function initLz4FromNodePackageBytes(
  mod: Lz4Module,
): Promise<void> {
  const [{ createRequire }, { readFile }, { dirname, resolve }] = await Promise
    .all([
      import("node:module"),
      import("node:fs/promises"),
      import("node:path"),
    ]);
  const require = createRequire(import.meta.url);
  const wasmPath = resolve(
    dirname(require.resolve("@paddor/lz4rip")),
    "pkg/lz4rip_wasm_bg.wasm",
  );
  mod.initSyncFromBytes(await readFile(wasmPath));
}

function isNodeRuntime(): boolean {
  const processLike = globalThis as typeof globalThis & {
    process?: { versions?: { node?: string } };
  };
  return Boolean(processLike.process?.versions?.node);
}

function readSentinel(data: Uint8Array): number {
  if (data.byteLength < 4) throw new Error("Part too short for sentinel");
  return (data[0]! << 24) | (data[1]! << 16) | (data[2]! << 8) | data[3]!;
}

function readU64LE(data: Uint8Array, offset: number): number {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const value = view.getBigUint64(offset, true);
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("Decompressed size exceeds safe integer range");
  }
  return Number(value);
}

function validateBlockSize(blockSize: number): void {
  if (
    !Number.isInteger(blockSize) || blockSize < 1 ||
    blockSize > LZ4M_BLOCK_SIZE
  ) {
    throw new RangeError(
      `LZ4 block size must be between 1 and ${LZ4M_BLOCK_SIZE}`,
    );
  }
}

function rejectImplausibleCompressedSize(
  kind: string,
  decompressedSize: number,
  compressedBytes: number,
): void {
  if (decompressedSize > compressedBytes * LZ4_MAX_EXPANSION) {
    throw new Error(
      `${kind} declared size implausibly large for compressed input`,
    );
  }
}

/** Return whether a compressed transport part carries an LZ4 dictionary. */
export function isLz4DictionaryShipment(data: Uint8Array): boolean {
  return data.byteLength >= 4 && readSentinel(data) === SENTINEL_LZ4D;
}

function isArena(data: Uint8Array): boolean {
  return data.byteLength >= 4 && readSentinel(data) === SENTINEL_LZ4A;
}

/** Inner wire size of a message small enough for an arena. */
function arenaMessageSize(parts: Uint8Array[]): number | undefined {
  if (parts.length === 0 || parts.length > MAX_ARENA_PARTS) return undefined;
  const size = messageWireSize(parts);
  return size <= MAX_ARENA_MESSAGE_BYTES ? size : undefined;
}

/**
 * Validate a decoded arena completely, then split it into messages. Parts
 * are views into `bytes`.
 */
function splitArena(
  bytes: Uint8Array,
  count: number,
  maxMessageSize: number | undefined,
): Uint8Array[][] {
  const invalid = () => new Error("Invalid LZ4A arena");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const messages: Uint8Array[][] = [];
  let parts: Uint8Array[] = [];
  let partCount = 0;
  let messageSize = 0;
  let offset = 0;

  while (offset < bytes.byteLength) {
    if (offset + 2 > bytes.byteLength) throw invalid();
    const flags = bytes[offset]!;
    if (flags & ~(ZMTP_FLAG_MORE | ZMTP_FLAG_LONG)) throw invalid();
    let start = offset + 2;
    let length = bytes[offset + 1]!;
    if (flags & ZMTP_FLAG_LONG) {
      start = offset + 9;
      if (start > bytes.byteLength) throw invalid();
      const long = view.getBigUint64(offset + 1, false);
      if (long > BigInt(bytes.byteLength - start)) throw invalid();
      length = Number(long);
    }
    const end = start + length;
    if (end > bytes.byteLength || ++partCount > MAX_ARENA_PARTS) {
      throw invalid();
    }
    messageSize += length;
    if (maxMessageSize !== undefined && messageSize > maxMessageSize) {
      throw new Error(
        `Message size ${messageSize} exceeds max ${maxMessageSize}`,
      );
    }
    parts.push(bytes.subarray(start, end));
    offset = end;
    if ((flags & ZMTP_FLAG_MORE) === 0) {
      messages.push(parts);
      parts = [];
      messageSize = 0;
    }
  }

  if (parts.length > 0 || messages.length !== count) throw invalid();
  return messages;
}

/** Stateful decoder for the omq LZ4 transport envelope. */
export class Lz4Decoder {
  private decompressor: DecompressorInstance;
  private dictReceived = false;
  private maxMessageSize: number | undefined;
  private readonly blockSize: number;
  private freed = false;

  /** Create a decoder with an optional decompressed message size limit. */
  constructor(maxMessageSize?: number, blockSize = LZ4M_BLOCK_SIZE) {
    validateBlockSize(blockSize);
    this.decompressor = new (requireLz4().Decompressor)();
    this.maxMessageSize = maxMessageSize;
    this.blockSize = blockSize;
  }

  /** Decode one enveloped part and consume message size budget. */
  decodePart(
    data: Uint8Array,
    budgetRemaining: number,
  ): { result: Uint8Array; consumed: number } | null {
    this.assertOpen();
    const sentinel = readSentinel(data);

    if (sentinel === SENTINEL_LZ4D) {
      if (this.dictReceived) {
        throw new Error("Duplicate LZ4D dictionary shipment");
      }
      const dict = data.subarray(4);
      if (dict.byteLength < 1 || dict.byteLength > MAX_DICT_BYTES) {
        throw new Error(`Invalid dictionary size: ${dict.byteLength}`);
      }
      this.decompressor.free();
      this.decompressor = requireLz4().Decompressor.withDict(
        new Uint8Array(dict),
      );
      this.dictReceived = true;
      return null;
    }

    if (sentinel === SENTINEL_PLAIN) {
      const plaintext = data.subarray(4);
      if (plaintext.byteLength > budgetRemaining) {
        throw new Error(
          `Part size ${plaintext.byteLength} exceeds remaining budget ${budgetRemaining}`,
        );
      }
      return { result: plaintext, consumed: plaintext.byteLength };
    }

    if (sentinel === SENTINEL_LZ4B) {
      if (data.byteLength < ENVELOPE_LZ4B) {
        throw new Error("LZ4B part too short for header");
      }
      const decompressedSize = readU64LE(data, 4);
      if (decompressedSize > this.blockSize) {
        throw new Error(
          `LZ4B size ${decompressedSize} exceeds block limit ${this.blockSize}`,
        );
      }
      if (decompressedSize > budgetRemaining) {
        throw new Error(
          `Declared size ${decompressedSize} exceeds remaining budget ${budgetRemaining}`,
        );
      }
      const compressed = data.subarray(ENVELOPE_LZ4B);
      rejectImplausibleCompressedSize(
        "LZ4B",
        decompressedSize,
        compressed.byteLength,
      );
      const result = this.decompressor.decompress(compressed, decompressedSize);
      return { result, consumed: decompressedSize };
    }

    if (sentinel === SENTINEL_LZ4M) {
      return this.decodeMultiBlock(data, budgetRemaining);
    }

    throw new Error(
      `Unknown sentinel: 0x${sentinel.toString(16).padStart(8, "0")}`,
    );
  }

  /** Decode one enveloped multipart message, or `null` for a dictionary shipment. */
  decodeMessage(parts: Uint8Array[]): Uint8Array[] | null {
    this.assertOpen();
    const budget = this.maxMessageSize ?? Infinity;
    let budgetRemaining = budget;
    const decoded: Uint8Array[] = [];

    for (const part of parts) {
      if (part.byteLength < 4) {
        throw new Error("Part too short for sentinel");
      }
      if (isLz4DictionaryShipment(part) && parts.length !== 1) {
        throw new Error("LZ4D dictionary shipment must be single-part");
      }

      const result = this.decodePart(part, budgetRemaining);
      if (result === null) return null;
      decoded.push(result.result);
      budgetRemaining -= result.consumed;
    }

    return decoded;
  }

  /**
   * Decode one wire message into application messages: none for a
   * dictionary shipment, several for an `LZ4A` arena.
   */
  decodeMessages(parts: Uint8Array[]): Uint8Array[][] {
    this.assertOpen();
    const first = parts[0];
    if (first && isArena(first)) {
      if (parts.length !== 1) {
        throw new Error("LZ4A arena must be single-part");
      }
      return this.decodeArena(first);
    }
    const decoded = this.decodeMessage(parts);
    return decoded === null ? [] : [decoded];
  }

  /** Release native WASM decoder memory. Safe to call more than once. */
  free(): void {
    if (this.freed) return;
    this.decompressor.free();
    this.freed = true;
  }

  /** @ignore */
  private assertOpen(): void {
    if (this.freed) throw new Error("LZ4 decoder closed");
  }

  /** @ignore */
  private decodeMultiBlock(
    data: Uint8Array,
    budgetRemaining: number,
  ): { result: Uint8Array; consumed: number } {
    if (data.byteLength < ENVELOPE_LZ4B) {
      throw new Error("LZ4M part too short for header");
    }

    const decompressedSize = readU64LE(data, 4);
    if (decompressedSize > budgetRemaining) {
      throw new Error(
        `Declared size ${decompressedSize} exceeds remaining budget ${budgetRemaining}`,
      );
    }
    rejectImplausibleCompressedSize(
      "LZ4M",
      decompressedSize,
      data.byteLength - ENVELOPE_LZ4B,
    );

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const result = new Uint8Array(decompressedSize);
    let inputOffset = ENVELOPE_LZ4B;
    let outputOffset = 0;

    while (outputOffset < decompressedSize) {
      if (inputOffset + 4 > data.byteLength) {
        throw new Error("LZ4M block length truncated");
      }
      const compressedLen = view.getUint32(inputOffset, true);
      inputOffset += 4;
      if (inputOffset + compressedLen > data.byteLength) {
        throw new Error("LZ4M block truncated");
      }

      const blockSize = Math.min(
        this.blockSize,
        decompressedSize - outputOffset,
      );
      const compressed = data.subarray(
        inputOffset,
        inputOffset + compressedLen,
      );
      inputOffset += compressedLen;

      const block = this.decompressor.decompress(compressed, blockSize);
      if (block.byteLength !== blockSize) {
        throw new Error("LZ4M decompressed block size mismatch");
      }
      result.set(block, outputOffset);
      outputOffset += blockSize;
    }

    if (inputOffset !== data.byteLength) {
      throw new Error("LZ4M trailing bytes");
    }

    return { result, consumed: decompressedSize };
  }

  /** @ignore */
  private decodeArena(data: Uint8Array): Uint8Array[][] {
    const invalid = () => new Error("Invalid LZ4A arena envelope");
    if (
      data.byteLength < ARENA_HEADER ||
      data.byteLength > ARENA_HEADER + MAX_ARENA_BYTES
    ) {
      throw invalid();
    }
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const mode = data[4]!;
    const size = view.getUint32(8, true);
    const count = view.getUint32(12, true);
    if (
      mode > ARENA_MODE_LZ4 || data[5] !== 0 || data[6] !== 0 ||
      data[7] !== 0 || size < 1 || size > MAX_ARENA_BYTES || count < 1 ||
      count > MAX_ARENA_MESSAGES
    ) {
      throw invalid();
    }

    const body = data.subarray(ARENA_HEADER);
    const decoded = mode === ARENA_MODE_LZ4
      ? this.decompressor.decompress(body, size)
      : body;
    if (decoded.byteLength !== size) {
      throw new Error("LZ4A arena size mismatch");
    }
    return splitArena(decoded, count, this.maxMessageSize);
  }
}

/** Stateful encoder for the omq LZ4 transport envelope. */
export class Lz4Encoder {
  private compressor: CompressorInstance;
  private sendDict: Uint8Array | null;
  private dictShipped = false;
  private readonly blockSize: number;
  private freed = false;

  /** Create an encoder with an optional pre-shared dictionary. */
  constructor(sendDict?: Uint8Array, blockSize = LZ4M_BLOCK_SIZE) {
    validateBlockSize(blockSize);
    this.sendDict = sendDict ?? null;
    if (this.sendDict && this.sendDict.byteLength > MAX_DICT_BYTES) {
      throw new Error(
        `Dictionary too large: ${this.sendDict.byteLength} > ${MAX_DICT_BYTES}`,
      );
    }
    const { Compressor } = requireLz4();
    this.compressor = sendDict
      ? Compressor.withDict(sendDict)
      : new Compressor();
    this.blockSize = blockSize;
  }

  /** @ignore Smallest part or arena worth compressing. */
  private get threshold(): number {
    return this.sendDict ? MIN_COMPRESS_WITH_DICT : MIN_COMPRESS_NO_DICT;
  }

  /** @ignore */
  private encodePart(plaintext: Uint8Array): Uint8Array {
    if (plaintext.byteLength < this.threshold) {
      return wrapPlain(plaintext);
    }

    if (plaintext.byteLength > MAX_TS_SEND_PART_SIZE) {
      throw new Error(
        `LZ4 send part ${plaintext.byteLength} exceeds TypeScript limit ${MAX_TS_SEND_PART_SIZE}`,
      );
    }

    if (plaintext.byteLength > this.blockSize) {
      return this.encodeMultiBlock(plaintext);
    }

    const compressed = this.compressor.compress(plaintext);

    if (
      compressed.byteLength + ENVELOPE_LZ4B >=
        plaintext.byteLength + ENVELOPE_PLAIN
    ) {
      return wrapPlain(plaintext);
    }

    return wrapLz4b(plaintext.byteLength, compressed);
  }

  /** Encode one multipart message, including dictionary shipment if needed. */
  encodeMessage(parts: Uint8Array[]): Uint8Array[][] {
    this.assertOpen();
    return this.withDictShipment(parts.map((p) => this.encodePart(p)));
  }

  /**
   * How many leading queued messages fit in one `LZ4A` arena. Below 2,
   * send the first message alone with `encodeMessage`.
   */
  arenaLength(queued: Iterable<{ parts: Uint8Array[] }>): number {
    let count = 0;
    let bytes = 0;
    let parts = 0;
    for (const msg of queued) {
      const size = arenaMessageSize(msg.parts);
      if (
        size === undefined || bytes + size > ARENA_TARGET_BYTES ||
        count === MAX_ARENA_MESSAGES ||
        parts + msg.parts.length > MAX_ARENA_PARTS
      ) {
        break;
      }
      count++;
      bytes += size;
      parts += msg.parts.length;
    }
    return count;
  }

  /** Encode messages as one arena, after a dictionary shipment if needed. */
  encodeArena(messages: Uint8Array[][]): Uint8Array[][] {
    this.assertOpen();
    let size = 0;
    let parts = 0;
    for (const msg of messages) {
      const msgSize = arenaMessageSize(msg);
      if (msgSize === undefined) {
        throw new Error("Message is not eligible for an LZ4A arena");
      }
      size += msgSize;
      parts += msg.length;
    }
    if (
      messages.length < 1 || messages.length > MAX_ARENA_MESSAGES ||
      size > MAX_ARENA_BYTES || parts > MAX_ARENA_PARTS
    ) {
      throw new Error("LZ4A arena exceeds limits");
    }

    let arena = new Uint8Array(ARENA_HEADER + size);
    let offset = ARENA_HEADER;
    for (const msg of messages) offset = writeMessage(msg, arena, offset);
    if (size >= this.threshold) {
      const compressed = this.compressor.compress(
        arena.subarray(ARENA_HEADER),
      );
      if (compressed.byteLength < size) {
        arena = new Uint8Array(ARENA_HEADER + compressed.byteLength);
        arena.set(compressed, ARENA_HEADER);
        arena[4] = ARENA_MODE_LZ4;
      }
    }
    arena[0] = 0x4c; // 'L'
    arena[1] = 0x5a; // 'Z'
    arena[2] = 0x34; // '4'
    arena[3] = 0x41; // 'A'
    const view = new DataView(arena.buffer);
    view.setUint32(8, size, true);
    view.setUint32(12, messages.length, true);
    return this.withDictShipment([arena]);
  }

  /** Release native WASM encoder memory. Safe to call more than once. */
  free(): void {
    if (this.freed) return;
    this.compressor.free();
    this.freed = true;
  }

  /** @ignore */
  private assertOpen(): void {
    if (this.freed) throw new Error("LZ4 encoder closed");
  }

  /** @ignore The dictionary ships once, before the first payload using it. */
  private withDictShipment(wire: Uint8Array[]): Uint8Array[][] {
    if (this.dictShipped || !this.sendDict) return [wire];
    this.dictShipped = true;
    return [[buildDictShipment(this.sendDict)], wire];
  }

  /** @ignore */
  private encodeMultiBlock(plaintext: Uint8Array): Uint8Array {
    const blocks: Uint8Array[] = [];
    for (
      let offset = 0;
      offset < plaintext.byteLength;
      offset += this.blockSize
    ) {
      const end = Math.min(offset + this.blockSize, plaintext.byteLength);
      blocks.push(this.compressor.compress(plaintext.subarray(offset, end)));
    }
    return wrapLz4m(plaintext.byteLength, blocks);
  }
}

function wrapPlain(data: Uint8Array): Uint8Array {
  const buf = new Uint8Array(ENVELOPE_PLAIN + data.byteLength);
  buf.set(data, ENVELOPE_PLAIN);
  return buf;
}

function wrapLz4b(
  decompressedSize: number,
  compressed: Uint8Array,
): Uint8Array {
  const buf = new Uint8Array(ENVELOPE_LZ4B + compressed.byteLength);
  buf[0] = 0x4c; // 'L'
  buf[1] = 0x5a; // 'Z'
  buf[2] = 0x34; // '4'
  buf[3] = 0x42; // 'B'
  const view = new DataView(buf.buffer);
  view.setBigUint64(4, BigInt(decompressedSize), true);
  buf.set(compressed, ENVELOPE_LZ4B);
  return buf;
}

function wrapLz4m(decompressedSize: number, blocks: Uint8Array[]): Uint8Array {
  let size = ENVELOPE_LZ4B;
  for (const block of blocks) size += 4 + block.byteLength;

  const buf = new Uint8Array(size);
  buf[0] = 0x4c; // 'L'
  buf[1] = 0x5a; // 'Z'
  buf[2] = 0x34; // '4'
  buf[3] = 0x4d; // 'M'
  const view = new DataView(buf.buffer);
  view.setBigUint64(4, BigInt(decompressedSize), true);

  let offset = ENVELOPE_LZ4B;
  for (const block of blocks) {
    view.setUint32(offset, block.byteLength, true);
    offset += 4;
    buf.set(block, offset);
    offset += block.byteLength;
  }

  return buf;
}

function buildDictShipment(dict: Uint8Array): Uint8Array {
  const buf = new Uint8Array(4 + dict.byteLength);
  buf[0] = 0x4c; // 'L'
  buf[1] = 0x5a; // 'Z'
  buf[2] = 0x34; // '4'
  buf[3] = 0x44; // 'D'
  buf.set(dict, 4);
  return buf;
}
