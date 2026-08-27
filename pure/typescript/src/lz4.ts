import type {
  Compressor as CompressorInstance,
  Decompressor as DecompressorInstance,
} from "@paddor/lz4rip";
import { setLz4Factories } from "./lz4-registry.ts";

type Lz4Module = typeof import("@paddor/lz4rip");

const SENTINEL_PLAIN = 0x00000000;
const SENTINEL_LZ4B = 0x4c5a3442; // "LZ4B"
const SENTINEL_LZ4M = 0x4c5a344d; // "LZ4M"
const SENTINEL_LZ4D = 0x4c5a3444; // "LZ4D"

const MAX_DICT_BYTES = 8192;
const LZ4M_BLOCK_SIZE = 0x40000000;
const LZ4_MAX_EXPANSION = 255;
const MAX_TS_SEND_PART_SIZE = LZ4M_BLOCK_SIZE - 1;
const MIN_COMPRESS_NO_DICT = 512;
const MIN_COMPRESS_WITH_DICT = 128;

let lz4Module: Lz4Module | null = null;

// ENVELOPE_PLAIN = 4 bytes (sentinel only)
// ENVELOPE_LZ4B = 12 bytes (sentinel + u64 LE decompressed_size)
const ENVELOPE_PLAIN = 4;
const ENVELOPE_LZ4B = 12;

/** Initialize the LZ4 WASM module. Call once before connecting to `lz4+` URLs. */
export async function initLz4(): Promise<void> {
  if (lz4Module) {
    installLz4Factories();
    return;
  }
  const mod = await import("@paddor/lz4rip");
  await mod.init();
  lz4Module = mod;
  installLz4Factories();
}

/** Initialize LZ4 from bytes. Useful in runtimes where fetch cannot load file URLs. */
export async function initLz4FromBytes(bytes: BufferSource): Promise<void> {
  const mod = await import("@paddor/lz4rip");
  mod.initSyncFromBytes(bytes);
  lz4Module = mod;
  installLz4Factories();
}

function installLz4Factories(): void {
  setLz4Factories({
    createDecoder: (maxMessageSize) => new Lz4Decoder(maxMessageSize),
    createEncoder: (dict) => new Lz4Encoder(dict),
    isDictionaryShipment: isLz4DictionaryShipment,
  });
}

function requireLz4(): Lz4Module {
  if (!lz4Module) {
    throw new Error("LZ4 not initialized. Call initLz4() before lz4+ sockets.");
  }
  return lz4Module;
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

export function isLz4DictionaryShipment(data: Uint8Array): boolean {
  return data.byteLength >= 4 && readSentinel(data) === SENTINEL_LZ4D;
}

export class Lz4Decoder {
  private decompressor: DecompressorInstance;
  private dictReceived = false;
  private maxMessageSize: number | undefined;
  private readonly blockSize: number;
  private freed = false;

  constructor(maxMessageSize?: number, blockSize = LZ4M_BLOCK_SIZE) {
    validateBlockSize(blockSize);
    this.decompressor = new (requireLz4().Decompressor)();
    this.maxMessageSize = maxMessageSize;
    this.blockSize = blockSize;
  }

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

  free(): void {
    if (this.freed) return;
    this.decompressor.free();
    this.freed = true;
  }

  private assertOpen(): void {
    if (this.freed) throw new Error("LZ4 decoder closed");
  }

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
}

export class Lz4Encoder {
  private compressor: CompressorInstance;
  private sendDict: Uint8Array | null;
  private dictShipped = false;
  private readonly blockSize: number;
  private freed = false;

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

  private encodePart(plaintext: Uint8Array): Uint8Array {
    const threshold = this.sendDict
      ? MIN_COMPRESS_WITH_DICT
      : MIN_COMPRESS_NO_DICT;

    if (plaintext.byteLength < threshold) {
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

  encodeMessage(parts: Uint8Array[]): Uint8Array[][] {
    this.assertOpen();
    const encoded = parts.map((p) => this.encodePart(p));
    if (!this.dictShipped && this.sendDict) {
      this.dictShipped = true;
      const dictMsg = buildDictShipment(this.sendDict);
      return [[dictMsg], encoded];
    }
    return [encoded];
  }

  free(): void {
    if (this.freed) return;
    this.compressor.free();
    this.freed = true;
  }

  private assertOpen(): void {
    if (this.freed) throw new Error("LZ4 encoder closed");
  }

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
