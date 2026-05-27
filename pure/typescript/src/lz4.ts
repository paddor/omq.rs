import {
  Compressor,
  Decompressor,
} from "@paddor/lz4rip"

const SENTINEL_PLAIN = 0x00000000
const SENTINEL_LZ4B = 0x4c5a3442 // "LZ4B"
const SENTINEL_LZ4M = 0x4c5a344d // "LZ4M"
const SENTINEL_LZ4D = 0x4c5a3444 // "LZ4D"

const MAX_DICT_BYTES = 8192
const MIN_COMPRESS_NO_DICT = 512
const MIN_COMPRESS_WITH_DICT = 32

// ENVELOPE_PLAIN = 4 bytes (sentinel only)
// ENVELOPE_LZ4B = 12 bytes (sentinel + u64 LE decompressed_size)
const ENVELOPE_PLAIN = 4
const ENVELOPE_LZ4B = 12

function readSentinel(data: Uint8Array): number {
  if (data.byteLength < 4) throw new Error("Part too short for sentinel")
  return (data[0]! << 24) | (data[1]! << 16) | (data[2]! << 8) | data[3]!
}

function readU64LE(data: Uint8Array, offset: number): number {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const lo = view.getUint32(offset, true)
  const hi = view.getUint32(offset + 4, true)
  if (hi > 0) throw new Error("Decompressed size exceeds safe integer range")
  return lo
}

export class Lz4Decoder {
  private decompressor: Decompressor
  private dictReceived = false
  private maxMessageSize: number | undefined

  constructor(maxMessageSize?: number) {
    this.decompressor = new Decompressor()
    this.maxMessageSize = maxMessageSize
  }

  decodePart(data: Uint8Array, budgetRemaining: number): { result: Uint8Array; consumed: number } | null {
    const sentinel = readSentinel(data)

    if (sentinel === SENTINEL_LZ4D) {
      if (this.dictReceived) {
        throw new Error("Duplicate LZ4D dictionary shipment")
      }
      const dict = data.subarray(4)
      if (dict.byteLength < 1 || dict.byteLength > MAX_DICT_BYTES) {
        throw new Error(`Invalid dictionary size: ${dict.byteLength}`)
      }
      this.decompressor.free()
      this.decompressor = Decompressor.withDict(new Uint8Array(dict))
      this.dictReceived = true
      return null
    }

    if (sentinel === SENTINEL_PLAIN) {
      const plaintext = data.subarray(4)
      if (plaintext.byteLength > budgetRemaining) {
        throw new Error(`Part size ${plaintext.byteLength} exceeds remaining budget ${budgetRemaining}`)
      }
      return { result: plaintext, consumed: plaintext.byteLength }
    }

    if (sentinel === SENTINEL_LZ4B) {
      if (data.byteLength < ENVELOPE_LZ4B) {
        throw new Error("LZ4B part too short for header")
      }
      const decompressedSize = readU64LE(data, 4)
      if (decompressedSize > budgetRemaining) {
        throw new Error(`Declared size ${decompressedSize} exceeds remaining budget ${budgetRemaining}`)
      }
      const compressed = data.subarray(ENVELOPE_LZ4B)
      const result = this.decompressor.decompress(compressed, decompressedSize)
      return { result, consumed: decompressedSize }
    }

    if (sentinel === SENTINEL_LZ4M) {
      throw new Error("LZ4M (multi-block) not supported")
    }

    throw new Error(`Unknown sentinel: 0x${sentinel.toString(16).padStart(8, "0")}`)
  }

  decodeMessage(parts: Uint8Array[]): Uint8Array[] | null {
    const budget = this.maxMessageSize ?? Infinity
    let budgetRemaining = budget
    const decoded: Uint8Array[] = []

    for (const part of parts) {
      if (part.byteLength < 4) {
        if (part.byteLength > budgetRemaining) {
          throw new Error(`Part size ${part.byteLength} exceeds remaining budget ${budgetRemaining}`)
        }
        decoded.push(part)
        budgetRemaining -= part.byteLength
        continue
      }

      const result = this.decodePart(part, budgetRemaining)
      if (result === null) return null
      decoded.push(result.result)
      budgetRemaining -= result.consumed
    }

    return decoded
  }
}


export class Lz4Encoder {
  private compressor: Compressor
  private sendDict: Uint8Array | null
  private dictShipped = false

  constructor(sendDict?: Uint8Array) {
    this.sendDict = sendDict ?? null
    if (this.sendDict && this.sendDict.byteLength > MAX_DICT_BYTES) {
      throw new Error(`Dictionary too large: ${this.sendDict.byteLength} > ${MAX_DICT_BYTES}`)
    }
    this.compressor = sendDict
      ? Compressor.withDict(sendDict)
      : new Compressor()
  }

  private encodePart(plaintext: Uint8Array): Uint8Array {
    const threshold = this.sendDict ? MIN_COMPRESS_WITH_DICT : MIN_COMPRESS_NO_DICT

    if (plaintext.byteLength < threshold) {
      return wrapPlain(plaintext)
    }

    const compressed = this.compressor.compress(plaintext)

    if (compressed.byteLength + ENVELOPE_LZ4B >= plaintext.byteLength + ENVELOPE_PLAIN) {
      return wrapPlain(plaintext)
    }

    return wrapLz4b(plaintext.byteLength, compressed)
  }

  encodeMessage(parts: Uint8Array[]): Uint8Array[][] {
    const encoded = parts.map((p) => this.encodePart(p))
    if (!this.dictShipped && this.sendDict) {
      this.dictShipped = true
      const dictMsg = buildDictShipment(this.sendDict)
      return [[dictMsg], encoded]
    }
    return [encoded]
  }
}


function wrapPlain(data: Uint8Array): Uint8Array {
  const buf = new Uint8Array(ENVELOPE_PLAIN + data.byteLength)
  buf.set(data, ENVELOPE_PLAIN)
  return buf
}

function wrapLz4b(decompressedSize: number, compressed: Uint8Array): Uint8Array {
  const buf = new Uint8Array(ENVELOPE_LZ4B + compressed.byteLength)
  buf[0] = 0x4c // 'L'
  buf[1] = 0x5a // 'Z'
  buf[2] = 0x34 // '4'
  buf[3] = 0x42 // 'B'
  const view = new DataView(buf.buffer)
  view.setUint32(4, decompressedSize, true) // u64 LE low
  view.setUint32(8, 0, true)               // u64 LE high
  buf.set(compressed, ENVELOPE_LZ4B)
  return buf
}

function buildDictShipment(dict: Uint8Array): Uint8Array {
  const buf = new Uint8Array(4 + dict.byteLength)
  buf[0] = 0x4c // 'L'
  buf[1] = 0x5a // 'Z'
  buf[2] = 0x34 // '4'
  buf[3] = 0x44 // 'D'
  buf.set(dict, 4)
  return buf
}
