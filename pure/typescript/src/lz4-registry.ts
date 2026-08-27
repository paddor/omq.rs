export interface Lz4DecoderLike {
  decodeMessage(parts: Uint8Array[]): Uint8Array[] | null;
  free(): void;
}

export interface Lz4EncoderLike {
  encodeMessage(parts: Uint8Array[]): Uint8Array[][];
  free(): void;
}

interface Lz4Factories {
  createDecoder(maxMessageSize?: number): Lz4DecoderLike;
  createEncoder(dict?: Uint8Array): Lz4EncoderLike;
  isDictionaryShipment(part: Uint8Array): boolean;
}

let factories: Lz4Factories | null = null;

export function setLz4Factories(next: Lz4Factories): void {
  factories = next;
}

export function createLz4Decoder(
  maxMessageSize?: number,
): Lz4DecoderLike {
  return requireFactories().createDecoder(maxMessageSize);
}

export function createLz4Encoder(dict?: Uint8Array): Lz4EncoderLike {
  return requireFactories().createEncoder(dict);
}

export function isLz4DictionaryShipment(part: Uint8Array): boolean {
  return factories?.isDictionaryShipment(part) ?? false;
}

function requireFactories(): Lz4Factories {
  if (!factories) {
    throw new Error("LZ4 not initialized. Call initLz4() before lz4+ sockets.");
  }
  return factories;
}
