import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { Compressor, initSyncFromBytes } from "@paddor/lz4rip";
import { initLz4FromBytes, Lz4Decoder, Lz4Encoder } from "../src/lz4.ts";

const enc = new TextEncoder();

beforeAll(async () => {
  const wasmPath = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "../node_modules/@paddor/lz4rip/src/pkg/lz4rip_wasm_bg.wasm",
  );
  const wasm = readFileSync(wasmPath);
  initSyncFromBytes(wasm);
  await initLz4FromBytes(wasm);
});

function lz4m(payload: Uint8Array, blockSize = payload.byteLength): Uint8Array {
  const compressor = new Compressor();
  const blocks: Uint8Array[] = [];
  for (let offset = 0; offset < payload.byteLength; offset += blockSize) {
    const end = Math.min(offset + blockSize, payload.byteLength);
    blocks.push(compressor.compress(payload.subarray(offset, end)));
  }

  let size = 12;
  for (const block of blocks) size += 4 + block.byteLength;

  const buf = new Uint8Array(size);
  buf[0] = 0x4c;
  buf[1] = 0x5a;
  buf[2] = 0x34;
  buf[3] = 0x4d;
  const view = new DataView(buf.buffer);
  view.setBigUint64(4, BigInt(payload.byteLength), true);

  let offset = 12;
  for (const block of blocks) {
    view.setUint32(offset, block.byteLength, true);
    offset += 4;
    buf.set(block, offset);
    offset += block.byteLength;
  }

  return buf;
}

describe("LZ4 transport transform", () => {
  it("round-trips plaintext envelope parts", () => {
    const encoded = new Lz4Encoder().encodeMessage([enc.encode("short")]);
    expect(encoded.length).toBe(1);
    expect(encoded[0]![0]!.subarray(0, 4)).toEqual(
      new Uint8Array([0, 0, 0, 0]),
    );

    const decoded = new Lz4Decoder().decodeMessage(encoded[0]!);
    expect(decoded?.length).toBe(1);
    expect(new TextDecoder().decode(decoded![0])).toBe("short");
  });

  it("rejects use after explicit free", () => {
    const encoder = new Lz4Encoder();
    encoder.free();
    encoder.free();
    expect(() => encoder.encodeMessage([enc.encode("payload")])).toThrow(
      "encoder closed",
    );

    const decoder = new Lz4Decoder();
    decoder.free();
    decoder.free();
    expect(() => decoder.decodeMessage([new Uint8Array([0, 0, 0, 0])]))
      .toThrow("decoder closed");
  });

  it("rejects compressed-transport parts shorter than the sentinel", () => {
    const decoder = new Lz4Decoder();
    expect(() => decoder.decodeMessage([new Uint8Array([1, 2, 3])])).toThrow(
      "sentinel",
    );
  });

  it("requires dictionary shipments to be single-part messages", () => {
    const dict = enc.encode("market-price-market-price-market-price");
    const payload = enc.encode("market-price=".repeat(20));
    const [dictMessage, dataMessage] = new Lz4Encoder(dict).encodeMessage([
      payload,
    ]);

    expect(() =>
      new Lz4Decoder().decodeMessage([dictMessage![0]!, dataMessage![0]!])
    ).toThrow("single-part");
  });

  it("installs a dictionary and decodes later parts", () => {
    const dict = enc.encode("market-price-market-price-market-price");
    const payload = enc.encode("market-price=".repeat(20));
    const [dictMessage, dataMessage] = new Lz4Encoder(dict).encodeMessage([
      payload,
    ]);
    const decoder = new Lz4Decoder();

    expect(decoder.decodeMessage(dictMessage!)).toBeNull();
    const decoded = decoder.decodeMessage(dataMessage!);
    expect(decoded).toEqual([payload]);
  });

  it("rejects LZ4B declared sizes above the single-block limit", () => {
    const part = new Uint8Array(12);
    part[0] = 0x4c;
    part[1] = 0x5a;
    part[2] = 0x34;
    part[3] = 0x42;
    new DataView(part.buffer).setBigUint64(4, BigInt(0x40000001), true);

    expect(() => new Lz4Decoder().decodeMessage([part])).toThrow("block limit");
  });

  it("rejects implausible LZ4B declared sizes before allocation", () => {
    const part = new Uint8Array(12);
    part[0] = 0x4c;
    part[1] = 0x5a;
    part[2] = 0x34;
    part[3] = 0x42;
    new DataView(part.buffer).setBigUint64(4, BigInt(0x40000000), true);

    expect(() => new Lz4Decoder().decodeMessage([part])).toThrow(
      "implausibly large",
    );
  });

  it("rejects implausible LZ4M declared sizes before allocation", () => {
    const part = new Uint8Array(12);
    part[0] = 0x4c;
    part[1] = 0x5a;
    part[2] = 0x34;
    part[3] = 0x4d;
    new DataView(part.buffer).setBigUint64(4, BigInt(0x40000001), true);

    expect(() => new Lz4Decoder().decodeMessage([part])).toThrow(
      "implausibly large",
    );
  });

  it("rejects TypeScript sends at the 1 GiB LZ4M boundary", () => {
    const encoder = new Lz4Encoder() as unknown as {
      encodePart(plaintext: Uint8Array): Uint8Array;
    };
    const hugePart = { byteLength: 0x40000000 } as Uint8Array;

    expect(() => encoder.encodePart(hugePart)).toThrow("TypeScript limit");
  });

  it("decodes LZ4M multi-block envelope parts with a test block size", () => {
    const payload = enc.encode("lz4m payload ".repeat(60));
    const decoded = new Lz4Decoder(undefined, 256).decodeMessage([
      lz4m(payload, 256),
    ]);

    expect(decoded).toEqual([payload]);
  });

  it("round-trips LZ4M with a test block size", () => {
    const payload = enc.encode("lz4m payload ".repeat(60));
    const encoded = new Lz4Encoder(undefined, 256).encodeMessage([payload]);

    expect(encoded[0]![0]!.subarray(0, 4)).toEqual(
      new Uint8Array([0x4c, 0x5a, 0x34, 0x4d]),
    );
    const decoded = new Lz4Decoder(undefined, 256).decodeMessage(encoded[0]!);

    expect(decoded).toEqual([payload]);
  });
});

/** Raw (mode 0) arena around hand-written inner ZMTP frames. */
function rawArena(body: number[], count: number): Uint8Array {
  const arena = new Uint8Array(16 + body.length);
  arena.set([0x4c, 0x5a, 0x34, 0x41]);
  const view = new DataView(arena.buffer);
  view.setUint32(8, body.length, true);
  view.setUint32(12, count, true);
  arena.set(body, 16);
  return arena;
}

describe("LZ4A arenas", () => {
  const messages = [
    [new Uint8Array(0)],
    [enc.encode("x".repeat(300))],
    [new Uint8Array(0), enc.encode("tail"), new Uint8Array(0)],
  ];

  it("round-trips short, long, and empty parts raw and compressed", () => {
    const compressible = [...messages, [enc.encode("y".repeat(600))]];
    for (const batch of [messages.slice(0, 1), compressible]) {
      const wire = new Lz4Encoder().encodeArena(batch);
      expect(wire.length).toBe(1);
      const arena = wire[0]![0]!;
      expect(arena.subarray(0, 4)).toEqual(enc.encode("LZ4A"));
      expect(arena[4]).toBe(batch === compressible ? 1 : 0);
      expect(new Lz4Decoder().decodeMessages(wire[0]!)).toEqual(batch);
    }
  });

  it("ships the dictionary once before the first arena", () => {
    const dict = enc.encode("market-price-market-price-market-price");
    const batch = [[enc.encode("market-price=".repeat(20))], ...messages];
    const encoder = new Lz4Encoder(dict);
    const [dictMessage, arena] = encoder.encodeArena(batch);
    const decoder = new Lz4Decoder();

    expect(decoder.decodeMessages(dictMessage!)).toEqual([]);
    expect(decoder.decodeMessages(arena!)).toEqual(batch);
    const later = encoder.encodeArena(batch);
    expect(later.length).toBe(1);
    expect(decoder.decodeMessages(later[0]!)).toEqual(batch);
  });

  it("decodes per-part messages through decodeMessages", () => {
    const wire = new Lz4Encoder().encodeMessage([enc.encode("short")]);
    expect(new Lz4Decoder().decodeMessages(wire[0]!)).toEqual([
      [enc.encode("short")],
    ]);
  });

  it("batches queued messages up to the arena limits", () => {
    const encoder = new Lz4Encoder();
    const small = { parts: [enc.encode("small")] };
    const large = { parts: [new Uint8Array(8 * 1024)] };
    const medium = { parts: [new Uint8Array(4000)] };

    expect(encoder.arenaLength([small, small, large, small])).toBe(2);
    expect(encoder.arenaLength([large, small])).toBe(0);
    expect(encoder.arenaLength(Array(300).fill(small))).toBe(256);
    expect(encoder.arenaLength(Array(5).fill(medium))).toBe(4);
    expect(encoder.arenaLength([{ parts: [] }, small])).toBe(0);
  });

  it("applies maxMessageSize to each message in an arena", () => {
    const wire = new Lz4Encoder().encodeArena(
      Array(4).fill([new Uint8Array(64)]),
    )[0]!;

    expect(new Lz4Decoder(64).decodeMessages(wire).length).toBe(4);
    expect(() => new Lz4Decoder(63).decodeMessages(wire)).toThrow(
      "exceeds max",
    );
  });

  it("rejects malformed arenas before delivering any message", () => {
    for (
      const arena of [
        rawArena([1, 0], 1), // MORE without a final part
        rawArena([4, 0], 1), // command flag
        rawArena([8, 0], 1), // reserved flag
        rawArena([0, 2, 0x61], 1), // truncated body
        rawArena([2, 0], 1), // truncated long length
        rawArena([0, 0, 0], 1), // trailing byte
        rawArena([0, 0], 0), // zero messages
        rawArena([0, 0], 2), // message count mismatch
        rawArena([], 1), // empty arena
      ]
    ) {
      expect(() => new Lz4Decoder().decodeMessages([arena])).toThrow("LZ4A");
    }

    const valid = rawArena([0, 0, 0, 0], 2);
    expect(new Lz4Decoder().decodeMessages([valid]).length).toBe(2);
    for (const [offset, value] of [[4, 2], [5, 1], [7, 1], [10, 2], [15, 1]]) {
      const bad = new Uint8Array(valid);
      bad[offset!] = value!;
      expect(() => new Lz4Decoder().decodeMessages([bad])).toThrow("LZ4A");
    }
    expect(() => new Lz4Decoder().decodeMessages([valid, new Uint8Array(4)]))
      .toThrow("single-part");
    const compressedFlagOnRaw = new Uint8Array(valid);
    compressedFlagOnRaw[4] = 1;
    expect(() => new Lz4Decoder().decodeMessages([compressedFlagOnRaw]))
      .toThrow();
    expect(() =>
      new Lz4Decoder().decodeMessages([rawArena(Array(2 * 4097).fill(0), 2)])
    ).toThrow("LZ4A");
  });

  it("rejects compressed arenas with unused input", () => {
    const arena = new Lz4Encoder().encodeArena([
      [enc.encode("z".repeat(600))],
      [enc.encode("z".repeat(600))],
    ])[0]![0]!;
    expect(arena[4]).toBe(1);
    const padded = new Uint8Array(arena.byteLength + 1);
    padded.set(arena);

    expect(() => new Lz4Decoder().decodeMessages([padded])).toThrow();
  });
});
