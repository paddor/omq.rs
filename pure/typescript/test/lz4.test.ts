import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { Compressor, initSyncFromBytes } from "@paddor/lz4rip";
import { Lz4Decoder, Lz4Encoder } from "../src/lz4.ts";

const enc = new TextEncoder();

beforeAll(() => {
  const wasmPath = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "../node_modules/@paddor/lz4rip/src/pkg/lz4rip.wasm",
  );
  initSyncFromBytes(readFileSync(wasmPath));
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
