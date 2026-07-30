import { describe, expect, it } from "vitest";
import {
  decodeZwsFrame,
  encodeCommandFrame,
  encodeDataFrames,
  encodeZwsFrame,
  FLAG_COMMAND,
  FLAG_FINAL,
  FLAG_MORE,
} from "../src/zws.ts";

describe("ZWS flag constants", () => {
  it("FINAL is 0x00", () => expect(FLAG_FINAL).toBe(0x00));
  it("MORE is 0x01", () => expect(FLAG_MORE).toBe(0x01));
  it("COMMAND is 0x02", () => expect(FLAG_COMMAND).toBe(0x02));
});

describe("decodeZwsFrame", () => {
  it("decodes FINAL frame", () => {
    const buf = new Uint8Array([0x00, 0x41, 0x42]).buffer;
    const frame = decodeZwsFrame(buf);
    expect(frame.flag).toBe(FLAG_FINAL);
    expect(frame.payload).toEqual(new Uint8Array([0x41, 0x42]));
  });

  it("decodes MORE frame", () => {
    const buf = new Uint8Array([0x01, 0x01]).buffer;
    const frame = decodeZwsFrame(buf);
    expect(frame.flag).toBe(FLAG_MORE);
    expect(frame.payload).toEqual(new Uint8Array([0x01]));
  });

  it("decodes COMMAND frame", () => {
    const buf = new Uint8Array([0x02, 0x05]).buffer;
    const frame = decodeZwsFrame(buf);
    expect(frame.flag).toBe(FLAG_COMMAND);
  });

  it("decodes frame with empty payload", () => {
    const buf = new Uint8Array([0x00]).buffer;
    const frame = decodeZwsFrame(buf);
    expect(frame.flag).toBe(FLAG_FINAL);
    expect(frame.payload.byteLength).toBe(0);
  });

  it("rejects empty buffer", () => {
    expect(() => decodeZwsFrame(new ArrayBuffer(0))).toThrow("too short");
  });

  it("rejects invalid flag", () => {
    const buf = new Uint8Array([0x03, 0x00]).buffer;
    expect(() => decodeZwsFrame(buf)).toThrow("Invalid ZWS flag");
  });

  it("rejects flag 0xFF", () => {
    const buf = new Uint8Array([0xff]).buffer;
    expect(() => decodeZwsFrame(buf)).toThrow("Invalid ZWS flag");
  });
});

describe("encodeZwsFrame", () => {
  it("prepends flag byte", () => {
    const payload = new Uint8Array([0x41, 0x42]);
    const encoded = encodeZwsFrame(FLAG_FINAL, payload);
    expect(encoded).toEqual(new Uint8Array([0x00, 0x41, 0x42]));
  });

  it("handles empty payload", () => {
    const encoded = encodeZwsFrame(FLAG_MORE, new Uint8Array(0));
    expect(encoded).toEqual(new Uint8Array([0x01]));
  });
});

describe("encodeZwsFrame / decodeZwsFrame round-trip", () => {
  for (const flag of [FLAG_FINAL, FLAG_MORE, FLAG_COMMAND]) {
    it(`round-trips flag 0x${flag.toString(16).padStart(2, "0")}`, () => {
      const payload = new Uint8Array([10, 20, 30]);
      const encoded = encodeZwsFrame(flag, payload);
      const decoded = decodeZwsFrame(encoded.buffer as ArrayBuffer);
      expect(decoded.flag).toBe(flag);
      expect(decoded.payload).toEqual(payload);
    });
  }
});

describe("encodeDataFrames", () => {
  it("single part gets FINAL flag", () => {
    const frames = encodeDataFrames({ parts: [new Uint8Array([0x41])] });
    expect(frames.length).toBe(1);
    expect(frames[0]![0]).toBe(FLAG_FINAL);
    expect(frames[0]![1]).toBe(0x41);
  });

  it("multiple parts: all MORE except last FINAL", () => {
    const parts = [
      new Uint8Array([1]),
      new Uint8Array([2]),
      new Uint8Array([3]),
    ];
    const frames = encodeDataFrames({ parts });
    expect(frames.length).toBe(3);
    expect(frames[0]![0]).toBe(FLAG_MORE);
    expect(frames[1]![0]).toBe(FLAG_MORE);
    expect(frames[2]![0]).toBe(FLAG_FINAL);
  });
});

describe("encodeCommandFrame", () => {
  it("uses COMMAND flag", () => {
    const payload = new Uint8Array([0x05]);
    const frame = encodeCommandFrame(payload);
    expect(frame[0]).toBe(FLAG_COMMAND);
    expect(frame[1]).toBe(0x05);
  });
});
