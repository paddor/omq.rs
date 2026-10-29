import { describe, expect, it } from "vitest";
import {
  decodeGreeting,
  encodeCommandFrame,
  encodeGreeting,
  encodeMessage,
  MAX_COMMAND_BYTES,
  ZmtpDecoder,
  type ZmtpEvent,
} from "../src/zmtp.ts";
import { encodeReady } from "../src/command.ts";

function feedBytewise(decoder: ZmtpDecoder, bytes: Uint8Array): ZmtpEvent[] {
  const events: ZmtpEvent[] = [];
  for (const byte of bytes) {
    events.push(...decoder.push(new Uint8Array([byte])));
  }
  return events;
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

describe("ZMTP greeting", () => {
  it("encodes signature, version 3.1, mechanism, and role", () => {
    const greeting = encodeGreeting("PLAIN", true);
    expect(greeting.byteLength).toBe(64);
    expect(greeting[0]).toBe(0xff);
    expect(greeting[9]).toBe(0x7f);
    expect([greeting[10], greeting[11]]).toEqual([3, 1]);
    expect(decodeGreeting(greeting)).toEqual({
      major: 3,
      minor: 1,
      mechanism: "PLAIN",
      asServer: true,
    });
  });

  it("rejects bad signatures, old versions, and dirty padding", () => {
    const bad = encodeGreeting("NULL");
    bad[9] = 0;
    expect(() => decodeGreeting(bad)).toThrow(/signature/);
    const old = encodeGreeting("NULL");
    old[10] = 2;
    expect(() => decodeGreeting(old)).toThrow(/version/);
    const dirty = encodeGreeting("NULL");
    dirty[20] = 0x41;
    expect(() => decodeGreeting(dirty)).toThrow(/padding/);
  });
});

describe("ZMTP frames", () => {
  it("round-trips multipart, empty, and long parts across chunk splits", () => {
    const long = new Uint8Array(70_000).map((_, i) => i % 251);
    const bytes = concat(
      encodeGreeting("NULL"),
      encodeCommandFrame(encodeReady("PULL", new Uint8Array(0))),
      encodeMessage([new Uint8Array([1, 2]), new Uint8Array(0), long]),
      encodeMessage([new Uint8Array(0)]),
    );
    const events = feedBytewise(new ZmtpDecoder(), bytes);
    expect(events.map((e) => e.type)).toEqual([
      "greeting",
      "command",
      "message",
      "message",
    ]);
    const message = events[2]!;
    if (message.type !== "message") throw new Error("expected message");
    expect(message.parts.length).toBe(3);
    expect(message.parts[1]!.byteLength).toBe(0);
    expect(message.parts[2]).toEqual(long);
  });

  it("uses long headers above 255 bytes only", () => {
    expect(encodeMessage([new Uint8Array(255)])[0]).toBe(0x00);
    expect(encodeMessage([new Uint8Array(256)])[0]).toBe(0x02);
    expect(encodeMessage([new Uint8Array(1), new Uint8Array(1)])[0]).toBe(0x01);
  });

  it("never emits a partial message", () => {
    const decoder = new ZmtpDecoder();
    decoder.push(encodeGreeting("NULL"));
    const first = encodeMessage([new Uint8Array([1]), new Uint8Array([2])]);
    expect(decoder.push(first.subarray(0, 3))).toEqual([]);
    expect(decoder.hasPartialMessage).toBe(true);
    expect(decoder.push(first.subarray(3)).length).toBe(1);
  });

  it("rejects oversize messages from the header before the body arrives", () => {
    const decoder = new ZmtpDecoder({ maxMessageSize: 100 });
    decoder.push(encodeGreeting("NULL"));
    const header = new Uint8Array([0x02, 0, 0, 0, 0, 0, 0, 0, 101]);
    expect(() => decoder.push(header)).toThrow(/exceeds max/);
    const split = new ZmtpDecoder({ maxMessageSize: 100 });
    split.push(encodeGreeting("NULL"));
    split.push(
      encodeMessage([new Uint8Array(60), new Uint8Array(1)]).subarray(0, 62),
    );
    expect(() => split.push(new Uint8Array([0x00, 41]))).toThrow(/exceeds max/);
  });

  it("rejects invalid flags, huge lengths, and bad commands", () => {
    const flags = new ZmtpDecoder();
    flags.push(encodeGreeting("NULL"));
    expect(() => flags.push(new Uint8Array([0x08, 0]))).toThrow(/flags/);
    const huge = new ZmtpDecoder();
    huge.push(encodeGreeting("NULL"));
    expect(() =>
      huge.push(
        new Uint8Array([0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
      )
    ).toThrow(/range/);
    const more = new ZmtpDecoder();
    more.push(encodeGreeting("NULL"));
    expect(() => more.push(new Uint8Array([0x05, 1, 0]))).toThrow(/MORE/);
    const big = new ZmtpDecoder();
    big.push(encodeGreeting("NULL"));
    const size = MAX_COMMAND_BYTES + 1;
    const header = new Uint8Array(9);
    header[0] = 0x06;
    new DataView(header.buffer).setBigUint64(1, BigInt(size), false);
    expect(() => big.push(header)).toThrow(/command too large/);
  });
});
