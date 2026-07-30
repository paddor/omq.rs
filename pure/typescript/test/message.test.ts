import { describe, expect, it } from "vitest";
import { Message } from "../src/message.ts";

describe("Message", () => {
  it("constructs from strings", () => {
    const msg = new Message("hello", "world");
    expect(msg.parts.length).toBe(2);
    expect(msg.string(0)).toBe("hello");
    expect(msg.string(1)).toBe("world");
  });

  it("constructs from Uint8Array", () => {
    const data = new Uint8Array([0x01, 0x02, 0x03]);
    const msg = new Message(data);
    expect(msg.parts.length).toBe(1);
    expect(msg.parts[0]).toBe(data);
  });

  it("constructs from mixed string and Uint8Array", () => {
    const data = new Uint8Array([0xff]);
    const msg = new Message("topic", data);
    expect(msg.parts.length).toBe(2);
    expect(msg.string(0)).toBe("topic");
    expect(msg.parts[1]).toBe(data);
  });

  it("Message.from creates single-part string message", () => {
    const msg = Message.from("COLOR 10:20 FF0000");
    expect(msg.parts.length).toBe(1);
    expect(msg.string(0)).toBe("COLOR 10:20 FF0000");
  });

  it("Message.fromParts wraps existing arrays", () => {
    const parts = [new Uint8Array([1]), new Uint8Array([2, 3])];
    const msg = Message.fromParts(parts);
    expect(msg.parts.length).toBe(2);
    expect(msg.parts[0]).toBe(parts[0]);
    expect(msg.parts[1]).toBe(parts[1]);
  });

  it("string() throws on out-of-range index", () => {
    const msg = new Message("only");
    expect(() => msg.string(1)).toThrow(RangeError);
    expect(() => msg.string(-1)).toThrow(RangeError);
  });

  it("size sums all part lengths", () => {
    const msg = new Message("abc", new Uint8Array(10));
    expect(msg.size).toBe(3 + 10);
  });

  it("empty message has zero parts and zero size", () => {
    const msg = new Message();
    expect(msg.parts.length).toBe(0);
    expect(msg.size).toBe(0);
  });
});
