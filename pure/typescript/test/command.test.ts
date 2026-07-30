import { describe, expect, it } from "vitest";
import {
  decodeCommand,
  decodeReadyProperties,
  encodeCancel,
  encodeJoin,
  encodeLeave,
  encodeReady,
  encodeSubscribe,
  isCompatibleSocketType,
  isSocketTypeName,
} from "../src/command.ts";

const enc = new TextEncoder();

describe("encodeReady / decodeCommand / decodeReadyProperties round-trip", () => {
  it("encodes and decodes REQ with identity", () => {
    const identity = enc.encode("web:paddor");
    const buf = encodeReady("REQ", identity);

    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("READY");

    const props = decodeReadyProperties(cmd.body);
    expect(props.socketType).toBe("REQ");
    expect(props.identity).toEqual(identity);
  });

  it("encodes and decodes SUB with empty identity", () => {
    const buf = encodeReady("SUB", new Uint8Array(0));
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("READY");

    const props = decodeReadyProperties(cmd.body);
    expect(props.socketType).toBe("SUB");
    expect(props.identity).toBeUndefined();
  });

  it("encodes and decodes PUSH", () => {
    const buf = encodeReady("PUSH", enc.encode("cli:test"));
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("READY");

    const props = decodeReadyProperties(cmd.body);
    expect(props.socketType).toBe("PUSH");
    expect(new TextDecoder().decode(props.identity)).toBe("cli:test");
  });
});

describe("READY wire format", () => {
  it("starts with name length 5 followed by READY", () => {
    const buf = encodeReady("REQ", new Uint8Array(0));
    expect(buf[0]).toBe(5);
    expect(new TextDecoder().decode(buf.subarray(1, 6))).toBe("READY");
  });

  it("Socket-Type property uses big-endian value length", () => {
    const buf = encodeReady("SUB", new Uint8Array(0));
    const cmd = decodeCommand(buf);

    // After "READY": first property key
    const body = cmd.body;
    const keyLen = body[0]!;
    const key = new TextDecoder().decode(body.subarray(1, 1 + keyLen));
    expect(key).toBe("Socket-Type");

    // Value length is 4 bytes big-endian
    const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
    const valueLen = view.getUint32(1 + keyLen, false);
    expect(valueLen).toBe(3); // "SUB"
  });
});

describe("decodeCommand", () => {
  it("rejects empty input", () => {
    expect(() => decodeCommand(new Uint8Array(0))).toThrow("Empty command");
  });

  it("rejects truncated name", () => {
    expect(() => decodeCommand(new Uint8Array([10, 0x41]))).toThrow(
      "truncated",
    );
  });

  it("parses command with no body", () => {
    // name_len=4, name="PING", no body
    const buf = new Uint8Array([4, 0x50, 0x49, 0x4e, 0x47]);
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("PING");
    expect(cmd.body.byteLength).toBe(0);
  });
});

describe("encodeSubscribe", () => {
  it("encodes SUBSCRIBE with prefix", () => {
    const prefix = enc.encode("canvas:");
    const buf = encodeSubscribe(prefix);
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(cmd.body)).toBe("canvas:");
  });

  it("encodes SUBSCRIBE with empty prefix (subscribe to all)", () => {
    const buf = encodeSubscribe(new Uint8Array(0));
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("SUBSCRIBE");
    expect(cmd.body.byteLength).toBe(0);
  });
});

describe("encodeCancel", () => {
  it("encodes CANCEL with prefix", () => {
    const prefix = enc.encode("gol:");
    const buf = encodeCancel(prefix);
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("CANCEL");
    expect(new TextDecoder().decode(cmd.body)).toBe("gol:");
  });
});

describe("encodeJoin", () => {
  it("encodes JOIN with group name", () => {
    const group = enc.encode("stocks");
    const buf = encodeJoin(group);
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("JOIN");
    expect(new TextDecoder().decode(cmd.body)).toBe("stocks");
  });

  it("encodes JOIN with empty group", () => {
    const buf = encodeJoin(new Uint8Array(0));
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("JOIN");
    expect(cmd.body.byteLength).toBe(0);
  });
});

describe("encodeLeave", () => {
  it("encodes LEAVE with group name", () => {
    const group = enc.encode("stocks");
    const buf = encodeLeave(group);
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("LEAVE");
    expect(new TextDecoder().decode(cmd.body)).toBe("stocks");
  });
});

describe("decodeCommand edge cases", () => {
  it("parses unknown command and preserves body", () => {
    const name = enc.encode("FOOBAR");
    const body = new Uint8Array([0xde, 0xad]);
    const buf = new Uint8Array(1 + name.byteLength + body.byteLength);
    buf[0] = name.byteLength;
    buf.set(name, 1);
    buf.set(body, 1 + name.byteLength);

    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("FOOBAR");
    expect(cmd.body).toEqual(body);
  });

  it("handles max name length (255 bytes)", () => {
    const nameLen = 255;
    const name = new Uint8Array(nameLen).fill(0x41);
    const buf = new Uint8Array(1 + nameLen);
    buf[0] = nameLen;
    buf.set(name, 1);

    const cmd = decodeCommand(buf);
    expect(cmd.name.length).toBe(255);
    expect(cmd.body.byteLength).toBe(0);
  });

  it("handles command with body but no properties", () => {
    const buf = new Uint8Array([4, 0x50, 0x4f, 0x4e, 0x47, 0x01, 0x02]); // PONG + context
    const cmd = decodeCommand(buf);
    expect(cmd.name).toBe("PONG");
    expect(cmd.body).toEqual(new Uint8Array([0x01, 0x02]));
  });
});

describe("encodeReady wire format details", () => {
  it("Identity property uses big-endian value length", () => {
    const identity = enc.encode("web:paddor");
    const buf = encodeReady("REQ", identity);
    const cmd = decodeCommand(buf);
    const props = decodeReadyProperties(cmd.body);
    expect(props.identity).toEqual(identity);
  });

  it("encodes all standard socket types", () => {
    for (
      const st of [
        "REQ",
        "REP",
        "PUB",
        "SUB",
        "PUSH",
        "PULL",
        "DEALER",
        "ROUTER",
        "PAIR",
      ] as const
    ) {
      const buf = encodeReady(st, new Uint8Array(0));
      const cmd = decodeCommand(buf);
      const props = decodeReadyProperties(cmd.body);
      expect(props.socketType).toBe(st);
    }
  });

  it("encodes all draft socket types", () => {
    for (
      const st of [
        "CLIENT",
        "SERVER",
        "RADIO",
        "DISH",
        "GATHER",
        "SCATTER",
        "PEER",
        "CHANNEL",
      ] as const
    ) {
      const buf = encodeReady(st, new Uint8Array(0));
      const cmd = decodeCommand(buf);
      const props = decodeReadyProperties(cmd.body);
      expect(props.socketType).toBe(st);
    }
  });
});

describe("decodeReadyProperties edge cases", () => {
  it("handles truncated property key", () => {
    // key_len=50 but only 2 bytes follow
    const body = new Uint8Array([50, 0x41, 0x42]);
    expect(() => decodeReadyProperties(body)).toThrow("truncated");
  });

  it("handles truncated property value length", () => {
    // key_len=1, key="A", then only 2 bytes for value_len (needs 4)
    const body = new Uint8Array([1, 0x41, 0x00, 0x00]);
    expect(() => decodeReadyProperties(body)).toThrow("truncated");
  });

  it("handles truncated property value", () => {
    // key_len=1, key="A", value_len=100, but only 1 byte follows
    const body = new Uint8Array([1, 0x41, 0x00, 0x00, 0x00, 0x64, 0xff]);
    expect(() => decodeReadyProperties(body)).toThrow("truncated");
  });

  it("handles zero-length property value", () => {
    // key_len=1, key="X", value_len=0
    const body = new Uint8Array([1, 0x58, 0x00, 0x00, 0x00, 0x00]);
    const props = decodeReadyProperties(body);
    expect(props.other.get("X")).toEqual(new Uint8Array(0));
  });

  it("handles empty body (no properties)", () => {
    const props = decodeReadyProperties(new Uint8Array(0));
    expect(props.socketType).toBeUndefined();
    expect(props.identity).toBeUndefined();
    expect(props.other.size).toBe(0);
  });

  it("handles multiple custom properties", () => {
    function prop(key: string, val: Uint8Array): Uint8Array {
      const keyBytes = enc.encode(key);
      const buf = new Uint8Array(1 + keyBytes.byteLength + 4 + val.byteLength);
      buf[0] = keyBytes.byteLength;
      buf.set(keyBytes, 1);
      new DataView(buf.buffer).setUint32(
        1 + keyBytes.byteLength,
        val.byteLength,
        false,
      );
      buf.set(val, 1 + keyBytes.byteLength + 4);
      return buf;
    }

    const p1 = prop("A", enc.encode("1"));
    const p2 = prop("B", enc.encode("22"));
    const p3 = prop("C", enc.encode("333"));
    const body = new Uint8Array(p1.byteLength + p2.byteLength + p3.byteLength);
    body.set(p1);
    body.set(p2, p1.byteLength);
    body.set(p3, p1.byteLength + p2.byteLength);

    const props = decodeReadyProperties(body);
    expect(props.other.size).toBe(3);
    expect(new TextDecoder().decode(props.other.get("A"))).toBe("1");
    expect(new TextDecoder().decode(props.other.get("B"))).toBe("22");
    expect(new TextDecoder().decode(props.other.get("C"))).toBe("333");
  });

  it("preserves unknown properties", () => {
    // Build a READY with Socket-Type=REQ and a custom property X-Foo=bar
    const identity = new Uint8Array(0);
    const readyBuf = encodeReady("REQ", identity);
    const cmd = decodeCommand(readyBuf);

    // Append a custom property manually
    const customKey = enc.encode("X-Foo");
    const customVal = enc.encode("bar");
    const extra = new Uint8Array(
      1 + customKey.byteLength + 4 + customVal.byteLength,
    );
    extra[0] = customKey.byteLength;
    extra.set(customKey, 1);
    new DataView(extra.buffer).setUint32(
      1 + customKey.byteLength,
      customVal.byteLength,
      false,
    );
    extra.set(customVal, 1 + customKey.byteLength + 4);

    const combined = new Uint8Array(cmd.body.byteLength + extra.byteLength);
    combined.set(cmd.body);
    combined.set(extra, cmd.body.byteLength);

    const props = decodeReadyProperties(combined);
    expect(props.socketType).toBe("REQ");
    expect(props.other.get("X-Foo")).toEqual(customVal);
  });

  it("rejects duplicate Socket-Type properties", () => {
    const first = encodeReady("REQ", new Uint8Array(0));
    const second = encodeReady("REP", new Uint8Array(0));
    const firstBody = decodeCommand(first).body;
    const secondBody = decodeCommand(second).body;
    const combined = new Uint8Array(
      firstBody.byteLength + secondBody.byteLength,
    );
    combined.set(firstBody);
    combined.set(secondBody, firstBody.byteLength);

    expect(() => decodeReadyProperties(combined)).toThrow(
      "Duplicate Socket-Type",
    );
  });
});

describe("socket type compatibility", () => {
  it("recognizes known socket types", () => {
    expect(isSocketTypeName("REQ")).toBe(true);
    expect(isSocketTypeName("STREAM")).toBe(false);
    expect(isSocketTypeName("req")).toBe(false);
  });

  it("accepts compatible pairs", () => {
    expect(isCompatibleSocketType("REQ", "ROUTER")).toBe(true);
    expect(isCompatibleSocketType("PUB", "XSUB")).toBe(true);
    expect(isCompatibleSocketType("CLIENT", "SERVER")).toBe(true);
    expect(isCompatibleSocketType("CHANNEL", "CHANNEL")).toBe(true);
  });

  it("rejects incompatible pairs", () => {
    expect(isCompatibleSocketType("REQ", "REQ")).toBe(false);
    expect(isCompatibleSocketType("PUB", "PUB")).toBe(false);
    expect(isCompatibleSocketType("REP", "ROUTER")).toBe(false);
    expect(isCompatibleSocketType("RADIO", "SUB")).toBe(false);
  });
});
