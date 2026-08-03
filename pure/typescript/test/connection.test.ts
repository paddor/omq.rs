import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { initSyncFromBytes } from "@paddor/lz4rip";
import {
  decodeCommand,
  decodeReadyProperties,
  encodeReady,
  encodeSubscribe,
  type SocketTypeName,
} from "../src/command.ts";
import {
  encodeCommandFrame,
  encodeZwsFrame,
  FLAG_COMMAND,
  FLAG_FINAL,
  FLAG_MORE,
} from "../src/zws.ts";
import { Connection } from "../src/connection.ts";
import { Lz4Decoder, Lz4Encoder } from "../src/lz4.ts";
import { Message } from "../src/message.ts";

beforeAll(() => {
  const wasmPath = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "../node_modules/@paddor/lz4rip/src/pkg/lz4rip_wasm_bg.wasm",
  );
  initSyncFromBytes(readFileSync(wasmPath));
});

// Mock WebSocket for Node.js environment
class MockWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  binaryType = "blob";
  protocol = "ZWS2.0";
  readyState = MockWebSocket.CONNECTING;
  sentFrames: Uint8Array[] = [];

  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(
    public url: string,
    public protocols: string[],
  ) {}

  send(data: Uint8Array): void {
    this.sentFrames.push(new Uint8Array(data));
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.();
  }

  // Test helpers
  simulateOpen(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.();
  }

  simulateMessage(data: Uint8Array): void {
    this.onmessage?.({
      data: data.buffer.slice(
        data.byteOffset,
        data.byteOffset + data.byteLength,
      ) as ArrayBuffer,
    });
  }

  simulateClose(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.();
  }
}

let lastCreatedWs: MockWebSocket | null = null;

beforeEach(() => {
  lastCreatedWs = null;
  // @ts-expect-error mock global WebSocket
  globalThis.WebSocket = class extends MockWebSocket {
    constructor(url: string, protocols: string[]) {
      super(url, protocols);
      lastCreatedWs = this;
    }
  };
});

function serverReady(serverType: SocketTypeName = "PUB"): Uint8Array {
  const readyPayload = encodeReady(serverType, new Uint8Array(0));
  return encodeCommandFrame(readyPayload);
}

function commandFrame(name: string, body = new Uint8Array(0)): Uint8Array {
  const nameBytes = new TextEncoder().encode(name);
  const payload = new Uint8Array(1 + nameBytes.byteLength + body.byteLength);
  payload[0] = nameBytes.byteLength;
  payload.set(nameBytes, 1);
  payload.set(body, 1 + nameBytes.byteLength);
  return encodeCommandFrame(payload);
}

describe("Connection", () => {
  it("opens WebSocket with ZWS2.0 subprotocol", () => {
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });
    expect(lastCreatedWs).not.toBeNull();
    expect(lastCreatedWs!.url).toBe("ws://localhost:8081");
    expect(lastCreatedWs!.protocols).toEqual(["ZWS2.0"]);
  });

  it("sets binaryType to arraybuffer", () => {
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });
    expect(lastCreatedWs!.binaryType).toBe("arraybuffer");
  });

  it("opens WebSocket with ZWS2.0 subprotocol when PLAIN is configured", () => {
    new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new Uint8Array(0),
      plain: { username: "alice", password: "secret" },
    });

    expect(lastCreatedWs!.protocols).toEqual(["ZWS2.0"]);
  });

  it("strips lz4+ prefix from URL", () => {
    new Connection("lz4+ws://localhost:8087", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });
    expect(lastCreatedWs!.url).toBe("ws://localhost:8087");
  });

  it("rejects unsupported prefixed secure URLs", () => {
    const prefix = "lz4+";
    const secureUrl = "wss://localhost:8087";
    expect(() =>
      new Connection(prefix + secureUrl, {
        socketType: "SUB",
        identity: new Uint8Array(0),
      })
    ).toThrow("Unsupported WebSocket URL");
  });

  it("opens plain wss URLs", () => {
    new Connection("wss://localhost:8087", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });
    expect(lastCreatedWs!.url).toBe("wss://localhost:8087");
  });

  it("frees LZ4 contexts on close", () => {
    const decoderFree = vi.spyOn(Lz4Decoder.prototype, "free");
    const encoderFree = vi.spyOn(Lz4Encoder.prototype, "free");
    try {
      const conn = new Connection("lz4+ws://localhost:8087", {
        socketType: "PUSH",
        identity: new Uint8Array(0),
      });
      lastCreatedWs!.simulateOpen();
      lastCreatedWs!.simulateMessage(serverReady("PULL"));

      conn.close();

      expect(decoderFree).toHaveBeenCalledTimes(1);
      expect(encoderFree).toHaveBeenCalledTimes(1);
    } finally {
      decoderFree.mockRestore();
      encoderFree.mockRestore();
    }
  });

  it("sends READY command on open", () => {
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new TextEncoder().encode("web:paddor"),
    });
    lastCreatedWs!.simulateOpen();

    expect(lastCreatedWs!.sentFrames.length).toBe(1);
    const frame = lastCreatedWs!.sentFrames[0]!;
    expect(frame[0]).toBe(FLAG_COMMAND); // ZWS command flag

    const cmd = decodeCommand(frame.subarray(1));
    expect(cmd.name).toBe("READY");

    const props = decodeReadyProperties(cmd.body);
    expect(props.socketType).toBe("SUB");
    expect(new TextDecoder().decode(props.identity)).toBe("web:paddor");
  });

  it("completes a PLAIN client handshake", () => {
    const onReady = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new TextEncoder().encode("client-1"),
      plain: { username: "alice", password: "secret" },
      onReady,
    });
    lastCreatedWs!.simulateOpen();

    expect(lastCreatedWs!.sentFrames.length).toBe(1);
    let cmd = decodeCommand(lastCreatedWs!.sentFrames[0]!.subarray(1));
    expect(cmd.name).toBe("HELLO");
    expect(cmd.body).toEqual(
      new Uint8Array([
        5,
        ...new TextEncoder().encode("alice"),
        6,
        ...new TextEncoder().encode("secret"),
      ]),
    );

    lastCreatedWs!.simulateMessage(commandFrame("WELCOME"));
    expect(lastCreatedWs!.sentFrames.length).toBe(2);
    cmd = decodeCommand(lastCreatedWs!.sentFrames[1]!.subarray(1));
    expect(cmd.name).toBe("INITIATE");
    const initiateProps = decodeReadyProperties(cmd.body);
    expect(initiateProps.socketType).toBe("PUSH");
    expect(new TextDecoder().decode(initiateProps.identity)).toBe("client-1");

    lastCreatedWs!.simulateMessage(serverReady("PULL"));
    expect(onReady).toHaveBeenCalledTimes(1);
  });

  it("rejects PLAIN ERROR commands during handshake", () => {
    const onError = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new Uint8Array(0),
      plain: { username: "alice", password: "wrong" },
      onError,
    });
    lastCreatedWs!.simulateOpen();

    lastCreatedWs!.simulateMessage(
      commandFrame(
        "ERROR",
        new Uint8Array([
          21,
          ...new TextEncoder().encode("Authentication failed"),
        ]),
      ),
    );

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![1].message).toContain(
      "Authentication failed",
    );
  });

  it("transitions to ready state after receiving server READY", () => {
    const onReady = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onReady,
    });

    lastCreatedWs!.simulateOpen();
    expect(onReady).not.toHaveBeenCalled();

    lastCreatedWs!.simulateMessage(serverReady());
    expect(onReady).toHaveBeenCalledTimes(1);
  });

  it("rejects incompatible socket types during handshake", () => {
    const onError = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onError,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady("PULL"));

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![1].message).toContain("Incompatible");
  });

  it("rejects non-READY command during handshake", () => {
    const onError = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onError,
    });

    lastCreatedWs!.simulateOpen();

    const badCmd = new Uint8Array([4, 0x50, 0x49, 0x4e, 0x47]); // "PING"
    lastCreatedWs!.simulateMessage(encodeCommandFrame(badCmd));
    expect(onError).toHaveBeenCalled();
    expect(onError.mock.calls[0]![1].message).toContain("Expected READY");
  });

  it("rejects data frame during handshake", () => {
    const onError = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onError,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_FINAL, new Uint8Array([1, 2, 3])),
    );
    expect(onError).toHaveBeenCalled();
    expect(onError.mock.calls[0]![1].message).toContain(
      "Expected command",
    );
  });

  it("closes with onError for malformed ZWS frames", () => {
    const onError = vi.fn();
    const onClose = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onError,
      onClose,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());
    lastCreatedWs!.simulateMessage(new Uint8Array([0xff]));

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![1].message).toContain("Invalid ZWS flag");
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("delivers complete single-frame messages", () => {
    const onMessage = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onMessage,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    const payload = new TextEncoder().encode("hello");
    lastCreatedWs!.simulateMessage(encodeZwsFrame(FLAG_FINAL, payload));

    expect(onMessage).toHaveBeenCalledTimes(1);
    const msg: Message = onMessage.mock.calls[0]![1];
    expect(msg.parts.length).toBe(1);
    expect(msg.string(0)).toBe("hello");
  });

  it("assembles multipart messages (MORE + FINAL)", () => {
    const onMessage = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onMessage,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    const topic = new TextEncoder().encode("canvas:");
    const version = new Uint8Array([0, 0, 0, 1]);
    const payload = new TextEncoder().encode("COLOR 10:20 FF0000 web");

    lastCreatedWs!.simulateMessage(encodeZwsFrame(FLAG_MORE, topic));
    expect(onMessage).not.toHaveBeenCalled();

    lastCreatedWs!.simulateMessage(encodeZwsFrame(FLAG_MORE, version));
    expect(onMessage).not.toHaveBeenCalled();

    lastCreatedWs!.simulateMessage(encodeZwsFrame(FLAG_FINAL, payload));
    expect(onMessage).toHaveBeenCalledTimes(1);

    const msg: Message = onMessage.mock.calls[0]![1];
    expect(msg.parts.length).toBe(3);
    expect(msg.string(0)).toBe("canvas:");
    expect(msg.parts[1]).toEqual(version);
    expect(msg.string(2)).toBe("COLOR 10:20 FF0000 web");
  });

  it("enforces maxMessageSize on uncompressed messages", () => {
    const onError = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      maxMessageSize: 10,
      onError,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    const bigPayload = new Uint8Array(100);
    lastCreatedWs!.simulateMessage(encodeZwsFrame(FLAG_FINAL, bigPayload));
    expect(onError).toHaveBeenCalled();
    expect(onError.mock.calls[0]![1].message).toContain("exceeds max");
  });

  it("enforces maxMessageSize across multipart frames", () => {
    const onError = vi.fn();
    const onMessage = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      maxMessageSize: 15,
      onError,
      onMessage,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_MORE, new Uint8Array(10)),
    );
    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_FINAL, new Uint8Array(10)),
    );

    expect(onMessage).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalled();
    expect(onError.mock.calls[0]![1].message).toContain("exceeds max");
  });

  it("sends data frames with correct ZWS flags", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new Uint8Array(0),
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady("PULL"));

    conn.send(new Message("part1", "part2"));

    // sentFrames[0] is the READY command from handshake
    // sentFrames[1] should be MORE + "part1"
    // sentFrames[2] should be FINAL + "part2"
    expect(lastCreatedWs!.sentFrames.length).toBe(3);
    expect(lastCreatedWs!.sentFrames[1]![0]).toBe(FLAG_MORE);
    expect(lastCreatedWs!.sentFrames[2]![0]).toBe(FLAG_FINAL);
  });

  it("sends single-part message with FINAL flag", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new Uint8Array(0),
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady("PULL"));

    conn.send(Message.from("hello"));

    expect(lastCreatedWs!.sentFrames.length).toBe(2); // READY + data
    expect(lastCreatedWs!.sentFrames[1]![0]).toBe(FLAG_FINAL);
  });

  it("throws when sending on non-ready connection", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new Uint8Array(0),
    });

    expect(() => conn.send(Message.from("hello"))).toThrow("not ready");
  });

  it("calls onClose when WebSocket closes", () => {
    const onClose = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onClose,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());
    lastCreatedWs!.simulateClose();

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("close() closes the WebSocket", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    expect(conn.connectionState).toBe("ready");
    conn.close();
    expect(conn.connectionState).toBe("closed");
  });

  it("state transitions: connecting -> handshaking -> ready", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });

    expect(conn.connectionState).toBe("connecting");
    expect(conn.isReady).toBe(false);

    lastCreatedWs!.simulateOpen();
    expect(conn.connectionState).toBe("handshaking");
    expect(conn.isReady).toBe(false);

    lastCreatedWs!.simulateMessage(serverReady());
    expect(conn.connectionState).toBe("ready");
    expect(conn.isReady).toBe(true);
  });

  it("stores peer properties from server READY", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    expect(conn.peerProperties).not.toBeNull();
    expect(conn.peerProperties!.socketType).toBe("PUB");
  });

  it("handles multiple messages in sequence", () => {
    const onMessage = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onMessage,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    for (let i = 0; i < 10; i++) {
      lastCreatedWs!.simulateMessage(
        encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode(`msg${i}`)),
      );
    }

    expect(onMessage).toHaveBeenCalledTimes(10);
    const msg5: Message = onMessage.mock.calls[5]![1];
    expect(msg5.string(0)).toBe("msg5");
  });

  it("resets pending parts between messages", () => {
    const onMessage = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onMessage,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    // First multipart message
    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_MORE, new TextEncoder().encode("a")),
    );
    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("b")),
    );

    // Second multipart message
    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_MORE, new TextEncoder().encode("c")),
    );
    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("d")),
    );

    expect(onMessage).toHaveBeenCalledTimes(2);

    const msg1: Message = onMessage.mock.calls[0]![1];
    expect(msg1.parts.length).toBe(2);
    expect(msg1.string(0)).toBe("a");
    expect(msg1.string(1)).toBe("b");

    const msg2: Message = onMessage.mock.calls[1]![1];
    expect(msg2.parts.length).toBe(2);
    expect(msg2.string(0)).toBe("c");
    expect(msg2.string(1)).toBe("d");
  });

  it("does not deliver command frames as messages", () => {
    const onMessage = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      onMessage,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    const pingCmd = new Uint8Array([4, 0x50, 0x49, 0x4e, 0x47, 0x00, 0x00]);
    lastCreatedWs!.simulateMessage(encodeCommandFrame(pingCmd));

    expect(onMessage).not.toHaveBeenCalled();
  });

  it("auto-answers ZMTP PING with PONG", () => {
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    const context = new TextEncoder().encode("ctx");
    const pingCmd = new Uint8Array([
      4,
      0x50,
      0x49,
      0x4e,
      0x47,
      0x00,
      0x05,
      ...context,
    ]);
    lastCreatedWs!.simulateMessage(encodeCommandFrame(pingCmd));

    const sent = lastCreatedWs!.sentFrames.at(-1)!;
    expect(sent[0]).toBe(FLAG_COMMAND);
    const cmd = decodeCommand(sent.subarray(1));
    expect(cmd.name).toBe("PONG");
    expect(new TextDecoder().decode(cmd.body)).toBe("ctx");
  });

  it("delivers command frames via onCommand callback", () => {
    const onCommand = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "PUB",
      identity: new Uint8Array(0),
      onCommand,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady("SUB"));

    const subscribeCmd = encodeSubscribe(new TextEncoder().encode("market."));
    lastCreatedWs!.simulateMessage(encodeCommandFrame(subscribeCmd));

    expect(onCommand).toHaveBeenCalledTimes(1);
    expect(onCommand.mock.calls[0]![1]).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(onCommand.mock.calls[0]![2])).toBe(
      "market.",
    );
  });

  it("sends empty-body part correctly", () => {
    const conn = new Connection("ws://localhost:8081", {
      socketType: "PUSH",
      identity: new Uint8Array(0),
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady("PULL"));

    conn.send(new Message(new Uint8Array(0), "data"));

    // READY + empty-MORE + data-FINAL
    expect(lastCreatedWs!.sentFrames.length).toBe(3);
    expect(lastCreatedWs!.sentFrames[1]![0]).toBe(FLAG_MORE);
    expect(lastCreatedWs!.sentFrames[1]!.byteLength).toBe(1); // flag only, empty payload
  });

  it("maxMessageSize accepts message exactly at limit", () => {
    const onMessage = vi.fn();
    const onError = vi.fn();
    new Connection("ws://localhost:8081", {
      socketType: "SUB",
      identity: new Uint8Array(0),
      maxMessageSize: 10,
      onMessage,
      onError,
    });

    lastCreatedWs!.simulateOpen();
    lastCreatedWs!.simulateMessage(serverReady());

    lastCreatedWs!.simulateMessage(
      encodeZwsFrame(FLAG_FINAL, new Uint8Array(10)),
    );
    expect(onMessage).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
  });
});
