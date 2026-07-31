import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  decodeCommand,
  decodeReadyProperties,
  encodeCancel,
  encodeJoin,
  encodeLeave,
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
import { Message } from "../src/message.ts";
import { Push } from "../src/push.ts";
import { Pull } from "../src/pull.ts";
import { Sub } from "../src/sub.ts";
import { Req } from "../src/req.ts";
import { Rep } from "../src/rep.ts";
import { Pub } from "../src/pub.ts";
import { XPub } from "../src/xpub.ts";
import { XSub } from "../src/xsub.ts";
import { Dealer } from "../src/dealer.ts";
import { Router } from "../src/router.ts";
import { Pair } from "../src/pair.ts";
import { Client } from "../src/client.ts";
import { Server } from "../src/server.ts";
import { Radio } from "../src/radio.ts";
import { Dish } from "../src/dish.ts";
import { Gather } from "../src/gather.ts";
import { Scatter } from "../src/scatter.ts";
import { Peer } from "../src/peer.ts";
import { Channel } from "../src/channel.ts";

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

  simulateError(): void {
    this.onerror?.();
  }
}

let createdSockets: MockWebSocket[] = [];

beforeEach(() => {
  createdSockets = [];
  // @ts-expect-error mock global WebSocket
  globalThis.WebSocket = class extends MockWebSocket {
    constructor(url: string, protocols: string[]) {
      super(url, protocols);
      createdSockets.push(this);
    }
  };
});

function makeReady(
  ws: MockWebSocket,
  serverType: SocketTypeName = "PULL",
): void {
  ws.simulateOpen();
  const ready = encodeReady(serverType, new Uint8Array(0));
  ws.simulateMessage(encodeCommandFrame(ready));
}

function makeReadyWithIdentity(
  ws: MockWebSocket,
  serverType: SocketTypeName,
  identity: string,
): void {
  ws.simulateOpen();
  const ready = encodeReady(serverType, new TextEncoder().encode(identity));
  ws.simulateMessage(encodeCommandFrame(ready));
}

function sendDataFrame(
  ws: MockWebSocket,
  ...parts: (string | Uint8Array)[]
): void {
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i]!;
    const payload = typeof p === "string" ? new TextEncoder().encode(p) : p;
    const flag = i === parts.length - 1 ? FLAG_FINAL : FLAG_MORE;
    ws.simulateMessage(encodeZwsFrame(flag, payload));
  }
}

function sendCommand(ws: MockWebSocket, payload: Uint8Array): void {
  ws.simulateMessage(encodeCommandFrame(payload));
}

function dataFramesAfterReady(ws: MockWebSocket): Uint8Array[][] {
  const messages: Uint8Array[][] = [];
  let current: Uint8Array[] = [];
  for (const frame of ws.sentFrames.slice(1)) {
    const flag = frame[0]!;
    if (flag === FLAG_COMMAND) continue;
    current.push(frame.subarray(1));
    if (flag === FLAG_FINAL) {
      messages.push(current);
      current = [];
    }
  }
  return messages;
}

// ─── Push ───────────────────────────────────────────────────────────

describe("Push", () => {
  it("sends message after connection is ready", async () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;

    const sendPromise = push.send(Message.from("COLOR 10:20 FF0000"));
    makeReady(ws, "PULL");

    await sendPromise;
    expect(ws.sentFrames.length).toBe(2);
    expect(ws.sentFrames[1]![0]).toBe(FLAG_FINAL);
    const payload = ws.sentFrames[1]!.subarray(1);
    expect(new TextDecoder().decode(payload)).toBe("COLOR 10:20 FF0000");
  });

  it("round-robins across multiple connections", async () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    push.connect("ws://localhost:9083");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;

    makeReady(ws1, "PULL");
    makeReady(ws2, "PULL");

    await push.send(Message.from("msg1"));
    await push.send(Message.from("msg2"));

    const total = ws1.sentFrames.length + ws2.sentFrames.length;
    expect(total).toBe(4); // 2 READYs + 2 data frames
  });

  it("queues send until connection ready", async () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;

    const promise = push.send(Message.from("queued"));
    expect(ws.sentFrames.length).toBe(0);

    makeReady(ws, "PULL");
    await promise;
    expect(ws.sentFrames.length).toBe(2); // READY + data
  });

  it("sends multipart messages", async () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    makeReady(ws, "PULL");

    await push.send(new Message("topic", "body"));
    expect(ws.sentFrames[1]![0]).toBe(FLAG_MORE);
    expect(ws.sentFrames[2]![0]).toBe(FLAG_FINAL);
  });
});

// ─── Pull ───────────────────────────────────────────────────────────

describe("Pull", () => {
  it("receives messages via recv()", async () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUSH");

    const recvPromise = pull.recv();
    sendDataFrame(ws, "hello");

    const msg = await recvPromise;
    expect(msg.string(0)).toBe("hello");
  });

  it("receives multipart messages", async () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUSH");

    const recvPromise = pull.recv();
    sendDataFrame(ws, "topic", "body");

    const msg = await recvPromise;
    expect(msg.parts.length).toBe(2);
    expect(msg.string(0)).toBe("topic");
    expect(msg.string(1)).toBe("body");
  });

  it("queues messages when no recv() is pending", async () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUSH");

    sendDataFrame(ws, "first");
    sendDataFrame(ws, "second");

    const msg1 = await pull.recv();
    const msg2 = await pull.recv();
    expect(msg1.string(0)).toBe("first");
    expect(msg2.string(0)).toBe("second");
  });

  it("fair-queues across multiple connections", async () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8083");
    pull.connect("ws://localhost:9083");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "PUSH");
    makeReady(ws2, "PUSH");

    sendDataFrame(ws1, "from-ws1");
    sendDataFrame(ws2, "from-ws2");

    const msg1 = await pull.recv();
    const msg2 = await pull.recv();
    const received = [msg1.string(0), msg2.string(0)].sort();
    expect(received).toEqual(["from-ws1", "from-ws2"]);
  });

  it("async iterator yields messages", async () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUSH");

    sendDataFrame(ws, "iter1");
    sendDataFrame(ws, "iter2");

    const results: string[] = [];
    let count = 0;
    for await (const msg of pull) {
      results.push(msg.string(0));
      if (++count === 2) break;
    }
    expect(results).toEqual(["iter1", "iter2"]);
  });

  it("announces PULL socket type in handshake", () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(cmd.name).toBe("READY");
    const props = decodeReadyProperties(cmd.body);
    expect(props.socketType).toBe("PULL");
  });
});

// ─── Sub ────────────────────────────────────────────────────────────

describe("Sub", () => {
  it("sends SUBSCRIBE commands after handshake", () => {
    const sub = new Sub();
    sub.subscribe("canvas:");
    sub.subscribe("gol:");

    sub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    expect(ws.sentFrames.length).toBe(3);

    const sub1 = decodeCommand(ws.sentFrames[1]!.subarray(1));
    expect(sub1.name).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(sub1.body)).toBe("canvas:");

    const sub2 = decodeCommand(ws.sentFrames[2]!.subarray(1));
    expect(sub2.name).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(sub2.body)).toBe("gol:");
  });

  it("sends subscriptions to new connections too", () => {
    const sub = new Sub();
    sub.subscribe("feed:");
    sub.connect("ws://localhost:8081");
    makeReady(createdSockets[0]!, "PUB");

    sub.connect("ws://localhost:9081");
    makeReady(createdSockets[1]!, "PUB");

    const frames = createdSockets[1]!.sentFrames;
    expect(frames.length).toBe(2); // READY + SUBSCRIBE
    const cmd = decodeCommand(frames[1]!.subarray(1));
    expect(cmd.name).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(cmd.body)).toBe("feed:");
  });

  it("receives messages via recv()", async () => {
    const sub = new Sub();
    sub.subscribe("canvas:");
    sub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    const recvPromise = sub.recv();
    sendDataFrame(ws, "hello");

    const msg = await recvPromise;
    expect(msg.string(0)).toBe("hello");
  });

  it("receives multipart messages", async () => {
    const sub = new Sub();
    sub.subscribe("");
    sub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    const recvPromise = sub.recv();
    sendDataFrame(ws, "topic", "body");

    const msg = await recvPromise;
    expect(msg.parts.length).toBe(2);
    expect(msg.string(0)).toBe("topic");
    expect(msg.string(1)).toBe("body");
  });

  it("queues messages when no recv() is pending", async () => {
    const sub = new Sub();
    sub.subscribe("");
    sub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    sendDataFrame(ws, "first");
    sendDataFrame(ws, "second");

    const msg1 = await sub.recv();
    const msg2 = await sub.recv();
    expect(msg1.string(0)).toBe("first");
    expect(msg2.string(0)).toBe("second");
  });

  it("sends CANCEL on unsubscribe", () => {
    const sub = new Sub();
    sub.subscribe("canvas:");
    sub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    sub.unsubscribe("canvas:");

    const lastFrame = ws.sentFrames[ws.sentFrames.length - 1]!;
    const cmd = decodeCommand(lastFrame.subarray(1));
    expect(cmd.name).toBe("CANCEL");
    expect(new TextDecoder().decode(cmd.body)).toBe("canvas:");
  });

  it("unsubscribe is idempotent", () => {
    const sub = new Sub();
    sub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    const before = ws.sentFrames.length;
    sub.unsubscribe("nonexistent:");
    expect(ws.sentFrames.length).toBe(before);
  });

  it("fair-queues across connections", async () => {
    const sub = new Sub();
    sub.subscribe("");
    sub.connect("ws://localhost:8081");
    sub.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;

    makeReady(ws1, "PUB");
    makeReady(ws2, "PUB");

    sendDataFrame(ws1, "from-ws1");
    sendDataFrame(ws2, "from-ws2");

    const msg1 = await sub.recv();
    const msg2 = await sub.recv();

    const received = [msg1.string(0), msg2.string(0)].sort();
    expect(received).toEqual(["from-ws1", "from-ws2"]);
  });
});

// ─── Req ────────────────────────────────────────────────────────────

describe("Req", () => {
  it("sends with empty delimiter and receives reply", async () => {
    const req = new Req({ identity: "web:paddor" });
    req.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "ROUTER");

    const sendPromise = req.send(Message.from("CANVAS"));

    expect(ws.sentFrames.length).toBe(3);
    expect(ws.sentFrames[1]![0]).toBe(FLAG_MORE); // empty delimiter
    expect(ws.sentFrames[1]!.subarray(1).byteLength).toBe(0);
    expect(ws.sentFrames[2]![0]).toBe(FLAG_FINAL);
    expect(new TextDecoder().decode(ws.sentFrames[2]!.subarray(1))).toBe(
      "CANVAS",
    );

    sendDataFrame(
      ws,
      new Uint8Array(0),
      new Uint8Array([0, 0, 0, 1]),
      "snapshot data",
    );

    const reply = await sendPromise;
    expect(reply.parts.length).toBe(2); // delimiter stripped
    expect(reply.parts[0]).toEqual(new Uint8Array([0, 0, 0, 1]));
    expect(reply.string(1)).toBe("snapshot data");
  });

  it("rejects double send (strict alternation)", async () => {
    const req = new Req();
    req.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "ROUTER");

    req.send(Message.from("CANVAS")); // first send, pending reply

    await expect(req.send(Message.from("GOL"))).rejects.toThrow(
      "must receive a reply",
    );
  });

  it("allows send after receiving reply", async () => {
    const req = new Req();
    req.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "ROUTER");

    const promise1 = req.send(Message.from("CANVAS"));
    sendDataFrame(ws, new Uint8Array(0), "reply1");
    await promise1;

    const promise2 = req.send(Message.from("GOL"));
    sendDataFrame(ws, new Uint8Array(0), "reply2");
    const reply2 = await promise2;
    expect(reply2.string(0)).toBe("reply2");
  });

  it("waits for connection to be ready before first send", async () => {
    const req = new Req();
    req.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;

    const sendPromise = req.send(Message.from("CANVAS"));
    expect(ws.sentFrames.length).toBe(0);

    makeReady(ws, "ROUTER");
    await new Promise((r) => setTimeout(r, 0));
    expect(ws.sentFrames.length).toBe(3); // READY + delimiter + CANVAS

    sendDataFrame(ws, new Uint8Array(0), "snapshot");
    const reply = await sendPromise;
    expect(reply.string(0)).toBe("snapshot");
  });

  it("multipart reply preserves all parts after delimiter", async () => {
    const req = new Req();
    req.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "ROUTER");

    const promise = req.send(Message.from("CANVAS"));
    sendDataFrame(
      ws,
      new Uint8Array(0),
      new Uint8Array([0, 0, 0, 42]),
      "part1",
      "part2",
    );

    const reply = await promise;
    expect(reply.parts.length).toBe(3); // delimiter stripped
    expect(reply.parts[0]).toEqual(new Uint8Array([0, 0, 0, 42]));
    expect(reply.string(1)).toBe("part1");
    expect(reply.string(2)).toBe("part2");
  });

  it("ignores unsolicited replies", async () => {
    const req = new Req();
    req.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "ROUTER");

    sendDataFrame(ws, new Uint8Array(0), "unsolicited");

    const promise = req.send(Message.from("CANVAS"));
    sendDataFrame(ws, new Uint8Array(0), "real reply");

    const reply = await promise;
    expect(reply.string(0)).toBe("real reply");
  });

  it("ignores replies from non-request connections", async () => {
    const req = new Req();
    req.connect("ws://localhost:8082/a");
    req.connect("ws://localhost:8082/b");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "ROUTER");
    makeReady(ws2, "ROUTER");

    let settled = false;
    const promise = req.send(Message.from("CANVAS")).then((reply) => {
      settled = true;
      return reply;
    });

    sendDataFrame(ws2, new Uint8Array(0), "wrong reply");
    await new Promise((r) => setTimeout(r, 0));
    expect(settled).toBe(false);

    sendDataFrame(ws1, new Uint8Array(0), "real reply");
    const reply = await promise;
    expect(reply.string(0)).toBe("real reply");
  });
});

// ─── Rep ────────────────────────────────────────────────────────────

describe("Rep", () => {
  it("receives request with delimiter stripped", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "REQ");

    sendDataFrame(ws, new Uint8Array(0), "request body");

    const msg = await rep.recv();
    expect(msg.parts.length).toBe(1);
    expect(msg.string(0)).toBe("request body");
  });

  it("sends reply with delimiter prepended", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "REQ");

    sendDataFrame(ws, new Uint8Array(0), "request");
    await rep.recv();
    await rep.send(Message.from("reply"));

    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(1);
    expect(msgs[0]!.length).toBe(2); // empty delimiter + reply
    expect(msgs[0]![0]!.byteLength).toBe(0); // empty delimiter
    expect(new TextDecoder().decode(msgs[0]![1]!)).toBe("reply");
  });

  it("preserves routing envelope from DEALER peer", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "DEALER");

    // DEALER sends: [identity, empty delimiter, request]
    const identity = new TextEncoder().encode("peer-1");
    sendDataFrame(ws, identity, new Uint8Array(0), "request");

    const msg = await rep.recv();
    expect(msg.string(0)).toBe("request");

    await rep.send(Message.from("reply"));

    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(1);
    // Should prepend [identity, empty delimiter] back
    expect(msgs[0]!.length).toBe(3);
    expect(new TextDecoder().decode(msgs[0]![0]!)).toBe("peer-1");
    expect(msgs[0]![1]!.byteLength).toBe(0);
    expect(new TextDecoder().decode(msgs[0]![2]!)).toBe("reply");
  });

  it("enforces strict alternation: send before recv throws", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "REQ");

    await expect(rep.send(Message.from("nope"))).rejects.toThrow(
      "must receive a request",
    );
  });

  it("enforces strict alternation: double recv throws", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "REQ");

    sendDataFrame(ws, new Uint8Array(0), "req1");
    await rep.recv();

    // Haven't sent reply yet; second recv should throw
    await expect(rep.recv()).rejects.toThrow("must send a reply");
  });

  it("allows recv-send-recv-send cycle", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "REQ");

    sendDataFrame(ws, new Uint8Array(0), "req1");
    const msg1 = await rep.recv();
    expect(msg1.string(0)).toBe("req1");
    await rep.send(Message.from("rep1"));

    sendDataFrame(ws, new Uint8Array(0), "req2");
    const msg2 = await rep.recv();
    expect(msg2.string(0)).toBe("req2");
    await rep.send(Message.from("rep2"));

    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(2);
  });

  it("queues multiple requests", async () => {
    const rep = new Rep();
    rep.connect("ws://localhost:8082");
    const ws = createdSockets[0]!;
    makeReady(ws, "REQ");

    sendDataFrame(ws, new Uint8Array(0), "req1");
    sendDataFrame(ws, new Uint8Array(0), "req2");

    const msg1 = await rep.recv();
    expect(msg1.string(0)).toBe("req1");
    await rep.send(Message.from("rep1"));

    const msg2 = await rep.recv();
    expect(msg2.string(0)).toBe("req2");
    await rep.send(Message.from("rep2"));
  });

  it("reconnects after queued requests hit receiveHighWaterMark", async () => {
    vi.useFakeTimers();
    const onError = vi.fn();
    const rep = new Rep({
      receiveHighWaterMark: 1,
      reconnectInitialDelayMs: 5,
      reconnectMaxDelayMs: 5,
      onError,
    });
    try {
      rep.connect("ws://localhost:8082");
      const ws1 = createdSockets[0]!;
      makeReady(ws1, "REQ");

      sendDataFrame(ws1, new Uint8Array(0), "req1");
      sendDataFrame(ws1, new Uint8Array(0), "req2");

      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError.mock.calls[0]![0].message).toBe(
        "Receive high water mark reached",
      );
      expect(rep.connectionCount).toBe(0);
      expect(rep.endpointCount).toBe(1);

      const pending = rep.recv();

      await vi.advanceTimersByTimeAsync(5);
      const ws2 = createdSockets[1]!;
      makeReady(ws2, "REQ");
      sendDataFrame(ws2, new Uint8Array(0), "req3");

      const msg = await pending;
      expect(msg.string(0)).toBe("req3");
      await rep.send(Message.from("rep3"));
      expect(dataFramesAfterReady(ws2).length).toBe(1);
    } finally {
      rep.close();
      vi.useRealTimers();
    }
  });
});

// ─── Pub ────────────────────────────────────────────────────────────

describe("Pub", () => {
  it("sends to peers with matching subscriptions", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    // Peer subscribes to "market."
    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("market.")));

    await pub.send(new Message("market.BTC", "42000"));
    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(1);
    expect(new TextDecoder().decode(msgs[0]![0]!)).toBe("market.BTC");
  });

  it("does not send to peers without matching subscriptions", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("market.")));

    await pub.send(new Message("weather.NYC", "sunny"));
    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(0);
  });

  it("empty subscription matches all topics", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new Uint8Array(0)));

    await pub.send(new Message("anything", "data"));
    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(1);
  });

  it("handles CANCEL to remove subscription", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("market.")));
    sendCommand(ws, encodeCancel(new TextEncoder().encode("market.")));

    await pub.send(new Message("market.BTC", "42000"));
    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(0);
  });

  it("sends to multiple matching peers", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    pub.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "SUB");
    makeReady(ws2, "SUB");

    sendCommand(ws1, encodeSubscribe(new TextEncoder().encode("market.")));
    sendCommand(ws2, encodeSubscribe(new TextEncoder().encode("market.")));

    await pub.send(new Message("market.ETH", "3000"));
    expect(dataFramesAfterReady(ws1).length).toBe(1);
    expect(dataFramesAfterReady(ws2).length).toBe(1);
  });

  it("only sends to matching peers, not all", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    pub.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "SUB");
    makeReady(ws2, "SUB");

    sendCommand(ws1, encodeSubscribe(new TextEncoder().encode("market.")));
    sendCommand(ws2, encodeSubscribe(new TextEncoder().encode("weather.")));

    await pub.send(new Message("market.ETH", "3000"));
    expect(dataFramesAfterReady(ws1).length).toBe(1);
    expect(dataFramesAfterReady(ws2).length).toBe(0);
  });

  it("cleans up subscriptions on disconnect", async () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("market.")));
    pub.disconnect("ws://localhost:8081");

    // Should not throw or send to closed connection
    await pub.send(new Message("market.BTC", "42000"));
  });

  it("announces PUB socket type in handshake", () => {
    const pub = new Pub();
    pub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("PUB");
  });
});

// ─── XPub ───────────────────────────────────────────────────────────

describe("XPub", () => {
  it("surfaces SUBSCRIBE as 0x01-prefixed message", async () => {
    const xpub = new XPub();
    xpub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("topic")));

    const msg = await xpub.recv();
    expect(msg.parts[0]![0]).toBe(0x01);
    expect(new TextDecoder().decode(msg.parts[0]!.subarray(1))).toBe("topic");
  });

  it("surfaces CANCEL as 0x00-prefixed message", async () => {
    const xpub = new XPub();
    xpub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("topic")));
    sendCommand(ws, encodeCancel(new TextEncoder().encode("topic")));

    await xpub.recv(); // consume subscribe
    const msg = await xpub.recv();
    expect(msg.parts[0]![0]).toBe(0x00);
    expect(new TextDecoder().decode(msg.parts[0]!.subarray(1))).toBe("topic");
  });

  it("still filters sends like PUB", async () => {
    const xpub = new XPub();
    xpub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SUB");

    sendCommand(ws, encodeSubscribe(new TextEncoder().encode("market.")));
    await xpub.recv(); // consume subscription message

    await xpub.send(new Message("market.BTC", "42000"));
    expect(dataFramesAfterReady(ws).length).toBe(1);

    await xpub.send(new Message("weather.NYC", "rain"));
    expect(dataFramesAfterReady(ws).length).toBe(1); // still 1, weather didn't match
  });

  it("announces XPUB socket type in handshake", () => {
    const xpub = new XPub();
    xpub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("XPUB");
  });
});

// ─── XSub ───────────────────────────────────────────────────────────

describe("XSub", () => {
  it("translates 0x01 send to SUBSCRIBE command", async () => {
    const xsub = new XSub();
    xsub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    const prefix = new TextEncoder().encode("market.");
    const frame = new Uint8Array(1 + prefix.byteLength);
    frame[0] = 0x01;
    frame.set(prefix, 1);
    await xsub.send(Message.fromParts([frame]));

    const cmdFrame = ws.sentFrames[ws.sentFrames.length - 1]!;
    expect(cmdFrame[0]).toBe(FLAG_COMMAND);
    const cmd = decodeCommand(cmdFrame.subarray(1));
    expect(cmd.name).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(cmd.body)).toBe("market.");
  });

  it("translates 0x00 send to CANCEL command", async () => {
    const xsub = new XSub();
    xsub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    // Subscribe first
    const prefix = new TextEncoder().encode("market.");
    const subFrame = new Uint8Array(1 + prefix.byteLength);
    subFrame[0] = 0x01;
    subFrame.set(prefix, 1);
    await xsub.send(Message.fromParts([subFrame]));

    // Unsubscribe
    const unsubFrame = new Uint8Array(1 + prefix.byteLength);
    unsubFrame[0] = 0x00;
    unsubFrame.set(prefix, 1);
    await xsub.send(Message.fromParts([unsubFrame]));

    const cmdFrame = ws.sentFrames[ws.sentFrames.length - 1]!;
    expect(cmdFrame[0]).toBe(FLAG_COMMAND);
    const cmd = decodeCommand(cmdFrame.subarray(1));
    expect(cmd.name).toBe("CANCEL");
    expect(new TextDecoder().decode(cmd.body)).toBe("market.");
  });

  it("re-sends subscriptions on new connections", async () => {
    const xsub = new XSub();
    xsub.connect("ws://localhost:8081");
    const ws1 = createdSockets[0]!;
    makeReady(ws1, "PUB");

    const prefix = new TextEncoder().encode("market.");
    const frame = new Uint8Array(1 + prefix.byteLength);
    frame[0] = 0x01;
    frame.set(prefix, 1);
    await xsub.send(Message.fromParts([frame]));

    xsub.connect("ws://localhost:9081");
    const ws2 = createdSockets[1]!;
    makeReady(ws2, "PUB");

    // ws2 should get READY + SUBSCRIBE
    expect(ws2.sentFrames.length).toBe(2);
    const cmd = decodeCommand(ws2.sentFrames[1]!.subarray(1));
    expect(cmd.name).toBe("SUBSCRIBE");
    expect(new TextDecoder().decode(cmd.body)).toBe("market.");
  });

  it("receives messages", async () => {
    const xsub = new XSub();
    xsub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    sendDataFrame(ws, "market.BTC", "42000");

    const msg = await xsub.recv();
    expect(msg.string(0)).toBe("market.BTC");
    expect(msg.string(1)).toBe("42000");
  });

  it("subscribe is idempotent", async () => {
    const xsub = new XSub();
    xsub.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PUB");

    const prefix = new TextEncoder().encode("market.");
    const frame = new Uint8Array(1 + prefix.byteLength);
    frame[0] = 0x01;
    frame.set(prefix, 1);
    await xsub.send(Message.fromParts([frame]));
    const countAfterFirst = ws.sentFrames.length;

    await xsub.send(Message.fromParts([frame]));
    expect(ws.sentFrames.length).toBe(countAfterFirst); // no new frame
  });

  it("keeps binary prefixes distinct and replays exact bytes", async () => {
    const xsub = new XSub();
    xsub.connect("ws://localhost:8081");
    const ws1 = createdSockets[0]!;
    makeReady(ws1, "PUB");

    await xsub.send(Message.fromParts([new Uint8Array([0x01, 0xff])]));
    await xsub.send(Message.fromParts([new Uint8Array([0x01, 0xfe])]));

    const firstCommands = ws1.sentFrames.slice(1).map((f) =>
      decodeCommand(f.subarray(1))
    );
    expect(firstCommands.map((c) => c.name)).toEqual([
      "SUBSCRIBE",
      "SUBSCRIBE",
    ]);
    expect(firstCommands.map((c) => Array.from(c.body))).toEqual([
      [0xff],
      [0xfe],
    ]);

    xsub.connect("ws://localhost:9081");
    const ws2 = createdSockets[1]!;
    makeReady(ws2, "PUB");

    const replayed = ws2.sentFrames.slice(1).map((f) =>
      decodeCommand(f.subarray(1))
    );
    expect(replayed.map((c) => Array.from(c.body))).toEqual([
      [0xff],
      [0xfe],
    ]);
  });
});

// ─── Dealer ─────────────────────────────────────────────────────────

describe("Dealer", () => {
  it("sends messages round-robin", async () => {
    const dealer = new Dealer();
    dealer.connect("ws://localhost:8081");
    dealer.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "ROUTER");
    makeReady(ws2, "ROUTER");

    await dealer.send(Message.from("msg1"));
    await dealer.send(Message.from("msg2"));

    const total = ws1.sentFrames.length + ws2.sentFrames.length;
    expect(total).toBe(4); // 2 READYs + 2 data
  });

  it("receives messages fair-queued", async () => {
    const dealer = new Dealer();
    dealer.connect("ws://localhost:8081");
    dealer.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "ROUTER");
    makeReady(ws2, "ROUTER");

    sendDataFrame(ws1, "from-1");
    sendDataFrame(ws2, "from-2");

    const msg1 = await dealer.recv();
    const msg2 = await dealer.recv();
    const received = [msg1.string(0), msg2.string(0)].sort();
    expect(received).toEqual(["from-1", "from-2"]);
  });

  it("can send and recv independently (no alternation)", async () => {
    const dealer = new Dealer();
    dealer.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "ROUTER");

    // Send two without receiving
    await dealer.send(Message.from("a"));
    await dealer.send(Message.from("b"));

    sendDataFrame(ws, "reply-a");
    sendDataFrame(ws, "reply-b");

    const r1 = await dealer.recv();
    const r2 = await dealer.recv();
    expect(r1.string(0)).toBe("reply-a");
    expect(r2.string(0)).toBe("reply-b");
  });

  it("queues send until connection ready", async () => {
    const dealer = new Dealer();
    dealer.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;

    const promise = dealer.send(Message.from("queued"));
    expect(ws.sentFrames.length).toBe(0);

    makeReady(ws, "ROUTER");
    await promise;
    expect(ws.sentFrames.length).toBe(2); // READY + data
  });

  it("announces DEALER socket type in handshake", () => {
    const dealer = new Dealer();
    dealer.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("DEALER");
  });
});

// ─── Router ─────────────────────────────────────────────────────────

describe("Router", () => {
  it("prepends peer identity to received messages", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReadyWithIdentity(ws, "DEALER", "peer-A");

    sendDataFrame(ws, "hello");

    const msg = await router.recv();
    expect(msg.parts.length).toBe(2);
    expect(new TextDecoder().decode(msg.parts[0]!)).toBe("peer-A");
    expect(msg.string(1)).toBe("hello");
  });

  it("auto-generates identity for peers without one", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "DEALER"); // no identity

    sendDataFrame(ws, "hello");

    const msg = await router.recv();
    expect(msg.parts.length).toBe(2);
    expect(msg.parts[0]!.byteLength).toBe(5); // auto-generated
    expect(msg.parts[0]![0]).toBe(0); // leading zero for auto-generated
    expect(msg.string(1)).toBe("hello");
  });

  it("routes send to correct peer by identity", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    router.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReadyWithIdentity(ws1, "DEALER", "peer-A");
    makeReadyWithIdentity(ws2, "DEALER", "peer-B");

    const identityA = new TextEncoder().encode("peer-A");
    await router.send(new Message(identityA, "for-A"));

    expect(dataFramesAfterReady(ws1).length).toBe(1);
    expect(dataFramesAfterReady(ws2).length).toBe(0);
    expect(new TextDecoder().decode(dataFramesAfterReady(ws1)[0]![0]!)).toBe(
      "for-A",
    );
  });

  it("silently drops send to unknown identity", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReadyWithIdentity(ws, "DEALER", "peer-A");

    await router.send(
      new Message(new TextEncoder().encode("nonexistent"), "hello"),
    );
    expect(dataFramesAfterReady(ws).length).toBe(0);
  });

  it("strips identity frame before sending on wire", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReadyWithIdentity(ws, "DEALER", "peer-A");

    const identity = new TextEncoder().encode("peer-A");
    await router.send(new Message(identity, "part1", "part2"));

    const msgs = dataFramesAfterReady(ws);
    expect(msgs.length).toBe(1);
    expect(msgs[0]!.length).toBe(2); // identity stripped, part1 + part2 remain
    expect(new TextDecoder().decode(msgs[0]![0]!)).toBe("part1");
    expect(new TextDecoder().decode(msgs[0]![1]!)).toBe("part2");
  });

  it("cleans up identity mapping on disconnect", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReadyWithIdentity(ws, "DEALER", "peer-A");

    router.disconnect("ws://localhost:8081");

    const identity = new TextEncoder().encode("peer-A");
    await router.send(new Message(identity, "hello"));
    // Should silently drop (peer gone)
  });

  it("handles messages from multiple peers", async () => {
    const router = new Router();
    router.connect("ws://localhost:8081");
    router.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReadyWithIdentity(ws1, "DEALER", "peer-A");
    makeReadyWithIdentity(ws2, "DEALER", "peer-B");

    sendDataFrame(ws1, "from-A");
    sendDataFrame(ws2, "from-B");

    const msg1 = await router.recv();
    const msg2 = await router.recv();

    const received = [
      new TextDecoder().decode(msg1.parts[0]!) + ":" + msg1.string(1),
      new TextDecoder().decode(msg2.parts[0]!) + ":" + msg2.string(1),
    ].sort();
    expect(received).toEqual(["peer-A:from-A", "peer-B:from-B"]);
  });
});

// ─── Pair ───────────────────────────────────────────────────────────

describe("Pair", () => {
  it("sends and receives", async () => {
    const pair = new Pair();
    pair.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PAIR");

    await pair.send(Message.from("hello"));
    expect(dataFramesAfterReady(ws).length).toBe(1);

    sendDataFrame(ws, "world");
    const msg = await pair.recv();
    expect(msg.string(0)).toBe("world");
  });

  it("rejects second connection", () => {
    const pair = new Pair();
    pair.connect("ws://localhost:8081");
    expect(() => pair.connect("ws://localhost:9081")).toThrow(
      "only one connection",
    );
  });

  it("allows reconnect after disconnect", () => {
    const pair = new Pair();
    pair.connect("ws://localhost:8081");
    pair.disconnect("ws://localhost:8081");
    pair.connect("ws://localhost:9081");
    expect(pair.connectionCount).toBe(1);
  });

  it("queues send until peer is ready", async () => {
    const pair = new Pair();
    pair.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;

    const promise = pair.send(Message.from("waiting"));
    expect(ws.sentFrames.length).toBe(0);

    makeReady(ws, "PAIR");
    await promise;
    expect(ws.sentFrames.length).toBe(2); // READY + data
  });
});

// ─── Client (draft) ─────────────────────────────────────────────────

describe("Client", () => {
  it("announces CLIENT socket type", () => {
    const client = new Client();
    client.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("CLIENT");
  });

  it("behaves like Dealer: async send and recv", async () => {
    const client = new Client();
    client.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SERVER");

    await client.send(Message.from("request"));
    sendDataFrame(ws, "reply");

    const msg = await client.recv();
    expect(msg.string(0)).toBe("reply");
  });
});

// ─── Server (draft) ─────────────────────────────────────────────────

describe("Server", () => {
  it("announces SERVER socket type", () => {
    const server = new Server();
    server.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("SERVER");
  });

  it("prepends 4-byte routing ID to received messages", async () => {
    const server = new Server();
    server.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "CLIENT");

    sendDataFrame(ws, "request");

    const msg = await server.recv();
    expect(msg.parts.length).toBe(2);
    expect(msg.parts[0]!.byteLength).toBe(4); // uint32 routing ID
    expect(msg.string(1)).toBe("request");
  });

  it("routes reply by routing ID", async () => {
    const server = new Server();
    server.connect("ws://localhost:8081");
    server.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "CLIENT");
    makeReady(ws2, "CLIENT");

    sendDataFrame(ws1, "from-1");
    const msg = await server.recv();
    const routingId = msg.parts[0]!;

    await server.send(
      Message.fromParts([routingId, new TextEncoder().encode("reply-to-1")]),
    );
    expect(dataFramesAfterReady(ws1).length).toBe(1);
    expect(dataFramesAfterReady(ws2).length).toBe(0);
  });

  it("assigns distinct routing IDs to different peers", async () => {
    const server = new Server();
    server.connect("ws://localhost:8081");
    server.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "CLIENT");
    makeReady(ws2, "CLIENT");

    sendDataFrame(ws1, "from-1");
    sendDataFrame(ws2, "from-2");

    const msg1 = await server.recv();
    const msg2 = await server.recv();

    const id1 = new DataView(
      msg1.parts[0]!.buffer,
      msg1.parts[0]!.byteOffset,
      4,
    ).getUint32(0, false);
    const id2 = new DataView(
      msg2.parts[0]!.buffer,
      msg2.parts[0]!.byteOffset,
      4,
    ).getUint32(0, false);
    expect(id1).not.toBe(id2);
  });

  it("silently drops send with invalid routing ID size", async () => {
    const server = new Server();
    server.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "CLIENT");

    // 3-byte routing ID (invalid, needs 4)
    await server.send(
      Message.fromParts([new Uint8Array(3), new TextEncoder().encode("nope")]),
    );
    expect(dataFramesAfterReady(ws).length).toBe(0);
  });
});

// ─── Gather (draft) ─────────────────────────────────────────────────

describe("Gather", () => {
  it("announces GATHER socket type", () => {
    const gather = new Gather();
    gather.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("GATHER");
  });

  it("receives messages like Pull", async () => {
    const gather = new Gather();
    gather.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "SCATTER");

    sendDataFrame(ws, "hello");
    const msg = await gather.recv();
    expect(msg.string(0)).toBe("hello");
  });
});

// ─── Scatter (draft) ────────────────────────────────────────────────

describe("Scatter", () => {
  it("announces SCATTER socket type", () => {
    const scatter = new Scatter();
    scatter.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("SCATTER");
  });

  it("sends messages like Push", async () => {
    const scatter = new Scatter();
    scatter.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "GATHER");

    await scatter.send(Message.from("hello"));
    expect(dataFramesAfterReady(ws).length).toBe(1);
  });
});

// ─── Radio (draft) ──────────────────────────────────────────────────

describe("Radio", () => {
  it("announces RADIO socket type", () => {
    const radio = new Radio();
    radio.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("RADIO");
  });

  it("sends to peers that joined matching group", async () => {
    const radio = new Radio();
    radio.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "DISH");

    sendCommand(ws, encodeJoin(new TextEncoder().encode("stocks")));

    await radio.send(new Message("stocks", "AAPL 150"));
    expect(dataFramesAfterReady(ws).length).toBe(1);
  });

  it("does not send to peers that haven't joined the group", async () => {
    const radio = new Radio();
    radio.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "DISH");

    sendCommand(ws, encodeJoin(new TextEncoder().encode("weather")));

    await radio.send(new Message("stocks", "AAPL 150"));
    expect(dataFramesAfterReady(ws).length).toBe(0);
  });

  it("handles LEAVE to remove group membership", async () => {
    const radio = new Radio();
    radio.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "DISH");

    sendCommand(ws, encodeJoin(new TextEncoder().encode("stocks")));
    sendCommand(ws, encodeLeave(new TextEncoder().encode("stocks")));

    await radio.send(new Message("stocks", "AAPL 150"));
    expect(dataFramesAfterReady(ws).length).toBe(0);
  });

  it("cleans up groups on disconnect", async () => {
    const radio = new Radio();
    radio.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "DISH");

    sendCommand(ws, encodeJoin(new TextEncoder().encode("stocks")));
    radio.disconnect("ws://localhost:8081");

    await radio.send(new Message("stocks", "data"));
    // Should not throw
  });

  it("keeps binary group keys distinct", async () => {
    const radio = new Radio();
    radio.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "DISH");

    sendCommand(ws, encodeJoin(new Uint8Array([0xff])));

    await radio.send(
      Message.fromParts([
        new Uint8Array([0xfe]),
        new TextEncoder().encode("wrong"),
      ]),
    );
    expect(dataFramesAfterReady(ws).length).toBe(0);

    await radio.send(
      Message.fromParts([
        new Uint8Array([0xff]),
        new TextEncoder().encode("right"),
      ]),
    );
    expect(dataFramesAfterReady(ws).length).toBe(1);
  });
});

// ─── Dish (draft) ───────────────────────────────────────────────────

describe("Dish", () => {
  it("announces DISH socket type", () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("DISH");
  });

  it("sends JOIN command when joining a group", () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "RADIO");

    dish.join("stocks");

    const cmdFrame = ws.sentFrames[ws.sentFrames.length - 1]!;
    expect(cmdFrame[0]).toBe(FLAG_COMMAND);
    const cmd = decodeCommand(cmdFrame.subarray(1));
    expect(cmd.name).toBe("JOIN");
    expect(new TextDecoder().decode(cmd.body)).toBe("stocks");
  });

  it("sends LEAVE command when leaving a group", () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "RADIO");

    dish.join("stocks");
    dish.leave("stocks");

    const cmdFrame = ws.sentFrames[ws.sentFrames.length - 1]!;
    const cmd = decodeCommand(cmdFrame.subarray(1));
    expect(cmd.name).toBe("LEAVE");
    expect(new TextDecoder().decode(cmd.body)).toBe("stocks");
  });

  it("re-sends joins on new connections", () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws1 = createdSockets[0]!;
    makeReady(ws1, "RADIO");

    dish.join("stocks");
    dish.join("weather");

    dish.connect("ws://localhost:9081");
    const ws2 = createdSockets[1]!;
    makeReady(ws2, "RADIO");

    // ws2 should get READY + JOIN stocks + JOIN weather
    const cmds = ws2.sentFrames.slice(1).map((f) => {
      const cmd = decodeCommand(f.subarray(1));
      return { name: cmd.name, body: new TextDecoder().decode(cmd.body) };
    });
    expect(cmds.map((c) => c.name)).toEqual(["JOIN", "JOIN"]);
    expect(cmds.map((c) => c.body).sort()).toEqual(["stocks", "weather"]);
  });

  it("receives messages", async () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "RADIO");

    dish.join("stocks");
    sendDataFrame(ws, "stocks", "AAPL 150");

    const msg = await dish.recv();
    expect(msg.string(0)).toBe("stocks");
    expect(msg.string(1)).toBe("AAPL 150");
  });

  it("join is idempotent", () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "RADIO");

    dish.join("stocks");
    const countAfterFirst = ws.sentFrames.length;
    dish.join("stocks");
    expect(ws.sentFrames.length).toBe(countAfterFirst); // no new frame
  });

  it("leave for unjoined group is a no-op", () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "RADIO");

    const before = ws.sentFrames.length;
    dish.leave("nonexistent");
    expect(ws.sentFrames.length).toBe(before);
  });
});

// ─── Peer (draft) ───────────────────────────────────────────────────

describe("Peer", () => {
  it("announces PEER socket type", () => {
    const peer = new Peer();
    peer.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("PEER");
  });

  it("prepends 4-byte routing ID to received messages", async () => {
    const peer = new Peer();
    peer.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "PEER");

    sendDataFrame(ws, "hello");

    const msg = await peer.recv();
    expect(msg.parts[0]!.byteLength).toBe(4);
    expect(msg.string(1)).toBe("hello");
  });

  it("routes send by routing ID", async () => {
    const peer = new Peer();
    peer.connect("ws://localhost:8081");
    peer.connect("ws://localhost:9081");
    const ws1 = createdSockets[0]!;
    const ws2 = createdSockets[1]!;
    makeReady(ws1, "PEER");
    makeReady(ws2, "PEER");

    sendDataFrame(ws1, "from-1");
    const msg = await peer.recv();
    const routingId = msg.parts[0]!;

    await peer.send(
      Message.fromParts([routingId, new TextEncoder().encode("reply")]),
    );
    expect(dataFramesAfterReady(ws1).length).toBe(1);
    expect(dataFramesAfterReady(ws2).length).toBe(0);
  });
});

// ─── Channel (draft) ────────────────────────────────────────────────

describe("Channel", () => {
  it("announces CHANNEL socket type", () => {
    const channel = new Channel();
    channel.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    const readyFrame = ws.sentFrames[0]!;
    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(decodeReadyProperties(cmd.body).socketType).toBe("CHANNEL");
  });

  it("behaves like Pair: send and recv", async () => {
    const channel = new Channel();
    channel.connect("ws://localhost:8081");
    const ws = createdSockets[0]!;
    makeReady(ws, "CHANNEL");

    await channel.send(Message.from("hello"));
    sendDataFrame(ws, "world");

    const msg = await channel.recv();
    expect(msg.string(0)).toBe("world");
  });

  it("rejects second connection", () => {
    const channel = new Channel();
    channel.connect("ws://localhost:8081");
    expect(() => channel.connect("ws://localhost:9081")).toThrow(
      "only one connection",
    );
  });
});

// ─── Draft single-frame validation ──────────────────────────────────

describe("Draft single-frame validation", () => {
  it("rejects multipart CLIENT sends", async () => {
    const client = new Client();

    await expect(client.send(new Message("a", "b"))).rejects.toThrow(
      "single-part",
    );
  });

  it("rejects multipart SCATTER sends", async () => {
    const scatter = new Scatter();

    await expect(scatter.send(new Message("a", "b"))).rejects.toThrow(
      "single-part",
    );
  });

  it("rejects multipart CHANNEL sends", async () => {
    const channel = new Channel();

    await expect(channel.send(new Message("a", "b"))).rejects.toThrow(
      "single-part",
    );
  });

  it("rejects SERVER sends with multipart bodies", async () => {
    const server = new Server();
    const routingId = new Uint8Array(4);

    await expect(
      server.send(
        Message.fromParts([
          routingId,
          new TextEncoder().encode("a"),
          new TextEncoder().encode("b"),
        ]),
      ),
    ).rejects.toThrow("SERVER socket requires");
  });

  it("drops multipart GATHER receives", async () => {
    const gather = new Gather();
    gather.connect("ws://localhost:8085");
    const ws = createdSockets[0]!;
    makeReady(ws, "SCATTER");

    sendDataFrame(ws, "a", "b");
    const pending = gather.recv();
    gather.close();

    await expect(pending).rejects.toThrow("Socket closed");
  });

  it("drops malformed DISH receives", async () => {
    const dish = new Dish();
    dish.connect("ws://localhost:8086");
    const ws = createdSockets[0]!;
    makeReady(ws, "RADIO");

    sendDataFrame(ws, "missing-body");
    const pending = dish.recv();
    dish.close();

    await expect(pending).rejects.toThrow("Socket closed");
  });
});

// ─── Socket connection management ───────────────────────────────────

describe("Socket connection management", () => {
  it("connect is idempotent for same URL", () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    push.connect("ws://localhost:8083");
    expect(push.connectionCount).toBe(1);
    expect(createdSockets.length).toBe(1);
  });

  it("disconnect removes connection", () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    expect(push.connectionCount).toBe(1);
    push.disconnect("ws://localhost:8083");
    expect(push.connectionCount).toBe(0);
  });

  it("close removes all connections", () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    push.connect("ws://localhost:9083");
    expect(push.connectionCount).toBe(2);
    push.close();
    expect(push.connectionCount).toBe(0);
  });

  it("tracks ready count", () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    push.connect("ws://localhost:9083");
    expect(push.readyCount).toBe(0);

    makeReady(createdSockets[0]!, "PULL");
    expect(push.readyCount).toBe(1);

    makeReady(createdSockets[1]!, "PULL");
    expect(push.readyCount).toBe(2);
  });

  it("uses valid ZWS subprotocol with PLAIN socket options", () => {
    const push = new Push({
      plain: { username: "alice", password: "secret" },
    });
    push.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    ws.simulateOpen();

    expect(ws.protocols).toEqual(["ZWS2.0"]);
    const cmd = decodeCommand(ws.sentFrames[0]!.subarray(1));
    expect(cmd.name).toBe("HELLO");
  });

  it("readyCount decreases on disconnect", () => {
    const push = new Push();
    push.connect("ws://localhost:8083");
    push.connect("ws://localhost:9083");
    makeReady(createdSockets[0]!, "PULL");
    makeReady(createdSockets[1]!, "PULL");
    expect(push.readyCount).toBe(2);

    push.disconnect("ws://localhost:8083");
    expect(push.readyCount).toBe(1);
    expect(push.connectionCount).toBe(1);
  });

  it("reconnects after peer close and replays SUB subscriptions", async () => {
    vi.useFakeTimers();
    const sub = new Sub({ reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 5 });
    try {
      sub.subscribe("market.");
      sub.connect("ws://localhost:8081");
      const ws1 = createdSockets[0]!;
      makeReady(ws1, "PUB");

      ws1.simulateClose();
      expect(sub.connectionCount).toBe(0);
      expect(sub.endpointCount).toBe(1);

      await vi.advanceTimersByTimeAsync(5);
      expect(createdSockets.length).toBe(2);

      const ws2 = createdSockets[1]!;
      makeReady(ws2, "PUB");
      const cmd = decodeCommand(ws2.sentFrames[1]!.subarray(1));
      expect(cmd.name).toBe("SUBSCRIBE");
      expect(new TextDecoder().decode(cmd.body)).toBe("market.");
    } finally {
      sub.close();
      vi.useRealTimers();
    }
  });

  it("keeps pending sends waiting across reconnect", async () => {
    vi.useFakeTimers();
    const push = new Push({
      reconnectInitialDelayMs: 5,
      reconnectMaxDelayMs: 5,
    });
    try {
      push.connect("ws://localhost:8083");
      const ws1 = createdSockets[0]!;
      const sendPromise = push.send(Message.from("late"));

      ws1.simulateClose();
      await vi.advanceTimersByTimeAsync(5);
      const ws2 = createdSockets[1]!;
      makeReady(ws2, "PULL");

      await sendPromise;
      const messages = dataFramesAfterReady(ws2);
      expect(messages.length).toBe(1);
      expect(new TextDecoder().decode(messages[0]![0])).toBe("late");
    } finally {
      push.close();
      vi.useRealTimers();
    }
  });

  it("rejects sends beyond sendHighWaterMark while waiting for ready", async () => {
    const push = new Push({ sendHighWaterMark: 1 });
    push.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;

    const first = push.send(Message.from("first"));
    await expect(push.send(Message.from("second"))).rejects.toThrow(
      "Send high water mark",
    );

    makeReady(ws, "PULL");
    await first;
    const messages = dataFramesAfterReady(ws);
    expect(messages.length).toBe(1);
    expect(new TextDecoder().decode(messages[0]![0])).toBe("first");
  });

  it("validates sendHighWaterMark", () => {
    expect(() => new Push({ sendHighWaterMark: -1 })).toThrow(
      "sendHighWaterMark",
    );
  });

  it("rejects receive waiters on close", async () => {
    const pull = new Pull();
    pull.connect("ws://localhost:8084");

    const pending = pull.recv();
    pull.close();

    await expect(pending).rejects.toThrow("Socket closed");
  });

  it("rejects send when no endpoint exists", async () => {
    const push = new Push();

    await expect(push.send(Message.from("orphan"))).rejects.toThrow(
      "Socket has no connections",
    );
  });

  it("reconnects after inbound queue hits receiveHighWaterMark", async () => {
    vi.useFakeTimers();
    const onError = vi.fn();
    const pull = new Pull({
      receiveHighWaterMark: 1,
      reconnectInitialDelayMs: 5,
      reconnectMaxDelayMs: 5,
      onError,
    });
    try {
      pull.connect("ws://localhost:8084");
      const ws1 = createdSockets[0]!;
      makeReady(ws1, "PUSH");

      sendDataFrame(ws1, "first");
      sendDataFrame(ws1, "second");

      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError.mock.calls[0]![0].message).toBe(
        "Receive high water mark reached",
      );
      expect(pull.connectionCount).toBe(0);
      expect(pull.endpointCount).toBe(1);

      const msg = await pull.recv();
      expect(msg.string(0)).toBe("first");

      const pending = pull.recv();
      await vi.advanceTimersByTimeAsync(5);
      const ws2 = createdSockets[1]!;
      makeReady(ws2, "PUSH");
      sendDataFrame(ws2, "after reconnect");

      const next = await pending;
      expect(next.string(0)).toBe("after reconnect");
    } finally {
      pull.close();
      vi.useRealTimers();
    }
  });
});

// ─── Push with identity ─────────────────────────────────────────────

describe("Push with identity", () => {
  it("sends identity in READY handshake", async () => {
    const push = new Push({ identity: "cli:bot" });
    push.connect("ws://localhost:8083");
    const ws = createdSockets[0]!;
    makeReady(ws, "PULL");

    const readyFrame = ws.sentFrames[0]!;
    expect(readyFrame[0]).toBe(FLAG_COMMAND);

    const cmd = decodeCommand(readyFrame.subarray(1));
    expect(cmd.name).toBe("READY");

    const { decodeReadyProperties } = await import("../src/command.ts");
    const props = decodeReadyProperties(cmd.body);
    expect(props.socketType).toBe("PUSH");
    expect(new TextDecoder().decode(props.identity)).toBe("cli:bot");
  });
});
