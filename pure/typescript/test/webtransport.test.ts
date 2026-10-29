import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  decodeCommand,
  decodeReadyProperties,
  encodeReady,
  encodeSubscribe,
} from "../src/command.ts";
import type { ConnectionOptions } from "../src/connection.ts";
import { Message } from "../src/message.ts";
import type { PeerConnection } from "../src/peer_connection.ts";
import {
  DATA_PREFACE,
  LIVENESS_PREFACE,
  WebTransportConnection,
  webTransportUrl,
} from "../src/webtransport.ts";
import {
  encodeCommandFrame,
  encodeGreeting,
  encodeMessage,
  ZmtpDecoder,
  type ZmtpEvent,
} from "../src/zmtp.ts";

/** Peer side of one bidirectional stream. */
class PeerStream {
  readonly toClient: WritableStreamDefaultWriter<Uint8Array>;
  private reader: ReadableStreamDefaultReader<Uint8Array>;
  private pending = new Uint8Array(0);

  constructor(
    public readonly clientSide: {
      readable: ReadableStream<Uint8Array>;
      writable: WritableStream<Uint8Array>;
    },
  ) {
    const down = new TransformStream<Uint8Array, Uint8Array>();
    const up = new TransformStream<Uint8Array, Uint8Array>();
    this.toClient = down.writable.getWriter();
    this.reader = up.readable.getReader();
    clientSide.readable = down.readable;
    clientSide.writable = up.writable;
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.pending.byteLength < n) {
      const { value, done } = await this.reader.read();
      if (done) throw new Error("client finished stream");
      const next = new Uint8Array(this.pending.byteLength + value.byteLength);
      next.set(this.pending, 0);
      next.set(value, this.pending.byteLength);
      this.pending = next;
    }
    const out = this.pending.slice(0, n);
    this.pending = this.pending.slice(n);
    return out;
  }

  /** Read until the client finishes the stream. */
  async readToEnd(): Promise<Uint8Array> {
    for (;;) {
      const { value, done } = await this.reader.read();
      if (done) break;
      const next = new Uint8Array(this.pending.byteLength + value.byteLength);
      next.set(this.pending, 0);
      next.set(value, this.pending.byteLength);
      this.pending = next;
    }
    const out = this.pending;
    this.pending = new Uint8Array(0);
    return out;
  }

  async readAvailable(): Promise<Uint8Array> {
    if (this.pending.byteLength > 0) {
      const out = this.pending;
      this.pending = new Uint8Array(0);
      return out;
    }
    const { value } = await this.reader.read();
    return value ?? new Uint8Array(0);
  }
}

class FakeWebTransport {
  static last: FakeWebTransport | null = null;
  readonly ready = Promise.resolve();
  readonly streams: PeerStream[] = [];
  closedWith: WebTransportCloseInfo | null = null;
  private resolveClosed!: () => void;
  readonly closed = new Promise<void>((
    resolve,
  ) => (this.resolveClosed = resolve));

  constructor(
    public readonly url: string,
    public readonly options: WebTransportOptions,
  ) {
    FakeWebTransport.last = this;
  }

  createBidirectionalStream() {
    const side = {
      readable: null as unknown as ReadableStream<Uint8Array>,
      writable: null as unknown as WritableStream<Uint8Array>,
    };
    this.streams.push(new PeerStream(side));
    return Promise.resolve(side);
  }

  close(info: WebTransportCloseInfo): void {
    this.closedWith = info;
    this.resolveClosed();
  }

  /** The server closes the session, as after reading data EOF. */
  peerClose(): void {
    this.resolveClosed();
  }
}

const original = (globalThis as { WebTransport?: unknown }).WebTransport;
beforeEach(() => {
  (globalThis as { WebTransport?: unknown }).WebTransport = FakeWebTransport;
});
afterEach(() => {
  (globalThis as { WebTransport?: unknown }).WebTransport = original;
});

async function until(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 200 && !predicate(); i++) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  expect(predicate()).toBe(true);
}

/** Open a NULL DEALER connection and complete the handshake as ROUTER. */
async function readyDealer(opts: Partial<ConnectionOptions> = {}) {
  const conn = new WebTransportConnection("https://127.0.0.1:1/omq", {
    socketType: "DEALER",
    identity: new Uint8Array(0),
    ...opts,
  });
  await until(() => (FakeWebTransport.last?.streams.length ?? 0) === 2);
  const wt = FakeWebTransport.last!;
  const [data, liveness] = wt.streams as [PeerStream, PeerStream];
  await data.readExact(DATA_PREFACE.byteLength);
  const decoder = new ZmtpDecoder();
  decoder.push(await data.readExact(64));
  await data.toClient.write(encodeGreeting("NULL"));
  await data.toClient.write(
    encodeCommandFrame(encodeReady("ROUTER", new Uint8Array(0))),
  );
  await events(data, decoder, 1);
  await until(() => conn.isReady);
  return { conn, wt, data, liveness, decoder };
}

async function events(stream: PeerStream, decoder: ZmtpDecoder, count: number) {
  const out: ZmtpEvent[] = [];
  while (out.length < count) {
    out.push(...decoder.push(await stream.readAvailable()));
  }
  return out;
}

describe("WebTransport ZMTP connection", () => {
  it("accepts HTTPS URLs and requires WebTransport", () => {
    expect(webTransportUrl("https://host:4433/omq")).toBe(
      "https://host:4433/omq",
    );
    expect(webTransportUrl("https://host/omq")).toBe("https://host/omq");
    expect(() => webTransportUrl("h3+quic://host/omq")).toThrow();
    expect(() => webTransportUrl("ws://host")).toThrow();
  });

  it("writes role prefaces, handshakes, and exchanges messages", async () => {
    const received: Message[] = [];
    let ready = false;
    const conn = new WebTransportConnection("https://127.0.0.1:4433/omq", {
      socketType: "DEALER",
      identity: new Uint8Array(0),
      onReady: () => (ready = true),
      onMessage: (_c: PeerConnection, msg: Message) => received.push(msg),
    });
    await until(() => (FakeWebTransport.last?.streams.length ?? 0) === 2);
    const wt = FakeWebTransport.last!;
    expect(wt.url).toBe("https://127.0.0.1:4433/omq");
    expect(wt.options.allowPooling).toBe(false);
    expect(wt.options.requireUnreliable).toBe(true);
    const [data, liveness] = wt.streams as [PeerStream, PeerStream];
    expect(await data.readExact(8)).toEqual(DATA_PREFACE);
    expect(await liveness.readExact(8)).toEqual(LIVENESS_PREFACE);
    const greeting = await data.readExact(64);
    expect(greeting[12]).toBe("N".charCodeAt(0));

    await data.toClient.write(encodeGreeting("NULL"));
    const decoder = new ZmtpDecoder();
    decoder.push(greeting);
    const [ready1] = await events(data, decoder, 1);
    if (ready1?.type !== "command") throw new Error("expected READY");
    const readyCmd = decodeCommand(ready1.payload);
    expect(readyCmd.name).toBe("READY");
    expect(decodeReadyProperties(readyCmd.body).socketType).toBe("DEALER");

    await data.toClient.write(
      encodeCommandFrame(encodeReady("ROUTER", new Uint8Array(0))),
    );
    await until(() => ready && conn.isReady);

    // Peer message split across chunk boundaries.
    const wire = encodeMessage([
      new Uint8Array([1]),
      new Uint8Array(300).fill(7),
    ]);
    await data.toClient.write(wire.subarray(0, 5));
    await data.toClient.write(wire.subarray(5));
    await until(() => received.length === 1);
    expect(received[0]!.parts.length).toBe(2);
    expect(received[0]!.parts[1]!.byteLength).toBe(300);

    await conn.send(Message.fromParts([new Uint8Array([9, 9])]));
    const [sent] = await events(data, decoder, 1);
    if (sent?.type !== "message") throw new Error("expected message");
    expect(sent.parts[0]).toEqual(new Uint8Array([9, 9]));
    conn.close();
    expect(await data.readToEnd()).toEqual(new Uint8Array(0));
    expect(wt.closedWith).toBeNull();
    wt.peerClose();
    await until(() => wt.closedWith !== null);
    expect(wt.closedWith?.closeCode).toBe(0);
  });

  it("flushes resolved sends and finishes the data stream on close", async () => {
    const { conn, wt, data, decoder } = await readyDealer();
    const wire = data.readToEnd();
    for (let i = 0; i < 3; i++) {
      await conn.send(Message.fromParts([new Uint8Array([i])]));
    }
    conn.close();
    expect(conn.connectionState).toBe("closed");
    const parts = decoder.push(await wire).map((event) =>
      event.type === "message" ? event.parts[0]![0] : -1
    );
    expect(parts).toEqual([0, 1, 2]);
    expect(wt.closedWith).toBeNull();
    wt.peerClose();
    await until(() => wt.closedWith !== null);
    expect(wt.closedWith?.closeCode).toBe(0);
  });

  it("aborts when the peer does not close within lingerMs", async () => {
    for (const lingerMs of [20, 0]) {
      const { conn, wt } = await readyDealer({ lingerMs });
      conn.close();
      await until(() => wt.closedWith !== null);
      expect(wt.closedWith?.closeCode).toBe(5);
    }
  });

  it("answers liveness PINGs with the newest nonce", async () => {
    new WebTransportConnection("https://127.0.0.1:1/omq", {
      socketType: "PULL",
      identity: new Uint8Array(0),
    });
    await until(() => (FakeWebTransport.last?.streams.length ?? 0) === 2);
    const liveness = FakeWebTransport.last!.streams[1]!;
    await liveness.readExact(8);
    await liveness.toClient.write(LIVENESS_PREFACE);
    await liveness.toClient.write(new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0, 42]));
    expect(await liveness.readExact(9)).toEqual(
      new Uint8Array([2, 0, 0, 0, 0, 0, 0, 0, 42]),
    );
  });

  it("closes on a bad liveness record, peer FIN, or mechanism mismatch", async () => {
    for (const scenario of ["record", "fin", "mechanism"] as const) {
      const errors: string[] = [];
      new WebTransportConnection("https://127.0.0.1:1/omq", {
        socketType: "PULL",
        identity: new Uint8Array(0),
        onError: (_c, error) => errors.push(error.message),
      });
      await until(() => (FakeWebTransport.last?.streams.length ?? 0) === 2);
      const wt = FakeWebTransport.last!;
      const [data, liveness] = wt.streams as [PeerStream, PeerStream];
      if (scenario === "record") {
        await liveness.toClient.write(LIVENESS_PREFACE);
        await liveness.toClient.write(
          new Uint8Array([7, 0, 0, 0, 0, 0, 0, 0, 1]),
        );
      } else if (scenario === "fin") {
        await data.toClient.close();
      } else {
        await data.toClient.write(encodeGreeting("PLAIN", true));
      }
      await until(() => wt.closedWith !== null);
      expect(errors.length).toBe(1);
    }
  });

  it("detects liveness timeout when the peer stays silent", async () => {
    const errors: string[] = [];
    new WebTransportConnection("https://127.0.0.1:1/omq", {
      socketType: "PULL",
      identity: new Uint8Array(0),
      heartbeatIntervalMs: 10,
      heartbeatTimeoutMs: 40,
      onError: (_c, error) => errors.push(error.message),
    });
    await until(() => errors.some((e) => e.includes("Liveness timeout")));
  });

  it("delivers post-handshake commands to the socket in stream order", async () => {
    const commands: string[] = [];
    new WebTransportConnection("https://127.0.0.1:1/omq", {
      socketType: "XPUB",
      identity: new Uint8Array(0),
      onCommand: (_c, name) => commands.push(name),
    });
    await until(() => (FakeWebTransport.last?.streams.length ?? 0) === 2);
    const data = FakeWebTransport.last!.streams[0]!;
    await data.toClient.write(encodeGreeting("NULL"));
    await data.toClient.write(
      encodeCommandFrame(encodeReady("XSUB", new Uint8Array(0))),
    );
    await data.toClient.write(
      encodeCommandFrame(encodeSubscribe(new Uint8Array([1]))),
    );
    await until(() => commands.length === 1);
    expect(commands).toEqual(["SUBSCRIBE"]);
  });
});
