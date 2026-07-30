import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { initSyncFromBytes } from "@paddor/lz4rip";
import {
  Channel,
  Client,
  Dish,
  Gather,
  Message,
  Pub,
  Pull,
  Push,
  Radio,
  Req,
  Scatter,
  Sub,
} from "../src/mod.ts";

interface Peer {
  child: ChildProcessWithoutNullStreams;
  endpoint: string;
  stderr: () => string;
}

const runInterop = process.env.OMQ_TS_INTEROP === "1";
const describeInterop = runInterop ? describe : describe.skip;
const peers: Peer[] = [];

beforeAll(() => {
  if (!runInterop) return;
  const wasmPath = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "../node_modules/@paddor/lz4rip/src/pkg/lz4rip.wasm",
  );
  initSyncFromBytes(readFileSync(wasmPath));
});

afterEach(() => {
  for (const peer of peers.splice(0)) {
    if (peer.child.exitCode === null) peer.child.kill("SIGTERM");
  }
});

function fixtureManifest(): string {
  return resolve(
    dirname(fileURLToPath(import.meta.url)),
    "fixtures/omq-rs-peer/Cargo.toml",
  );
}

function startPeer(args: string[]): Promise<Peer> {
  const child = spawn("cargo", [
    "run",
    "--quiet",
    "--manifest-path",
    fixtureManifest(),
    "--",
    ...args,
  ], {
    env: { ...process.env, RUST_BACKTRACE: "1" },
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  return new Promise((resolvePeer, rejectPeer) => {
    const lines = createInterface({ input: child.stdout });
    let resolved = false;

    lines.on("line", (line) => {
      if (!line.startsWith("ENDPOINT ")) return;
      resolved = true;
      const peer = {
        child,
        endpoint: line.slice("ENDPOINT ".length),
        stderr: () => stderr,
      };
      peers.push(peer);
      resolvePeer(peer);
    });

    child.on("error", rejectPeer);
    child.on("exit", (code, signal) => {
      if (resolved) return;
      rejectPeer(
        new Error(
          `omq.rs peer exited before endpoint code=${code} signal=${signal}\n${stderr}`,
        ),
      );
    });
  });
}

function waitForExit(peer: Peer): Promise<void> {
  return new Promise((resolveExit, rejectExit) => {
    if (peer.child.exitCode !== null) {
      if (peer.child.exitCode === 0) resolveExit();
      else rejectExit(new Error(peer.stderr()));
      return;
    }
    peer.child.on("exit", (code, signal) => {
      if (code === 0) {
        resolveExit();
      } else {
        rejectExit(
          new Error(
            `omq.rs peer failed code=${code} signal=${signal}\n${peer.stderr()}`,
          ),
        );
      }
    });
  });
}

function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  describeFailure: () => string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(describeFailure())), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer!));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(
  predicate: () => boolean,
  describeFailure: () => string,
): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await delay(25);
  }
  throw new Error(describeFailure());
}

async function tsPushToRustPull(
  endpoint: string,
  payload: string,
  auth: "null" | "plain" = "null",
): Promise<void> {
  const peer = await startPeer(["pull-bind", endpoint, payload, auth]);
  const errors: Error[] = [];
  const push = new Push(
    auth === "plain"
      ? {
        plain: { username: "alice", password: "secret" },
        reconnect: false,
        onError: (error) => errors.push(error),
      }
      : { reconnect: false, onError: (error) => errors.push(error) },
  );
  try {
    push.connect(peer.endpoint);
    await withTimeout(
      push.send(Message.from(payload)),
      5_000,
      () =>
        `TS PUSH send timed out ready=${push.readyCount} conn=${push.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust PULL did not exit after TS send\n${peer.stderr()}`,
    );
  } finally {
    push.close();
  }
}

async function rustPushToTsPull(
  endpoint: string,
  payload: string,
  auth: "null" | "plain" = "null",
): Promise<void> {
  const peer = await startPeer(["push-bind", endpoint, payload, auth]);
  const errors: Error[] = [];
  const pull = new Pull(
    auth === "plain"
      ? {
        plain: { username: "alice", password: "secret" },
        reconnect: false,
        onError: (error) => errors.push(error),
      }
      : { reconnect: false, onError: (error) => errors.push(error) },
  );
  try {
    pull.connect(peer.endpoint);
    const msg = await withTimeout(
      pull.recv(),
      5_000,
      () =>
        `TS PULL recv timed out ready=${pull.readyCount} conn=${pull.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(payload);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust PUSH did not exit after TS recv\n${peer.stderr()}`,
    );
  } finally {
    pull.close();
  }
}

async function tsReqToRustRep(
  endpoint: string,
  request: string,
  reply: string,
  auth: "null" | "plain" = "null",
): Promise<void> {
  const peer = await startPeer(["rep-bind", endpoint, request, reply, auth]);
  const errors: Error[] = [];
  const req = new Req(
    auth === "plain"
      ? {
        plain: { username: "alice", password: "secret" },
        reconnect: false,
        onError: (error) => errors.push(error),
      }
      : { reconnect: false, onError: (error) => errors.push(error) },
  );
  try {
    req.connect(peer.endpoint);
    const msg = await withTimeout(
      req.send(Message.from(request)),
      5_000,
      () =>
        `TS REQ timed out ready=${req.readyCount} conn=${req.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(reply);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust REP did not exit after TS REQ\n${peer.stderr()}`,
    );
  } finally {
    req.close();
  }
}

async function rustPubToTsSub(
  endpoint: string,
  topic: string,
  payload: string,
  auth: "null" | "plain" = "null",
): Promise<void> {
  const peer = await startPeer(["pub-bind", endpoint, topic, payload, auth]);
  const errors: Error[] = [];
  const sub = new Sub(
    auth === "plain"
      ? {
        plain: { username: "alice", password: "secret" },
        reconnect: false,
        onError: (error) => errors.push(error),
      }
      : { reconnect: false, onError: (error) => errors.push(error) },
  );
  try {
    sub.subscribe("news.");
    sub.connect(peer.endpoint);
    const msg = await withTimeout(
      sub.recv(),
      5_000,
      () =>
        `TS SUB recv timed out ready=${sub.readyCount} conn=${sub.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(topic);
    expect(msg.string(1)).toBe(payload);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust PUB did not exit after TS SUB recv\n${peer.stderr()}`,
    );
  } finally {
    sub.close();
  }
}

async function tsPubToRustSub(
  endpoint: string,
  topic: string,
  payload: string,
  auth: "null" | "plain" = "null",
): Promise<void> {
  const peer = await startPeer(["sub-bind", endpoint, topic, payload, auth]);
  const errors: Error[] = [];
  const pub = new Pub(
    auth === "plain"
      ? {
        plain: { username: "alice", password: "secret" },
        reconnect: false,
        onError: (error) => errors.push(error),
      }
      : { reconnect: false, onError: (error) => errors.push(error) },
  );
  try {
    pub.connect(peer.endpoint);
    await waitUntil(
      () => pub.readyCount > 0,
      () =>
        `TS PUB never became ready conn=${pub.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await delay(300);
    await pub.send(new Message(topic, payload));
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust SUB did not exit after TS PUB send\n${peer.stderr()}`,
    );
  } finally {
    pub.close();
  }
}

async function tsPushReconnectsToRustPullRestart(
  endpoint: string,
  auth: "null" | "plain" = "null",
): Promise<void> {
  const peer = await startPeer([
    "pull-restart-bind",
    endpoint,
    "before",
    "after",
    auth,
  ]);
  const errors: Error[] = [];
  const push = new Push(
    auth === "plain"
      ? {
        plain: { username: "alice", password: "secret" },
        reconnectInitialDelayMs: 50,
        reconnectMaxDelayMs: 50,
        onError: (error) => errors.push(error),
      }
      : {
        reconnectInitialDelayMs: 50,
        reconnectMaxDelayMs: 50,
        onError: (error) => errors.push(error),
      },
  );
  try {
    push.connect(peer.endpoint);
    await withTimeout(
      push.send(Message.from("before")),
      5_000,
      () =>
        `initial TS PUSH send timed out ready=${push.readyCount} conn=${push.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await waitUntil(
      () => push.readyCount === 0,
      () =>
        `TS PUSH did not observe Rust restart ready=${push.readyCount} conn=${push.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await withTimeout(
      push.send(Message.from("after")),
      10_000,
      () =>
        `post-restart TS PUSH send timed out ready=${push.readyCount} conn=${push.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust restart PULL did not exit\n${peer.stderr()}`,
    );
  } finally {
    push.close();
  }
}

async function tsScatterToRustGather(
  endpoint: string,
  payload: string,
): Promise<void> {
  const peer = await startPeer(["gather-bind", endpoint, payload]);
  const errors: Error[] = [];
  const scatter = new Scatter({
    reconnect: false,
    onError: (error) => errors.push(error),
  });
  try {
    scatter.connect(peer.endpoint);
    await withTimeout(
      scatter.send(Message.from(payload)),
      5_000,
      () =>
        `TS SCATTER send timed out ready=${scatter.readyCount} conn=${scatter.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust GATHER did not exit after TS send\n${peer.stderr()}`,
    );
  } finally {
    scatter.close();
  }
}

async function rustScatterToTsGather(
  endpoint: string,
  payload: string,
): Promise<void> {
  const peer = await startPeer(["scatter-bind", endpoint, payload]);
  const errors: Error[] = [];
  const gather = new Gather({
    reconnect: false,
    onError: (error) => errors.push(error),
  });
  try {
    gather.connect(peer.endpoint);
    const msg = await withTimeout(
      gather.recv(),
      5_000,
      () =>
        `TS GATHER recv timed out ready=${gather.readyCount} conn=${gather.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(payload);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust SCATTER did not exit after TS recv\n${peer.stderr()}`,
    );
  } finally {
    gather.close();
  }
}

async function tsClientToRustServer(
  endpoint: string,
  request: string,
  reply: string,
): Promise<void> {
  const peer = await startPeer(["server-bind", endpoint, request, reply]);
  const errors: Error[] = [];
  const client = new Client({
    identity: "ts-client",
    reconnect: false,
    onError: (error) => errors.push(error),
  });
  try {
    client.connect(peer.endpoint);
    await withTimeout(
      client.send(Message.from(request)),
      5_000,
      () =>
        `TS CLIENT send timed out ready=${client.readyCount} conn=${client.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    const msg = await withTimeout(
      client.recv(),
      5_000,
      () =>
        `TS CLIENT recv timed out ready=${client.readyCount} conn=${client.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(reply);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust SERVER did not exit after TS CLIENT\n${peer.stderr()}`,
    );
  } finally {
    client.close();
  }
}

async function rustRadioToTsDish(
  endpoint: string,
  group: string,
  payload: string,
): Promise<void> {
  const peer = await startPeer(["radio-bind", endpoint, group, payload]);
  const errors: Error[] = [];
  const dish = new Dish({
    reconnect: false,
    onError: (error) => errors.push(error),
  });
  try {
    dish.join(group);
    dish.connect(peer.endpoint);
    const msg = await withTimeout(
      dish.recv(),
      5_000,
      () =>
        `TS DISH recv timed out ready=${dish.readyCount} conn=${dish.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(group);
    expect(msg.string(1)).toBe(payload);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust RADIO did not exit after TS DISH\n${peer.stderr()}`,
    );
  } finally {
    dish.close();
  }
}

async function tsRadioToRustDish(
  endpoint: string,
  group: string,
  payload: string,
): Promise<void> {
  const peer = await startPeer(["dish-bind", endpoint, group, payload]);
  const errors: Error[] = [];
  const radio = new Radio({
    reconnect: false,
    onError: (error) => errors.push(error),
  });
  try {
    radio.connect(peer.endpoint);
    await waitUntil(
      () => radio.readyCount > 0,
      () =>
        `TS RADIO never became ready conn=${radio.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    await delay(300);
    await radio.send(new Message(group, payload));
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust DISH did not exit after TS RADIO\n${peer.stderr()}`,
    );
  } finally {
    radio.close();
  }
}

async function tsChannelToRustChannel(
  endpoint: string,
  request: string,
  reply: string,
): Promise<void> {
  const peer = await startPeer(["channel-bind", endpoint, request, reply]);
  const errors: Error[] = [];
  const channel = new Channel({
    reconnect: false,
    onError: (error) => errors.push(error),
  });
  try {
    channel.connect(peer.endpoint);
    await withTimeout(
      channel.send(Message.from(request)),
      5_000,
      () =>
        `TS CHANNEL send timed out ready=${channel.readyCount} conn=${channel.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    const msg = await withTimeout(
      channel.recv(),
      5_000,
      () =>
        `TS CHANNEL recv timed out ready=${channel.readyCount} conn=${channel.connectionCount} errors=${
          errors.map((e) => e.message).join("; ")
        } rust=${peer.stderr()}`,
    );
    expect(msg.string(0)).toBe(reply);
    await withTimeout(
      waitForExit(peer),
      5_000,
      () => `Rust CHANNEL did not exit after TS CHANNEL\n${peer.stderr()}`,
    );
  } finally {
    channel.close();
  }
}

describeInterop("omq.rs WebSocket interop", () => {
  it("sends from TypeScript PUSH to Rust PULL over ws", async () => {
    await tsPushToRustPull("ws://127.0.0.1:0/", "ts-to-rust");
  }, 120_000);

  it("receives from Rust PUSH on TypeScript PULL over ws", async () => {
    await rustPushToTsPull("ws://127.0.0.1:0/", "rust-to-ts");
  }, 120_000);

  it("sends from TypeScript PUSH to Rust PULL over lz4+ws", async () => {
    await tsPushToRustPull("lz4+ws://127.0.0.1:0/", "ts-lz4-to-rust");
  }, 120_000);

  it("receives from Rust PUSH on TypeScript PULL over lz4+ws", async () => {
    await rustPushToTsPull("lz4+ws://127.0.0.1:0/", "rust-lz4-to-ts");
  }, 120_000);

  it("sends from TypeScript PUSH to Rust PULL over PLAIN ws", async () => {
    await tsPushToRustPull("ws://127.0.0.1:0/", "ts-plain-to-rust", "plain");
  }, 120_000);

  it("receives from Rust PUSH on TypeScript PULL over PLAIN ws", async () => {
    await rustPushToTsPull("ws://127.0.0.1:0/", "rust-plain-to-ts", "plain");
  }, 120_000);

  it("round-trips TypeScript REQ to Rust REP over ws", async () => {
    await tsReqToRustRep("ws://127.0.0.1:0/", "question", "answer");
  }, 120_000);

  it("round-trips TypeScript REQ to Rust REP over PLAIN ws", async () => {
    await tsReqToRustRep(
      "ws://127.0.0.1:0/",
      "plain-question",
      "plain-answer",
      "plain",
    );
  }, 120_000);

  it("receives Rust PUB on TypeScript SUB over ws", async () => {
    await rustPubToTsSub("ws://127.0.0.1:0/", "news.world", "headline");
  }, 120_000);

  it("receives Rust PUB on TypeScript SUB over lz4+ws", async () => {
    await rustPubToTsSub(
      "lz4+ws://127.0.0.1:0/",
      "news.lz4",
      "compressed-headline",
    );
  }, 120_000);

  it("sends TypeScript PUB to Rust SUB over ws", async () => {
    await tsPubToRustSub("ws://127.0.0.1:0/", "news.ts", "published");
  }, 120_000);

  it("sends TypeScript PUB to Rust SUB over lz4+ws", async () => {
    await tsPubToRustSub(
      "lz4+ws://127.0.0.1:0/",
      "news.ts.lz4",
      "published-lz4",
    );
  }, 120_000);

  it("reconnects TypeScript PUSH after Rust PULL restart over ws", async () => {
    await tsPushReconnectsToRustPullRestart("ws://127.0.0.1:0/");
  }, 120_000);

  it(
    "reconnects TypeScript PUSH after Rust PULL restart over lz4+ws",
    async () => {
      await tsPushReconnectsToRustPullRestart("lz4+ws://127.0.0.1:0/");
    },
    120_000,
  );

  it("sends TypeScript SCATTER to Rust GATHER over ws", async () => {
    await tsScatterToRustGather("ws://127.0.0.1:0/", "scatter-to-gather");
  }, 120_000);

  it("receives Rust SCATTER on TypeScript GATHER over ws", async () => {
    await rustScatterToTsGather("ws://127.0.0.1:0/", "gather-from-scatter");
  }, 120_000);

  it("round-trips TypeScript CLIENT to Rust SERVER over ws", async () => {
    await tsClientToRustServer(
      "ws://127.0.0.1:0/",
      "client-request",
      "server-reply",
    );
  }, 120_000);

  it("receives Rust RADIO on TypeScript DISH over ws", async () => {
    await rustRadioToTsDish("ws://127.0.0.1:0/", "weather", "sunny");
  }, 120_000);

  it("sends TypeScript RADIO to Rust DISH over ws", async () => {
    await tsRadioToRustDish("ws://127.0.0.1:0/", "weather", "rain");
  }, 120_000);

  it("round-trips TypeScript CHANNEL to Rust CHANNEL over ws", async () => {
    await tsChannelToRustChannel(
      "ws://127.0.0.1:0/",
      "channel-request",
      "channel-reply",
    );
  }, 120_000);
});
