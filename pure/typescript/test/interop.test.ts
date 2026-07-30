import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { initSyncFromBytes } from "@paddor/lz4rip";
import { Message, Pub, Pull, Push, Req, Sub } from "../src/mod.ts";

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
});
