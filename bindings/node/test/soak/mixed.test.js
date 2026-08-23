"use strict";

const fs = require("node:fs");
const os = require("node:os");
const test = require("node:test");
const assert = require("node:assert/strict");
const { Context, Message, Pair, Pub, Push, Pull, Req, Rep, Sub, curveKeypair } = require("../../dist");
const { inprocEndpoint, ipcEndpoint } = require("../support");

const DEFAULT_DURATION_SECS = 15 * 60;
const DURATION_SECS = Number(process.env.OMQ_NODE_SOAK_DURATION_SECS ?? DEFAULT_DURATION_SECS);
const BATCH = Number(process.env.OMQ_NODE_SOAK_BATCH ?? 256);
const FD_BUDGET = 80;

test("mixed socket soak", { timeout: (DURATION_SECS + 30) * 1000 }, async () => {
  const deadline = Date.now() + DURATION_SECS * 1000;
  traceStage(0, "open-tcp");
  const tcp = await openPushPullTcp(BATCH);
  traceStage(0, "open-ipc");
  const ipc = await openPushPull(ipcEndpoint("node-soak"), BATCH);
  traceStage(0, "open-inproc");
  const inproc = await openPushPullInproc(BATCH);
  traceStage(0, "open-abortable");
  const abortable = await openAbortablePushPull(Math.max(16, Math.floor(BATCH / 4)));
  traceStage(0, "open-lz4");
  const lz4 = await openPushPull("lz4+tcp://127.0.0.1:0", BATCH);
  traceStage(0, "open-curve");
  const curve = await openCurvePushPull(BATCH);
  traceStage(0, "open-reqrep");
  const reqRep = await openReqRepTcp(Math.max(16, Math.floor(BATCH / 4)));
  traceStage(0, "open-pair");
  const pair = await openPairTcp();
  traceStage(0, "open-pubsub");
  const pubSub = await openPubSubTcp();
  const baseline = sampleResources();
  const samples = [];
  let cycles = 0;
  let messages = 0;
  let nextReport = Date.now() + 10_000;

  try {
    while (Date.now() < deadline) {
      traceStage(cycles, "tcp");
      messages += await pushPullCycle(tcp, "tcp");
      traceStage(cycles, "ipc");
      messages += await pushPullCycle(ipc, "ipc");
      traceStage(cycles, "inproc");
      messages += await pushPullCycle(inproc, "inproc");
      traceStage(cycles, "abortable");
      messages += await abortablePushPullCycle(abortable);
      traceStage(cycles, "lz4");
      messages += await pushPullCycle(lz4, "lz4");
      traceStage(cycles, "curve");
      messages += await pushPullCycle(curve, "curve");
      traceStage(cycles, "reqrep");
      messages += await reqRepCycle(reqRep);
      traceStage(cycles, "pair");
      messages += await pairCycle(pair, cycles);
      traceStage(cycles, "pubsub");
      messages += await pubSubCycle(pubSub, cycles);
      cycles++;

      if (cycles % 25 === 0) {
        traceStage(cycles, "context-churn");
        messages += await contextChurnCycle();
        traceStage(cycles, "large-multipart");
        messages += await largeMultipartCycle(tcp, cycles);
        const current = sampleResources();
        samples.push({ at: Date.now(), ...current });
        assert.ok(current.fd <= baseline.fd + FD_BUDGET, `fd leak: baseline=${baseline.fd} current=${current.fd}`);
      }

      if (Date.now() >= nextReport) {
        const current = sampleResources();
        console.error(`[node-soak] cycles=${cycles} messages=${messages} rss=${Math.round(current.rss / 1048576)}MB fds=${current.fd}`);
        nextReport = Date.now() + 10_000;
      }
    }
  } finally {
    pubSub.close();
    pair.close();
    reqRep.close();
    curve.close();
    lz4.close();
    abortable.close();
    inproc.close();
    ipc.close();
    tcp.close();
  }

  assert.ok(cycles > 0);
  assert.ok(messages >= cycles);
  assertResourceSlope(samples);
});

async function openPushPullTcp(count) {
  return openPushPull("tcp://127.0.0.1:0", count);
}

async function openPushPull(bindEndpoint, count, pullOptions = {}, pushOptions = {}) {
  const ctx = new Context();
  const common = { sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 };
  const pull = new Pull({ ...common, ...pullOptions }, ctx);
  const push = new Push({ ...common, ...pushOptions }, ctx);
  try {
    const endpoint = await pull.bind(bindEndpoint);
    await push.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    return {
      count,
      payload: Buffer.alloc(32, 7),
      push,
      pull,
      close() {
        push.close();
        pull.close();
        ctx.close();
      },
    };
  } catch (error) {
    push.close();
    pull.close();
    ctx.close();
    throw error;
  }
}

async function openPushPullInproc(count) {
  return openPushPull(inprocEndpoint("node-soak"), count);
}

async function openAbortablePushPull(count) {
  const pair = await openPushPull(inprocEndpoint("node-soak-abortable"), count);
  const controller = new AbortController();
  return {
    ...pair,
    signal: controller.signal,
    close() {
      controller.abort();
      pair.close();
    },
  };
}

async function openCurvePushPull(count) {
  const server = curveKeypair();
  const client = curveKeypair();
  return openPushPull("tcp://127.0.0.1:0", count, {
    curve: { publicKey: server.publicKey, secretKey: server.secretKey, server: true },
  }, {
    curve: {
      publicKey: client.publicKey,
      secretKey: client.secretKey,
      serverKey: server.publicKey,
    },
  });
}

async function openReqRepTcp(count) {
  const ctx = new Context();
  const rep = new Rep({ workloadProfile: "latency", lingerMs: 0 }, ctx);
  const req = new Req({ workloadProfile: "latency", lingerMs: 0 }, ctx);
  try {
    const endpoint = await rep.bind("tcp://127.0.0.1:0");
    await req.connect(endpoint);
    req.waitConnectedSync(1, 5000);
    return {
      count,
      req,
      rep,
      close() {
        req.close();
        rep.close();
        ctx.close();
      },
    };
  } catch (error) {
    req.close();
    rep.close();
    ctx.close();
    throw error;
  }
}

async function contextChurnCycle() {
  const ctx = new Context();
  const pull = new Pull({ lingerMs: 0 }, ctx);
  const push = new Push({ lingerMs: 0 }, ctx);
  try {
    const endpoint = inprocEndpoint("node-soak-churn");
    await pull.bind(endpoint);
    await push.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    await sendWithin(push, "x", "context churn PUSH");
    assert.equal((await recvWithin(pull, "context churn PULL")).string(), "x");
    return 1;
  } finally {
    push.close();
    pull.close();
    ctx.close();
  }
}

async function openPairTcp() {
  const ctx = new Context();
  const left = new Pair({ lingerMs: 0 }, ctx);
  const right = new Pair({ lingerMs: 0 }, ctx);
  try {
    const endpoint = await left.bind("tcp://127.0.0.1:0");
    await right.connect(endpoint);
    right.waitConnectedSync(1, 5000);
    return {
      left,
      right,
      close() { right.close(); left.close(); ctx.close(); },
    };
  } catch (error) {
    right.close(); left.close(); ctx.close(); throw error;
  }
}

async function openPubSubTcp() {
  const ctx = new Context();
  const pub = new Pub({ lingerMs: 0 }, ctx);
  const first = new Sub({ lingerMs: 0 }, ctx);
  const second = new Sub({ lingerMs: 0 }, ctx);
  try {
    const endpoint = await pub.bind("tcp://127.0.0.1:0");
    await first.subscribe("soak.");
    await second.subscribe("soak.");
    await first.connect(endpoint);
    await second.connect(endpoint);
    first.waitConnectedSync(1, 5000);
    second.waitConnectedSync(1, 5000);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const sockets = {
      pub,
      first,
      second,
      close() { second.close(); first.close(); pub.close(); ctx.close(); },
    };
    await waitForPubSub(sockets);
    return sockets;
  } catch (error) {
    second.close(); first.close(); pub.close(); ctx.close(); throw error;
  }
}

async function pushPullCycle(pair, label) {
  await Promise.all([
    sendBatch(pair.push, pair.payload, pair.count, label),
    recvBatch(pair.pull, pair.count, label),
  ]);
  return pair.count;
}

async function abortablePushPullCycle(pair) {
  for (let index = 0; index < pair.count; index++) {
    const received = pair.pull.recv({ signal: pair.signal });
    await pair.push.send(pair.payload);
    await received;
  }
  return pair.count;
}

async function reqRepCycle(pair) {
  for (let i = 0; i < pair.count; i++) {
    const payload = Buffer.from(`soak-${i}`);
    await sendWithin(pair.req, payload, "REQ send");
    assert.equal((await recvWithin(pair.rep, "REP receive")).string(), `soak-${i}`);
    await sendWithin(pair.rep, payload, "REP send");
    assert.equal((await recvWithin(pair.req, "REQ receive")).string(), `soak-${i}`);
  }
  return pair.count * 2;
}

async function pairCycle(pair, cycle) {
  await sendWithin(pair.left, `left-${cycle}`, "PAIR left send");
  assert.equal((await recvWithin(pair.right, "PAIR right receive")).string(), `left-${cycle}`);
  await sendWithin(pair.right, `right-${cycle}`, "PAIR right send");
  assert.equal((await recvWithin(pair.left, "PAIR left receive")).string(), `right-${cycle}`);
  return 2;
}

async function pubSubCycle(sockets, cycle) {
  const payload = `soak.${cycle}`;
  await sendWithin(sockets.pub, payload, "PUB send");
  const firstMessage = await recvWithin(sockets.first, "first SUB");
  const secondMessage = await recvWithin(sockets.second, "second SUB");
  assert.equal(firstMessage.string(), payload);
  assert.equal(secondMessage.string(), payload);
  return 2;
}

async function waitForPubSub(sockets) {
  const deadline = Date.now() + 5000;
  let attempt = 0;
  while (Date.now() < deadline) {
    const payload = `soak.ready.${attempt++}`;
    try {
      await sendWithin(sockets.pub, payload, "PUB readiness send", 500);
      const received = [
        await recvWithin(sockets.first, "first SUB readiness", 500),
        await recvWithin(sockets.second, "second SUB readiness", 500),
      ];
      if (received.every((message) => message.string() === payload)) return;
    } catch {
      // Subscription propagation can legitimately lose early PUB messages.
    }
  }
  throw new Error("PUB/SUB readiness timed out");
}

async function recvWithin(socket, label, timeoutMs = 5000) {
  const messages = socket.recvManySync(1, timeoutMs);
  if (messages.length === 1) {
    return messages[0];
  }
  throw new Error(`${label} stalled`);
}

async function sendWithin(socket, message, label, timeoutMs = 5000) {
  void timeoutMs;
  try {
    socket.sendSync(message);
  } catch (error) {
    throw new Error(`${label} stalled`, { cause: error });
  }
}

async function largeMultipartCycle(pair, cycle) {
  const payload = Buffer.alloc(1024 * 1024, cycle & 0xff);
  await sendWithin(pair.push, new Message([Buffer.from(String(cycle)), payload]), "large multipart send");
  const received = await recvWithin(pair.pull, "large multipart receive");
  assert.equal(received.parts.length, 2);
  assert.equal(received.string(0), String(cycle));
  assert.ok(Buffer.from(received.part(1)).equals(payload));
  return 1;
}

function sampleResources() {
  collectGarbage();
  return {
    fd: fdCount(),
    rss: process.memoryUsage().rss,
  };
}

function assertResourceSlope(samples) {
  if (samples.length < 12) return;
  const warm = samples.slice(Math.floor(samples.length / 5));
  const baseline = warm.slice(0, Math.max(1, Math.floor(warm.length / 10)));
  const tail = warm.slice(-Math.max(1, Math.floor(warm.length / 5)));
  const baseRss = baseline.reduce((sum, item) => sum + item.rss, 0) / baseline.length;
  const tailRss = Math.max(...tail.map((item) => item.rss));
  const peakRss = Math.max(...samples.map((item) => item.rss));
  const growth = tailRss - baseRss;
  console.error(`[node-soak] RSS baseline=${mib(baseRss)}MB tail-max=${mib(tailRss)}MB peak=${mib(peakRss)}MB growth=${(growth / baseRss * 100).toFixed(1)}%`);
  assert.ok(growth < 64 * 1024 * 1024 || growth / baseRss < 0.25,
    `RSS slope: baseline=${baseRss} tail=${tailRss} growth=${growth}`);
}

function mib(bytes) {
  return (bytes / 1048576).toFixed(1);
}

function collectGarbage() {
  if (typeof global.gc === "function") {
    global.gc();
    global.gc();
  }
}

function traceStage(cycle, stage) {
  if (process.env.OMQ_SOAK_TRACE === "1") {
    console.error(`[node-soak-stage] cycle=${cycle} stage=${stage}`);
  }
}

async function recvBatch(socket, count, label) {
  for (let received = 0; received < count; received++) {
    await recvWithin(socket, `${label} receive`);
  }
}

async function sendBatch(socket, payload, count, label) {
  for (let sent = 0; sent < count; sent++) {
    await sendWithin(socket, payload, `${label} send`);
  }
}

function fdCount() {
  if (os.platform() !== "linux") {
    return 0;
  }
  return fs.readdirSync("/proc/self/fd").length;
}
