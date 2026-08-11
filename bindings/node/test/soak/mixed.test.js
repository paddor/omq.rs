"use strict";

const fs = require("node:fs");
const os = require("node:os");
const test = require("node:test");
const assert = require("node:assert/strict");
const { Context, Push, Pull, Req, Rep } = require("../../dist");
const { inprocEndpoint } = require("../support");

const DEFAULT_DURATION_SECS = 15 * 60;
const DURATION_SECS = Number(process.env.OMQ_NODE_SOAK_DURATION_SECS ?? DEFAULT_DURATION_SECS);
const BATCH = Number(process.env.OMQ_NODE_SOAK_BATCH ?? 256);
const FD_BUDGET = 80;
const RSS_BUDGET = 256 * 1024 * 1024;

test("mixed socket soak", { timeout: (DURATION_SECS + 30) * 1000 }, async () => {
  const deadline = Date.now() + DURATION_SECS * 1000;
  const tcp = await openPushPullTcp(BATCH);
  const inproc = await openPushPullInproc(BATCH);
  const reqRep = await openReqRepTcp(Math.max(16, Math.floor(BATCH / 4)));
  const baseline = sampleResources();
  let cycles = 0;
  let messages = 0;

  try {
    while (Date.now() < deadline) {
      messages += pushPullCycle(tcp);
      messages += pushPullCycle(inproc);
      messages += reqRepCycle(reqRep);
      cycles++;

      if (cycles % 25 === 0) {
        messages += await contextChurnCycle();
        const current = sampleResources();
        assert.ok(current.fd <= baseline.fd + FD_BUDGET, `fd leak: baseline=${baseline.fd} current=${current.fd}`);
        assert.ok(current.rss <= baseline.rss + RSS_BUDGET, `rss leak: baseline=${baseline.rss} current=${current.rss}`);
      }
    }
  } finally {
    reqRep.close();
    inproc.close();
    tcp.close();
  }

  assert.ok(cycles > 0);
  assert.ok(messages >= cycles);
});

async function openPushPullTcp(count) {
  const ctx = new Context();
  const pull = new Pull({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  const push = new Push({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
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
  const ctx = new Context();
  const pull = new Pull({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  const push = new Push({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  try {
    const endpoint = inprocEndpoint("node-soak");
    await pull.bind(endpoint);
    await push.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    return {
      count,
      payload: Buffer.alloc(16, 3),
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
    push.sendSync("x");
    assert.equal(pull.recvSync().string(), "x");
    return 1;
  } finally {
    push.close();
    pull.close();
    ctx.close();
  }
}

function pushPullCycle(pair) {
  sendBatch(pair.push, pair.payload, pair.count);
  recvBatch(pair.pull, pair.count);
  return pair.count;
}

function reqRepCycle(pair) {
  for (let i = 0; i < pair.count; i++) {
    const payload = Buffer.from(`soak-${i}`);
    pair.req.sendSync(payload);
    assert.equal(pair.rep.recvSync().string(), `soak-${i}`);
    pair.rep.sendSync(payload);
    assert.equal(pair.req.recvSync().string(), `soak-${i}`);
  }
  return pair.count * 2;
}

function sampleResources() {
  collectGarbage();
  return {
    fd: fdCount(),
    rss: process.memoryUsage().rss,
  };
}

function collectGarbage() {
  if (typeof global.gc === "function") {
    global.gc();
    global.gc();
  }
}

function recvBatch(socket, count) {
  for (let received = 0; received < count; received++) {
    socket.recvSync();
  }
}

function sendBatch(socket, payload, count) {
  for (let sent = 0; sent < count; sent++) {
    socket.sendSync(payload);
  }
}

function fdCount() {
  if (os.platform() !== "linux") {
    return 0;
  }
  return fs.readdirSync("/proc/self/fd").length;
}
