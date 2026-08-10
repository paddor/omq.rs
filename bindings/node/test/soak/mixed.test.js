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
  const baseline = sampleResources();
  let cycles = 0;
  let messages = 0;

  while (Date.now() < deadline) {
    messages += await pushPullTcpCycle(BATCH);
    messages += await pushPullInprocCycle(BATCH);
    messages += await reqRepTcpCycle(Math.max(16, Math.floor(BATCH / 4)));
    cycles++;

    if (cycles % 25 === 0) {
      const current = sampleResources();
      assert.ok(current.fd <= baseline.fd + FD_BUDGET, `fd leak: baseline=${baseline.fd} current=${current.fd}`);
      assert.ok(current.rss <= baseline.rss + RSS_BUDGET, `rss leak: baseline=${baseline.rss} current=${current.rss}`);
    }
  }

  assert.ok(cycles > 0);
  assert.ok(messages >= cycles);
});

async function pushPullTcpCycle(count) {
  const ctx = new Context();
  const pull = new Pull({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  const push = new Push({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    const payload = Buffer.alloc(32, 7);
    await sendBatch(push, payload, count);
    await recvBatch(pull, count);
    return count;
  } finally {
    push.close();
    pull.close();
    ctx.close();
  }
}

async function pushPullInprocCycle(count) {
  const ctx = new Context();
  const pull = new Pull({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  const push = new Push({ sendHighWaterMark: count * 2, receiveHighWaterMark: count * 2, lingerMs: 0 }, ctx);
  try {
    const endpoint = inprocEndpoint("node-soak");
    await pull.bind(endpoint);
    await push.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    await sendBatch(push, Buffer.alloc(16, 3), count);
    await recvBatch(pull, count);
    return count;
  } finally {
    push.close();
    pull.close();
    ctx.close();
  }
}

async function reqRepTcpCycle(count) {
  const ctx = new Context();
  const rep = new Rep({ workloadProfile: "latency", lingerMs: 0 }, ctx);
  const req = new Req({ workloadProfile: "latency", lingerMs: 0 }, ctx);
  try {
    const endpoint = await rep.bind("tcp://127.0.0.1:0");
    await req.connect(endpoint);
    req.waitConnectedSync(1, 5000);
    for (let i = 0; i < count; i++) {
      const payload = Buffer.from(`soak-${i}`);
      req.sendSync(payload);
      assert.equal(rep.recvSync().string(), `soak-${i}`);
      rep.sendSync(payload);
      assert.equal(req.recvSync().string(), `soak-${i}`);
    }
    return count * 2;
  } finally {
    req.close();
    rep.close();
    ctx.close();
  }
}

function sampleResources() {
  return {
    fd: fdCount(),
    rss: process.memoryUsage().rss,
  };
}

async function recvBatch(socket, count) {
  for (let received = 0; received < count; received++) {
    socket.recvSync();
  }
}

async function sendBatch(socket, payload, count) {
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
