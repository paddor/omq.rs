#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { fork, spawnSync } = require("node:child_process");
const { Worker, isMainThread, parentPort, workerData } = require("node:worker_threads");

const CACHE_DIR = path.join(os.homedir(), ".cache", "omq.node");
const DATA_FILE = path.join(CACHE_DIR, "bindings.jsonl");
const CHART_FILE = path.resolve(__dirname, "../doc/charts/bindings.svg");
const DEFAULT_LATENCY_COUNT = 50_000;
const DEFAULT_THROUGHPUT_DURATION_MS = 2500;
const DEFAULT_WARMUP_DURATION_MS = 500;
const DEFAULT_BENCH_TIMEOUT_MS = 60_000;
const DEFAULT_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768];
const QUICK_SIZES = [16];
const STOP_PAYLOAD = "__OMQ_NODE_BENCH_STOP__";
const STOP_BUFFER = Buffer.from(STOP_PAYLOAD);
const THROUGHPUT_HWM_BYTES = 128 * 1024 * 1024;

if (process.env.OMQ_NODE_BENCH_DEBUG_ARGV) {
  console.error(JSON.stringify(process.argv));
}

const activeChildren = new Set();
const childStates = new WeakMap();
const processMessages = [];
const processWaiters = [];
let activeMain;
let keepAlive;

if (!isMainThread) {
  activeMain = runWorker(workerData).catch((error) => {
    parentPort?.postMessage({ kind: "error", error: serializeError(error) });
  });
} else if (process.argv[2] === "peer") {
  process.on("message", (message) => {
    const waiterIndex = processWaiters.findIndex((waiter) => message.kind === waiter.kind);
    if (waiterIndex === -1) {
      processMessages.push(message);
      return;
    }
    const [waiter] = processWaiters.splice(waiterIndex, 1);
    waiter.resolve(message);
  });
  activeMain = runProcessPeer(JSON.parse(process.argv[3])).catch((error) => {
    if (process.send) process.send({ kind: "error", error: serializeError(error) });
    process.exitCode = 1;
  });
} else {
  keepAlive = setInterval(() => {}, 60_000);
  activeMain = main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  }).finally(() => {
    clearInterval(keepAlive);
  });
}

async function main() {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  const latencyCount = positiveInt(process.env.OMQ_NODE_BENCH_LATENCY_MESSAGES, DEFAULT_LATENCY_COUNT);
  const throughputDurationMs = throughputDurationMsFromEnv();
  const warmupDurationMs = warmupDurationMsFromEnv();
  const timeoutMs = positiveInt(process.env.OMQ_NODE_BENCH_TIMEOUT_MS, DEFAULT_BENCH_TIMEOUT_MS);
  const sizes = parseSizes(process.env.OMQ_NODE_BENCH_SIZES ?? process.env.OMQ_NODE_BENCH_SIZE);
  const impls = parseImpls(process.env.OMQ_NODE_BENCH_IMPLS);
  const zeromqLoadable = canLoadZeromq();
  const runId = `${Date.now()}-${process.pid}`;
  const runOmq = impls.has("omq-node");
  const runZeromq = impls.has("zeromq.js") && zeromqLoadable;

  buildDistIfNeeded();

  if (impls.has("zeromq.js") && !zeromqLoadable) {
    console.log("zeromq.js baseline skipped: package not loadable");
  }

  for (const size of sizes) {
    const latencyCountForThisSize = latencyCountForSize(latencyCount, size);

    if (runOmq) {
      await warmupThroughput(warmupDurationMs, (durationMs) =>
        runOmqInprocThroughput({ mode: "sync", durationMs, size, runId: `${runId}-warmup` }),
      );
      appendRecord(await runOmqInprocThroughput({ mode: "sync", durationMs: throughputDurationMs, size, runId }));
      await warmupThroughput(warmupDurationMs, (durationMs) =>
        runOmqTcpThroughput({ mode: "sync", durationMs, size, runId: `${runId}-warmup`, timeoutMs }),
      );
      appendRecord(await runOmqTcpThroughput({ mode: "sync", durationMs: throughputDurationMs, size, runId, timeoutMs }));
      appendRecord(await runOmqTcpLatency({ mode: "sync", count: latencyCountForThisSize, size, runId, timeoutMs }));

      await warmupThroughput(warmupDurationMs, (durationMs) =>
        runOmqInprocThroughput({ mode: "async", durationMs, size, runId: `${runId}-warmup` }),
      );
      appendRecord(await runOmqInprocThroughput({ mode: "async", durationMs: throughputDurationMs, size, runId }));
      await warmupThroughput(warmupDurationMs, (durationMs) =>
        runOmqTcpThroughput({ mode: "async", durationMs, size, runId: `${runId}-warmup`, timeoutMs }),
      );
      appendRecord(await runOmqTcpThroughput({ mode: "async", durationMs: throughputDurationMs, size, runId, timeoutMs }));
      appendRecord(await runOmqTcpLatency({ mode: "async", count: latencyCountForThisSize, size, runId, timeoutMs }));
    }

    if (runZeromq) {
      await warmupThroughput(warmupDurationMs, (durationMs) =>
        runZeromqTcpThroughput({ durationMs, size, runId: `${runId}-warmup`, timeoutMs }),
      );
      appendRecord(await runZeromqTcpThroughput({
        durationMs: throughputDurationMs,
        size,
        runId,
        timeoutMs,
      }));
      appendRecord(await runZeromqTcpLatency({
        count: latencyCountForThisSize,
        size,
        runId,
        timeoutMs,
      }));
    }
  }
  if (process.env.OMQ_NODE_BENCH_NO_CHART === "1") {
    console.log("chart: skipped");
  } else {
    renderChart();
    console.log(`chart: ${CHART_FILE}`);
  }
}

function appendRecord(record) {
  fs.appendFileSync(DATA_FILE, `${JSON.stringify(record)}\n`);
  printRecord(record);
}

async function warmupThroughput(durationMs, run) {
  if (durationMs <= 0) {
    return;
  }
  await run(durationMs);
}

function buildDistIfNeeded() {
  const dist = path.resolve(__dirname, "../dist/index.js");
  const addon = path.resolve(__dirname, "../omq_node.node");
  if (!fs.existsSync(dist) || !fs.existsSync(addon)) {
    throw new Error("build missing. Run `npm run build` first.");
  }
}

async function runOmqInprocThroughput({ mode, durationMs, size, runId }) {
  const { Context, Push } = require("../dist");
  const context = new Context();
  const push = new Push(benchOptions({ durationMs, metric: "throughput", mode, size }), context);
  let receiver;
  try {
    const endpoint = `inproc://omq-node-bench-${process.pid}-${Date.now()}-${size}`;
    receiver = new Worker(__filename, {
      workerData: {
        kind: "omq-inproc-pull",
        endpoint,
        shareKey: context.shareKey(),
        durationMs,
        mode,
        size,
        metric: "throughput",
      },
    });
    await waitWorker(receiver, "ready");
    await push.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    receiver.postMessage({ kind: "start" });
    const payload = Buffer.alloc(size, 7);
    const send = mode === "async"
      ? await sendOmqForDurationAsync(push, payload, durationMs)
      : sendOmqForDurationSync(push, payload, durationMs);
    if (mode === "async") await push.send(STOP_PAYLOAD);
    else push.sendSync(STOP_PAYLOAD);
    const recv = await waitWorker(receiver, "done");
    return throughputRecord({
      runId,
      impl: "omq-node",
      mode,
      transport: "inproc",
      count: recv.count,
      size,
      elapsedSec: Math.max(recv.elapsedSec, send.elapsedSec),
    });
  } finally {
    push.close();
    context.close();
    if (receiver !== undefined) {
      await receiver.terminate();
    }
  }
}

async function runOmqTcpThroughput({ mode, durationMs, size, runId, timeoutMs }) {
  const endpoint = await freeTcpEndpoint();
  const common = { impl: "omq-node", mode, transport: "tcp", metric: "throughput", endpoint, durationMs, size };
  const receiver = forkPeer({ ...common, role: "pull" });
  let sender;
  try {
    await waitChildWithTimeout(receiver, "ready", timeoutMs, `omq-node tcp throughput ${size}B pull ready`);
    sender = forkPeer({ ...common, role: "push" });
    await waitChildWithTimeout(sender, "ready", timeoutMs, `omq-node tcp throughput ${size}B push ready`);
    await Promise.all([sendChild(receiver, { kind: "start" }), sendChild(sender, { kind: "start" })]);
    const [recv, send] = await withTimeout(
      Promise.all([waitChild(receiver, "done"), waitChild(sender, "done")]),
      timeoutMs,
      `omq-node tcp throughput ${size}B`,
    );
    await sendChild(sender, { kind: "shutdown" });
    await Promise.all([waitExit(receiver), waitExit(sender)]);
    return throughputRecord({
      runId,
      impl: "omq-node",
      mode,
      transport: "tcp",
      count: recv.count,
      size,
      elapsedSec: Math.max(recv.elapsedSec, send.elapsedSec),
    });
  } finally {
    killIfRunning(receiver);
    if (sender) killIfRunning(sender);
  }
}

async function runOmqTcpLatency({ mode, count, size, runId, timeoutMs }) {
  const endpoint = await freeTcpEndpoint();
  const common = { impl: "omq-node", mode, transport: "tcp", metric: "latency", endpoint, count, size };
  const rep = forkPeer({ ...common, role: "rep" });
  let req;
  try {
    await waitChildWithTimeout(rep, "ready", timeoutMs, `omq-node tcp latency ${size}B rep ready`);
    req = forkPeer({ ...common, role: "req" });
    await waitChildWithTimeout(req, "ready", timeoutMs, `omq-node tcp latency ${size}B req ready`);
    await Promise.all([sendChild(rep, { kind: "start" }), sendChild(req, { kind: "start" })]);
    const result = await waitChildWithTimeout(req, "done", timeoutMs, `omq-node tcp latency ${size}B`);
    await Promise.all([waitExit(req), waitExit(rep)]);
    return latencyRecord({
      runId,
      impl: "omq-node",
      mode,
      transport: "tcp",
      count,
      size,
      latencyUs: result.p50Us,
      avgLatencyUs: result.latencyUs,
      p99Us: result.p99Us,
      p999Us: result.p999Us,
      maxUs: result.maxUs,
    });
  } finally {
    killIfRunning(rep);
    if (req) killIfRunning(req);
  }
}

async function runZeromqTcpThroughput({ durationMs, size, runId, timeoutMs }) {
  const endpoint = await freeTcpEndpoint();
  const common = { impl: "zeromq.js", transport: "tcp", metric: "throughput", endpoint, durationMs, size };
  const receiver = forkPeer({ ...common, role: "pull" });
  let sender;
  try {
    await waitChildWithTimeout(receiver, "ready", timeoutMs, `zeromq.js tcp throughput ${size}B pull ready`);
    sender = forkPeer({ ...common, role: "push" });
    await waitChildWithTimeout(sender, "ready", timeoutMs, `zeromq.js tcp throughput ${size}B push ready`);
    await Promise.all([sendChild(receiver, { kind: "start" }), sendChild(sender, { kind: "start" })]);
    const [recv, send] = await withTimeout(
      Promise.all([waitChild(receiver, "done"), waitChild(sender, "done")]),
      timeoutMs,
      `zeromq.js tcp throughput ${size}B`,
    );
    await sendChild(sender, { kind: "shutdown" });
    await Promise.all([waitExit(receiver), waitExit(sender)]);
    return throughputRecord({
      runId,
      impl: "zeromq.js",
      mode: "async",
      transport: "tcp",
      count: recv.count,
      size,
      elapsedSec: Math.max(recv.elapsedSec, send.elapsedSec),
    });
  } finally {
    killIfRunning(receiver);
    if (sender) killIfRunning(sender);
  }
}

async function runZeromqTcpLatency({ count, size, runId, timeoutMs }) {
  const endpoint = await freeTcpEndpoint();
  const common = { impl: "zeromq.js", transport: "tcp", metric: "latency", endpoint, count, size };
  const rep = forkPeer({ ...common, role: "rep" });
  let req;
  try {
    await waitChildWithTimeout(rep, "ready", timeoutMs, `zeromq.js tcp latency ${size}B rep ready`);
    req = forkPeer({ ...common, role: "req" });
    await waitChildWithTimeout(req, "ready", timeoutMs, `zeromq.js tcp latency ${size}B req ready`);
    await Promise.all([sendChild(rep, { kind: "start" }), sendChild(req, { kind: "start" })]);
    const result = await waitChildWithTimeout(req, "done", timeoutMs, `zeromq.js tcp latency ${size}B`);
    await Promise.all([waitExit(req), waitExit(rep)]);
    return latencyRecord({
      runId,
      impl: "zeromq.js",
      mode: "async",
      transport: "tcp",
      count,
      size,
      latencyUs: result.p50Us,
      avgLatencyUs: result.latencyUs,
      p99Us: result.p99Us,
      p999Us: result.p999Us,
      maxUs: result.maxUs,
    });
  } finally {
    killIfRunning(rep);
    if (req) killIfRunning(req);
  }
}

async function runProcessPeer(config) {
  if (config.impl === "omq-node") {
    await runOmqProcessPeer(config);
  } else {
    await runZeromqProcessPeer(config);
  }
}

async function runWorker(config) {
  if (config.kind !== "omq-inproc-pull") {
    throw new Error(`unknown worker kind ${config.kind}`);
  }
  const { Context, Pull } = require("../dist");
  const context = Context.fromShareKey(config.shareKey);
  const pull = new Pull(benchOptions(config), context);
  try {
    await pull.bind(config.endpoint);
    parentPort.postMessage({ kind: "ready" });
    await waitWorkerStart();
    const result = config.mode === "async" ? await recvUntilStopAsync(pull) : recvUntilStopSync(pull);
    parentPort.postMessage({ kind: "done", count: result.count, elapsedSec: result.elapsedSec });
  } finally {
    pull.close();
    context.close();
  }
}

async function runOmqProcessPeer(config) {
  const { Pull, Push, Rep, Req } = require("../dist");
  if (config.role === "pull" || config.role === "push") {
    const socket =
      config.role === "pull" ? new Pull(benchOptions(config)) : new Push(benchOptions(config));
    if (config.role === "pull") await socket.bind(config.endpoint);
    else {
      await socket.connect(config.endpoint);
      socket.waitConnectedSync(1, 5000);
    }
    await sendReady();
    await waitUntilStart(config);
    if (config.role === "pull") {
      const result = config.mode === "async" ? await recvUntilStopAsync(socket) : recvUntilStopSync(socket);
      await sendIpc({ kind: "done", count: result.count, elapsedSec: result.elapsedSec });
      socket.close();
      process.exit(0);
      return;
    }
    const payload = Buffer.alloc(config.size, 7);
    const result =
      config.mode === "async"
        ? await sendOmqForDurationAsync(socket, payload, config.durationMs)
        : sendOmqForDurationSync(socket, payload, config.durationMs);
    if (config.mode === "async") await socket.send(STOP_PAYLOAD);
    else socket.sendSync(STOP_PAYLOAD);
    await sendIpc({ kind: "done", count: result.count, elapsedSec: result.elapsedSec });
    await waitProcess("shutdown");
    socket.close();
    process.exit(0);
    return;
  }

  if (config.role === "rep") {
    const rep = new Rep({ ...benchOptions(config), workloadProfile: "latency" });
    await rep.bind(config.endpoint);
    await sendReady();
    await waitUntilStart(config);
    while (true) {
      const request = config.mode === "async" ? await rep.recv() : rep.recvSync();
      if (isStopMessage(request)) {
        if (config.mode === "async") await rep.send("ok");
        else rep.sendSync("ok");
        break;
      }
      if (config.mode === "async") await rep.send(request);
      else rep.sendSync(request);
    }
    process.exit(0);
    return;
  }

  const req = new Req({ ...benchOptions(config), workloadProfile: "latency" });
  await req.connect(config.endpoint);
  req.waitConnectedSync(1, 5000);
  await sendReady();
  await waitUntilStart(config);
  const payload = Buffer.alloc(config.size, 7);
  const start = process.hrtime.bigint();
  const rtts = [];
  for (let i = 0; i < config.count; i++) {
    const roundStart = process.hrtime.bigint();
    if (config.mode === "async") {
      await req.send(payload);
      await req.recv();
    } else {
      req.sendSync(payload);
      req.recvSync();
    }
    rtts.push(secondsSince(roundStart) * 1_000_000);
  }
  const elapsedSec = secondsSince(start);
  rtts.sort((a, b) => a - b);
  if (config.mode === "async") {
    await req.send(STOP_PAYLOAD);
    await req.recv();
  } else {
    req.sendSync(STOP_PAYLOAD);
    req.recvSync();
  }
  await sendIpc({
    kind: "done",
    latencyUs: elapsedSec * 1_000_000 / config.count,
    p50Us: percentile(rtts, 50),
    p99Us: percentile(rtts, 99),
    p999Us: percentile(rtts, 99.9),
    maxUs: percentile(rtts, 100),
  });
  process.exit(0);
}

async function runZeromqProcessPeer(config) {
  const zmq = require("zeromq");
  if (config.role === "pull") {
    const pull = new zmq.Pull({ receiveHighWaterMark: throughputHighWaterMarkForSize(config.size) });
    await pull.bind(config.endpoint);
    await sendReady();
    await waitUntilStart(config);
    const start = process.hrtime.bigint();
    let count = 0;
    while (true) {
      const [message] = await pull.receive();
      if (isStopMessage(message)) {
        break;
      }
      count++;
    }
    const elapsedSec = secondsSince(start);
    await sendIpc({ kind: "done", count, elapsedSec });
    pull.close();
    process.exit(0);
    return;
  }
  if (config.role === "push") {
    const push = new zmq.Push({ sendHighWaterMark: throughputHighWaterMarkForSize(config.size) });
    push.connect(config.endpoint);
    await sendReady();
    await waitUntilStart(config);
    const payload = Buffer.alloc(config.size, 7);
    const start = process.hrtime.bigint();
    const deadline = start + BigInt(Math.round(config.durationMs * 1_000_000));
    let count = 0;
    do {
      await push.send(payload);
      count++;
    } while (process.hrtime.bigint() < deadline);
    await push.send(STOP_PAYLOAD);
    await sendIpc({ kind: "done", count, elapsedSec: secondsSince(start) });
    await waitProcess("shutdown");
    push.close();
    process.exit(0);
    return;
  }
  if (config.role === "rep") {
    const rep = new zmq.Reply();
    const stopPayload = Buffer.from(STOP_PAYLOAD);
    await rep.bind(config.endpoint);
    await sendReady();
    await waitUntilStart(config);
    for await (const [request] of rep) {
      if (request.length === stopPayload.length && request.equals(stopPayload)) {
        await rep.send("ok");
        break;
      }
      await rep.send(request);
    }
    rep.close();
    process.exit(0);
    return;
  }
  const req = new zmq.Request();
  req.connect(config.endpoint);
  await sendReady();
  await waitUntilStart(config);
  const payload = Buffer.alloc(config.size, 7);
  const start = process.hrtime.bigint();
  const rtts = [];
  for (let i = 0; i < config.count; i++) {
    const roundStart = process.hrtime.bigint();
    await req.send(payload);
    await req.receive();
    rtts.push(secondsSince(roundStart) * 1_000_000);
  }
  const elapsedSec = secondsSince(start);
  rtts.sort((a, b) => a - b);
  await req.send("__OMQ_NODE_BENCH_STOP__");
  await req.receive();
  await sendIpc({
    kind: "done",
    latencyUs: elapsedSec * 1_000_000 / config.count,
    p50Us: percentile(rtts, 50),
    p99Us: percentile(rtts, 99),
    p999Us: percentile(rtts, 99.9),
    maxUs: percentile(rtts, 100),
  });
  req.close();
  process.exit(0);
}

function benchOptions(config) {
  const hwmBase =
    config.metric === "throughput"
      ? throughputHighWaterMarkForSize(config.size)
      : Number.isFinite(config.count)
        ? config.count + 1
        : 262_144;
  return {
    sendHighWaterMark: Math.max(1000, Math.min(hwmBase, 262_144)),
    receiveHighWaterMark: Math.max(1000, Math.min(hwmBase, 262_144)),
    lingerMs: 0,
    workloadProfile: config.metric === "latency" ? "latency" : "throughput",
  };
}

function recvUntilStopSync(socket) {
  const start = process.hrtime.bigint();
  let count = 0;
  while (true) {
    const message = socket.recvSync();
    if (isStopMessage(message)) {
      break;
    }
    count++;
  }
  const elapsedSec = secondsSince(start);
  return { count, elapsedSec };
}

async function recvUntilStopAsync(socket) {
  const start = process.hrtime.bigint();
  let count = 0;
  while (true) {
    const message = await socket.recv();
    if (isStopMessage(message)) {
      break;
    }
    count++;
  }
  const elapsedSec = secondsSince(start);
  return { count, elapsedSec };
}

function sendOmqForDurationSync(socket, payload, durationMs) {
  const start = process.hrtime.bigint();
  const deadline = start + BigInt(Math.round(durationMs * 1_000_000));
  const checkInterval = timeCheckIntervalForSize(payload.length);
  let count = 0;
  do {
    for (let i = 0; i < checkInterval; i++) {
      socket.sendSync(payload);
      count++;
    }
  } while (process.hrtime.bigint() < deadline);
  return { count, elapsedSec: secondsSince(start) };
}

async function sendOmqForDurationAsync(socket, payload, durationMs) {
  const start = process.hrtime.bigint();
  const deadline = start + BigInt(Math.round(durationMs * 1_000_000));
  const checkInterval = timeCheckIntervalForSize(payload.length);
  let count = 0;
  do {
    for (let i = 0; i < checkInterval; i++) {
      await socket.send(payload);
      count++;
    }
  } while (process.hrtime.bigint() < deadline);
  return { count, elapsedSec: secondsSince(start) };
}

function timeCheckIntervalForSize(size) {
  if (size >= 16 * 1024) return 16;
  if (size >= 4 * 1024) return 64;
  return 512;
}

function isStopMessage(message) {
  const part = typeof message?.part === "function" ? message.part(0) : message;
  if (!(part instanceof Uint8Array)) {
    return false;
  }
  const buffer = Buffer.from(part.buffer, part.byteOffset, part.byteLength);
  return buffer.length === STOP_BUFFER.length && buffer.equals(STOP_BUFFER);
}

function forkPeer(config) {
  const child = fork(__filename, ["peer", JSON.stringify(config)], {
    cwd: path.resolve(__dirname, ".."),
    stdio: ["inherit", "inherit", "inherit", "ipc"],
  });
  const state = { messages: [], waiters: [] };
  childStates.set(child, state);
  child.on("message", (message) => {
    const waiterIndex = state.waiters.findIndex((waiter) => message.kind === waiter.kind || message.kind === "error");
    if (waiterIndex === -1) {
      state.messages.push(message);
      return;
    }
    const [waiter] = state.waiters.splice(waiterIndex, 1);
    child.off("exit", waiter.onExit);
    if (message.kind === "error") waiter.reject(deserializeError(message.error));
    else waiter.resolve(message);
  });
  activeChildren.add(child);
  child.once("exit", (code, signal) => {
    activeChildren.delete(child);
    for (const waiter of state.waiters.splice(0)) {
      waiter.reject(new Error(`peer exited before ${waiter.kind}: code=${code} signal=${signal}`));
    }
  });
  return child;
}

function waitWorker(worker, kind) {
  return new Promise((resolve, reject) => {
    function cleanup() {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    }
    function onMessage(message) {
      if (message.kind !== kind && message.kind !== "error") return;
      cleanup();
      if (message.kind === "error") reject(deserializeError(message.error));
      else resolve(message);
    }
    function onError(error) {
      cleanup();
      reject(error);
    }
    function onExit(code) {
      cleanup();
      reject(new Error(`worker exited before ${kind}: code=${code}`));
    }
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
  });
}

function waitWorkerStart() {
  return new Promise((resolve) => {
    parentPort.once("message", (message) => {
      if (message.kind === "start") {
        resolve();
      }
    });
  });
}

async function freeTcpEndpoint() {
  const net = require("node:net");
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return `tcp://127.0.0.1:${address.port}`;
}

function waitChild(child, kind) {
  const state = childStates.get(child);
  if (state === undefined) {
    return Promise.reject(new Error("unknown benchmark peer"));
  }
  const cachedIndex = state.messages.findIndex((message) => message.kind === kind || message.kind === "error");
  if (cachedIndex !== -1) {
    const [message] = state.messages.splice(cachedIndex, 1);
    if (message.kind === "error") return Promise.reject(deserializeError(message.error));
    return Promise.resolve(message);
  }
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.reject(new Error(`peer exited before ${kind}: code=${child.exitCode} signal=${child.signalCode}`));
  }
  return new Promise((resolve, reject) => {
    function onExit(code, signal) {
      cleanup();
      reject(new Error(`peer exited before ${kind}: code=${code} signal=${signal}`));
    }
    function cleanup() {
      child.off("exit", onExit);
      const index = state.waiters.findIndex((waiter) => waiter.onExit === onExit);
      if (index !== -1) state.waiters.splice(index, 1);
    }
    state.waiters.push({ kind, resolve, reject, onExit });
    child.on("exit", onExit);
  });
}

function waitChildWithTimeout(child, kind, timeoutMs, label) {
  return withTimeout(waitChild(child, kind), timeoutMs, label);
}

function withTimeout(promise, timeoutMs, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function waitProcess(kind) {
  const cachedIndex = processMessages.findIndex((message) => message.kind === kind);
  if (cachedIndex !== -1) {
    processMessages.splice(cachedIndex, 1);
    return Promise.resolve(kind);
  }
  return new Promise((resolve) => {
    processWaiters.push({ kind, resolve });
  });
}

function sendChild(child, message) {
  return new Promise((resolve, reject) => {
    if (child.exitCode !== null || child.signalCode !== null || !child.connected) {
      resolve();
      return;
    }
    child.send(message, (error) => (error ? reject(error) : resolve()));
  });
}

async function sendReady() {
  await new Promise((resolve) => setImmediate(resolve));
  await sendIpc({ kind: "ready" });
}

function sendIpc(message) {
  return new Promise((resolve, reject) => {
    if (!process.send) {
      resolve();
      return;
    }
    process.send(message, (error) => (error ? reject(error) : resolve()));
  });
}

async function waitUntilStart(config) {
  if (Number.isFinite(config.startAtMs)) {
    await sleep(Math.max(0, config.startAtMs - Date.now()));
    return;
  }
  await waitProcess("start");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function waitExit(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    if (child.exitCode === 0) return Promise.resolve();
    return Promise.reject(new Error(`peer failed: code=${child.exitCode} signal=${child.signalCode}`));
  }
  return new Promise((resolve, reject) => {
    child.once("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`peer failed: code=${code} signal=${signal}`));
    });
  });
}

function killIfRunning(child) {
  if (child.exitCode === null && child.signalCode === null) {
    child.kill();
  }
}

function canLoadZeromq() {
  try {
    require("zeromq");
    return true;
  } catch {
    return false;
  }
}

function throughputRecord({ runId, impl, mode, transport, count, size, elapsedSec }) {
  return {
    runId,
    date: new Date().toISOString(),
    impl,
    mode,
    metric: "throughput",
    transport,
    size,
    count,
    elapsedSec,
    msgPerSec: count / elapsedSec,
  };
}

function latencyRecord({ runId, impl, mode, transport, count, size, latencyUs, avgLatencyUs, p99Us, p999Us, maxUs }) {
  return {
    runId,
    date: new Date().toISOString(),
    impl,
    mode,
    metric: "latency",
    transport,
    size,
    count,
    latencyUs,
    avgLatencyUs,
    p99Us,
    p999Us,
    maxUs,
  };
}

function printRecord(record) {
  const mode = record.mode === undefined ? "" : ` ${record.mode}`;
  if (record.metric === "throughput") {
    console.log(
      `${record.impl}${mode} ${record.transport} throughput ${formatRate(record.msgPerSec)} msg/s ` +
        `(${record.count} x ${record.size}B)`,
    );
  } else {
    console.log(
      `${record.impl}${mode} ${record.transport} req/rep latency ${record.latencyUs.toFixed(1)} us ` +
        `p50 (${record.count} x ${record.size}B)`,
    );
  }
}

function renderChart() {
  const python = process.env.PYTHON ?? "python3";
  const args = [path.resolve(__dirname, "update_perf.py"), "--chart-only"];
  const result = spawnSync(python, args, {
    stdio: "inherit",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`chart generator failed: ${python} exited ${result.status}`);
  }
}

function formatRate(value) {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toFixed(0);
}

function positiveInt(raw, fallback) {
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return Math.floor(value);
}

function positiveNumber(raw, fallback) {
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return value;
}

function throughputDurationMsFromEnv() {
  const rawMs = process.env.OMQ_NODE_BENCH_THROUGHPUT_DURATION_MS;
  if (rawMs !== undefined) {
    return Math.round(positiveNumber(rawMs, DEFAULT_THROUGHPUT_DURATION_MS));
  }
  const rawSecs =
    process.env.OMQ_NODE_BENCH_THROUGHPUT_DURATION_SECS ??
    process.env.OMQ_NODE_BENCH_DURATION_SECS;
  const seconds = positiveNumber(rawSecs, DEFAULT_THROUGHPUT_DURATION_MS / 1000);
  return Math.round(seconds * 1000);
}

function warmupDurationMsFromEnv() {
  const rawMs = process.env.OMQ_NODE_BENCH_WARMUP_DURATION_MS;
  if (rawMs !== undefined) {
    return Math.round(positiveNumber(rawMs, DEFAULT_WARMUP_DURATION_MS));
  }
  const rawSecs = process.env.OMQ_NODE_BENCH_WARMUP_DURATION_SECS;
  const seconds = positiveNumber(rawSecs, DEFAULT_WARMUP_DURATION_MS / 1000);
  return Math.round(seconds * 1000);
}

function throughputHighWaterMarkForSize(size) {
  return Math.max(1000, Math.min(262_144, Math.floor(THROUGHPUT_HWM_BYTES / Math.max(size, 1))));
}

function parseImpls(raw) {
  if (raw === undefined || raw === "" || raw === "all") {
    return new Set(["omq-node", "zeromq.js"]);
  }
  const aliases = new Map([
    ["omq", "omq-node"],
    ["omq-node", "omq-node"],
    ["zeromq", "zeromq.js"],
    ["zeromq.js", "zeromq.js"],
    ["zmq", "zeromq.js"],
  ]);
  const impls = new Set();
  for (const item of String(raw).split(",")) {
    const impl = aliases.get(item.trim().toLowerCase());
    if (impl === undefined) {
      throw new Error(`invalid benchmark implementation ${item}`);
    }
    impls.add(impl);
  }
  if (impls.size === 0) {
    throw new Error("at least one benchmark implementation required");
  }
  return impls;
}

function parseSizes(raw) {
  if (raw === undefined || raw === "") {
    return process.env.OMQ_NODE_BENCH_QUICK === "1" ? QUICK_SIZES : DEFAULT_SIZES;
  }
  const sizes = [];
  for (const item of String(raw).split(",")) {
    const trimmed = item.trim().toLowerCase();
    if (!trimmed) continue;
    const multiplier = trimmed.endsWith("k") ? 1024 : 1;
    const numeric = multiplier === 1024 ? trimmed.slice(0, -1) : trimmed;
    const value = Number(numeric);
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`invalid benchmark size ${item}`);
    }
    sizes.push(Math.floor(value * multiplier));
  }
  if (sizes.length === 0) {
    throw new Error("at least one benchmark size required");
  }
  return sizes;
}

function latencyCountForSize(baseCount, size) {
  const byteBudget = 16 * 1024 * 1024;
  return Math.min(baseCount, 20_000, Math.max(200, Math.floor(byteBudget / size)));
}

function secondsSince(start) {
  return Number(process.hrtime.bigint() - start) / 1_000_000_000;
}

function percentile(values, pct) {
  if (values.length === 0) return 0;
  const index = Math.round((values.length - 1) * (pct / 100));
  return values[Math.min(index, values.length - 1)];
}

function serializeError(error) {
  return { message: error?.message ?? String(error), stack: error?.stack };
}

function deserializeError(error) {
  const out = new Error(error.message);
  out.stack = error.stack;
  return out;
}

function escapeXml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
