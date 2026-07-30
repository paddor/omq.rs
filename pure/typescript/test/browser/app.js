import {
  compress,
  initLz4,
  Lz4Decoder,
  Message,
  Pub,
  Pull,
  Push,
  Req,
  Sub,
} from "./omq.js";

const testsEl = document.querySelector("#tests");
const logEl = document.querySelector("#log");
const runBtn = document.querySelector("#run");
const baseInput = document.querySelector("#base-port");
const hostInput = document.querySelector("#host");

hostInput.value = location.hostname || "127.0.0.1";

const sockets = [];
let lz4Ready = false;

function endpoint(offset, lz4 = false, secure = false) {
  const base = Number(baseInput.value || "9105");
  const host = hostInput.value.trim() || "127.0.0.1";
  const scheme = lz4
    ? (secure ? "lz4+wss" : "lz4+ws")
    : (secure ? "wss" : "ws");
  return `${scheme}://${host}:${base + offset}/`;
}

function log(line) {
  const now = new Date().toLocaleTimeString();
  logEl.textContent += `[${now}] ${line}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

function addRow(name) {
  const row = document.createElement("div");
  row.className = "test pending";
  row.innerHTML =
    `<span class="status">RUN</span><span>${name}</span><code></code>`;
  testsEl.append(row);
  return row;
}

function setRow(row, status, detail = "") {
  row.className = `test ${status.toLowerCase()}`;
  row.querySelector(".status").textContent = status;
  row.querySelector("code").textContent = detail;
}

function closeAll() {
  while (sockets.length) {
    sockets.pop().close();
  }
}

function socket(sock) {
  sockets.push(sock);
  return sock;
}

function timeout(ms, detail) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(detail)), ms);
  });
}

async function withTimeout(promise, ms, detail) {
  return await Promise.race([promise, timeout(ms, detail)]);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runOne(name, fn) {
  const row = addRow(name);
  try {
    const detail = await fn();
    setRow(row, "PASS", detail || "");
    log(`PASS ${name}`);
    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    setRow(row, "FAIL", message);
    log(`FAIL ${name}: ${message}`);
    return false;
  }
}

function makeReq(offset, opts = {}) {
  const errors = [];
  const req = socket(
    new Req({
      reconnect: false,
      onError: (error) => errors.push(error),
      ...opts,
    }),
  );
  req.connect(endpoint(offset));
  return { req, errors };
}

async function control(cmd) {
  const { req, errors } = makeReq(0);
  try {
    const reply = await withTimeout(
      req.send(new Message(cmd)),
      4000,
      `control timeout: ${errors.map((e) => e.message).join("; ")}`,
    );
    return reply.string(0);
  } finally {
    req.close();
  }
}

async function waitControl(key, expected, ms = 5000) {
  const deadline = Date.now() + ms;
  let last = "";
  while (Date.now() < deadline) {
    last = await control(`get:${key}`);
    if (last === expected) return last;
    await delay(100);
  }
  throw new Error(`${key} expected "${expected}", got "${last}"`);
}

async function waitReady(sock, label, ms = 5000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (sock.readyCount > 0) return;
    await delay(50);
  }
  throw new Error(`${label} not ready`);
}

async function ensureLz4() {
  if (lz4Ready) return;
  await initLz4();
  lz4Ready = true;
}

async function expectSendRejected(promise, ms) {
  let timer;
  try {
    await Promise.race([
      promise.then(
        () => "resolved",
        (error) => ({ error }),
      ),
      new Promise((resolve) => {
        timer = setTimeout(() => resolve("timeout"), ms);
      }),
    ]).then((result) => {
      if (result === "resolved") {
        throw new Error("send unexpectedly resolved");
      }
      if (result === "timeout") {
        throw new Error("send did not reject");
      }
    });
  } finally {
    clearTimeout(timer);
  }
}

function makeLz4mWire(text, blockSize) {
  const plain = new TextEncoder().encode(text);
  const blocks = [];
  let compressedBytes = 0;
  for (let offset = 0; offset < plain.byteLength; offset += blockSize) {
    const block = plain.subarray(offset, offset + blockSize);
    const compressed = compress(block);
    blocks.push(compressed);
    compressedBytes += 4 + compressed.byteLength;
  }

  const wire = new Uint8Array(12 + compressedBytes);
  wire.set([0x4c, 0x5a, 0x34, 0x4d], 0);
  new DataView(wire.buffer).setBigUint64(4, BigInt(plain.byteLength), true);
  let pos = 12;
  for (const block of blocks) {
    new DataView(wire.buffer).setUint32(pos, block.byteLength, true);
    pos += 4;
    wire.set(block, pos);
    pos += block.byteLength;
  }
  return wire;
}

const cases = [
  ["control REQ/REP", async () => {
    const reply = await control("ping");
    if (reply !== "pong") throw new Error(`bad reply: ${reply}`);
    const cleared = await control("clear");
    if (cleared !== "ok") throw new Error(`clear failed: ${cleared}`);
    return endpoint(0);
  }],
  ["REQ -> Rust REP over ws", async () => {
    const { req, errors } = makeReq(1);
    try {
      const reply = await withTimeout(
        req.send(new Message("question")),
        5000,
        errors.map((e) => e.message).join("; ") || "REQ timeout",
      );
      if (reply.string(0) !== "reply:question") {
        throw new Error(`bad reply: ${reply.string(0)}`);
      }
      return endpoint(1);
    } finally {
      req.close();
    }
  }],
  ["PUSH -> Rust PULL over ws", async () => {
    const push = socket(new Push({ reconnect: false }));
    push.connect(endpoint(2));
    await withTimeout(
      push.send(new Message("push-ws-from-firefox")),
      5000,
      "send timeout",
    );
    await waitControl("pull_ws", "push-ws-from-firefox");
    return endpoint(2);
  }],
  ["PULL <- Rust PUSH over ws", async () => {
    const pull = socket(new Pull({ reconnect: false }));
    pull.connect(endpoint(3));
    const msg = await withTimeout(pull.recv(), 5000, "recv timeout");
    if (msg.string(0) !== "push-ws-from-rust") {
      throw new Error(`bad payload: ${msg.string(0)}`);
    }
    return endpoint(3);
  }],
  ["PUB -> Rust SUB over ws", async () => {
    const pub = socket(new Pub({ reconnect: false }));
    pub.connect(endpoint(4));
    await waitReady(pub, "PUB");
    await delay(500);
    await pub.send(new Message("news.browser", "pub-ws-from-firefox"));
    await waitControl("sub_ws", "news.browser|pub-ws-from-firefox");
    return endpoint(4);
  }],
  ["SUB <- Rust PUB over ws", async () => {
    const sub = socket(new Sub({ reconnect: false }));
    sub.subscribe("news.");
    sub.connect(endpoint(5));
    const msg = await withTimeout(sub.recv(), 8000, "recv timeout");
    const got = `${msg.string(0)}|${msg.string(1)}`;
    if (got !== "news.rust|pub-ws-from-rust") {
      throw new Error(`bad message: ${got}`);
    }
    return endpoint(5);
  }],
  ["PLAIN accepted", async () => {
    const push = socket(
      new Push({
        reconnect: false,
        plain: { username: "alice", password: "secret" },
      }),
    );
    push.connect(endpoint(6));
    await withTimeout(
      push.send(new Message("plain-from-firefox")),
      5000,
      "plain send timeout",
    );
    await waitControl("pull_plain", "plain-from-firefox");
    return endpoint(6);
  }],
  ["PLAIN rejected", async () => {
    const errors = [];
    const push = socket(
      new Push({
        reconnect: false,
        plain: { username: "alice", password: "wrong" },
        onError: (error) => errors.push(error),
      }),
    );
    push.connect(endpoint(7));
    await expectSendRejected(push.send(new Message("must-not-arrive")), 5000);
    const received = await control("get:pull_plain_reject");
    if (received !== "") throw new Error(`server received: ${received}`);
    return errors.map((e) => e.message).join("; ") || endpoint(7);
  }],
  ["PUSH -> Rust PULL over lz4+ws", async () => {
    await ensureLz4();
    const push = socket(new Push({ reconnect: false }));
    push.connect(endpoint(8, true));
    const payload = "lz4-from-firefox-" + "x".repeat(4096);
    await withTimeout(
      push.send(new Message(payload)),
      5000,
      "lz4 send timeout",
    );
    await waitControl("pull_lz4", payload);
    return endpoint(8, true);
  }],
  ["PULL <- Rust PUSH over lz4+ws", async () => {
    await ensureLz4();
    const pull = socket(new Pull({ reconnect: false }));
    pull.connect(endpoint(9, true));
    const msg = await withTimeout(pull.recv(), 8000, "lz4 recv timeout");
    if (msg.string(0) !== "push-lz4-from-rust") {
      throw new Error(`bad payload: ${msg.string(0)}`);
    }
    return endpoint(9, true);
  }],
  ["Reconnect after Rust PULL restart", async () => {
    const push = socket(
      new Push({
        reconnectInitialDelayMs: 50,
        reconnectMaxDelayMs: 50,
      }),
    );
    push.connect(endpoint(10));
    await withTimeout(
      push.send(new Message("before")),
      5000,
      "first send timeout",
    );
    await waitControl("restart_first", "before");
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && push.readyCount !== 0) await delay(25);
    await withTimeout(
      push.send(new Message("after")),
      10000,
      "second send timeout",
    );
    await waitControl("restart_second", "after", 10000);
    return endpoint(10);
  }],
  ["Synthetic LZ4M decode branch", async () => {
    await ensureLz4();
    const text = "small synthetic LZ4M payload";
    const decoder = new Lz4Decoder(undefined, 7);
    const decoded = decoder.decodeMessage([makeLz4mWire(text, 7)]);
    if (decoded === null) throw new Error("got dictionary shipment");
    const got = new TextDecoder().decode(decoded[0]);
    if (got !== text) throw new Error(`bad decode: ${got}`);
    return "blockSize=7";
  }],
  ["REQ -> Rust REP over wss", async () => {
    const errors = [];
    const req = socket(
      new Req({
        reconnect: false,
        onError: (error) => errors.push(error),
      }),
    );
    try {
      req.connect(endpoint(11, false, true));
      const reply = await withTimeout(
        req.send(new Message("secure-question")),
        5000,
        errors.map((e) => e.message).join("; ") || "WSS REQ timeout",
      );
      if (reply.string(0) !== "reply:secure-question") {
        throw new Error(`bad reply: ${reply.string(0)}`);
      }
      return endpoint(11, false, true);
    } finally {
      req.close();
    }
  }],
  ["PUSH -> Rust PULL over wss", async () => {
    const push = socket(new Push({ reconnect: false }));
    push.connect(endpoint(12, false, true));
    await withTimeout(
      push.send(new Message("push-wss-from-firefox")),
      5000,
      "wss send timeout",
    );
    await waitControl("wss_pull", "push-wss-from-firefox");
    return endpoint(12, false, true);
  }],
  ["PULL <- Rust PUSH over wss", async () => {
    const pull = socket(new Pull({ reconnect: false }));
    pull.connect(endpoint(13, false, true));
    const msg = await withTimeout(pull.recv(), 5000, "wss recv timeout");
    if (msg.string(0) !== "push-wss-from-rust") {
      throw new Error(`bad payload: ${msg.string(0)}`);
    }
    return endpoint(13, false, true);
  }],
  ["PLAIN accepted over wss", async () => {
    const push = socket(
      new Push({
        reconnect: false,
        plain: { username: "alice", password: "secret" },
      }),
    );
    push.connect(endpoint(14, false, true));
    await withTimeout(
      push.send(new Message("plain-wss-from-firefox")),
      5000,
      "plain wss send timeout",
    );
    await waitControl("wss_pull_plain", "plain-wss-from-firefox");
    return endpoint(14, false, true);
  }],
  ["PUSH -> Rust PULL over lz4+wss", async () => {
    await ensureLz4();
    const push = socket(new Push({ reconnect: false }));
    push.connect(endpoint(15, true, true));
    const payload = "lz4-wss-from-firefox-" + "x".repeat(4096);
    await withTimeout(
      push.send(new Message(payload)),
      5000,
      "lz4 wss send timeout",
    );
    await waitControl("lz4_wss_pull", payload);
    return endpoint(15, true, true);
  }],
  ["PULL <- Rust PUSH over lz4+wss", async () => {
    await ensureLz4();
    const pull = socket(new Pull({ reconnect: false }));
    pull.connect(endpoint(16, true, true));
    const msg = await withTimeout(pull.recv(), 8000, "lz4 wss recv timeout");
    if (msg.string(0) !== "push-lz4-wss-from-rust") {
      throw new Error(`bad payload: ${msg.string(0)}`);
    }
    return endpoint(16, true, true);
  }],
];

document.body.dataset.expected = String(cases.length);

runBtn.addEventListener("click", async () => {
  runBtn.disabled = true;
  document.body.dataset.status = "running";
  testsEl.textContent = "";
  logEl.textContent = "";
  closeAll();
  try {
    let passed = 0;
    for (const [name, fn] of cases) {
      if (await runOne(name, fn)) passed += 1;
    }
    log(`${passed}/${cases.length} passed`);
    document.body.dataset.status = passed === cases.length
      ? "passed"
      : "failed";
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    log(`FATAL ${message}`);
    document.body.dataset.status = "failed";
  } finally {
    closeAll();
    runBtn.disabled = false;
  }
});
