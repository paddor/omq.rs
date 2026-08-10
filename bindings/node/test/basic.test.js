const assert = require("node:assert/strict");
const test = require("node:test");

const { Context, Message, Pull, Push } = require("../dist");
const { freeTcpEndpoint, inprocEndpoint, ipcEndpoint } = require("./support");

test("message exposes parts and strings", () => {
  const msg = new Message(["hello", Buffer.from("world")]);
  assert.equal(msg.length, 2);
  assert.equal(msg.string(0), "hello");
  assert.equal(msg.string(1), "world");
  assert.deepEqual([...msg].map((part) => Buffer.from(part).toString()), ["hello", "world"]);
});

test("PUSH/PULL over TCP", async () => {
  const pull = new Pull();
  const push = new Push();
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await push.send("hello");
    assert.equal((await pull.recv()).string(), "hello");
  } finally {
    push.close();
    pull.close();
  }
});

test("PUSH/PULL over inproc", async () => {
  const context = new Context();
  const pull = new Pull({}, context);
  const push = new Push({}, context);
  try {
    const endpoint = inprocEndpoint();
    await pull.bind(endpoint);
    await push.connect(endpoint);
    await push.send("hello");
    assert.equal((await pull.recv()).string(), "hello");
  } finally {
    push.close();
    pull.close();
    context.close();
  }
});

test("shared context key shares inproc namespace", async () => {
  const owner = new Context();
  const shared = Context.fromShareKey(owner.shareKey());
  const pull = new Pull({}, owner);
  const push = new Push({}, shared);
  try {
    const endpoint = inprocEndpoint("shared");
    await pull.bind(endpoint);
    await push.connect(endpoint);
    await push.send("shared");
    assert.equal((await pull.recv()).string(), "shared");
  } finally {
    push.close();
    pull.close();
    shared.close();
    owner.close();
  }
});

test("PUSH/PULL over IPC", { skip: process.platform === "win32" }, async () => {
  const pull = new Pull();
  const push = new Push();
  try {
    const endpoint = ipcEndpoint();
    await pull.bind(endpoint);
    await push.connect(endpoint);
    await push.send("hello");
    assert.equal((await pull.recv()).string(), "hello");
  } finally {
    push.close();
    pull.close();
  }
});

test("connect-before-bind works", async () => {
  const endpoint = await freeTcpEndpoint();
  const push = new Push();
  const pull = new Pull();
  try {
    await push.connect(endpoint);
    await pull.bind(endpoint);
    await push.send("hello");
    assert.equal((await pull.recv()).string(), "hello");
  } finally {
    push.close();
    pull.close();
  }
});
