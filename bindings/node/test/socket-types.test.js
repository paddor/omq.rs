const assert = require("node:assert/strict");
const test = require("node:test");

const { Dealer, Dish, Pair, Pull, Push, Radio, Router } = require("../dist");

test("PAIR sends both directions", async () => {
  const left = new Pair();
  const right = new Pair();
  try {
    const endpoint = await left.bind("tcp://127.0.0.1:0");
    await right.connect(endpoint);
    await right.send("hello");
    assert.equal((await left.recv()).string(), "hello");
    await left.send("world");
    assert.equal((await right.recv()).string(), "world");
  } finally {
    right.close();
    left.close();
  }
});

test("DEALER/ROUTER identity routing", async () => {
  const router = new Router();
  const dealer = new Dealer({ identity: "node-dealer" });
  try {
    const endpoint = await router.bind("tcp://127.0.0.1:0");
    await dealer.connect(endpoint);
    await dealer.send("hello");
    const incoming = await router.recv();
    assert.equal(incoming.string(0), "node-dealer");
    assert.equal(incoming.string(1), "hello");
    await router.send([incoming.part(0), Buffer.from("world")]);
    assert.equal((await dealer.recv()).string(), "world");
  } finally {
    dealer.close();
    router.close();
  }
});

test("RADIO sendGroup sends to a joined DISH group", async () => {
  const radio = new Radio();
  const dish = new Dish();
  try {
    const endpoint = await radio.bind(`inproc://node-radio-${process.pid}`);
    await dish.join("canvas");
    await dish.connect(endpoint);
    await radio.sendGroup("canvas", "delta");
    const incoming = await dish.recv();
    assert.equal(incoming.string(0), "canvas");
    assert.equal(incoming.string(1), "delta");
  } finally {
    dish.close();
    radio.close();
  }
});

test("recvManySync drains messages", async () => {
  const pull = new Pull();
  const push = new Push();
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    for (let i = 0; i < 1000; i++) {
      push.sendSync("x");
    }
    let received = 0;
    while (received < 1000) {
      received += pull.recvManySync(256, 1000).length;
    }
    assert.equal(received, 1000);
  } finally {
    push.close();
    pull.close();
  }
});
