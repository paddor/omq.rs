const assert = require("node:assert/strict");
const test = require("node:test");

const { Context, Dealer, Pull, Push, Router, curveKeypair, curvePublic } = require("../dist");

test("identity option is copied before use", async () => {
  const identity = Buffer.from("before");
  const router = new Router();
  const dealer = new Dealer({ identity });
  identity.fill("x");
  try {
    const endpoint = await router.bind("tcp://127.0.0.1:0");
    await dealer.connect(endpoint);
    await dealer.send("hello");
    assert.equal((await router.recv()).string(0), "before");
  } finally {
    dealer.close();
    router.close();
  }
});

test("selected typed options are accepted before socket use", () => {
  const context = new Context({ ioThreads: 1 });
  const push = new Push(
    {
      sendHighWaterMark: 64,
      receiveHighWaterMark: 32,
      reconnectInitialDelayMs: 10,
      reconnectMaxDelayMs: 1000,
      lingerMs: 0,
      onMute: "block",
      workloadProfile: "throughput",
      conflate: false,
      routerMandatory: false,
      xpubNodrop: false,
    },
    context,
  );
  push.close();
  context.close();
});

test("invalid option values reject early", () => {
  assert.throws(() => new Push({ onMute: "bad" }), /unknown onMute/);
  assert.throws(() => new Push({ workloadProfile: "bad" }), /unknown workloadProfile/);
  assert.throws(() => new Push({ identity: Buffer.alloc(256) }), /identity length/);
  assert.throws(() => new Push({ lz4: { dictionary: Buffer.alloc(0) } }), /compression dict/);
});

test("PLAIN PUSH/PULL accepts correct credentials", async () => {
  const pull = new Pull({ plain: { username: "alice", password: "secret", server: true } });
  const push = new Push({ plain: { username: "alice", password: "secret" } });
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await push.send("hello over plain");
    assert.equal((await pull.recv()).string(), "hello over plain");
  } finally {
    push.close();
    pull.close();
  }
});

test("PLAIN rejects wrong credentials", async () => {
  const pull = new Pull({ plain: { username: "alice", password: "secret", server: true } });
  const push = new Push({ plain: { username: "alice", password: "wrong" } });
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await push.send("blocked");
    assert.equal(await pull.recv({ signal: AbortSignal.timeout(300) }).catch(() => null), null);
  } finally {
    push.close();
    pull.close();
  }
});

test("CURVE key helpers and PUSH/PULL", async () => {
  const server = curveKeypair();
  const client = curveKeypair();
  assert.equal(server.publicKey.length, 40);
  assert.equal(server.secretKey.length, 40);
  assert.equal(curvePublic(server.secretKey), server.publicKey);

  const pull = new Pull({
    curve: { publicKey: server.publicKey, secretKey: server.secretKey, server: true },
  });
  const push = new Push({
    curve: {
      publicKey: client.publicKey,
      secretKey: client.secretKey,
      serverKey: server.publicKey,
    },
  });
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await push.send("hello over curve");
    assert.equal((await pull.recv()).string(), "hello over curve");
  } finally {
    push.close();
    pull.close();
  }
});

test("CURVE rejects mismatched keypair", () => {
  const first = curveKeypair();
  const second = curveKeypair();
  assert.throws(
    () =>
      new Pull({
        curve: { publicKey: first.publicKey, secretKey: second.secretKey, server: true },
      }),
    /does not match/,
  );
});
