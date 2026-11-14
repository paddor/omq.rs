const assert = require("node:assert/strict");
const test = require("node:test");

const { Context, Dealer, Pull, Push, Router, Sub, XPub, curveKeypair, curvePublic } = require("../dist");

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
      maxMessageParts: 16,
      maxPeerSubscriptions: 0,
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
  assert.throws(() => new Push({ lz4: true }), /lz4\+tcp:\/\//);
  assert.throws(() => new Push({ lz4: { dictionary: Buffer.alloc(0) } }), /compression dict/);
  assert.throws(() => new Push({ maxMessageParts: 0 }), /max_message_parts/);
  assert.throws(
    () =>
      new Push({
        plain: { server: true, credentials: [{ username: "has space", password: "secret" }] },
      }),
    /ASCII VCHAR/,
  );
});

test("maxMessageParts drops a message over the limit", async () => {
  const pull = new Pull({ maxMessageParts: 2 });
  const push = new Push();
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await push.send(["a", "b"]);
    assert.equal((await pull.recv()).length, 2);
    await push.send(["a", "b", "c"]);
    assert.equal(await pull.recv({ signal: AbortSignal.timeout(300) }).catch(() => null), null);
  } finally {
    push.close();
    pull.close();
  }
});

test("negative maxMessageParts lifts the default limit", async () => {
  const pull = new Pull({ maxMessageParts: -1 });
  const push = new Push();
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await push.send(Array.from({ length: 1025 }, () => "x"));
    assert.equal((await pull.recv()).length, 1025);
  } finally {
    push.close();
    pull.close();
  }
});

test("maxPeerSubscriptions ignores a prefix over the limit", async () => {
  const xpub = new XPub({ maxPeerSubscriptions: 1 });
  const sub = new Sub();
  try {
    const endpoint = await xpub.bind("tcp://127.0.0.1:0");
    await sub.subscribe("a");
    await sub.connect(endpoint);
    assert.equal((await xpub.recv()).string(), "\x01a");

    // "b" is over the limit, so XPUB never reports it.
    await sub.subscribe("b");
    await sub.unsubscribe("a");
    await sub.subscribe("c");
    assert.equal((await xpub.recv()).string(), "\x00a");
    assert.equal((await xpub.recv()).string(), "\x01c");
  } finally {
    sub.close();
    xpub.close();
  }
});

test("negative maxPeerSubscriptions rejects no subscription", async () => {
  const xpub = new XPub({ maxPeerSubscriptions: -1 });
  const sub = new Sub();
  try {
    const endpoint = await xpub.bind("tcp://127.0.0.1:0");
    await sub.connect(endpoint);
    await sub.subscribe("a");
    await sub.subscribe("b");
    assert.equal((await xpub.recv()).string(), "\x01a");
    assert.equal((await xpub.recv()).string(), "\x01b");
  } finally {
    sub.close();
    xpub.close();
  }
});

test("PLAIN PUSH/PULL accepts credential allowlist", async () => {
  const pull = new Pull({
    plain: {
      server: true,
      credentials: [
        { username: "alice", password: "secret" },
        { username: "bob", password: "hunter2" },
      ],
    },
  });
  const push = new Push({ plain: { username: "alice", password: "secret" } });
  const bob = new Push({ plain: { username: "bob", password: "hunter2" } });
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    await push.connect(endpoint);
    await bob.connect(endpoint);
    await push.send("hello over plain");
    assert.equal((await pull.recv()).string(), "hello over plain");
    await bob.send("hello from bob");
    assert.equal((await pull.recv()).string(), "hello from bob");
  } finally {
    bob.close();
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
