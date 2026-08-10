const assert = require("node:assert/strict");
const test = require("node:test");

const { Dealer, Pub, Pull, Push, Rep, Req, Router, Sub } = require("../dist");
const { delay } = require("./support");

const zeromq = loadZeromq();
const interopTest = zeromq === null ? test.skip : test;

interopTest("OMQ PUSH sends to zeromq.js PULL", async () => {
  const pull = new zeromq.Pull();
  const push = new Push();
  try {
    const endpoint = await push.bind("tcp://127.0.0.1:0");
    pull.connect(endpoint);
    push.waitConnectedSync(1, 5000);
    await push.send("from omq");
    const [message] = await receiveZmq(pull, "zeromq PULL receive");
    assert.equal(message.toString(), "from omq");
  } finally {
    push.close();
    pull.close();
  }
});

interopTest("zeromq.js PUSH sends to OMQ PULL", async () => {
  const pull = new Pull();
  const push = new zeromq.Push();
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    push.connect(endpoint);
    await push.send("from zeromq");
    const message = await pull.recv({ signal: AbortSignal.timeout(2000) });
    assert.equal(message.string(), "from zeromq");
  } finally {
    push.close();
    pull.close();
  }
});

interopTest("OMQ REQ talks to zeromq.js REP", async () => {
  const rep = new zeromq.Reply();
  const req = new Req();
  try {
    await rep.bind("tcp://127.0.0.1:0");
    const endpoint = rep.lastEndpoint;
    await req.connect(endpoint);
    req.waitConnectedSync(1, 5000);
    await req.send("ping");
    const [request] = await receiveZmq(rep, "zeromq REP receive");
    assert.equal(request.toString(), "ping");
    await rep.send("pong");
    assert.equal((await req.recv({ signal: AbortSignal.timeout(2000) })).string(), "pong");
  } finally {
    req.close();
    rep.close();
  }
});

interopTest("zeromq.js REQ talks to OMQ REP", async () => {
  const rep = new Rep();
  const req = new zeromq.Request();
  try {
    const endpoint = await rep.bind("tcp://127.0.0.1:0");
    req.connect(endpoint);
    await req.send("ping");
    assert.equal((await rep.recv({ signal: AbortSignal.timeout(2000) })).string(), "ping");
    await rep.send("pong");
    const [reply] = await receiveZmq(req, "zeromq REQ receive");
    assert.equal(reply.toString(), "pong");
  } finally {
    req.close();
    rep.close();
  }
});

interopTest("OMQ PUB sends subscribed messages to zeromq.js SUB", async () => {
  const pub = new Pub();
  const sub = new zeromq.Subscriber();
  try {
    const endpoint = await pub.bind("tcp://127.0.0.1:0");
    sub.subscribe("topic");
    sub.connect(endpoint);
    const [message] = await receiveWhileSending(
      receiveZmq(sub, "zeromq SUB receive", 3000),
      () => pub.send("topic from omq"),
    );
    assert.equal(message.toString(), "topic from omq");
  } finally {
    sub.close();
    pub.close();
  }
});

interopTest("zeromq.js PUB sends subscribed messages to OMQ SUB", async () => {
  const pub = new zeromq.Publisher();
  const sub = new Sub();
  try {
    await pub.bind("tcp://127.0.0.1:0");
    const endpoint = pub.lastEndpoint;
    await sub.subscribe("topic");
    await sub.connect(endpoint);
    const message = await receiveWhileSending(
      sub.recv({ signal: AbortSignal.timeout(3000) }),
      () => pub.send("topic from zeromq"),
    );
    assert.equal(message.string(), "topic from zeromq");
  } finally {
    sub.close();
    pub.close();
  }
});

interopTest("OMQ DEALER talks to zeromq.js ROUTER with identity", async () => {
  const router = new zeromq.Router();
  const dealer = new Dealer({ identity: "omq-dealer" });
  try {
    await router.bind("tcp://127.0.0.1:0");
    const endpoint = router.lastEndpoint;
    await dealer.connect(endpoint);
    dealer.waitConnectedSync(1, 5000);
    await dealer.send("hello");
    const [identity, request] = await receiveZmq(router, "zeromq ROUTER receive");
    assert.equal(identity.toString(), "omq-dealer");
    assert.equal(request.toString(), "hello");
    await router.send([identity, "world"]);
    assert.equal((await dealer.recv({ signal: AbortSignal.timeout(2000) })).string(), "world");
  } finally {
    dealer.close();
    router.close();
  }
});

interopTest("zeromq.js DEALER talks to OMQ ROUTER with identity", async () => {
  const router = new Router();
  const dealer = new zeromq.Dealer({ routingId: "zmq-dealer" });
  try {
    const endpoint = await router.bind("tcp://127.0.0.1:0");
    dealer.connect(endpoint);
    await dealer.send("hello");
    const request = await router.recv({ signal: AbortSignal.timeout(2000) });
    assert.equal(request.string(0), "zmq-dealer");
    assert.equal(request.string(1), "hello");
    await router.send([request.part(0), Buffer.from("world")]);
    const [reply] = await receiveZmq(dealer, "zeromq DEALER receive");
    assert.equal(reply.toString(), "world");
  } finally {
    dealer.close();
    router.close();
  }
});

function loadZeromq() {
  try {
    return require("zeromq");
  } catch {
    return null;
  }
}

function receiveZmq(socket, label, timeoutMs = 2000) {
  const received = socket.receive();
  received.catch(() => {});
  return withTimeout(received, timeoutMs, label);
}

async function receiveWhileSending(receive, send, intervalMs = 10) {
  let stopped = false;
  const sender = (async () => {
    while (!stopped) {
      await send();
      await delay(intervalMs);
    }
  })();
  try {
    return await receive;
  } finally {
    stopped = true;
    await sender.catch(() => {});
  }
}

function withTimeout(promise, timeoutMs, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
