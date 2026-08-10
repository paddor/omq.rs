const assert = require("node:assert/strict");
const test = require("node:test");

const { Pub, Sub } = require("../dist");
const { waitFor } = require("./support");

test("PUB/SUB subscription replay over TCP", async () => {
  const pub = new Pub();
  const sub = new Sub();
  try {
    const endpoint = await pub.bind("tcp://127.0.0.1:0");
    await sub.subscribe("topic");
    await sub.connect(endpoint);

    const seen = await waitFor(async () => {
      await pub.send("topic hello");
      const msg = await sub.recv({ signal: AbortSignal.timeout(100) }).catch(() => null);
      return msg?.string() === "topic hello";
    }, 3000);

    assert.equal(seen, true);
  } finally {
    sub.close();
    pub.close();
  }
});
