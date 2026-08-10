const assert = require("node:assert/strict");
const test = require("node:test");

const { Rep, Req } = require("../dist");

test("REQ/REP over TCP", async () => {
  const rep = new Rep();
  const req = new Req();
  try {
    const endpoint = await rep.bind("tcp://127.0.0.1:0");
    await req.connect(endpoint);
    await req.send("ping");
    assert.equal((await rep.recv()).string(), "ping");
    await rep.send("pong");
    assert.equal((await req.recv()).string(), "pong");
  } finally {
    req.close();
    rep.close();
  }
});
