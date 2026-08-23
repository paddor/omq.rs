const assert = require("node:assert/strict");
const test = require("node:test");

const { Pull, Push } = require("../dist");
const { ipcEndpoint } = require("./support");

test("linux IPC endpoint uses the abstract namespace", {
  skip: process.platform !== "linux",
}, () => {
  assert.match(ipcEndpoint("node-test"), /^ipc:\/\/@node-test-/);
});

test("bind-before-connect with tcp wildcard endpoint", async () => {
  const pull = new Pull();
  const push = new Push();
  try {
    const endpoint = await pull.bind("tcp://127.0.0.1:0");
    assert.match(endpoint, /^tcp:\/\/127\.0\.0\.1:\d+$/);
    await push.connect(endpoint);
    await push.send("ok");
    assert.equal((await pull.recv()).string(), "ok");
  } finally {
    push.close();
    pull.close();
  }
});
