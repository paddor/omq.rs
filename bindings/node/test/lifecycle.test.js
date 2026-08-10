const assert = require("node:assert/strict");
const test = require("node:test");

const { Pull } = require("../dist");

test("close rejects pending recv", async () => {
  const pull = new Pull();
  const pending = pull.recv();
  setTimeout(() => pull.close(), 25);
  await assert.rejects(pending, /closed/);
});

test("async iterator exits on close", async () => {
  const pull = new Pull();
  const seen = [];
  const done = (async () => {
    for await (const msg of pull) {
      seen.push(msg);
    }
  })();
  setTimeout(() => pull.close(), 25);
  await done;
  assert.deepEqual(seen, []);
});
