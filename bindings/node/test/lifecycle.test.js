const assert = require("node:assert/strict");
const test = require("node:test");

const { Context, Pull } = require("../dist");

test("close rejects pending recv", async () => {
  const pull = new Pull();
  const pending = pull.recv();
  setTimeout(() => pull.close(), 25);
  await assert.rejects(pending, /closed/);
});

test("abort signal rejects pending recv", async () => {
  const pull = new Pull();
  try {
    await assert.rejects(pull.recv({ signal: AbortSignal.timeout(25) }), { name: "AbortError" });
  } finally {
    pull.close();
  }
});

test("dispose hooks close resources", () => {
  const context = new Context();
  const pull = new Pull({}, context);
  pull[Symbol.dispose]();
  assert.throws(() => pull.tryRecv(), /closed/);
  context[Symbol.dispose]();
  assert.throws(() => context.shareKey(), /closed/);
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
