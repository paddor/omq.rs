const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const path = require("node:path");
const test = require("node:test");
const { promisify } = require("node:util");

const { Context, Pull, Push } = require("../dist");
const execFileAsync = promisify(execFile);

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

test("pending recv does not spin event loop", async () => {
  const pull = new Pull();
  try {
    const started = process.cpuUsage();
    await assert.rejects(pull.recv({ signal: AbortSignal.timeout(250) }), { name: "AbortError" });
    const used = process.cpuUsage(started);
    const cpuMs = (used.user + used.system) / 1000;
    assert.ok(cpuMs < 60, `CPU time ${cpuMs.toFixed(1)}ms during idle recv`);
  } finally {
    pull.close();
  }
});

test("pending send does not block the event loop", async () => {
  const push = new Push();
  const pending = push.send("blocked");
  const winner = await Promise.race([
    pending.then(() => "send"),
    new Promise((resolve) => setTimeout(() => resolve("timer"), 25)),
  ]);
  assert.equal(winner, "timer");
  push.close();
  await assert.rejects(pending, /closed/);
});

test("async recv does not exhaust the libuv worker pool", async () => {
  const dist = path.resolve(__dirname, "../dist");
  const script = `
    const { Context, Pull, Push } = require(${JSON.stringify(dist)});

    (async () => {
      const context = new Context();
      const pulls = [];
      const pushes = [];
      for (let index = 0; index < 5; index++) {
        const pull = new Pull({}, context);
        const push = new Push({}, context);
        const endpoint = \`inproc://node-worker-pool-\${process.pid}-\${index}\`;
        await pull.bind(endpoint);
        await push.connect(endpoint);
        push.waitConnectedSync(1, 2000);
        pulls.push(pull);
        pushes.push(push);
      }

      const pending = pulls.map((pull) => pull.recv());
      await pushes[4].send("ready");
      const message = await Promise.race([
        pending[4],
        new Promise((_, reject) => setTimeout(() => reject(new Error("fifth recv starved")), 500)),
      ]);
      if (message.string() !== "ready") throw new Error("unexpected message");
      process.exit(0);
    })().catch((error) => {
      console.error(error);
      process.exit(1);
    });
  `;

  await execFileAsync(process.execPath, ["-e", script], {
    env: { ...process.env, UV_THREADPOOL_SIZE: "4" },
    timeout: 3000,
  });
});

test("async recv wakes with many TCP sockets", async () => {
  const context = new Context();
  const pulls = [];
  const pushes = [];
  const controllers = [];
  let pending = [];
  try {
    for (let index = 0; index < 12; index++) {
      const pull = new Pull({}, context);
      const push = new Push({}, context);
      const endpoint = await pull.bind("tcp://127.0.0.1:0");
      await push.connect(endpoint);
      push.waitConnectedSync(1, 2000);
      pulls.push(pull);
      pushes.push(push);
    }

    pending = pulls.map((pull) => {
      const controller = new AbortController();
      controllers.push(controller);
      return pull.recv({ signal: controller.signal });
    });
    await pushes[11].send("ready");
    const message = await Promise.race([
      pending[11],
      new Promise((_, reject) => setTimeout(() => reject(new Error("TCP recv starved")), 1000)),
    ]);
    assert.equal(message.string(), "ready");
  } finally {
    for (const controller of controllers) controller.abort();
    await Promise.allSettled(pending);
    for (const push of pushes) push.close();
    for (const pull of pulls) pull.close();
    context.close();
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

test("context close closes live sockets", () => {
  const context = new Context();
  const pull = new Pull({}, context);
  context.close();
  assert.throws(() => pull.tryRecv(), /closed/);
  assert.doesNotThrow(() => pull.close());
});

test("context close rejects pending recv", async () => {
  const context = new Context();
  const pull = new Pull({}, context);
  const pending = pull.recv();
  setTimeout(() => context.close(), 25);
  await assert.rejects(pending, /closed/);
  assert.throws(() => pull.tryRecv(), /closed/);
  assert.doesNotThrow(() => pull.close());
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
