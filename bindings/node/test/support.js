const net = require("node:net");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");

async function freeTcpEndpoint() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return `tcp://127.0.0.1:${address.port}`;
}

function inprocEndpoint(prefix = "node") {
  return `inproc://${prefix}-${process.pid}-${randomUUID()}`;
}

function ipcEndpoint(prefix = "node") {
  return `ipc://${join(tmpdir(), `${prefix}-${process.pid}-${randomUUID()}.sock`)}`;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(fn, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await delay(10);
  }
  if (lastError) throw lastError;
  throw new Error("condition timed out");
}

module.exports = {
  delay,
  freeTcpEndpoint,
  inprocEndpoint,
  ipcEndpoint,
  waitFor,
};
