import { expect, test } from "@playwright/test";
import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

// WebTransport interop between real browsers and the local omq.rs
// `quic://` listener. The fixture's ECDSA certificate is valid for ten
// days, so the page trusts it through W3C `serverCertificateHashes`, not by
// disabling verification.

const execFileAsync = promisify(execFile);
const testDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(testDir, "..");
const fixtureManifest = join(testDir, "fixtures/omq-rs-peer/Cargo.toml");

async function prepareSite() {
  const dir = await mkdtemp(join(tmpdir(), "omq-ts-webtransport-"));
  await writeFile(
    join(dir, "index.html"),
    "<!DOCTYPE html><meta charset=utf-8><title>omq.ts WebTransport</title>",
  );
  await execFileAsync("deno", [
    "bundle",
    "--platform",
    "browser",
    "--config",
    join(repoRoot, "deno.json"),
    join(repoRoot, "src/mod.ts"),
    "-o",
    join(dir, "omq.js"),
  ], { cwd: repoRoot, maxBuffer: 10 * 1024 * 1024 });
  return dir;
}

async function startSite(root) {
  const server = createServer(async (req, res) => {
    const path = new URL(req.url ?? "/", "http://127.0.0.1").pathname;
    const file = path === "/" ? "index.html" : path.replace(/^\/+/, "");
    if (!["index.html", "omq.js"].includes(file)) {
      res.writeHead(404);
      res.end();
      return;
    }
    const body = await readFile(join(root, file));
    res.writeHead(200, {
      "content-type": file.endsWith(".js")
        ? "text/javascript; charset=utf-8"
        : "text/html; charset=utf-8",
    });
    res.end(body);
  });
  await new Promise((ok) => server.listen(0, "127.0.0.1", ok));
  const { port } = server.address();
  return { server, origin: `http://127.0.0.1:${port}` };
}

async function startFixture(origin) {
  const child = spawn("cargo", [
    "run",
    "--quiet",
    "--manifest-path",
    fixtureManifest,
    "--",
    "webtransport-browser",
    "127.0.0.1",
  ], {
    cwd: repoRoot,
    env: { ...process.env, RUST_BACKTRACE: "1", OMQ_TS_BROWSER_ORIGIN: origin },
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => (stderr += chunk.toString()));
  const endpoints = {};
  let certDer = null;
  await new Promise((ready, fail) => {
    const timer = setTimeout(
      () => fail(new Error(`fixture timeout\n${stderr}`)),
      300_000,
    );
    createInterface({ input: child.stdout }).on("line", (line) => {
      const [kind, a, b] = line.split(" ");
      if (kind === "CERT_DER") certDer = Buffer.from(a, "hex");
      if (kind === "ENDPOINT") endpoints[a] = b;
      if (kind === "READY") {
        clearTimeout(timer);
        ready();
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      fail(new Error(`fixture exited ${code}\n${stderr}`));
    });
  });
  const hash = [...createHash("sha256").update(certDer).digest()];
  return { child, endpoints, hash, stderr: () => stderr };
}

async function stop(child) {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  await Promise.race([
    once(child, "exit"),
    new Promise((r) => setTimeout(r, 5000)),
  ]);
}

async function runInPage(page, endpoints, hash) {
  return await page.evaluate(async ({ endpoints, hash }) => {
    const { Message, Push, Req, Sub } = await import("./omq.js");
    const opts = {
      reconnect: false,
      handshakeTimeoutMs: 10_000,
      heartbeatIntervalMs: 200,
      heartbeatTimeoutMs: 5_000,
      webTransport: {
        serverCertificateHashes: [{
          algorithm: "sha-256",
          value: new Uint8Array(hash),
        }],
      },
    };
    const withTimeout = (promise, ms, what) =>
      Promise.race([
        promise,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error(`${what} timeout`)), ms)
        ),
      ]);
    const results = {};
    const sockets = [];
    try {
      const req = new Req(opts);
      sockets.push(req);
      req.connect(endpoints["/rep"]);
      for (let i = 0; i < 50; i++) {
        const reply = await withTimeout(
          req.send(new Message(`echo-${i}`)),
          10_000,
          "req",
        );
        if (reply.string(0) !== `echo-${i}`) {
          throw new Error(`bad echo ${reply.string(0)}`);
        }
      }
      results.echo = 50;

      // omq-tokio REP splits its envelope at the last empty frame, so REQ
      // bodies here avoid empty parts; the codec tests cover empty parts.
      const small = new Uint8Array([7, 7]);
      const big = new Uint8Array(1024 * 1024).map((_, i) => i % 251);
      const multi = await withTimeout(
        req.send(Message.fromParts([new Uint8Array([1]), small, big])),
        20_000,
        "large req",
      );
      results.multipart = multi.parts.length;
      results.middlePart = multi.parts[1]?.byteLength;
      results.largeOk = multi.parts[2]?.every((b, i) => b === i % 251);
      if (results.multipart !== 3) {
        results.partLengths = multi.parts.map((p) => p.byteLength);
      }

      const sub = new Sub(opts);
      sockets.push(sub);
      sub.subscribe("wt.");
      sub.connect(endpoints["/pub"]);
      const first = await withTimeout(sub.recv(), 10_000, "sub recv");
      const second = await withTimeout(sub.recv(), 10_000, "sub recv");
      results.subTopic = first.string(0);
      results.subOrdered = Number(second.string(1)) > Number(first.string(1));

      const push = new Push(opts);
      sockets.push(push);
      push.connect(endpoints["/pull"]);
      for (let i = 0; i < 200; i++) {
        await withTimeout(push.send(new Message(`p${i}`)), 10_000, "push send");
      }
      const state = new Req(opts);
      sockets.push(state);
      state.connect(endpoints["/state"]);
      let pulled = 0;
      for (let attempt = 0; attempt < 100 && pulled < 200; attempt++) {
        const reply = await withTimeout(
          state.send(new Message("count")),
          10_000,
          "state",
        );
        pulled = Number(reply.string(0));
        if (pulled < 200) await new Promise((r) => setTimeout(r, 50));
      }
      results.pulled = pulled;
    } catch (error) {
      results.error = String(error?.stack ?? error);
    } finally {
      for (const socket of sockets) socket.close();
    }
    return results;
  }, { endpoints, hash });
}

test.setTimeout(420_000);

for (const browser of ["chromium", "firefox"]) {
  test(`${browser} WebTransport interop with omq.rs quic listener`, async ({ browserName, page }) => {
    test.skip(browserName !== browser, `runs on ${browser}`);
    const root = await prepareSite();
    const site = await startSite(root);
    const fixture = await startFixture(site.origin);
    try {
      await page.goto(`${site.origin}/`);
      const results = await runInPage(page, fixture.endpoints, fixture.hash);
      expect(results.error, fixture.stderr()).toBeUndefined();
      expect(results).toEqual({
        echo: 50,
        multipart: 3,
        middlePart: 2,
        largeOk: true,
        subTopic: "wt.topic",
        subOrdered: true,
        pulled: 200,
      });
    } finally {
      await stop(fixture.child);
      site.server.close();
      await rm(root, { recursive: true, force: true });
    }
  });
}
