import { expect, test } from "@playwright/test";
import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { copyFile, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const testDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(testDir, "..");
const browserDir = join(testDir, "browser");
const fixtureManifest = join(testDir, "fixtures/omq-rs-peer/Cargo.toml");
const wasmPath = join(
  repoRoot,
  "node_modules/@paddor/lz4rip/src/pkg/lz4rip.wasm",
);

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

async function prepareStaticSite() {
  const dir = await mkdtemp(join(tmpdir(), "omq-ts-browser-"));
  await copyFile(join(browserDir, "index.html"), join(dir, "index.html"));
  await copyFile(join(browserDir, "app.js"), join(dir, "app.js"));
  await mkdir(join(dir, "pkg"), { recursive: true });
  await copyFile(wasmPath, join(dir, "pkg/lz4rip.wasm"));
  await execFileAsync("deno", [
    "bundle",
    "--platform",
    "browser",
    "--config",
    join(repoRoot, "deno.json"),
    join(browserDir, "browser_entry.ts"),
    "-o",
    join(dir, "omq.js"),
  ], {
    cwd: repoRoot,
    maxBuffer: 10 * 1024 * 1024,
  });
  return dir;
}

function contentType(path) {
  if (path.endsWith(".html")) return "text/html; charset=utf-8";
  if (path.endsWith(".js")) return "text/javascript; charset=utf-8";
  if (path.endsWith(".wasm")) return "application/wasm";
  return "application/octet-stream";
}

async function startStaticServer(root) {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      const path = decodeURIComponent(url.pathname);
      const rel = path === "/" ? "index.html" : path.replace(/^\/+/, "");
      if (rel.includes("..") || rel.includes("\\")) {
        res.writeHead(400);
        res.end("bad path");
        return;
      }
      const file = join(root, rel);
      const body = await readFile(file);
      res.writeHead(200, {
        "content-length": String(body.byteLength),
        "content-type": contentType(file),
      });
      res.end(body);
    } catch {
      res.writeHead(404);
      res.end("not found");
    }
  });

  await new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", rejectListen);
      resolveListen();
    });
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("static server did not bind TCP port");
  }
  return { server, url: `http://127.0.0.1:${address.port}/` };
}

async function closeServer(server) {
  await new Promise((resolveClose, rejectClose) => {
    server.close((error) => {
      if (error) rejectClose(error);
      else resolveClose();
    });
  });
}

async function listenProbe(port) {
  const server = createServer();
  await new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", rejectListen);
      resolveListen();
    });
  });
  return server;
}

async function findFreePortRange(count) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const base = 20_000 + Math.floor(Math.random() * 20_000);
    const probes = [];
    try {
      for (let offset = 0; offset < count; offset++) {
        probes.push(await listenProbe(base + offset));
      }
      return base;
    } catch {
      continue;
    } finally {
      await Promise.allSettled(probes.map((server) => closeServer(server)));
    }
  }
  throw new Error("could not find free browser interop port range");
}

function startBrowserPeer(basePort) {
  const child = spawn("cargo", [
    "run",
    "--quiet",
    "--manifest-path",
    fixtureManifest,
    "--",
    "browser-bind",
    String(basePort),
    "127.0.0.1",
  ], {
    cwd: repoRoot,
    env: { ...process.env, RUST_BACKTRACE: "1" },
  });

  let stdout = "";
  let stderr = "";
  let resolved = false;
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  return new Promise((resolvePeer, rejectPeer) => {
    const lines = createInterface({ input: child.stdout });
    const timer = setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGTERM");
      rejectPeer(new Error(`omq.rs browser peer timed out\n${stderr}`));
    }, 120_000);

    lines.on("line", (line) => {
      if (line !== "BROWSER_READY") return;
      resolved = true;
      clearTimeout(timer);
      resolvePeer({
        child,
        stdout: () => stdout,
        stderr: () => stderr,
      });
    });

    child.on("error", (error) => {
      clearTimeout(timer);
      rejectPeer(error);
    });
    child.on("exit", (code, signal) => {
      if (resolved) return;
      clearTimeout(timer);
      rejectPeer(
        new Error(
          `omq.rs browser peer exited before ready code=${code} signal=${signal}\n${stderr}`,
        ),
      );
    });
  });
}

async function stopPeer(peer) {
  if (peer.child.exitCode !== null) return;
  peer.child.kill("SIGTERM");
  await Promise.race([once(peer.child, "exit"), delay(5_000)]);
  if (peer.child.exitCode === null) {
    peer.child.kill("SIGKILL");
    await Promise.race([once(peer.child, "exit"), delay(5_000)]);
  }
}

test.setTimeout(180_000);

test("Firefox browser interop with omq.rs WebSocket peer", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "firefox", "browser interop runs on Firefox");
  const consoleLines = [];
  let serverDir;
  let staticServer;
  let peer;

  page.on("console", (msg) => {
    consoleLines.push(`[${msg.type()}] ${msg.text()}`);
  });
  page.on("pageerror", (error) => {
    consoleLines.push(`[pageerror] ${error.message}`);
  });

  try {
    serverDir = await prepareStaticSite();
    staticServer = await startStaticServer(serverDir);
    const basePort = await findFreePortRange(17);
    peer = await startBrowserPeer(basePort);

    await page.goto(staticServer.url, { waitUntil: "domcontentloaded" });
    await page.fill("#host", "127.0.0.1");
    await page.fill("#base-port", String(basePort));
    await page.click("#run");

    await expect(page.locator("body")).toHaveAttribute(
      "data-status",
      "passed",
      { timeout: 60_000 },
    );
    const expected = await page.locator("body").getAttribute("data-expected");
    await expect(page.locator("#log")).toContainText(
      `${expected}/${expected} passed`,
    );
    await expect(page.locator(".test.fail")).toHaveCount(0);
  } finally {
    const pageLog = await page.locator("#log").textContent({ timeout: 1000 })
      .catch(() => "");
    await testInfo.attach("browser-page-log", {
      body: pageLog ?? "",
      contentType: "text/plain",
    });
    await testInfo.attach("browser-console-log", {
      body: consoleLines.join("\n"),
      contentType: "text/plain",
    });
    if (peer) {
      await testInfo.attach("omq-rs-browser-peer-stdout", {
        body: peer.stdout(),
        contentType: "text/plain",
      });
      await testInfo.attach("omq-rs-browser-peer-stderr", {
        body: peer.stderr(),
        contentType: "text/plain",
      });
      await stopPeer(peer);
    }
    if (staticServer) await closeServer(staticServer.server);
    if (serverDir) await rm(serverDir, { recursive: true, force: true });
  }
});
