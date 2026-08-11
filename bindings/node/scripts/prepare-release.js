#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const version = args.find((arg) => !arg.startsWith("--"));
const versionPattern = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

if (!version || !versionPattern.test(version)) {
  throw new Error("usage: node scripts/prepare-release.js VERSION [--dry-run]");
}

const rootDir = path.resolve(__dirname, "..");
const npmDir = path.join(rootDir, "npm");
const metadata = {
  repository: {
    type: "git",
    url: "git+https://github.com/paddor/omq.rs.git",
    directory: "bindings/node",
  },
  bugs: {
    url: "https://github.com/paddor/omq.rs/issues",
  },
  homepage: "https://github.com/paddor/omq.rs/tree/main/bindings/node#readme",
  publishConfig: {
    access: "public",
  },
};
const targets = [
  "linux-x64-gnu",
  "linux-x64-musl",
  "linux-arm64-gnu",
  "linux-arm64-musl",
  "darwin-x64",
  "darwin-arm64",
  "win32-x64-msvc",
  "win32-arm64-msvc",
];

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function writeJson(file, value) {
  if (!dryRun) {
    fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
  }
}

const rootPackagePath = path.join(rootDir, "package.json");
const rootPackage = readJson(rootPackagePath);
delete rootPackage.private;
rootPackage.version = version;
rootPackage.files = ["dist", "doc/charts/bindings.svg"];
Object.assign(rootPackage, metadata);
rootPackage.optionalDependencies = Object.fromEntries(
  targets.map((target) => [`${rootPackage.name}-${target}`, version]),
);
writeJson(rootPackagePath, rootPackage);

for (const target of targets) {
  const packagePath = path.join(npmDir, target, "package.json");
  const platformPackage = readJson(packagePath);
  platformPackage.name = `${rootPackage.name}-${target}`;
  platformPackage.version = version;
  platformPackage.description = rootPackage.description;
  platformPackage.license = rootPackage.license;
  platformPackage.engines = rootPackage.engines;
  Object.assign(platformPackage, metadata);
  writeJson(packagePath, platformPackage);
}

const mode = dryRun ? "would prepare" : "prepared";
console.log(
  `${mode} ${rootPackage.name}@${version} with ${targets.length} platform packages`,
);
