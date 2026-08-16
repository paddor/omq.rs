# Development

## Setup

Requires Node 24.11 or newer and Rust toolchain for native build.

```sh
cd bindings/node
npm install
npm run build
```

`npm run build` runs the release native addon build and TypeScript declaration
emit. For a debug native addon:

```sh
npm run build:debug
```

## Tests

Run unit, transport, lifecycle, option, socket type, and `zeromq.js` interop
tests:

```sh
npm test
```

Run one test file:

```sh
node --test test/interop.test.js
```

Run soak tests:

```sh
npm run soak
OMQ_NODE_SOAK_DURATION_SECS=60 npm run soak
```

Default soak duration is 15 minutes. The script runs Node with `--expose-gc`
and serial test concurrency.

## Benchmarks

Benchmarks require a built `dist/index.js` and `omq_node.node`.

```sh
npm run bench
```

Results append to:

```text
~/.cache/omq.node/bindings.jsonl
```

Useful controls:

```sh
OMQ_NODE_BENCH_QUICK=1 npm run bench
OMQ_NODE_BENCH_IMPLS=omq-node npm run bench
OMQ_NODE_BENCH_IMPLS=zeromq.js npm run bench
OMQ_NODE_BENCH_SIZES=8,64,1k,32k npm run bench
OMQ_NODE_BENCH_ROUNDS=1 npm run bench
OMQ_NODE_BENCH_THROUGHPUT_DURATION_SECS=2.5 npm run bench
OMQ_NODE_BENCH_WARMUP_DURATION_SECS=0.5 npm run bench
OMQ_NODE_BENCH_LATENCY_DURATION_SECS=1.5 npm run bench
OMQ_NODE_BENCH_NO_CHART=1 npm run bench
```

Defaults are 3 measured rounds per implementation and size. Throughput keeps
the median msg/s round over 2.5 second windows. Latency keeps the median p50
round over 1.5 second windows and only measures sizes up to 4 KiB. Each round
uses a 0.5 second warmup window.

Do not run benchmarks or profilers in parallel with other CPU-heavy work.

## Chart

Regenerate chart from existing JSONL:

```sh
npm run chart
```

Chart output:

```text
bindings/node/doc/charts/bindings.svg
```

Hardware subtitle comes from the repo-root `.chart_hw`:

```text
label=Ryzen 9 9950X, 16 cores, turbo off, performance governor
```

`.chart_hw` is local-only and gitignored.

## Static Checks

```sh
npm run build:ts
node --check scripts/omq-node-bench.js
node --check scripts/prepare-release.js
git diff --check
```

## CI Release Packaging

npm packages are published only by `.github/workflows/release-node.yml`.
Do not run `npm publish` locally.

Trigger CI release with a tag:

```sh
git tag omq-node-v0.1.2
```

Or run `.github/workflows/release-node.yml` with `version=0.1.2`.

Native jobs build platform addons named `omq_node.<platform>.node`.
Package job copies them into `npm/<platform>/`, writes root
`optionalDependencies`, packs platform tarballs first, then packs the root
package. Publish job publishes platform tarballs before the root tarball.

Local platform package dry run for the current host:

```sh
npm run build:ts
npm run build:native:platform -- --target x86_64-unknown-linux-gnu
cp omq_node.linux-x64-gnu.node npm/linux-x64-gnu/
npm pack ./npm/linux-x64-gnu --dry-run
```

Use `npm run release:prepare -- 0.1.2 --dry-run` to validate metadata without
rewriting manifests.

`npm run artifacts` expects every configured platform addon under `artifacts/`.
Use it only with the full release artifact set.
