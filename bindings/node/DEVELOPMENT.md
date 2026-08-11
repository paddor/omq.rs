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
OMQ_NODE_BENCH_THROUGHPUT_DURATION_SECS=3 npm run bench
OMQ_NODE_BENCH_WARMUP_DURATION_SECS=1 npm run bench
OMQ_NODE_BENCH_LATENCY_MESSAGES=100000 npm run bench
OMQ_NODE_BENCH_NO_CHART=1 npm run bench
```

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

Hardware subtitle comes from `bindings/node/.chart_hw` when present:

```text
prefix=Ryzen 9 9950X
postfix=turbo off, performance governor
```

`bindings/node/.chart_hw` is local-only and gitignored.

## Static Checks

```sh
npm run build:ts
node --check scripts/update_perf.js
git diff --check
```
