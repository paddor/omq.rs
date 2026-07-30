# Development

## Test Commands

Run the normal local check suite:

```sh
npm run ci
```

Run the live Node.js interop suite against the sibling `../omq.rs` checkout:

```sh
npm run test:interop
```

Run the headless Firefox browser interop suite against the sibling `../omq.rs`
checkout:

```sh
npx playwright install firefox
npm run test:browser
```

The browser suite starts a Rust WebSocket peer, serves a temporary browser
bundle, and checks native browser `WebSocket` plus WASM LZ4 paths. It covers
`ws://`, `wss://`, `lz4+ws://`, `lz4+wss://`, PLAIN auth, and reconnect.

Playwright writes `test-results/` and `playwright-report/`; both are ignored.
