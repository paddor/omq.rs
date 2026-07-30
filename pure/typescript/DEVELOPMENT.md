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
`ws://`, `wss://`, `lz4+ws://`, `lz4+wss://`, PLAIN auth, reconnect, and
connect-before-bind retries. The WSS fixture uses a generated self-signed
certificate; the Playwright test first verifies that normal TLS validation
rejects it, then lets Firefox accept that local test certificate.

Playwright writes `test-results/` and `playwright-report/`; both are ignored.

## CI

GitHub Actions runs `npm run ci` on every push and pull request. The interop job
also clones `paddor/omq.rs` as the sibling `../omq.rs`, installs Firefox, and
runs `npm run test:interop` plus `npm run test:browser`.
