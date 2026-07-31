# Development

## Test Commands

Run the normal local check suite:

```sh
npm run ci
```

Run the live Node.js interop suite against the crates.io `omq-tokio` fixture:

```sh
npm run test:interop
```

Run the headless Firefox browser interop suite against the crates.io `omq-tokio`
fixture:

```sh
npx playwright install firefox
npm run test:browser
```

The browser suite starts a Rust WebSocket peer from the crates.io `omq-tokio`
fixture, serves a temporary browser bundle, and checks native browser
`WebSocket` plus WASM compression paths. It covers `ws://`, `wss://`, PLAIN
auth, reconnect, and connect-before-bind retries. It also covers `lz4+ws://`.
The secure fixture uses a generated self-signed certificate; the Playwright test
first verifies that normal TLS validation rejects it, then lets Firefox accept
that local test certificate.

Playwright writes `test-results/` and `playwright-report/`; both are ignored.

## CI

GitHub Actions runs `npm run ci` on every push and pull request. The interop job
installs Firefox and runs `npm run test:interop` plus `npm run test:browser`
against the crates.io `omq-tokio` fixture.
