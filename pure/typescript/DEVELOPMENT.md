# Development

## Test Commands

Run the normal local check suite:

```sh
npm run ci
```

Build the npm package output:

```sh
npm run build:npm
```

Run the live Node.js interop suite against the workspace `omq-tokio` fixture:

```sh
npm run test:interop
```

Run the headless browser interop suites against the local `omq-tokio` fixture:

```sh
npx playwright install firefox chromium
npm run test:browser
```

`test/webtransport-browser.playwright.mjs` starts the fixture's
`webtransport-browser` mode, which binds `quic://` REP, PUB, PULL, and state
REP listeners with a ten-day ECDSA certificate. Chromium and Firefox trust it
through `serverCertificateHashes`; nothing disables certificate
verification. The liveness test freezes the fixture with `SIGSTOP`.

The browser suite starts a Rust WebSocket peer from the workspace `omq-tokio`
fixture, serves a temporary browser bundle, and checks native browser
`WebSocket` plus WASM compression paths. It covers `ws://`, `wss://`, PLAIN
auth, reconnect, and connect-before-bind retries. It also covers `lz4+ws://`.
The secure fixture uses a generated self-signed certificate; the Playwright test
first verifies that normal TLS validation rejects it, then lets Firefox accept
that local test certificate.

Playwright writes `test-results/` and `playwright-report/`; both are ignored.

## CI

The omq.rs `ci.yml` runs `npm run ci` and both interop suites when
`pure/typescript/`, `omq-proto/`, `omq-tokio/`, or `vendor/` change. The
fixture builds against the workspace, so Rust transport changes are tested
before release. The interop job installs Firefox and Chromium.

## Releasing

### npm

`@paddor/omq` publishes to npm from `.github/workflows/release-omq-ts-npm.yml`
in omq.rs. The workflow version comes from an `omq-ts-v*` tag or the manual
`workflow_dispatch` input. It runs checks, lint, tests, `deno publish
--dry-run`, packs the tarball, smoke-tests an installed tarball, then publishes
from the `npm` environment.

Publishing uses npm trusted publishing with GitHub Actions OIDC. In npm,
configure a trusted publisher for `@paddor/omq` before tagging:

```text
GitHub owner: paddor
GitHub repository: omq.rs
Workflow filename: release-omq-ts-npm.yml
Environment name: npm
Allowed action: npm publish
```

If the package does not exist yet, trusted publishing may need a bootstrap
publish first. Add a publish-capable GitHub Actions secret named `NPM_TOKEN`,
run `.github/workflows/release-omq-ts-npm.yml`, then remove or rotate that
token and configure the trusted publisher before the next CI release.

```sh
git tag -a omq-ts-v0.2.3 -m "OMQ.ts 0.2.3"
git push origin omq-ts-v0.2.3
gh run watch --repo paddor/omq.rs --exit-status
```

### JSR

`.github/workflows/release-omq-ts-jsr.yml` runs `deno publish` on `main`
pushes that touch `pure/typescript/` and on `workflow_dispatch`. JSR publishes
through GitHub Actions OIDC, so the `@paddor/omq` JSR package must be linked to
the `paddor/omq.rs` repository.
