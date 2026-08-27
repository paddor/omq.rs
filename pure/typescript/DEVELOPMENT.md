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

## Releasing

### npm

`@zeromq/omq` publishes to npm from `.github/workflows/release-npm.yml`. The
workflow version comes from an `omq-npm-v*` tag or the manual
`workflow_dispatch` input. It runs checks, lint, tests, `deno publish
--dry-run`, packs the tarball, smoke-tests an installed tarball, then publishes
from the `npm` environment.

Publishing uses npm trusted publishing with GitHub Actions OIDC. In npm,
configure a trusted publisher for `@zeromq/omq` before tagging:

```text
GitHub owner: zeromq
GitHub repository: omq.ts
Workflow filename: release-npm.yml
Environment name: npm
Allowed action: npm publish
```

If the package does not exist yet, trusted publishing may need a bootstrap
publish first. Add a publish-capable GitHub Actions secret named `NPM_TOKEN`,
run `.github/workflows/release-npm.yml`, then remove or rotate that token and
configure the trusted publisher before the next CI release.

```sh
git tag -a omq-npm-v0.2.3 -m "omq.ts npm 0.2.3"
git push origin omq-npm-v0.2.3
gh run watch --repo zeromq/omq.ts --exit-status
```
