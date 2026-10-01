# Releasing

Fuzz, soak, and release steps for omq.rs. Build, test, and CI notes live in
[DEVELOPMENT.md](DEVELOPMENT.md).

## Fuzz Tests

The hand-rolled fuzz suites are off by default. Enable with the `fuzz`
feature. Set `OMQ_FUZZ_ITERS=<n>` and `OMQ_FUZZ_SEED=<u64>` for long or
reproducible runs.

`OMQ_FUZZ_ITERS` counts a different unit per target, so one value
cannot serve both. A `fuzz_parsers` iteration is a buffer parse
(default 10M, ~7s per 1M); a `fuzz_socket_actions` iteration drives a
live socket (default 200, ~90ms each). Setting it globally silently
turns the socket suite into an hours-long run.

```sh
cargo test -p omq-tokio --features fuzz
OMQ_FUZZ_ITERS=500000000 cargo test -p omq-tokio --features fuzz --release -- --nocapture
```

## Soak Tests

Soak tests cover peer churn, reconnect storms, reconnect all types,
PUB/SUB churn, ROUTER/DEALER churn, HWM reconnect, WebSocket
throughput, WebSocket reconnect, large-message throughput, compression
with lz4/zstd, PLAIN, CURVE, multi-socket, inproc cross-thread,
cancel safety, and driver control responsiveness under stalled writes.

Set duration with `OMQ_SOAK_DURATION_SECS` (default 600s). `Context::new()`
uses one dedicated background IO thread. `OMQ_IO_THREADS=N` selects N
dedicated IO threads; `Context::current()` is the explicit current-thread
Tokio integration mode.

```sh
FEATURES="soak lz4 zstd plain curve ws"
cargo test -p omq-tokio --features "$FEATURES" --release --no-run
OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-tokio \
  --features "$FEATURES" --release --test omq_soak_peer_churn -- --nocapture
```

The explicit QUIC soaks run for 10, 30, and 60 minutes by default. They
check payload integrity, reconnect after listener replacement, and delivery
to a live subscriber while another subscriber remains backed up. Run them
serially; unset `OMQ_SOAK_DURATION_SECS` for the full durations:

```sh
env -u OMQ_SOAK_DURATION_SECS cargo test -p omq-tokio \
  --features 'soak quic' --release --test omq_soak_quic_long \
  -- --ignored --test-threads=1 --nocapture
```

For a short harness check, set `OMQ_SOAK_DURATION_SECS=5`. These tests are
ignored by default so the ordinary soak suite does not gain 100 minutes.

### pyomq Soak Tests

```sh
cd bindings/pyomq
maturin develop --release
OMQ_SOAK_DURATION_SECS=120 python3 -m pytest tests/soak/ -v --tb=short
```

### OMQ.go Soak Tests

```sh
bindings/go/scripts/soak.sh
OMQ_GO_SOAK_DURATIONS="300 600 1800 3600" OMQ_GO_SOAK_WORKERS=12 \
  bindings/go/scripts/soak.sh
```


## Automation

`release-plz` runs on every push to `main`
(`.github/workflows/release-plz.yml`). It opens or updates a release PR,
creates annotated tags after merge, publishes to crates.io, and creates
GitHub releases. Configuration lives in `release-plz.toml`.

## Steps

1. **Review the release-plz PR.** Verify semver bumps.

2. **Curate changelogs.** For each bumped crate, insert a new
   `## [x.y.z]` section below `## [Unreleased]`. Never modify existing
   versioned sections.

3. **Update zguide examples.** Bump `omq-tokio` versions in
   `examples/zguide/*/Cargo.toml`.

4. **Merge the release PR.** release-plz tags and publishes to
   crates.io automatically.

5. **Bindings** if changed: prepare the binding release on a PR, merge it,
   then tag from the merged `main` commit. Do not tag before the release PR is
   merged.

## Binding Releases

Push binding tags one at a time with `push.followTags` disabled. GitHub does
not create push events when one push creates more than three tags, and a local
`push.followTags=true` setting can silently add other annotated tags.

For workflow-backed releases, find and watch the run created for the tag:

```sh
gh run list --repo paddor/omq.rs --workflow WORKFLOW --branch TAG --limit 1
gh run watch RUN_ID --repo paddor/omq.rs --exit-status
```

`pyomq` publishes to PyPI from `.github/workflows/release-pyomq.yml`.
Prepare a release by bumping `bindings/pyomq/Cargo.toml` and
`bindings/pyomq/pyproject.toml`, running `cargo update -p pyomq` inside
`bindings/pyomq`, and adding a `bindings/pyomq/CHANGELOG.md` entry. After
the PR is merged, push a tag:

```sh
git tag -a pyomq-v0.21.0 -m "pyomq 0.21.0"
git -c push.followTags=false push origin pyomq-v0.21.0
```

`omq-rs` publishes to RubyGems from
`.github/workflows/release-rubygems.yml`. Prepare a release by bumping
`bindings/ruby/lib/omq/rs/version.rb`, updating
`bindings/ruby/CHANGELOG.md`, and running `cargo update -p omq_rs_native`
inside `bindings/ruby`. Publish the required `omq-proto` and `omq-tokio`
versions before pushing the Ruby tag. After the PR is merged, push a tag:

```sh
git tag -a ruby-v0.2.0 -m "omq-rs 0.2.0"
git -c push.followTags=false push origin ruby-v0.2.0
```

`OMQ.java` publishes to Maven Central from
`.github/workflows/release-java.yml`. Prepare a release by adding a
`bindings/java/CHANGELOG.md` entry. The workflow version comes from the tag
or manual `workflow_dispatch` input; `pom.xml` normally stays at
`${revision}`. Required repository secrets are `CENTRAL_USERNAME`,
`CENTRAL_PASSWORD`, `MAVEN_GPG_PRIVATE_KEY`, and `MAVEN_GPG_PASSPHRASE`.
After the PR is merged, push a tag:

```sh
git tag -a omq-java-v0.3.4 -m "OMQ.java 0.3.4"
git -c push.followTags=false push origin omq-java-v0.3.4
```

The workflow publishes with `central.autoPublish=true` and waits until Central
reports the deployment as published.

`OMQ.go` is a Go subdirectory module. It has no registry account or upload
step; the Git tag is the release. After the PR is merged, push a subdirectory
module tag from `main`:

```sh
git tag -a bindings/go/v0.1.2 -m "OMQ.go 0.1.2"
git -c push.followTags=false push origin bindings/go/v0.1.2
go list -m github.com/paddor/omq.rs/bindings/go@v0.1.2
```

The public Go module proxy discovers the version from that tag.

`OMQ.node` publishes to npm from `.github/workflows/release-node.yml`.
Prepare a release by bumping `bindings/node/Cargo.toml`, its local package in
`bindings/node/Cargo.lock`, the root `package.json` version, both root
`package-lock.json` package versions, every `bindings/node/npm/*/package.json`
version, and the changelog. Do not check root `optionalDependencies` into
`package.json` or `package-lock.json`.
`scripts/prepare-release.js` adds them after `npm ci` and after every platform
package has been built. Validate the prepared tree with:

```sh
(cd bindings/node && npm ci)
(cd bindings/node && npm run release:prepare -- VERSION --dry-run)
```

The workflow version comes from the tag or manual `workflow_dispatch` input.
It builds all platform native addons, packs platform packages, smoke-tests
host tarballs, publishes platform packages first, then publishes the root
package.

Publishing uses npm trusted publishing with GitHub Actions OIDC. No npm token
secret is required by the workflow. In npm, configure a trusted publisher for
each package before tagging:

```text
GitHub owner: paddor
GitHub repository: omq.rs
Workflow filename: release-node.yml
Environment name: npm
Allowed action: npm publish
```

Configure these packages:

```text
@paddor/omq-node
@paddor/omq-node-darwin-arm64
@paddor/omq-node-darwin-x64
@paddor/omq-node-linux-arm64-gnu
@paddor/omq-node-linux-arm64-musl
@paddor/omq-node-linux-x64-gnu
@paddor/omq-node-linux-x64-musl
@paddor/omq-node-win32-arm64-msvc
@paddor/omq-node-win32-x64-msvc
```

If the packages do not exist yet, npm trusted publishing cannot be configured
for them. Bootstrap the first publish with a user-approved manual publish or a
publish-capable npm token, then add the trusted publishers before the next CI
release. After the PR is merged and trusted publishers are configured, push a
tag:

```sh
git tag -a omq-node-v0.2.1 -m "OMQ.node 0.2.1"
git -c push.followTags=false push origin omq-node-v0.2.1
```

`OMQ.Net` publishes to NuGet from `.github/workflows/release-dotnet.yml`.
Prepare a release by bumping `bindings/dotnet/Omq.Net.csproj` and adding a
`bindings/dotnet/CHANGELOG.md` entry. After the PR is merged, push a tag:

```sh
git tag -a omq-dotnet-v0.2.0 -m "OMQ.Net 0.2.0"
git -c push.followTags=false push origin omq-dotnet-v0.2.0
```

`OMQ.lua` publishes to LuaRocks. Prepare a release by adding a versioned
rockspec and `bindings/lua/CHANGELOG.md` entry. After the PR is merged, tag the
same commit before uploading the rockspec:

```sh
git tag -a omq-lua-v0.2.2 -m "OMQ.lua 0.2.2"
git -c push.followTags=false push origin omq-lua-v0.2.2
luarocks upload bindings/lua/omq-0.2.2-1.rockspec
```

`OMQ.beam` publishes the `omq`, `omq_elixir`, and `omq_gleam` packages to Hex.
Prepare them by updating all three package versions, the native crate version
and `omq-tokio` dependency, and `bindings/beam/CHANGELOG.md`. Publish `omq`
first, then the Elixir and Gleam wrappers, following
`bindings/beam/DEVELOPMENT.md`.

`OMQ.zig` is released from the repository tag. Prepare a release by updating
`bindings/zig/build.zig.zon` and `bindings/zig/CHANGELOG.md`. After the PR is
merged, push a tag:

```sh
git tag -a omq-zig-v0.1.0 -m "OMQ.zig 0.1.0"
git -c push.followTags=false push origin omq-zig-v0.1.0
```

## Crates To Check

`omq-proto`, `omq-tokio`, `omq-libzmq`, `pyomq`.
