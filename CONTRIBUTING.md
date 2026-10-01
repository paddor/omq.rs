# Contributing to omq

## Getting started

```sh
git clone https://github.com/paddor/omq.rs.git
cd omq.rs
```

MSRV is Rust 1.93, edition 2024. See [DEVELOPMENT.md](DEVELOPMENT.md) for build and test commands, [RUNNING_BENCHMARKS.md](RUNNING_BENCHMARKS.md) for benchmarks, and [RELEASING.md](RELEASING.md) for fuzz, soak, and release steps.

## Making changes

`omq-tokio` owns the async `Socket` API. `omq-libzmq` and `pyomq`
build on top of it.

New socket types, transports, and mechanisms must be added to
`omq-proto` and wired through `omq-tokio`.
See the Architecture section below for where each piece lives.

Before committing:

```sh
cargo clippy --workspace --all-targets   # pedantic warnings are enabled
cargo fmt                                # rustfmt.toml: edition 2024, max_width 100
```

## Pull requests

- Keep PRs focused. One feature or fix per PR.
- Include tests for new functionality.
- Update the relevant crate's `CHANGELOG.md` under the `[Unreleased]` section.
  Extend only; never modify existing versioned sections.

## Architecture

The codebase uses a three-layer split: a sans-I/O codec (`omq-proto`),
the `omq-tokio` async I/O backend, and user-facing bindings.

- [`doc/architecture.md`](doc/architecture.md) -- diagrams, two-queue model,
  transport and mechanism tables, tokio backend internals

## License

Contributions are licensed under ISC, the same license as the project.
