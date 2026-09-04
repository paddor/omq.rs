# OMQ.beam Changelog

## [Unreleased]

## [0.2.0] - 2026-09-04

- Add fixed PLAIN server credentials to the Erlang, Elixir, and Gleam APIs.
- Require explicit PLAIN server policy; bare PLAIN server mode fails closed.
- Reject CURVE public keys that do not match the configured secret key.
- Expand generated Erlang API documentation.
- Bundle `omq-tokio` 0.22.0.

## [0.1.0] - 2026-09-01

- First Hex releases of the `omq`, `omq_elixir`, and `omq_gleam` packages.
- Provide all 20 OMQ socket types, PLAIN and CURVE security, compression,
  monitoring, polling, proxies, and context sharing across wrappers.
