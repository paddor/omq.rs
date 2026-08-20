# Changelog

## [Unreleased]

- Add standalone `omq-rs` Ruby binding with all 20 OMQ socket types.
- Add PLAIN and CURVE sockets, Z85 key generation, CURVE client authentication,
  and pyzmq CURVE interoperability.
- Add LZ4/zstd transports and compression options.
- Add socket lifecycle monitoring with Fiber-aware waits.
- Add receive wake and monitor-fd hooks for backend adapters.
- Support Ractor-owned sockets and cross-Ractor inproc/TCP messaging on Ruby 4.
