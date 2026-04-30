//! Pure-Rust ZeroMQ. Wire-compatible with libzmq.
//!
//! `omq` is a thin facade that re-exports a backend chosen at build
//! time. The default `compio-backend` feature pulls in
//! [`omq-compio`](https://crates.io/crates/omq-compio), a
//! single-thread io_uring (Linux), IOCP (Windows), or kqueue (macOS)
//! runtime; the alternative
//! `tokio-backend` feature swaps in
//! [`omq-tokio`](https://crates.io/crates/omq-tokio), a multi-thread
//! mio/tokio runtime. The two are mutually exclusive — enabling
//! both is a compile error. The public `Socket` / `Endpoint` /
//! `Options` / `Message` API is identical either way, verified in
//! lockstep by the `coverage_matrix` and `interop_compio` test
//! suites in the repository.
//!
//! # Example
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use omq::{Endpoint, Message, Options, Socket, SocketType};
//!
//! let pull = Socket::new(SocketType::Pull, Options::default());
//! pull.bind("tcp://127.0.0.1:5555".parse()?).await?;
//!
//! let push = Socket::new(SocketType::Push, Options::default());
//! push.connect("tcp://127.0.0.1:5555".parse()?).await?;
//! push.send(Message::single("hi")).await?;
//!
//! let m = pull.recv().await?;
//! assert_eq!(m.parts()[0].coalesce(), &b"hi"[..]);
//! # Ok(()) }
//! ```
//!
//! # Picking a backend
//!
//! ```toml
//! # Default: compio (io_uring on Linux, IOCP on Windows, kqueue on
//! # macOS). Single-thread by design — instantiate one runtime per
//! # core for thread-per-core.
//! [dependencies]
//! omq = "0"
//!
//! # Alternative: tokio (multi-thread by default).
//! [dependencies]
//! omq = { version = "0", default-features = false, features = ["tokio-backend"] }
//! ```
//!
//! # Cargo features
//!
//! All optional, all opt-in:
//!
//! - `compio-backend` — (default) compio runtime backend.
//! - `tokio-backend` — tokio runtime backend (mutually exclusive with compio).
//! - `curve` — CURVE encrypted-handshake mechanism (RFC 26).
//! - `blake3zmq` — omq-native AEAD (X25519 + BLAKE3 + ChaCha20).
//! - `lz4` — `lz4+tcp://` compression transport.
//! - `zstd` — `zstd+tcp://` compression transport.
//! - `priority` — strict per-pipe priority on `Socket::connect_with`.
//!
//! See [BENCHMARKS.md](https://github.com/paddor/omq.rs/blob/main/BENCHMARKS.md)
//! for throughput / latency tables.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(feature = "compio-backend", feature = "tokio-backend"))]
compile_error!(
    "omq features `compio-backend` and `tokio-backend` are mutually \
     exclusive; pick exactly one"
);

#[cfg(not(any(feature = "compio-backend", feature = "tokio-backend")))]
compile_error!(
    "omq needs exactly one backend: enable `compio-backend` (default) \
     or `tokio-backend`"
);

#[cfg(feature = "compio-backend")]
pub use omq_compio::*;

#[cfg(feature = "tokio-backend")]
pub use omq_tokio::*;

/// Name of the active backend. Useful for runtime version banners.
pub const BACKEND: &str = if cfg!(feature = "compio-backend") {
    "compio"
} else {
    "tokio"
};
