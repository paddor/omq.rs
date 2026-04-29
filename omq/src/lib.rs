//! omq: pure Rust ZMTP. Facade over a backend chosen at build time.
//!
//! By default this re-exports [`omq-compio`](https://crates.io/crates/omq-compio)
//! (single-thread io_uring/IOCP). Switch to the multi-thread tokio
//! backend with `--no-default-features --features tokio-backend`.
//! The two are mutually exclusive — enabling both is a compile error.
//!
//! ```ignore
//! use omq::{Endpoint, Message, Options, Socket, SocketType};
//! ```

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
