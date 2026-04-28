//! Per-socket-type state and message transforms.
//!
//! Pure transform logic lives in [`omq_proto::type_state::TypeState`];
//! this module re-exports it so the existing
//! `crate::socket::type_state::TypeState` import path continues to
//! resolve. omq-compio uses the same shared core.

pub(crate) use omq_proto::type_state::TypeState;
