//! Shared ZMQ C API constants.
//!
//! Defined once here and imported by the modules that need them.

use std::ffi::c_int;

// Send/recv flags.
pub(crate) const ZMQ_DONTWAIT: c_int = 1;
pub(crate) const ZMQ_SNDMORE: c_int = 2;

// Poll event masks.
pub(crate) const ZMQ_POLLIN: c_int = 1;
pub(crate) const ZMQ_POLLOUT: c_int = 2;

#[allow(dead_code)]
pub(crate) const ZMQ_POLLERR: c_int = 4;
