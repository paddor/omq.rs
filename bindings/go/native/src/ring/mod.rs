//! Native send and receive rings.

mod codec;
pub(crate) mod recv;
pub(crate) mod send;

const RING_BATCH: usize = 512;
