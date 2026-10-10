//! Bounded receive turns for the separate-process throughput benchmarks.

const RECEIVE_MESSAGES: usize = 128;
const RECEIVE_BYTES: usize = 64 * 1024;

pub(super) fn receive_batch(size: usize) -> usize {
    (RECEIVE_BYTES / size.max(1)).clamp(1, RECEIVE_MESSAGES)
}
