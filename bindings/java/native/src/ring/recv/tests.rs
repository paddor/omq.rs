use super::*;
use std::mem::{align_of, offset_of, size_of};

#[test]
fn recv_ring_layout_matches_java_memory_access() {
    assert_eq!(size_of::<RecvRingControl>(), 256);
    assert_eq!(align_of::<RecvRingControl>(), 128);
    assert_eq!(offset_of!(RecvRingControl, head), 0);
    assert_eq!(offset_of!(RecvRingControl, tail), 128);
    assert_eq!(size_of::<RecvRingDesc>(), 64);
    assert_eq!(align_of::<RecvRingDesc>(), 64);
    assert_eq!(offset_of!(RecvRingDesc, payload), 0);
    assert_eq!(offset_of!(RecvRingDesc, payload_end), 40);
}
