use super::*;
use std::mem::{align_of, offset_of, size_of};

#[test]
fn send_ring_layout_matches_java_memory_access() {
    assert_eq!(size_of::<SendRingControl>(), 640);
    assert_eq!(align_of::<SendRingControl>(), 128);
    assert_eq!(offset_of!(SendRingControl, head), 0);
    assert_eq!(offset_of!(SendRingControl, tail), 128);
    assert_eq!(offset_of!(SendRingControl, closed), 256);
    assert_eq!(offset_of!(SendRingControl, worker_parked), 384);
    assert_eq!(offset_of!(SendRingControl, producer_waiting), 512);
    assert_eq!(size_of::<SendRingDesc>(), 64);
    assert_eq!(align_of::<SendRingDesc>(), 64);
    assert_eq!(offset_of!(SendRingDesc, payload), 0);
    assert_eq!(offset_of!(SendRingDesc, payload_len), 8);
    assert_eq!(offset_of!(SendRingDesc, payload_end), 16);
}
