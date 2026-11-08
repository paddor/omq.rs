use super::*;
use omq_tokio::{Context, Options, SocketType};
use std::mem::{align_of, offset_of, size_of};

fn ring(capacity: usize) -> OmqGoRecvRing {
    let context = Context::new();
    let socket = context.blocking_socket(SocketType::Pull, Options::default());
    OmqGoRecvRing::new(socket, capacity, 4096)
}

fn body(ring: &OmqGoRecvRing, index: usize) -> &[u8] {
    let desc = ring.desc[index];
    let start = desc.payload as usize;
    &ring.payload[start..start + desc.payload_len as usize]
}

#[test]
fn rejects_zero_limit_without_receiving() {
    let result = ring(4).fill_with(0, |_, _, _| panic!("unexpected receive"));
    assert!(matches!(result, Err(Error::Config(_))));
}

#[test]
fn receive_batch_obeys_descriptor_message_and_batch_limits() {
    for (capacity, limit, expected) in [(2, 10, 2), (1024, 600, RING_BATCH), (16, 3, 3)] {
        let mut ring = ring(capacity);
        let result = ring.fill_with(limit, |_, max, out| {
            assert_eq!(max, expected);
            out.extend((0..max).map(|_| Message::single("body")));
            Ok(Some(max))
        });
        assert_eq!(result.unwrap(), Some(expected));
        assert_eq!(ring.control.tail.load(Ordering::Acquire), expected);
    }
}

#[test]
fn empty_receive_cancellation_and_errors_keep_their_results() {
    let mut ring = ring(4);
    assert!(matches!(
        ring.fill_with(1, |_, _, _| Ok(Some(0))),
        Err(Error::WouldBlock)
    ));
    assert_eq!(ring.fill_with(1, |_, _, _| Ok(None)).unwrap(), None);
    assert!(matches!(
        ring.fill_with(1, |_, _, _| Err(Error::Timeout)),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        ring.fill_with(1, |_, _, _| Err(Error::Closed)),
        Err(Error::Closed)
    ));
    assert_eq!(ring.control.tail.load(Ordering::Acquire), 0);
}

#[test]
fn full_descriptors_return_would_block_without_receiving() {
    let mut ring = ring(1);
    ring.publish(&Message::single("occupied"));
    ring.flush(0);
    let result = ring.fill_with(2, |_, _, _| panic!("unexpected receive"));
    assert!(matches!(result, Err(Error::WouldBlock)));
    assert_eq!(ring.control.tail.load(Ordering::Acquire), 1);
}

#[test]
fn received_batch_publishes_in_order_and_reuses_scratch_storage() {
    let mut ring = ring(4);
    let storage = ring.scratch.as_ptr();
    let capacity = ring.scratch.capacity();
    let result = ring.fill_with(3, |_, max, out| {
        assert_eq!(max, 3);
        out.extend(["first", "second", "third"].map(Message::single));
        Ok(Some(3))
    });
    assert_eq!(result.unwrap(), Some(3));
    assert_eq!(body(&ring, 0), b"first");
    assert_eq!(body(&ring, 1), b"second");
    assert_eq!(body(&ring, 2), b"third");
    assert!(ring.scratch.is_empty());
    assert_eq!(ring.scratch.as_ptr(), storage);
    assert_eq!(ring.scratch.capacity(), capacity);
}

#[test]
fn consumed_descriptors_free_space_for_the_next_batch() {
    let mut ring = ring(2);
    let fill = |ring: &mut OmqGoRecvRing, body: &'static str| {
        ring.fill_with(2, |_, max, out| {
            out.extend((0..max).map(|_| Message::single(body)));
            Ok(Some(max))
        })
    };
    assert_eq!(fill(&mut ring, "first").unwrap(), Some(2));
    assert!(matches!(fill(&mut ring, "blocked"), Err(Error::WouldBlock)));
    ring.control.head.store(1, Ordering::Release);
    assert_eq!(fill(&mut ring, "second").unwrap(), Some(1));
    assert_eq!(body(&ring, 0), b"second");
    assert_eq!(ring.control.tail.load(Ordering::Acquire), 3);
}

#[test]
fn receive_ring_layout_matches_go_memory_access() {
    assert_eq!(size_of::<RecvRingControl>(), 256);
    assert_eq!(align_of::<RecvRingControl>(), 128);
    assert_eq!(offset_of!(RecvRingControl, head), 0);
    assert_eq!(offset_of!(RecvRingControl, tail), 128);
    assert_eq!(size_of::<RecvRingDesc>(), 64);
    assert_eq!(align_of::<RecvRingDesc>(), 64);
    assert_eq!(offset_of!(RecvRingDesc, payload), 0);
    assert_eq!(offset_of!(RecvRingDesc, payload_len), 8);
    assert_eq!(offset_of!(RecvRingDesc, total_len), 16);
    assert_eq!(offset_of!(RecvRingDesc, part_count), 24);
    assert_eq!(offset_of!(RecvRingDesc, flags), 32);
    assert_eq!(offset_of!(RecvRingDesc, payload_end), 40);
}
