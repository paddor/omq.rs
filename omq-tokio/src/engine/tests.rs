use super::{QUEUE_RING_INITIAL_SLOTS, queue_ring};

#[test]
fn queue_ring_grows_past_its_initial_slots_to_full_capacity() {
    let capacity = QUEUE_RING_INITIAL_SLOTS * 4;
    let (mut producer, mut consumer) = queue_ring::<usize>(capacity);

    for value in 0..capacity {
        producer.push(value).unwrap();
    }
    assert_eq!(producer.push(capacity), Err(capacity));
    producer.flush();

    assert_eq!(consumer.prefetch(), capacity);
    for value in 0..capacity {
        assert_eq!(consumer.pop(), Some(value));
    }
    assert_eq!(consumer.pop(), None);
}
