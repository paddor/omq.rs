use std::os::unix::net::UnixStream;
use std::time::Duration;

use super::*;
use crate::mapping::{HEADER, read_u64};
use crate::pool::LOOKUP;
use crate::wire::REFERENCE_SIZE;
use crate::{
    BATCH_MAX_MESSAGES, COPIED_BATCH_MAX_BODY, is_pooled, payload_pool, pooled_prefix,
    prepare_batch, prepare_reference_batch,
};

fn pair(capacity: usize) -> (Connection, Connection) {
    let (first, second) = UnixStream::pair().unwrap();
    let mut sender = Connection::new(&first, capacity, None).unwrap();
    let mut receiver = Connection::new(&second, capacity, None).unwrap();
    transfer(&mut sender, &mut receiver);
    transfer(&mut receiver, &mut sender);
    sender.accept_preface(&receiver.preface()).unwrap();
    receiver.accept_preface(&sender.preface()).unwrap();
    (sender, receiver)
}

fn pair_with_send_pool(capacity: usize, pool: SendPool) -> (Connection, Connection) {
    let (first, second) = UnixStream::pair().unwrap();
    let mut sender = Connection::with_send_pool(&first, capacity, None, pool).unwrap();
    let mut receiver = Connection::new(&second, capacity, None).unwrap();
    transfer(&mut sender, &mut receiver);
    transfer(&mut receiver, &mut sender);
    sender.accept_preface(&receiver.preface()).unwrap();
    receiver.accept_preface(&sender.preface()).unwrap();
    (sender, receiver)
}

#[test]
fn data_descriptor_before_lease_table_is_rejected() {
    let (first, second) = UnixStream::pair().unwrap();
    let mut receiver = Connection::new(&second, 4, None).unwrap();
    let region = Mapping::create(HEADER + 128, crate::mapping::DATA).unwrap();
    let descriptors = std::collections::VecDeque::from([region.descriptor().unwrap()]);
    assert_eq!(
        socket::send(&first, &[IoSlice::new(b"x")], &descriptors).unwrap(),
        1
    );
    assert_eq!(
        receiver.try_read(&second, &mut [0]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(receiver.incoming.is_none());
    assert!(receiver.regions.is_empty());
}

#[test]
fn automatic_storage_is_shared_across_recipient_connections() {
    let maximum = HEADER + 2 * 128;
    let pool = SendPool::new(2, Some(maximum));
    let (mut sender1, mut receiver1) = pair_with_send_pool(2, pool.clone());
    let (mut sender2, mut receiver2) = pair_with_send_pool(2, pool.clone());
    let first = encoded(
        &mut sender1,
        &mut receiver1,
        &Message::from_slice(&[1; 128]),
    );
    let second = encoded(
        &mut sender2,
        &mut receiver2,
        &Message::from_slice(&[2; 128]),
    );
    let reference1 = wire::Reference::decode(first.part_slice(0).unwrap(), BODY).unwrap();
    let reference2 = wire::Reference::decode(second.part_slice(0).unwrap(), BODY).unwrap();
    assert_eq!(
        (reference1.id, reference1.token),
        (reference2.id, reference2.token)
    );
    assert_ne!(reference1.offset, reference2.offset);
    assert_eq!(pool.memory_used(), maximum);
    assert_eq!(
        receiver1.decode(first).unwrap().part_slice(0).unwrap(),
        &[1; 128]
    );
    assert_eq!(
        receiver2.decode(second).unwrap().part_slice(0).unwrap(),
        &[2; 128]
    );
}

#[test]
fn automatic_memory_cap_includes_large_pool_alignment() {
    const SIZE: usize = 4096;
    let message = Message::from_slice(&[7; SIZE]);
    for maximum in [HEADER + SIZE, crate::pool::payload_offset(SIZE) + SIZE] {
        let pool = SendPool::new(1, Some(maximum));
        let (mut sender, mut receiver) = pair_with_send_pool(2, pool.clone());
        let wire = encoded(&mut sender, &mut receiver, &message);
        if maximum == HEADER + SIZE {
            assert_eq!(body(&wire).unwrap()[0], COPIED);
            assert_eq!(pool.memory_used(), 0);
        } else {
            assert_eq!(body(&wire).unwrap()[0], BODY);
            assert_eq!(pool.memory_used(), maximum);
        }
        assert_eq!(receiver.decode(wire).unwrap(), message);
        assert!(pool.memory_used() <= maximum);
    }
}

#[test]
fn memory_pressure_and_oversized_bodies_use_carrier_delivery() {
    for maximum in [0, HEADER + 128] {
        let pool = SendPool::new(4, Some(maximum));
        let (mut sender, mut receiver) = pair_with_send_pool(4, pool.clone());
        let first = delivered(&mut sender, &mut receiver, &Message::from_slice(&[1; 128]));
        let view = first.part_bytes(0).unwrap();
        drop(first);
        for size in [128, 1024, 65536] {
            let message = Message::with_group("g", vec![3; size]);
            let wire = encoded(&mut sender, &mut receiver, &message);
            assert_eq!(body(&wire).unwrap()[0], COPIED);
            assert_eq!(receiver.decode(wire).unwrap(), message);
            assert!(pool.memory_used() <= maximum);
        }
        assert_eq!(view.as_ref(), &[1; 128]);
        receiver.max_message_size = Some(1024);
        let wire = encoded(&mut sender, &mut receiver, &Message::from(vec![4; 2048]));
        assert!(receiver.decode(wire).is_err());
    }
}

#[test]
fn idle_size_classes_are_reclaimed_after_final_retained_view() {
    let maximum = HEADER + 1024;
    let pool = SendPool::new(1, Some(maximum));
    let (mut sender, mut receiver) = pair_with_send_pool(2, pool.clone());
    let first = delivered(&mut sender, &mut receiver, &Message::from_slice(&[5; 128]));
    let view = first.part_bytes(0).unwrap();
    drop(first);
    let larger = Message::from(vec![6; 1024]);
    let wire = encoded(&mut sender, &mut receiver, &larger);
    assert_eq!(body(&wire).unwrap()[0], COPIED);
    assert_eq!(receiver.decode(wire).unwrap(), larger);
    assert_eq!(view.as_ref(), &[5; 128]);
    drop(view);
    let wire = encoded(&mut sender, &mut receiver, &larger);
    assert_eq!(body(&wire).unwrap()[0], BODY);
    assert_eq!(pool.memory_used(), maximum);
    assert_eq!(receiver.decode(wire).unwrap(), larger);
}

#[test]
fn explicit_producer_storage_is_independent_of_the_automatic_cap() {
    let automatic = SendPool::new(1, Some(0));
    let explicit = payload_pool([(1024, 1)]).unwrap();
    let (mut sender, mut receiver) = pair_with_send_pool(1, automatic.clone());
    let message = explicit
        .try_message(1024, |bytes| bytes.fill(9))
        .unwrap()
        .unwrap();
    let wire = encoded(&mut sender, &mut receiver, &message);
    assert_eq!(body(&wire).unwrap()[0], BODY);
    assert_eq!(automatic.memory_used(), 0);
    let delivered = receiver.decode(wire).unwrap();
    drop(message);
    assert_eq!(explicit.available(), 0);
    assert_eq!(delivered.part_slice(0).unwrap(), &[9; 1024]);
    drop(delivered);
    assert_eq!(explicit.available(), 1);
}

#[test]
fn prepared_fanout_body_is_copied_once_and_retained_by_every_recipient() {
    let pool = SendPool::new(1, Some(HEADER + 1024));
    let (mut sender1, mut receiver1) = pair_with_send_pool(2, pool.clone());
    let (mut sender2, mut receiver2) = pair_with_send_pool(2, pool.clone());
    let original = Message::from(vec![7; 1024]);
    let prepared = pool.prepare(&original).unwrap().unwrap();
    let first = encoded(&mut sender1, &mut receiver1, &prepared);
    let second = encoded(&mut sender2, &mut receiver2, &prepared);
    let first_reference = wire::Reference::decode(body(&first).unwrap(), BODY).unwrap();
    let second_reference = wire::Reference::decode(body(&second).unwrap(), BODY).unwrap();
    assert_eq!(
        (
            first_reference.id,
            first_reference.token,
            first_reference.offset
        ),
        (
            second_reference.id,
            second_reference.token,
            second_reference.offset
        )
    );
    let first = receiver1.decode(first).unwrap();
    let second = receiver2.decode(second).unwrap();
    drop(prepared);
    drop(first);
    assert!(pool.prepare(&original).unwrap().is_none());
    assert_eq!(second, original);
    drop(second);
    assert!(pool.prepare(&original).unwrap().is_some());
}

#[test]
fn large_automatic_bodies_fill_the_hwm_window_without_carrier_fallback() {
    const SIZE: usize = 8 * 1024 * 1024;
    let pool = SendPool::new(4, Some(omq_proto::options::DEFAULT_SHM_MAX_MEMORY));
    let (mut sender, mut receiver) = pair_with_send_pool(4, pool.clone());
    let original = Message::from(vec![7; SIZE]);
    let mut held = Vec::new();
    for _ in 0..4 {
        let wire = encoded(&mut sender, &mut receiver, &original);
        assert_eq!(body(&wire).unwrap()[0], BODY);
        held.push(receiver.decode(wire).unwrap());
    }
    assert_eq!(
        pool.memory_used(),
        crate::pool::payload_offset(SIZE) + 4 * SIZE
    );
    assert!(sender.encode(&original).unwrap().is_none());
    assert!(held.iter().all(|message| message == &original));
    held.pop();
    let wire = encoded(&mut sender, &mut receiver, &original);
    assert_eq!(body(&wire).unwrap()[0], BODY);
    assert_eq!(receiver.decode(wire).unwrap(), original);
}

fn transfer(sender: &mut Connection, receiver: &mut Connection) {
    let (first, second) = UnixStream::pair().unwrap();
    assert_eq!(
        sender
            .try_write(&first, &[std::io::IoSlice::new(b"x")])
            .unwrap(),
        1
    );
    assert_eq!(receiver.try_read(&second, &mut [0]).unwrap(), 1);
}

fn encoded(sender: &mut Connection, receiver: &mut Connection, message: &Message) -> Message {
    let wire = sender.encode(message).unwrap().unwrap();
    transfer(sender, receiver);
    wire
}

fn delivered(sender: &mut Connection, receiver: &mut Connection, message: &Message) -> Message {
    let wire = encoded(sender, receiver, message);
    receiver.decode(wire).unwrap()
}

#[test]
fn native_carrier_waits_for_region_import_and_reader_close() {
    let (mut sender, mut receiver) = pair(2);
    let pool = payload_pool([(256, 1)]).unwrap();
    let message = pool.message(256, |bytes| bytes.fill(7)).unwrap();
    let wire = sender.encode(&message).unwrap().unwrap();
    let bytes = [std::io::IoSlice::new(b"body")];
    assert_eq!(
        sender.try_write_carrier(&bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let (first, second) = UnixStream::pair().unwrap();
    sender
        .try_write(&first, &[std::io::IoSlice::new(b"\0")])
        .unwrap();
    assert!(!sender.has_pending_descriptors());
    assert_eq!(
        sender.try_write_carrier(&bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    sender.arm_carrier_write();
    receiver.try_read(&second, &mut [0]).unwrap();
    assert_eq!(sender.try_write_carrier(&bytes).unwrap(), 4);
    let mut output = [0; 4];
    assert_eq!(receiver.try_read_carrier(&mut output).unwrap(), 4);
    assert_eq!(&output, b"body");
    assert_eq!(receiver.decode(wire).unwrap(), message);
    drop(receiver);
    assert_eq!(
        sender.try_write_carrier(&bytes).unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
}

#[test]
fn final_byte_view_returns_lease_after_both_connections_close() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let (mut sender, mut receiver) = pair(1);
    let message = pool
        .try_message(128, |bytes| bytes.fill(0xa5))
        .unwrap()
        .unwrap();
    let wire = encoded(&mut sender, &mut receiver, &message);
    drop(message);
    let message = receiver.decode(wire).unwrap();
    sender.peer_exited();
    assert_eq!(pool.available(), 0, "a live reader cannot be declared dead");
    let clone = message.clone();
    let view = clone.part_bytes(0).unwrap();
    drop(message);
    drop(clone);
    drop(receiver);
    assert!(matches!(
        sender.encode(&Message::from_slice(&[8; 128])),
        Err(omq_proto::Error::Closed)
    ));
    drop(sender);
    assert_eq!(pool.available(), 0);
    assert_eq!(view.as_ref(), &[0xa5; 128]);
    drop(view);
    assert_eq!(pool.available(), 1);
}

#[test]
fn every_fanout_recipient_must_release_before_reuse() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let (mut sender1, mut receiver1) = pair(2);
    let (mut sender2, mut receiver2) = pair(2);
    let message = pool
        .try_message(128, |bytes| bytes.fill(7))
        .unwrap()
        .unwrap();
    let first = delivered(&mut sender1, &mut receiver1, &message);
    let second = delivered(&mut sender2, &mut receiver2, &message);
    drop(message);
    drop(first);
    assert_eq!(pool.available(), 0);
    assert_eq!(second.part_slice(0).unwrap(), &[7; 128]);
    drop(second);
    assert_eq!(pool.available(), 1);
    let message = pool
        .try_message(128, |bytes| bytes.fill(8))
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(0).unwrap(), &[8; 128]);
}

#[test]
fn unread_reference_is_cancelled_by_reader_close() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let (mut sender, receiver) = pair(1);
    let message = pool.message(128, |bytes| bytes.fill(1)).unwrap();
    let _wire = sender.encode(&message).unwrap().unwrap();
    drop(message);
    assert_eq!(pool.available(), 0);
    drop(receiver);
    assert_eq!(pool.available(), 1);
}

#[test]
fn descriptor_survives_source_pool_and_message_drop() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let (mut sender, mut receiver) = pair(1);
    let message = pool.message(128, |bytes| bytes.fill(4)).unwrap();
    let wire = sender.encode(&message).unwrap().unwrap();
    drop(message);
    drop(pool);
    transfer(&mut sender, &mut receiver);
    let delivered = receiver.decode(wire).unwrap();
    drop(sender);
    assert_eq!(delivered.part_slice(0).unwrap(), &[4; 128]);
}

#[test]
fn stale_generation_and_out_of_bounds_references_are_rejected() {
    let (mut sender, mut receiver) = pair(1);
    let message = Message::from_slice(&[7; 128]);
    let wire = encoded(&mut sender, &mut receiver, &message);
    let stale = wire.clone();
    let mut invalid_wire = wire.part_slice(0).unwrap().to_vec();
    invalid_wire[13..21].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(receiver.decode(Message::from(invalid_wire)).is_err());
    let delivered = receiver.decode(wire).unwrap();
    assert!(receiver.decode(stale.clone()).is_err());
    drop(delivered);
    let next = encoded(&mut sender, &mut receiver, &message);
    assert!(receiver.decode(stale).is_err());
    assert!(receiver.decode(next).is_ok());
}

#[test]
fn in_bounds_coordinates_cannot_borrow_a_different_writable_slot() {
    let pool = payload_pool([(128, 2)]).unwrap();
    let (mut sender, mut receiver) = pair(1);
    let source = pool.message(128, |bytes| bytes.fill(1)).unwrap();
    let wire = encoded(&mut sender, &mut receiver, &source);
    let mut other = pool.try_buffer(128).unwrap();
    let mut forged = wire.part_slice(0).unwrap().to_vec();
    let offset = read_u64(&forged, 13);
    forged[13..21].copy_from_slice(&(offset + 128).to_le_bytes());
    assert!(receiver.decode(Message::from(forged)).is_err());
    other.writable().fill(2);
    assert_eq!(source.part_slice(0).unwrap(), &[1; 128]);
}

#[test]
fn tiny_copies_and_logical_size_limits() {
    let (mut sender, mut receiver) = pair(2);
    for size in [0, 1, 54, 55, 56, 62, 63, 128, 1024] {
        let message = Message::from_slice(&vec![0xa5; size]);
        let wire = encoded(&mut sender, &mut receiver, &message);
        assert_eq!(wire.part_slice(0).unwrap()[0], u8::from(size > 55));
        let delivered = receiver.decode(wire).unwrap();
        assert_eq!(delivered, message);
    }
    receiver.max_message_size = Some(191);
    let wire = encoded(&mut sender, &mut receiver, &Message::from_slice(&[3; 128]));
    assert!(receiver.decode(wire).is_err());
    receiver.max_message_size = Some(192);
    let accepted = encoded(&mut sender, &mut receiver, &Message::from_slice(&[3; 128]));
    assert!(receiver.decode(accepted).is_ok());
    assert_eq!(std::mem::size_of::<Message>(), 64);
    assert_eq!(std::mem::size_of::<Payload>(), 64);
}

#[test]
fn bulk_slot_lookup_handles_mixed_regions_sizes_and_missing_bodies() {
    let pool = payload_pool([(128, 8), (256, 1)]).unwrap();
    let other = payload_pool([(128, 1)]).unwrap();
    let messages: Vec<_> = (0..8)
        .map(|value| pool.message(128, |body| body.fill(value)).unwrap())
        .collect();
    let bodies = std::array::from_fn(|index| messages[index].part_slice(0));
    let slots = crate::pool::find_slots(bodies);
    for (body, slot) in bodies.into_iter().zip(slots) {
        let expected = find_slot(body.unwrap()).unwrap();
        assert!(Arc::ptr_eq(&expected, &slot.unwrap()));
    }
    let large = pool.message(256, |_| {}).unwrap();
    let foreign = other.message(128, |_| {}).unwrap();
    let slot = find_slot(bodies[0].unwrap()).unwrap();
    let spanning = slot.mapping.bytes(slot.offset, slot.size + 1);
    let mixed = [
        bodies[0],
        large.part_slice(0),
        foreign.part_slice(0),
        Some(&bodies[3].unwrap()[16..80]),
        Some(spanning),
        Some(&[0; 128]),
        None,
        bodies[7],
    ];
    let slots = crate::pool::find_slots(mixed);
    for (body, actual) in mixed.into_iter().zip(slots) {
        let expected = body.and_then(find_slot);
        assert_eq!(actual.is_some(), expected.is_some());
        if let (Some(actual), Some(expected)) = (actual, expected) {
            assert!(Arc::ptr_eq(&actual, &expected));
        }
    }
    // The first body need not be pooled for subsequent entries to resolve.
    let mut mixed = mixed;
    mixed[0] = None;
    assert!(crate::pool::find_slots(mixed)[7].is_some());

    let mut selected: Vec<_> = messages.iter().collect();
    selected.push(&large);
    selected.push(&foreign);
    assert_eq!(pooled_prefix(&selected), 10);
    let heap = Message::from_slice(&[0; 128]);
    selected.insert(7, &heap);
    assert_eq!(pooled_prefix(&selected), 7);
    selected.insert(0, &heap);
    assert_eq!(pooled_prefix(&selected), 0);
    assert_eq!(pooled_prefix(&[]), 0);
}

#[test]
fn region_lookup_handles_slots_slices_and_pool_destruction() {
    let pool = payload_pool([(128, 8), (1024, 4), (65536, 2)]).unwrap();
    let messages: Vec<_> = [128; 8]
        .into_iter()
        .chain([1024; 4])
        .chain([65536; 2])
        .map(|size| {
            pool.try_message(size, |bytes| bytes.fill(7))
                .unwrap()
                .unwrap()
        })
        .collect();
    let mut regions = Vec::new();
    for message in &messages {
        let body = message.part_slice(0).unwrap();
        let slot = find_slot(body).unwrap();
        assert_eq!(body.as_ptr() as usize, slot.mapping.address() + slot.offset);
        assert!(Arc::ptr_eq(&slot, &find_slot(&body[32..64]).unwrap()));
        if crate::pool::payload_offset(body.len()) > HEADER {
            let padding = slot.mapping.bytes(HEADER, 1);
            assert!(find_slot(padding).is_none());
        }
        if slot.offset + slot.size < slot.mapping.len() {
            let crossing = slot.mapping.bytes(slot.offset + slot.size - 1, 2);
            assert!(find_slot(crossing).is_none());
        }
        regions.push(LOOKUP.with(|cached| cached.borrow().region.clone()));
    }
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                for message in &messages {
                    assert!(is_pooled(message));
                }
            })
            .join()
            .unwrap();
    });
    drop(messages);
    drop(pool);
    assert!(regions.iter().all(|region| region.upgrade().is_none()));
    assert!(find_slot(&[7; 128]).is_none());
}

#[test]
fn repeated_publication_keeps_completed_lease_bookkeeping_bounded() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let body = pool
        .try_message(128, |bytes| bytes.fill(7))
        .unwrap()
        .unwrap();
    let (mut sender, mut receiver) = pair(64);
    for _ in 0..1024 {
        drop(delivered(&mut sender, &mut receiver, &body));
    }
    let slot = find_slot(body.part_slice(0).unwrap()).unwrap();
    assert!(slot.state.lock().unwrap().leases.len() <= 32);
    drop(slot);
    drop(body);
    assert_eq!(pool.available(), 1);
}

#[test]
fn copied_body_framing_preserves_envelopes_and_uses_no_lease() {
    let (mut sender, _receiver) = pair(1);
    let mut frames = omq_proto::frame_buffer::FrameBuffer::new();
    let message = Message::with_group("group", "abc").with_routing_id(7);
    assert!(sender.encode_and_frame(&message, &mut frames).unwrap());
    assert_eq!(frames.arena_bytes(), b"\x01\x05group\x00\x04\x00abc");
    assert_eq!(sender.free.len(), 1);
    assert!(sender.active.is_empty());
    assert_eq!(message.routing_id(), Some(7));
    frames.clear_arena();
    assert!(
        sender
            .encode_and_frame(&Message::from_slice(&[]), &mut frames)
            .unwrap()
    );
    assert_eq!(frames.arena_bytes(), b"\x00\x01\x00");
}

#[test]
fn queued_copy_policy_releases_small_sources_but_retains_large_references() {
    for size in [
        crate::POOLED_BATCH_COPY_THRESHOLD,
        crate::POOLED_BATCH_COPY_THRESHOLD + 1,
    ] {
        let sources = payload_pool([(size, 8)]).unwrap();
        let messages: Vec<_> = (0..8)
            .map(|index| {
                Message::with_prefix(
                    bytes::Bytes::from_static(b"group"),
                    sources.message(size, |bytes| bytes.fill(index)).unwrap(),
                )
                .with_routing_id(7)
            })
            .collect();
        let copied = size <= crate::POOLED_BATCH_COPY_THRESHOLD;
        let kind = BatchKind::for_message(&messages[0], crate::POOLED_BATCH_COPY_THRESHOLD);
        let (mut sender, mut receiver) = pair(messages.len());
        let packed = sender
            .prepare_batch(&messages.iter().collect::<Vec<_>>(), kind)
            .unwrap()
            .unwrap();
        let wire = sender
            .encode_record(&packed, messages.len(), kind.record())
            .unwrap()
            .unwrap();
        transfer(&mut sender, &mut receiver);
        let mut decoded = vec![receiver.decode_received(wire).unwrap()];
        decoded.extend(std::iter::from_fn(|| receiver.poll_received()));
        assert_eq!(decoded, messages);
        let held = decoded[0].part_bytes(1).unwrap().slice(1..17);
        drop(messages);
        drop(packed);
        drop(decoded);
        assert_eq!(sources.available(), if copied { 8 } else { 0 });
        drop(sender);
        drop(receiver);
        assert_eq!(held.as_ref(), &[0; 16]);
        drop(held);
        assert_eq!(sources.available(), 8);
    }
}

#[test]
fn inline_grouped_messages_never_check_out_heap_or_mapped_pool_slots() {
    for pool in [
        omq_proto::PayloadPool::new([(4096, 2)]).unwrap(),
        payload_pool([(4096, 2)]).unwrap(),
    ] {
        let message = Message::with_prefix(
            bytes::Bytes::from_static(b"group"),
            pool.message(16, |body| body.fill(7)).unwrap(),
        );
        assert!(!is_pooled(&message));
        assert_eq!(pool.available(), pool.capacity());
        let (mut sender, mut receiver) = pair(1);
        let wire = encoded(&mut sender, &mut receiver, &message);
        assert_eq!(body(&wire).unwrap()[0], COPIED);
        assert_eq!(receiver.decode(wire).unwrap(), message);
        assert!(sender.active.is_empty());
        assert_eq!(sender.send_pool.memory_used(), 0);
    }
}

#[test]
fn single_frame_batches_preserve_bodies_routes_and_logical_credit() {
    for count in [2, 8, BATCH_MAX_MESSAGES] {
        for kind in [BatchKind::Copied, BatchKind::Referenced] {
            let (mut sender, mut receiver) = pair(count);
            let slot_size = match kind {
                BatchKind::Copied => 4096,
                BatchKind::Referenced => 128 * 1024,
            };
            let sources = payload_pool([(slot_size, count)]).unwrap();
            let sizes: &[usize] = match kind {
                BatchKind::Copied => &[0, 16, 55, 64, 256, 4096],
                BatchKind::Referenced => &[64, 256, 128 * 1024],
            };
            let messages: Vec<_> = (0..count)
                .map(|index| {
                    sources
                        .try_message(sizes[index % sizes.len()], |bytes| bytes.fill(index as u8))
                        .unwrap()
                        .unwrap()
                        .with_routing_id(7)
                })
                .collect();
            let packed = sender
                .prepare_batch(&messages.iter().collect::<Vec<_>>(), kind)
                .unwrap()
                .unwrap();
            assert_eq!(packed.len(), 1);
            assert_eq!(packed.routing_id(), Some(7));
            let wire = sender
                .encode_record(&packed, count, kind.record())
                .unwrap()
                .unwrap();
            assert_eq!(sender.active.len(), 1);
            assert_eq!(sender.leased_messages, count);
            transfer(&mut sender, &mut receiver);
            let mut decoded = vec![receiver.decode_received(wire).unwrap()];
            while let Some(message) = receiver.poll_received() {
                decoded.push(message);
            }
            assert_eq!(decoded, messages);
            assert!(
                decoded
                    .iter()
                    .all(|message| message.len() == 1 && message.routing_id() == Some(7))
            );
            let retained = decoded.last().unwrap().part_bytes(0).unwrap();
            drop(decoded);
            drop(packed);
            drop(messages);
            drop(sender);
            drop(receiver);
            assert_eq!(retained.as_ref(), vec![(count - 1) as u8; retained.len()]);
            drop(retained);
            assert_eq!(sources.available(), count);
        }
    }
}

#[test]
fn batches_reject_mixed_envelopes_routes_and_excess_hwm() {
    let (sender, _) = pair(2);
    let ordinary = Message::from_slice(&[1; 64]);
    let grouped = Message::with_group("g", vec![2; 64]);
    assert!(
        sender
            .prepare_batch(&[&ordinary, &grouped], BatchKind::Copied)
            .is_err()
    );
    let routed = ordinary.clone().with_routing_id(1);
    assert!(
        sender
            .prepare_batch(&[&ordinary, &routed], BatchKind::Copied)
            .is_err()
    );
    assert!(
        sender
            .prepare_batch(&[&ordinary; 3], BatchKind::Copied)
            .unwrap()
            .is_none()
    );
    assert_eq!(sender.batch_capacity(), 2);
}

#[test]
fn small_body_batches_preserve_order_and_charge_each_message_against_hwm() {
    for count in [2, 8, BATCH_MAX_MESSAGES] {
        let (mut sender, mut receiver) = pair(count);
        let pool = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 1)]).unwrap();
        let messages: Vec<_> = (0..count)
            .map(|index| Message::with_group("group", vec![index as u8; [0, 16, 55][index % 3]]))
            .collect();
        let references: Vec<_> = messages.iter().collect();
        let batch = prepare_batch(&pool, &references).unwrap().unwrap();
        let wire = sender.encode_record(&batch, count, 2).unwrap().unwrap();
        let stale = wire.clone();
        assert_eq!(sender.leased_messages, count);
        assert!(
            sender
                .encode(&Message::from_slice(&[8; 128]))
                .unwrap()
                .is_none()
        );
        drop(batch);
        assert_eq!(pool.available(), 0);
        transfer(&mut sender, &mut receiver);
        let mut decoded = vec![receiver.decode_received(wire).unwrap()];
        assert!(receiver.decode_received(stale.clone()).is_err());
        while let Some(message) = receiver.poll_received() {
            decoded.push(message);
        }
        assert_eq!(decoded, messages);
        // Small application bodies keep their independent inline ownership.
        assert_eq!(pool.available(), 1);
        assert!(receiver.decode_received(stale).is_err());
        assert!(
            sender
                .encode(&Message::from_slice(&[8; 128]))
                .unwrap()
                .is_some()
        );
        assert_eq!(sender.free.len(), count - 1);
        drop(sender);
        drop(receiver);
        assert_eq!(decoded, messages);
    }
}

#[test]
fn received_batches_ignore_size_table_changes_after_validation() {
    let (mut sender, mut receiver) = pair(4);
    let pool = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 1)]).unwrap();
    let messages: Vec<_> = (0..3_u8)
        .map(|index| Message::from(vec![index; 1024]))
        .collect();
    let references: Vec<_> = messages.iter().collect();
    let batch = prepare_batch(&pool, &references).unwrap().unwrap();
    let wire = sender
        .encode_record(&batch, messages.len(), crate::wire::COPIED_BATCH)
        .unwrap()
        .unwrap();
    transfer(&mut sender, &mut receiver);
    let Received::Batch(mut batch_view) = receiver.decode_delivery(wire).unwrap() else {
        panic!("expected a batch");
    };
    // The sender can still write its table after the receiver validated it.
    let table = batch.part_slice(0).unwrap();
    let slot = find_slot(table).unwrap();
    let offset = table.as_ptr() as usize - slot.mapping.address();
    slot.mapping
        .atomic64(offset + 8)
        .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
    let mut decoded = Vec::new();
    while let Some(message) = batch_view.pop_front() {
        decoded.push(message);
    }
    assert_eq!(decoded, messages);
}

#[test]
fn copied_batches_share_storage_and_keep_every_recipient_view_alive() {
    let pool = payload_pool([(BATCH_MAX_BYTES, 1)]).unwrap();
    let messages: Vec<_> = [16, 128, 1024, 4096, COPIED_BATCH_MAX_BODY]
        .into_iter()
        .enumerate()
        .map(|(index, size)| Message::with_group("group", vec![index as u8; size]))
        .collect();
    let batch = prepare_batch(&pool, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let packed = batch.part_slice(1).unwrap();
    let slot = find_slot(packed).unwrap();
    let (mut sender1, mut receiver1) = pair(messages.len());
    let (mut sender2, mut receiver2) = pair(messages.len());
    let wire1 = sender1
        .encode_record(&batch, messages.len(), 2)
        .unwrap()
        .unwrap();
    let wire2 = sender2
        .encode_record(&batch, messages.len(), 2)
        .unwrap()
        .unwrap();
    assert_eq!(sender1.active.len(), 1);
    assert_eq!(sender1.leased_messages, messages.len());
    transfer(&mut sender1, &mut receiver1);
    transfer(&mut sender2, &mut receiver2);
    let mut first = vec![receiver1.decode_received(wire1).unwrap()];
    let mut second = vec![receiver2.decode_received(wire2).unwrap()];
    while let Some(message) = receiver1.poll_received() {
        first.push(message);
    }
    while let Some(message) = receiver2.poll_received() {
        second.push(message);
    }
    assert_eq!(first, messages);
    assert_eq!(second, messages);
    assert!(
        first
            .iter()
            .all(|message| message.last_payload().unwrap().as_chunk().is_none()),
        "grouped bodies must retain compact shared-owner envelopes"
    );
    let region = receiver1
        .find_region(slot.mapping.id, slot.mapping.token)
        .unwrap();
    let payload_offset =
        packed.as_ptr() as usize - slot.mapping.address() + 2 + messages.len() * 8 + 16;
    assert_eq!(
        first[1].part_slice(1).unwrap().as_ptr() as usize,
        region.address() + payload_offset
    );
    let view1 = first[4].part_bytes(1).unwrap();
    let view2 = second[2].part_bytes(1).unwrap();
    drop(first);
    drop(second);
    drop(batch);
    drop(slot);
    drop(sender1);
    drop(sender2);
    drop(receiver1);
    drop(receiver2);
    assert_eq!(pool.available(), 0);
    assert_eq!(view1.as_ref(), vec![4; COPIED_BATCH_MAX_BODY]);
    drop(view1);
    assert_eq!(pool.available(), 0);
    assert_eq!(view2.as_ref(), vec![2; 1024]);
    drop(view2);
    assert_eq!(pool.available(), 1);
}

#[test]
fn copied_batches_bound_body_and_total_sizes_without_consuming_credit() {
    let pool = payload_pool([(BATCH_MAX_BYTES, 1)]).unwrap();
    let oversized = Message::with_group("group", vec![7; COPIED_BATCH_MAX_BODY + 1]);
    assert!(prepare_batch(&pool, &[&oversized; 4]).is_err());
    let large = Message::with_group("group", vec![7; COPIED_BATCH_MAX_BODY]);
    assert!(
        prepare_batch(&pool, &[&large; BATCH_MAX_MESSAGES])
            .unwrap()
            .is_none()
    );
    assert_eq!(pool.available(), 1);
}

#[test]
fn copied_batch_boundaries_use_inline_bodies_or_shared_byte_slices() {
    let sizes = [0, 16, 55, 56, 62, 63, 64, 256];
    let pool = payload_pool([(4096, 1)]).unwrap();
    let messages: Vec<_> = sizes
        .into_iter()
        .enumerate()
        .map(|(index, size)| Message::from_slice(&vec![index as u8; size]))
        .collect();
    let batch = prepare_batch(&pool, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let (mut sender, mut receiver) = pair(messages.len());
    let wire = sender
        .encode_record(&batch, messages.len(), 2)
        .unwrap()
        .unwrap();
    transfer(&mut sender, &mut receiver);
    let mut decoded = vec![receiver.decode_received(wire).unwrap()];
    decoded.extend(std::iter::from_fn(|| receiver.poll_received()));
    assert_eq!(decoded, messages);
    for (message, size) in decoded.iter().zip(sizes) {
        let payload = message.last_payload().unwrap();
        assert_eq!(payload.as_chunk().is_some(), size > COPY_THRESHOLD);
        assert_eq!(message.routing_id(), None);
    }
    let retained = decoded.last().unwrap().part_bytes(0).unwrap().slice(3..19);
    drop(decoded);
    drop(batch);
    drop(sender);
    drop(receiver);
    assert_eq!(pool.available(), 0);
    assert_eq!(retained.as_ref(), &[7; 16]);
    drop(retained);
    assert_eq!(pool.available(), 1);
}

#[test]
fn reference_batch_runs_preserve_order_and_leave_gaps_unborrowed() {
    // Full neighboring slots coalesce. Padding, gaps, reversed and repeated
    // coordinates, and region changes keep their independent view owners.
    for (size, order, expected) in [
        (128, vec![0, 1, 2, 3], vec![true; 4]),
        (64, vec![0, 1, 2, 3], vec![false; 4]),
        (128, vec![0, 2], vec![false; 2]),
        (128, vec![3, 2, 1, 0], vec![false; 4]),
        (128, vec![0, 0, 1], vec![false, true, true]),
        (128, vec![0, 1, 4, 5, 2, 3], vec![true; 6]),
    ] {
        let pools = [
            payload_pool([(128, 4)]).unwrap(),
            payload_pool([(128, 2)]).unwrap(),
        ];
        let mut messages: Vec<_> = (0..6)
            .map(|index| {
                pools[usize::from(index >= 4)]
                    .message(size, |body| body.fill(index))
                    .unwrap()
            })
            .collect();
        let metadata = payload_pool([(1024, 1)]).unwrap();
        let selected: Vec<_> = order.iter().map(|&index| &messages[index]).collect();
        let batch = prepare_reference_batch(&metadata, &selected)
            .unwrap()
            .unwrap();
        let (mut sender, mut receiver) = pair(selected.len());
        let wire = sender
            .encode_record(&batch, selected.len(), 3)
            .unwrap()
            .unwrap();
        transfer(&mut sender, &mut receiver);
        let mut decoded = vec![receiver.decode_received(wire).unwrap()];
        decoded.extend(std::iter::from_fn(|| receiver.poll_received()));
        assert_eq!(decoded.iter().collect::<Vec<_>>(), selected);
        for (message, shared) in decoded.iter().zip(expected) {
            assert_eq!(message.last_payload().unwrap().as_chunk().is_some(), shared);
        }
        drop(selected);
        if order == [0, 2] {
            drop(messages.remove(1));
            let reused = pools[0]
                .try_message(size, |body| body.fill(9))
                .unwrap()
                .unwrap();
            assert_eq!(reused.part_slice(0).unwrap(), vec![9; size]);
            assert_eq!(decoded[0].part_slice(0).unwrap(), vec![0; size]);
            assert_eq!(decoded[1].part_slice(0).unwrap(), vec![2; size]);
        }
        let retained = decoded[0].part_bytes(0).unwrap().slice(1..17);
        drop(messages);
        drop(batch);
        drop(sender);
        drop(receiver);
        drop(decoded);
        assert_eq!(retained.as_ref(), vec![order[0] as u8; 16]);
        assert!(pools[0].available() < 4);
        drop(retained);
        assert_eq!(pools[0].available(), 4);
        assert_eq!(pools[1].available(), 2);
    }
}

#[test]
fn reference_batches_reject_heap_metadata_without_retaining_sources() {
    let sources = payload_pool([(256, 2)]).unwrap();
    let metadata = omq_proto::PayloadPool::new([(128, 1)]).unwrap();
    let messages: Vec<_> = (0..2)
        .map(|index| {
            Message::with_prefix(
                b"group".as_slice().into(),
                sources.message(256, |bytes| bytes.fill(index)).unwrap(),
            )
        })
        .collect();
    assert!(
        prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
            .unwrap()
            .is_none()
    );
    assert_eq!(metadata.available(), 1);
    drop(messages);
    assert_eq!(sources.available(), 2);
}

#[test]
fn reference_batches_keep_source_slots_until_the_final_recipient_view() {
    for size in [16384, 1024 * 1024] {
        let sources = payload_pool([(size, 4)]).unwrap();
        let metadata = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 1)]).unwrap();
        let messages: Vec<_> = (0..4)
            .map(|index| {
                Message::with_prefix(
                    b"group".as_slice().into(),
                    sources
                        .try_message(size, |bytes| bytes.fill(index))
                        .unwrap()
                        .unwrap(),
                )
                .with_routing_id(7)
            })
            .collect();
        let batch = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        let (mut sender1, mut receiver1) = pair(4);
        let (mut sender2, mut receiver2) = pair(4);
        let wire1 = sender1.encode_record(&batch, 4, 3).unwrap().unwrap();
        let wire2 = sender2.encode_record(&batch, 4, 3).unwrap().unwrap();
        transfer(&mut sender1, &mut receiver1);
        transfer(&mut sender2, &mut receiver2);
        let mut first = vec![receiver1.decode_received(wire1).unwrap()];
        let mut second = vec![receiver2.decode_received(wire2).unwrap()];
        while let Some(message) = receiver1.poll_received() {
            first.push(message);
        }
        while let Some(message) = receiver2.poll_received() {
            second.push(message);
        }
        for (index, message) in first.iter().enumerate() {
            assert!(
                message.last_payload().unwrap().as_chunk().is_none(),
                "grouped bodies must retain compact shared-owner envelopes"
            );
            assert_eq!(message.part_slice(1).unwrap(), vec![index as u8; size]);
            assert_eq!(message.part_slice(0), Some(b"group".as_slice()));
            let reference = batch::references(batch.part_slice(1).unwrap()).unwrap()[index];
            let mapping = receiver1
                .find_region(reference.id, reference.token)
                .unwrap();
            let offset =
                read_u64(batch.part_slice(1).unwrap(), 2 + 4 * 8 + index * 20 + 12) as usize;
            assert_eq!(
                message.part_slice(1).unwrap().as_ptr() as usize,
                mapping.address() + offset
            );
        }
        let view = first[0].part_bytes(1).unwrap();
        drop(messages);
        drop(batch);
        drop(metadata);
        drop(sender1);
        drop(sender2);
        drop(receiver1);
        drop(receiver2);
        drop(first);
        assert_eq!(sources.available(), 0);
        drop(second);
        assert_eq!(
            sources.available(),
            0,
            "one view retains the whole batch's source slots"
        );
        assert_eq!(view.as_ref(), vec![0; size]);
        drop(view);
        assert_eq!(sources.available(), 4);
    }
}

#[test]
fn reference_batch_sources_return_without_exhausting_metadata_pool_or_lease_credit() {
    let sources = payload_pool([(16384, 4)]).unwrap();
    let metadata = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 64)]).unwrap();
    let (mut sender, mut receiver) = pair(4096);
    for _ in 0..8 {
        let messages: Vec<_> = (0..4)
            .map(|_| {
                Message::with_prefix(
                    b"group".as_slice().into(),
                    sources
                        .try_message(16384, |bytes| bytes.fill(7))
                        .unwrap()
                        .unwrap(),
                )
            })
            .collect();
        let batch = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        let wire = sender.encode_record(&batch, 4, 3).unwrap().unwrap();
        drop(messages);
        drop(batch);
        transfer(&mut sender, &mut receiver);
        drop(receiver.decode_received(wire).unwrap());
        while let Some(message) = receiver.poll_received() {
            drop(message);
        }
        assert_eq!(sources.available(), 4);
    }
}

#[test]
fn reference_batch_concurrent_local_views_release_sources_without_metadata_reclamation() {
    let sources = payload_pool([(16384, 4)]).unwrap();
    let metadata = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 64)]).unwrap();
    let (mut sender, mut receiver) = pair(4096);
    for _ in 0..8 {
        let messages: Vec<_> = (0..4)
            .map(|_| {
                Message::with_prefix(
                    b"group".as_slice().into(),
                    sources
                        .try_message(16384, |bytes| bytes.fill(7))
                        .unwrap()
                        .unwrap(),
                )
            })
            .collect();
        let batch = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        let wire = sender.encode_record(&batch, 4, 3).unwrap().unwrap();
        let views: Vec<_> = (0..8).map(|_| batch.clone()).collect();
        drop(messages);
        drop(batch);
        transfer(&mut sender, &mut receiver);
        drop(receiver.decode_received(wire).unwrap());
        while let Some(message) = receiver.poll_received() {
            drop(message);
        }
        assert_eq!(sources.available(), 0, "local batch views retain sources");
        let barrier = std::sync::Barrier::new(views.len());
        std::thread::scope(|scope| {
            for view in views {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    drop(view);
                });
            }
        });
        assert_eq!(sources.available(), 4);
    }
}

#[test]
fn reference_batches_preserve_unread_regions_across_registration_eviction() {
    let sources: Vec<_> = (0..128)
        .map(|_| payload_pool([(128, 1)]).unwrap())
        .collect();
    let metadata = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 32)]).unwrap();
    let (mut sender, mut receiver) = pair(128);
    let mut wires = Vec::new();
    for start in (0..128).step_by(4) {
        let messages: Vec<_> = (start..start + 4)
            .map(|index| {
                Message::with_prefix(
                    b"group".as_slice().into(),
                    sources[index]
                        .try_message(128, |bytes| bytes.fill(index as u8))
                        .unwrap()
                        .unwrap(),
                )
            })
            .collect();
        let batch = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        wires.push(sender.encode_record(&batch, 4, 3).unwrap().unwrap());
        transfer(&mut sender, &mut receiver);
    }
    assert!(!receiver.retired_regions.is_empty());
    let mut messages = Vec::new();
    for wire in wires {
        messages.push(receiver.decode_received(wire).unwrap());
        while let Some(message) = receiver.poll_received() {
            messages.push(message);
        }
    }
    for (index, message) in messages.iter().enumerate() {
        assert_eq!(message.part_slice(1).unwrap(), vec![index as u8; 128]);
        assert_eq!(sources[index].available(), 0);
    }
    drop(messages);
    assert!(sources.iter().all(|pool| pool.available() == 1));
}

#[test]
fn reference_batch_rejections_complete_the_shared_lease() {
    let sources = payload_pool([(1024, 2)]).unwrap();
    let metadata = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 1)]).unwrap();
    let messages: Vec<_> = [128, 1024]
        .into_iter()
        .map(|size| {
            Message::with_prefix(
                b"group".as_slice().into(),
                sources
                    .try_message(size, |bytes| bytes.fill(7))
                    .unwrap()
                    .unwrap(),
            )
        })
        .collect();
    let batch = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let (mut sender, mut receiver) = pair(2);
    receiver.max_message_size = Some(512);
    let wire = sender.encode_record(&batch, 2, 3).unwrap().unwrap();
    drop(messages);
    drop(batch);
    transfer(&mut sender, &mut receiver);
    assert!(receiver.decode_received(wire).is_err());
    assert!(receiver.poll_received().is_none());
    assert_eq!(sources.available(), 2);
}

#[test]
fn rejected_copied_batch_publishes_neither_views_nor_receive_cursor() {
    let metadata = payload_pool([(64, 2)]).unwrap();
    let messages = [
        Message::with_group("group", "first"),
        Message::with_group("group", "second"),
    ];
    let references: Vec<_> = messages.iter().collect();
    let (mut sender, mut receiver) = pair(2);
    assert!(receiver.received.is_none());
    let batch = prepare_batch(&metadata, &references).unwrap().unwrap();
    let forged = Message::with_prefix(
        b"group".as_slice().into(),
        metadata
            .message(64, |bytes| {
                bytes.copy_from_slice(batch.part_slice(1).unwrap());
                bytes[63] = 1;
            })
            .unwrap(),
    );
    let wire = sender.encode_record(&forged, 2, 2).unwrap().unwrap();
    drop(forged);
    drop(batch);
    transfer(&mut sender, &mut receiver);
    assert!(receiver.decode_received(wire).is_err());
    assert!(receiver.poll_received().is_none());
    assert!(receiver.received.is_none());
    assert_eq!(metadata.available(), 2);

    let batch = prepare_batch(&metadata, &references).unwrap().unwrap();
    let wire = sender.encode_record(&batch, 2, 2).unwrap().unwrap();
    transfer(&mut sender, &mut receiver);
    assert_eq!(receiver.decode_received(wire).unwrap(), messages[0]);
    assert_eq!(receiver.poll_received().unwrap(), messages[1]);
    assert!(receiver.poll_received().is_none());
    assert!(receiver.received.is_none());
}

#[test]
fn malformed_batches_and_logical_size_limits_release_their_lease() {
    for (count, length, maximum) in [
        (0_u16, 0_u64, None),
        (2, 56, None),
        (2, 16, Some(128)),
        (2, u64::from(u32::MAX) + 1, None),
        (2, u64::MAX, None),
    ] {
        let (mut sender, mut receiver) = pair(2);
        receiver.max_message_size = maximum;
        let pool = payload_pool([(64, 1)]).unwrap();
        let body = pool
            .message(64, |bytes| {
                bytes.fill(0);
                bytes[..2].copy_from_slice(&count.to_le_bytes());
                bytes[2..10].copy_from_slice(&length.to_le_bytes());
            })
            .unwrap();
        let batch = Message::with_prefix(b"group".as_slice().into(), body);
        let wire = sender.encode_record(&batch, 2, 2).unwrap().unwrap();
        drop(batch);
        transfer(&mut sender, &mut receiver);
        assert!(receiver.decode_received(wire).is_err());
        assert_eq!(pool.available(), 1);
        assert!(receiver.poll_received().is_none());
    }
}

#[test]
fn batch_size_tables_use_u64_and_reject_truncated_or_overflowing_lengths() {
    let metadata = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 1)]).unwrap();
    let small = [
        Message::with_group("group", "abc"),
        Message::with_group("group", "d"),
    ];
    let copied = prepare_batch(&metadata, &small.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let bytes = copied.part_slice(1).unwrap();
    assert_eq!(read_u64(bytes, 2), 3);
    assert_eq!(read_u64(bytes, 10), 1);
    assert_eq!(&bytes[18..22], b"abcd");
    for size in [2, 9, 17] {
        let truncated = Message::with_group("group", bytes[..size].to_vec());
        assert!(
            batch::decode(
                &truncated,
                None,
                &crate::views::Views::new(2),
                &mut std::collections::VecDeque::new()
            )
            .is_err()
        );
    }
    drop(copied);

    let sources = payload_pool([(128, 2)]).unwrap();
    let large: Vec<_> = [100, 128]
        .into_iter()
        .map(|size| {
            Message::with_prefix(
                b"group".as_slice().into(),
                sources
                    .try_message(size, |bytes| bytes.fill(7))
                    .unwrap()
                    .unwrap(),
            )
        })
        .collect();
    let referenced = prepare_reference_batch(&metadata, &large.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let bytes = referenced.part_slice(1).unwrap();
    assert_eq!(read_u64(bytes, 2), 100);
    assert_eq!(read_u64(bytes, 10), 128);
    for length in [(isize::MAX as u64) + 1, u64::MAX] {
        let mut forged = bytes.to_vec();
        forged[2..10].copy_from_slice(&length.to_le_bytes());
        assert!(batch::references(&forged).is_err());
    }
    #[cfg(target_pointer_width = "64")]
    {
        let length = u64::from(u32::MAX) + 1;
        let mut forged = bytes.to_vec();
        forged[2..10].copy_from_slice(&length.to_le_bytes());
        assert_eq!(batch::references(&forged).unwrap().len(), 2);
        let slot = find_slot(large[0].part_slice(1).unwrap()).unwrap();
        let mut decoded = std::collections::VecDeque::new();
        assert!(
            batch::decode_references(
                &Message::with_group("group", forged),
                None,
                &crate::views::Views::new(2),
                |id, token| (slot.mapping.id == id && slot.mapping.token == token)
                    .then(|| slot.mapping.clone()),
                &mut decoded,
            )
            .is_err()
        );
        assert!(decoded.is_empty());
    }
}

#[test]
fn copied_and_grouped_bodies_keep_the_existing_frame_size_budget() {
    for message in [
        Message::from("abc"),
        Message::with_group("group", vec![7; 128]),
    ] {
        let exact = message.byte_len() + message.len() * std::mem::size_of::<Payload>();
        for (limit, accepted) in [(exact - 1, false), (exact, true)] {
            let (mut sender, mut receiver) = pair(1);
            receiver.max_message_size = Some(limit);
            let wire = encoded(&mut sender, &mut receiver, &message);
            assert_eq!(receiver.decode(wire).is_ok(), accepted);
        }
    }
}

#[test]
fn futex_credit_wait_wakes_on_final_drop() {
    let (mut sender, mut receiver) = pair(1);
    let message = Message::from_slice(&[7; 128]);
    let held = delivered(&mut sender, &mut receiver, &message);
    assert!(sender.encode(&message).unwrap().is_none());
    let wait = sender.credit_wait();
    let (complete, delivered) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        wait.wait();
        complete.send(()).unwrap();
    });
    drop(held);
    delivered.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.join().unwrap();
    assert!(sender.encode(&message).unwrap().is_some());
}

#[test]
fn already_completed_cells_are_found_behind_held_cells() {
    let (mut sender, mut receiver) = pair(129);
    let mut held = Vec::new();
    for value in 0..129 {
        let message = Message::from_slice(&vec![value as u8; 1024]);
        held.push(delivered(&mut sender, &mut receiver, &message));
    }
    drop(held.pop());
    assert!(
        sender
            .encode(&Message::from_slice(&[5; 1024]))
            .unwrap()
            .is_some()
    );
    assert!(
        held.iter()
            .enumerate()
            .all(|(index, body)| body.part_slice(0).unwrap() == vec![index as u8; 1024])
    );
}

#[test]
fn batch_credit_finds_completed_cells_while_unused_positions_remain() {
    let filled = 3 * BATCH_MAX_MESSAGES;
    let (mut sender, mut receiver) = pair(filled + 1);
    let body = Message::from_slice(&[7; 128]);
    let mut held: Vec<_> = (0..filled)
        .map(|_| delivered(&mut sender, &mut receiver, &body))
        .collect();
    held.truncate(2 * BATCH_MAX_MESSAGES);
    assert_eq!(sender.free.len(), 1);
    let pool = payload_pool([(BATCH_MAX_BYTES.next_power_of_two(), 1)]).unwrap();
    let message = Message::with_group("group", vec![8; 16]);
    let batch = prepare_batch(&pool, &[&message; BATCH_MAX_MESSAGES])
        .unwrap()
        .unwrap();
    let wire = sender
        .encode_record(&batch, BATCH_MAX_MESSAGES, 2)
        .unwrap()
        .expect("completed leases must supply the entire batch's credit");
    transfer(&mut sender, &mut receiver);
    assert_eq!(receiver.decode_received(wire).unwrap(), message);
    for _ in 1..BATCH_MAX_MESSAGES {
        assert_eq!(receiver.poll_received().unwrap(), message);
    }
    assert!(held.iter().all(|received| received == &body));
}

#[test]
fn publication_racing_close_never_leaves_an_unread_lease_pinned() {
    for _ in 0..200 {
        let pool = payload_pool([(128, 1)]).unwrap();
        let (mut sender, receiver) = pair(1);
        let message = pool.message(128, |bytes| bytes.fill(7)).unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                barrier.wait();
                let result = sender.encode(&message);
                assert!(result.is_ok() || matches!(result, Err(omq_proto::Error::Closed)));
            });
            barrier.wait();
            drop(receiver);
            worker.join().unwrap();
        });
        drop(message);
        assert_eq!(pool.available(), 1);
    }
}

#[test]
fn queued_references_survive_registration_eviction_and_reregistration() {
    let pools: Vec<_> = (0..128)
        .map(|_| payload_pool([(128, 1)]).unwrap())
        .collect();
    let messages: Vec<_> = pools
        .iter()
        .enumerate()
        .map(|(index, pool)| pool.message(128, |bytes| bytes.fill(index as u8)).unwrap())
        .collect();
    let (mut sender, mut receiver) = pair(256);
    let mut references = Vec::new();
    for message in &messages[..64] {
        references.push(sender.encode(message).unwrap().unwrap());
    }
    assert!(sender.encode(&messages[64]).unwrap().is_none());
    let wait = sender.credit_wait();
    transfer(&mut sender, &mut receiver);
    // Draining registrations changes the credit sequence, so this wait does
    // not park even though no recipient has completed a lease yet.
    wait.wait();
    for message in &messages[64..] {
        references.push(sender.encode(message).unwrap().unwrap());
    }
    transfer(&mut sender, &mut receiver);
    assert_eq!(receiver.regions.len(), 64);
    assert_eq!(receiver.retired_regions.len(), 64);
    let held: Vec<_> = references
        .into_iter()
        .map(|wire| receiver.decode(wire).unwrap())
        .collect();
    // Register the evicted first region again while its earlier borrowed
    // mapping remains live. Both mappings address the same pinned storage.
    let again = delivered(&mut sender, &mut receiver, &messages[0]);
    drop(messages);
    assert!(pools.iter().all(|pool| pool.available() == 0));
    for (index, message) in held.into_iter().enumerate() {
        assert_eq!(message.part_slice(0).unwrap(), &[index as u8; 128]);
        drop(message);
        assert_eq!(pools[index].available(), usize::from(index != 0));
    }
    assert_eq!(again.part_slice(0).unwrap(), &[0; 128]);
    drop(again);
    assert!(pools.iter().all(|pool| pool.available() == 1));
}

#[test]
fn partial_writes_transfer_fds_once_and_blocked_writes_retain_them() {
    let (mut sender, mut receiver) = pair(4);
    let (first, second) = UnixStream::pair().unwrap();
    rustix::net::sockopt::set_socket_send_buffer_size(&first, 4096).unwrap();
    let first_pool = payload_pool([(128, 1)]).unwrap();
    let first_message = first_pool.message(128, |bytes| bytes.fill(1)).unwrap();
    let first_wire = sender.encode(&first_message).unwrap().unwrap();
    let bytes = vec![0; 1024 * 1024];
    let written = sender.try_write(&first, &[IoSlice::new(&bytes)]).unwrap();
    assert!(written > 0 && written < bytes.len());
    assert!(sender.descriptors.is_empty());
    // Saturate the byte stream before queuing another region descriptor.
    for attempt in 0..256 {
        match sender.try_write(&first, &[IoSlice::new(&bytes[..4096])]) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            result => {
                result.unwrap();
                assert!(attempt < 255, "send buffer did not fill");
            }
        }
    }
    let second_pool = payload_pool([(128, 1)]).unwrap();
    let second_message = second_pool.message(128, |bytes| bytes.fill(2)).unwrap();
    let second_wire = sender.encode(&second_message).unwrap().unwrap();
    assert_eq!(
        sender
            .try_write(&first, &[IoSlice::new(b"x")])
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(sender.descriptors.len(), 1);
    let mut bytes = [0; 4096];
    for attempt in 0..256 {
        match receiver.try_read(&second, &mut bytes) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            result => {
                assert!(result.unwrap() > 0);
                assert!(attempt < 255, "receive buffer did not drain");
            }
        }
    }
    assert_eq!(receiver.regions.len(), 1);
    assert_eq!(sender.try_write(&first, &[IoSlice::new(b"x")]).unwrap(), 1);
    assert_eq!(receiver.try_read(&second, &mut bytes).unwrap(), 1);
    assert!(sender.descriptors.is_empty());
    assert_eq!(receiver.regions.len(), 2);
    assert_eq!(receiver.decode(first_wire).unwrap(), first_message);
    assert_eq!(receiver.decode(second_wire).unwrap(), second_message);
}

fn receive_exact(connection: &mut Connection, stream: &UnixStream, bytes: &mut [u8]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut received = 0;
    while received < bytes.len() {
        assert!(
            std::time::Instant::now() < deadline,
            "descriptor peer timed out"
        );
        match connection.try_read(stream, &mut bytes[received..]) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            result => {
                let count = result.unwrap();
                assert_ne!(count, 0);
                received += count;
            }
        }
    }
}

fn exchange_preface(connection: &mut Connection, stream: &UnixStream) -> [u8; PREFACE_SIZE] {
    let preface = connection.preface();
    assert_eq!(
        connection
            .try_write(stream, &[IoSlice::new(&preface)])
            .unwrap(),
        PREFACE_SIZE
    );
    let mut peer = [0; PREFACE_SIZE];
    receive_exact(connection, stream, &mut peer);
    connection.accept_preface(&peer).unwrap();
    peer
}

#[test]
#[ignore = "helper invoked by descriptor permission test"]
fn descriptor_process_child() {
    rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable).unwrap();
    let stream = UnixStream::connect(std::env::var("OMQ_DESCRIPTOR_SOCKET").unwrap()).unwrap();
    let mut connection = Connection::new(&stream, 1, None).unwrap();
    exchange_preface(&mut connection, &stream);
    let pool = payload_pool([(128, 1)]).unwrap();
    let message = pool.message(128, |bytes| bytes.fill(7)).unwrap();
    let wire = connection.encode(&message).unwrap().unwrap();
    assert_eq!(
        connection
            .try_write(&stream, &[IoSlice::new(wire.part_slice(0).unwrap())])
            .unwrap(),
        REFERENCE_SIZE
    );
    let mut acknowledged = [0];
    receive_exact(&mut connection, &stream, &mut acknowledged);
}

#[test]
fn descriptor_transfer_works_when_procfs_fd_access_is_denied() {
    let path = std::env::temp_dir().join(format!(
        "omq-fd-{}-{}.sock",
        std::process::id(),
        rand::random::<u64>()
    ));
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "connection::tests::descriptor_process_child",
            "--ignored",
        ])
        .env("OMQ_DESCRIPTOR_SOCKET", &path)
        .spawn()
        .unwrap();
    let (stream, _) = listener.accept().unwrap();
    std::fs::remove_file(&path).unwrap();
    let mut connection = Connection::new(&stream, 1, None).unwrap();
    let peer = exchange_preface(&mut connection, &stream);
    let id = u32::from_le_bytes(peer[8..12].try_into().unwrap());
    let proc_fd = format!("/proc/{}/fd/{id}", child.id());
    assert!(
        File::open(proc_fd).is_err(),
        "test requires denied procfs access"
    );
    let mut wire = [0; REFERENCE_SIZE];
    receive_exact(&mut connection, &stream, &mut wire);
    let held = connection.decode(Message::from_slice(&wire)).unwrap();
    assert_eq!(
        connection
            .try_write(&stream, &[IoSlice::new(b"x")])
            .unwrap(),
        1
    );
    assert!(child.wait().unwrap().success());
    drop(connection);
    assert_eq!(held.part_slice(0).unwrap(), &[7; 128]);
}

#[test]
fn slab_slices_keep_the_whole_slot_until_every_recipient_releases() {
    for size in [1024, 2048, 4096, 16384] {
        for grouped in [false, true] {
            let pool = payload_pool([(size, 1)]).unwrap();
            let mut buffer = pool.try_buffer(size).unwrap();
            for (seq, body) in buffer
                .writable()
                .as_chunks_mut::<64>()
                .0
                .iter_mut()
                .enumerate()
            {
                body.fill(seq as u8);
            }
            buffer.set_len(size).unwrap();
            let slab: bytes::Bytes = buffer.into_payload().into();
            if size.is_multiple_of(4096) {
                assert!((slab.as_ptr() as usize).is_multiple_of(4096));
            }
            let messages: Vec<_> = (0..size)
                .step_by(64)
                .map(|offset| {
                    let message = Message::from(slab.slice(offset..offset + 64));
                    if grouped {
                        Message::with_prefix(bytes::Bytes::from_static(b"slab"), message)
                    } else {
                        message
                    }
                })
                .collect();
            let metadata = payload_pool([(8192, 1)]).unwrap();
            let batch = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
                .unwrap()
                .unwrap();
            let (mut first_sender, mut first_reader) = pair(messages.len());
            let (mut second_sender, mut second_reader) = pair(messages.len());
            let first_wire = first_sender
                .encode_record(&batch, messages.len(), 3)
                .unwrap()
                .unwrap();
            let second_wire = second_sender
                .encode_record(&batch, messages.len(), 3)
                .unwrap()
                .unwrap();
            transfer(&mut first_sender, &mut first_reader);
            transfer(&mut second_sender, &mut second_reader);
            let mut first = vec![first_reader.decode_received(first_wire).unwrap()];
            first.extend(std::iter::from_fn(|| first_reader.poll_received()));
            let mut second = vec![second_reader.decode_received(second_wire).unwrap()];
            second.extend(std::iter::from_fn(|| second_reader.poll_received()));
            assert_eq!(first, messages);
            assert_eq!(second, messages);
            let held = second[size / 128]
                .part_bytes(usize::from(grouped))
                .unwrap()
                .slice(1..17);
            drop(messages);
            drop(slab);
            drop(batch);
            drop(first);
            assert_eq!(pool.available(), 0);
            drop(second);
            assert_eq!(pool.available(), 0);
            drop(first_sender);
            drop(second_sender);
            drop(first_reader);
            drop(second_reader);
            assert_eq!(held.as_ref(), vec![(size / 128) as u8; 16]);
            assert_eq!(pool.available(), 0);
            drop(held);
            assert_eq!(pool.available(), 1);
        }
    }
}

#[test]
fn inline_batch_releases_metadata_before_split_cursors_are_received() {
    let metadata = payload_pool([(1024, 1)]).unwrap();
    let messages: Vec<_> = (0..8_u8)
        .map(|seq| Message::from_slice(&[seq; 16]))
        .collect();
    let prepared = prepare_batch(&metadata, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let (mut sender, mut receiver) = pair(8);
    let wire = sender.encode_record(&prepared, 8, 2).unwrap().unwrap();
    drop(prepared);
    transfer(&mut sender, &mut receiver);
    let Received::Batch(mut batch) = receiver.decode_delivery(wire).unwrap() else {
        panic!("expected inline batch");
    };
    assert_eq!(metadata.available(), 1);
    let mut prefix = batch.take_prefix(3);
    drop(sender);
    drop(receiver);
    let mut output = Vec::new();
    prefix.set_routing_id(19);
    assert_eq!(prefix.drain_into(&mut output, 3, |_| false), 1);
    assert_eq!(prefix.len(), 2);
    assert_eq!(prefix.drain_into(&mut output, 8, |_| true), 2);
    for (seq, message) in output.iter().enumerate() {
        assert_eq!(message.routing_id(), Some(19));
        assert_eq!(body(message).unwrap(), &[seq as u8; 16]);
    }
    for seq in 3..8 {
        assert_eq!(body(&batch.pop_front().unwrap()).unwrap(), &[seq; 16]);
    }
    assert!(batch.pop_front().is_none());
}

#[test]
fn lazy_batches_split_without_views_and_retain_storage_after_connection_close() {
    for kind in [BatchKind::Copied, BatchKind::Referenced] {
        for grouped in [false, true] {
            let pool = payload_pool([(128, 8)]).unwrap();
            let messages: Vec<_> = (0..8_u8)
                .map(|seq| {
                    let message = pool.message(128, |body| body.fill(seq)).unwrap();
                    if grouped {
                        Message::with_prefix(bytes::Bytes::from_static(b"lazy"), message)
                    } else {
                        message
                    }
                })
                .collect();
            let (mut sender, mut receiver) = pair(8);
            let prepared = sender
                .prepare_batch(&messages.iter().collect::<Vec<_>>(), kind)
                .unwrap()
                .unwrap();
            let wire = sender
                .encode_record(&prepared, 8, kind.record())
                .unwrap()
                .unwrap();
            transfer(&mut sender, &mut receiver);
            let Received::Batch(mut batch) = receiver.decode_delivery(wire).unwrap() else {
                panic!("expected batch")
            };
            drop(prepared);
            drop(messages);
            drop(sender);
            drop(receiver);
            assert_eq!(batch.len(), 8);
            assert_eq!(batch.parts(), if grouped { 2 } else { 1 });
            let empty = batch.take_prefix(0);
            assert!(empty.is_empty());
            drop(empty);
            let mut first = batch.take_prefix(3);
            let mut middle = batch.take_prefix(2);
            assert_eq!(batch.len(), 3);
            let mut retained = Vec::new();
            for (cursor, start, end) in
                [(&mut middle, 3, 5), (&mut batch, 5, 8), (&mut first, 0, 3)]
            {
                cursor.set_routing_id(19);
                for seq in start..end {
                    let message = cursor.pop_front().unwrap();
                    assert_eq!(message.routing_id(), Some(19));
                    assert_eq!(body(&message).unwrap(), &[seq; 128]);
                    if grouped {
                        assert_eq!(message.part_slice(0), Some(b"lazy".as_slice()));
                    }
                    retained.push(message);
                }
                assert!(cursor.pop_front().is_none());
            }
            let last = retained
                .pop()
                .unwrap()
                .part_bytes(usize::from(grouped))
                .unwrap()
                .slice(17..97);
            drop(retained);
            drop(first);
            drop(middle);
            drop(batch);
            if matches!(kind, BatchKind::Referenced) {
                assert_eq!(pool.available(), 0);
            }
            assert_eq!(last.as_ref(), &[2; 80]);
            drop(last);
            assert_eq!(pool.available(), 8);
        }
    }
}

#[test]
fn local_close_wakes_a_credit_waiter_even_if_the_peer_clears_its_parked_flag() {
    let (mut sender, mut receiver) = pair(1);
    let message = Message::from_slice(&[7; 128]);
    let held = delivered(&mut sender, &mut receiver, &message);
    assert!(sender.encode(&message).unwrap().is_none());
    let wait = sender.credit_wait();
    let (complete, done) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        wait.wait();
        let _ = complete.send(());
    });
    std::thread::sleep(Duration::from_millis(50));
    // The parked flag lives in sender-owned memory the receiver can write.
    receiver
        .incoming
        .as_ref()
        .unwrap()
        .mapping
        .atomic32(crate::wire::WAITER_PARKED)
        .store(0, std::sync::atomic::Ordering::SeqCst);
    drop(sender);
    done.recv_timeout(Duration::from_secs(2))
        .expect("local close must wake its own credit waiter");
    drop(held);
}

#[test]
fn local_close_wakes_a_credit_waiter_while_the_peer_pins_the_wake_sequence() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    let (mut sender, mut receiver) = pair(1);
    let message = Message::from_slice(&[7; 128]);
    let held = delivered(&mut sender, &mut receiver, &message);
    assert!(sender.encode(&message).unwrap().is_none());
    let wait = sender.credit_wait();
    let pinned = sender.outgoing.sequence();
    let (complete, done) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        wait.wait();
        let _ = complete.send(());
    });
    let incoming = receiver.incoming.clone().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let pinning = std::thread::spawn({
        let stop = stop.clone();
        move || {
            while !stop.load(Relaxed) {
                let mapping = &incoming.mapping;
                mapping
                    .atomic32(crate::wire::WAKE_SEQUENCE)
                    .store(pinned, Relaxed);
                mapping
                    .atomic32(crate::wire::WAITER_PARKED)
                    .store(0, Relaxed);
                std::hint::spin_loop();
            }
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    drop(sender);
    let woke = done.recv_timeout(Duration::from_secs(2));
    stop.store(true, Relaxed);
    pinning.join().unwrap();
    woke.expect("a local close must end the credit wait despite a pinned sequence");
    drop(held);
}

#[test]
fn sender_survives_a_receiver_that_scribbles_over_its_lease_table() {
    let patterns: [fn(usize) -> u64; 3] = [|_| 0, |_| u64::MAX, |_| rand::random()];
    for pattern in patterns {
        let (mut sender, mut receiver) = pair(4);
        let message = Message::from_slice(&[7; 128]);
        let held = delivered(&mut sender, &mut receiver, &message);
        let mapping = &receiver.incoming.as_ref().unwrap().mapping;
        for offset in (0..mapping.len()).step_by(8) {
            let value = if offset == READER_CLOSED {
                0
            } else {
                pattern(offset)
            };
            mapping
                .atomic64(offset)
                .store(value, std::sync::atomic::Ordering::Relaxed);
        }
        // Every sender operation must fail or proceed, never panic or hang.
        for _ in 0..8 {
            let _ = sender.encode(&message);
            let _ = sender.try_write_carrier(&[IoSlice::new(b"carrier")]);
        }
        let wait = sender.credit_wait();
        let (complete, done) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            wait.wait();
            let _ = complete.send(());
        });
        done.recv_timeout(Duration::from_secs(2))
            .expect("a credit wait must stay bounded");
        drop(sender);
        drop(held);
        drop(receiver);
    }
}
