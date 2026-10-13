//! Storage, leases, and retained views using the production pointer boundary.
#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::{
    batch,
    carrier::Carrier,
    coordination::{BORROWED, GENERATION_STEP},
    lease::{Borrowed, Lease, Table},
    mapping::{DATA, HEADER, LEASES, Mapping},
    pool::find_slot,
    process::Process,
    views::Views,
    wire, *,
};
use omq_proto::{Message, Payload};

fn table(count: usize) -> Arc<Table> {
    Arc::new(Table {
        mapping: Mapping::create(Carrier::length(count), LEASES).unwrap(),
        count,
        stopped: AtomicBool::new(false),
        carrier: Carrier::testing(count),
    })
}

fn publish(
    message: &Message,
    table: &Arc<Table>,
    index: usize,
    generation: u64,
    kind: u8,
) -> wire::Reference {
    let body = message.part_slice(message.len() - 1).unwrap();
    let slot = find_slot(body).unwrap();
    let reference = wire::Reference {
        kind,
        id: slot.mapping.id,
        token: slot.mapping.token,
        offset: body.as_ptr() as usize - slot.mapping.address(),
        length: body.len(),
        lease: index,
        generation,
    };
    assert!(!table.publish(reference));
    slot.track_lease(
        Lease {
            table: table.clone(),
            peer: Arc::new(Process::testing()),
            index,
            generation,
        },
        kind,
    );
    reference
}

fn borrow(
    message: &Message,
    table: &Arc<Table>,
    reference: wire::Reference,
    views: &Views,
) -> Payload {
    table.borrow(reference).unwrap();
    let slot = find_slot(message.part_slice(message.len() - 1).unwrap()).unwrap();
    views.borrowed(Borrowed {
        mapping: slot.mapping.clone(),
        offset: reference.offset,
        length: reference.length,
        table: table.clone(),
        index: reference.lease,
        generation: reference.generation,
    })
}

#[test]
fn slab_slices_keep_borrowed_views_live_while_another_slot_is_mutated() {
    let sources = payload_pool([(1024, 2)]).unwrap();
    let mut buffer = sources.try_buffer(1024).unwrap();
    for (index, body) in buffer
        .writable()
        .as_chunks_mut::<64>()
        .0
        .iter_mut()
        .enumerate()
    {
        body.fill(index as u8);
    }
    buffer.set_len(1024).unwrap();
    let mut bytes: bytes::Bytes = buffer.into_payload().into();
    let messages: Vec<_> = (0..16).map(|_| Message::from(bytes.split_to(64))).collect();
    drop(bytes);
    let metadata = payload_pool([(1024, 1)]).unwrap();
    let packed = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let slot = find_slot(packed.part_slice(0).unwrap()).unwrap();
    let regions = slot.state.lock().unwrap().regions.clone();
    let table = table(1);
    let reference = publish(&packed, &table, 0, GENERATION_STEP, wire::REFERENCE_BATCH);
    let views = Views::new(1);
    let received = Message::from(borrow(&packed, &table, reference, &views));
    let mut cursor = batch::ReceivedBatch::referenced(&received, None, &views, |id, token| {
        regions
            .iter()
            .find(|region| region.id == id && region.token == token)
            .cloned()
    })
    .unwrap();
    let held = cursor
        .pop_front()
        .unwrap()
        .part_bytes(0)
        .unwrap()
        .slice(1..17);
    drop(cursor);
    drop(received);
    drop(packed);
    drop(messages);
    table.close_reader();
    let spare = sources.message(1024, |body| body.fill(9)).unwrap();
    assert_eq!(spare.part_slice(0).unwrap(), &[9; 1024]);
    assert_eq!(held.as_ref(), &[0; 16]);
    drop(spare);
    assert_eq!(sources.available(), 1);
    drop(held);
    assert_eq!(sources.available(), 2);
    assert_eq!(metadata.available(), 1);
}

#[test]
fn disjoint_slots_and_slices_survive_other_slot_mutation() {
    let pool = payload_pool([(128, 2)]).unwrap();
    let first = pool.try_message(128, |body| body.fill(1)).unwrap().unwrap();
    let bytes = first.part_bytes(0).unwrap().slice(17..117);
    let second = pool.try_message(128, |body| body.fill(2)).unwrap().unwrap();
    assert_eq!(bytes.as_ref(), &[1; 100]);
    assert_eq!(second.part_slice(0).unwrap(), &[2; 128]);
    drop(second);
    let second = pool.try_message(128, |body| body.fill(3)).unwrap().unwrap();
    assert_eq!(bytes.as_ref(), &[1; 100]);
    assert_eq!(second.part_slice(0).unwrap(), &[3; 128]);
    drop(first);
    drop(pool);
    assert_eq!(bytes.as_ref(), &[1; 100]);
}

#[test]
fn inline_boundaries_do_not_create_mapping_owners() {
    let pool = payload_pool([(128, 1)]).unwrap();
    for size in [0, 1, 16, 54, 55, 56, 62, 63, 64, 127, 128] {
        let message = pool.message(size, |body| body.fill(7)).unwrap();
        assert_eq!(message.part_slice(0).unwrap(), vec![7; size]);
        if size <= 55 {
            assert!(!is_pooled(&message));
        }
        if size > 62 {
            assert!(is_pooled(&message));
        }
        drop(message);
        assert_eq!(pool.available(), 1);
    }
}

#[test]
fn final_local_byte_view_delays_reuse() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let message = pool.message(128, |body| body.fill(7)).unwrap();
    let clone = message.clone();
    let bytes = message.part_bytes(0).unwrap();
    drop(message);
    drop(clone);
    assert!(
        pool.try_message(128, |_| panic!("retained slot became writable"))
            .unwrap()
            .is_none()
    );
    assert_eq!(bytes.as_ref(), &[7; 128]);
    drop(bytes);
    assert_eq!(
        pool.try_message(128, |body| body.fill(9))
            .unwrap()
            .unwrap()
            .part_slice(0)
            .unwrap(),
        &[9; 128]
    );
}

#[test]
fn panic_during_checkout_returns_exclusive_storage() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.message(128, |body| {
            body[0] = 7;
            panic!("fill failed");
        })
    }));
    assert!(panic.is_err());
    assert_eq!(pool.available(), 1);
    assert_eq!(
        pool.message(128, |body| body.fill(9))
            .unwrap()
            .part_slice(0)
            .unwrap(),
        &[9; 128]
    );
}

#[test]
fn pool_lookup_rejects_foreign_and_cross_slot_ranges() {
    let pool = payload_pool([(128, 2)]).unwrap();
    let message = pool.message(128, |body| body.fill(7)).unwrap();
    let body = message.part_slice(0).unwrap();
    let slot = find_slot(body).unwrap();
    assert!(find_slot(&body[1..]).is_some());
    assert!(find_slot(slot.mapping.bytes(slot.offset + 127, 2)).is_none());
    assert!(find_slot(&[7; 128]).is_none());
    drop(message);
    drop(pool);
    assert!(find_slot(slot.mapping.bytes(slot.offset, 128)).is_some());
    let mapping = slot.mapping.clone();
    let offset = slot.offset;
    drop(slot);
    assert!(find_slot(mapping.bytes(offset, 128)).is_none());
}

#[test]
fn fanout_reuses_only_after_every_recipient_and_local_view_releases() {
    for reverse in [false, true] {
        let pool = payload_pool([(128, 1)]).unwrap();
        let message = pool.message(128, |body| body.fill(7)).unwrap();
        let views = Views::new(1);
        let tables = [table(1), table(1)];
        let references = tables
            .each_ref()
            .map(|table| publish(&message, table, 0, GENERATION_STEP, wire::BODY));
        let received = [
            borrow(&message, &tables[0], references[0], &views),
            borrow(&message, &tables[1], references[1], &views),
        ];
        let [first, second] = received;
        let (first, second) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        let local = message.part_bytes(0).unwrap();
        let bytes = second.as_bytes().slice(3..125);
        drop(message);
        for table in &tables {
            table.close_reader();
        }
        drop(first);
        drop(second);
        assert_eq!(pool.available(), 0);
        assert_eq!(bytes.as_ref(), &[7; 122]);
        drop(bytes);
        assert_eq!(pool.available(), 0);
        drop(local);
        assert_eq!(pool.available(), 1);
    }
}

#[test]
fn producer_pool_and_view_cache_teardown_preserve_borrowed_bytes() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let message = pool.message(128, |body| body.fill(7)).unwrap();
    let table = table(1);
    let views = Views::new(1);
    let reference = publish(&message, &table, 0, GENERATION_STEP, wire::BODY);
    let payload = borrow(&message, &table, reference, &views);
    let bytes = payload.as_bytes();
    drop(message);
    drop(pool);
    drop(views);
    drop(payload);
    table.close_reader();
    assert!(!table.lease_cell(0).completed(GENERATION_STEP));
    assert_eq!(bytes.as_ref(), &[7; 128]);
    drop(bytes);
    assert!(table.lease_cell(0).completed(GENERATION_STEP));
}

#[test]
fn stale_duplicate_and_wrong_coordinate_borrows_cannot_alias_writable_storage() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let message = pool.message(128, |body| body.fill(7)).unwrap();
    for field in 0..crate::coordination::COORDINATES {
        let table = table(1);
        let mut reference = publish(&message, &table, 0, GENERATION_STEP, wire::BODY);
        match field {
            0 => reference.id ^= 1,
            1 => reference.token ^= 1,
            2 => reference.offset += 1,
            3 => reference.length -= 1,
            _ => reference.kind ^= 1,
        }
        assert!(table.borrow(reference).is_err());
        assert!(table.lease_cell(0).completed(GENERATION_STEP));
    }
    let table = table(1);
    let reference = publish(&message, &table, 0, 2 * GENERATION_STEP, wire::BODY);
    let mut stale = reference;
    stale.generation = GENERATION_STEP;
    assert!(table.borrow(stale).is_err());
    assert!(!table.lease_cell(0).completed(reference.generation));
    assert!(table.borrow(reference).is_ok());
    assert!(table.borrow(reference).is_err());
    table.complete(0, reference.generation, BORROWED);
}

#[test]
fn invalid_lease_indices_generations_and_closed_tables_are_rejected() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let message = pool.message(128, |body| body.fill(7)).unwrap();
    let table = table(1);
    let reference = publish(&message, &table, 0, GENERATION_STEP, wire::BODY);
    for generation in [0, 1, 2, 3, 5, u64::MAX] {
        let mut invalid = reference;
        invalid.generation = generation;
        assert!(table.borrow(invalid).is_err());
    }
    for lease in [1, usize::MAX] {
        let mut invalid = reference;
        invalid.lease = lease;
        assert!(table.borrow(invalid).is_err());
    }
    table.close_reader();
    assert!(table.borrow(reference).is_err());
}

#[test]
fn mapping_bounds_and_atomic_alignment_are_checked_before_dereference() {
    let mapping = Mapping::create(HEADER + 16, LEASES).unwrap();
    for offset in 0..HEADER + 16 {
        for width in [4, 8] {
            let valid = offset.is_multiple_of(width) && offset <= mapping.len() - width;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if width == 4 {
                    mapping.atomic32(offset).load(Ordering::Relaxed);
                } else {
                    mapping.atomic64(offset).load(Ordering::Relaxed);
                }
            }));
            assert_eq!(result.is_ok(), valid);
        }
    }
    assert!(!mapping.contains(HEADER - 1, 1));
    assert!(mapping.contains(mapping.len(), 0));
    assert!(!mapping.contains(usize::MAX, 1));
    for (offset, length) in [(usize::MAX, 1), (0, usize::MAX), (mapping.len(), 1)] {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || mapping.bytes(offset, length)
            ))
            .is_err()
        );
    }
    assert_eq!(mapping.bytes(mapping.len(), 0), &[]);
    for length in [0, HEADER - 1, usize::MAX] {
        assert!(Mapping::create(length, DATA).is_err());
    }
}

#[test]
fn carrier_wraparound_partial_slices_and_returned_space_do_not_overlap() {
    let carrier = Carrier::testing(1);
    let mapping = Mapping::create(Carrier::length(1), LEASES).unwrap();
    let capacity = crate::carrier::CAPACITY;
    let first = vec![7; capacity - 3];
    assert_eq!(
        carrier
            .write(&mapping, &[std::io::IoSlice::new(&first)])
            .unwrap(),
        first.len()
    );
    let mut output = vec![0; capacity - 7];
    assert_eq!(carrier.read(&mapping, &mut output).unwrap(), output.len());
    assert_eq!(output, vec![7; output.len()]);
    let second = vec![9; capacity];
    assert_eq!(
        carrier
            .write(
                &mapping,
                &[std::io::IoSlice::new(&[]), std::io::IoSlice::new(&second)]
            )
            .unwrap(),
        capacity - 4
    );
    assert_eq!(
        carrier
            .write(&mapping, &[std::io::IoSlice::new(&[1])])
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    output.resize(capacity, 0);
    assert_eq!(carrier.read(&mapping, &mut output).unwrap(), capacity);
    assert_eq!(&output[..4], &[7; 4]);
    assert!(output[4..].iter().all(|&byte| byte == 9));
}

fn grouped(pool: &omq_proto::PayloadPool, size: usize, value: u8) -> Message {
    Message::with_prefix(
        b"g".as_slice().into(),
        pool.message(size, |body| body.fill(value)).unwrap(),
    )
    .with_routing_id(7)
}

#[test]
fn single_frame_batches_retain_storage_and_release_the_final_view() {
    for referenced in [false, true] {
        let sources = payload_pool([(256, 3)]).unwrap();
        let metadata = payload_pool([(1024, 1)]).unwrap();
        let sizes = if referenced {
            [64, 128, 256]
        } else {
            [0, 16, 256]
        };
        let messages: Vec<_> = sizes
            .into_iter()
            .map(|size| {
                sources
                    .try_message(size, |bytes| bytes.fill(7))
                    .unwrap()
                    .unwrap()
            })
            .collect();
        let references: Vec<_> = messages.iter().collect();
        let packed = if referenced {
            prepare_reference_batch(&metadata, &references)
        } else {
            prepare_batch(&metadata, &references)
        }
        .unwrap()
        .unwrap();
        let slot = find_slot(packed.part_slice(0).unwrap()).unwrap();
        let regions = slot.state.lock().unwrap().regions.clone();
        let table = table(1);
        let kind = if referenced {
            wire::REFERENCE_BATCH
        } else {
            wire::COPIED_BATCH
        };
        let reference = publish(&packed, &table, 0, GENERATION_STEP, kind);
        let views = Views::new(3);
        let received = Message::from(borrow(&packed, &table, reference, &views));
        let mut decoded = VecDeque::new();
        if referenced {
            batch::decode_references(
                &received,
                None,
                &views,
                |id, token| {
                    regions
                        .iter()
                        .find(|region| region.id == id && region.token == token)
                        .cloned()
                },
                &mut decoded,
            )
            .unwrap();
        } else {
            batch::decode(&received, None, &views, &mut decoded).unwrap();
        }
        assert_eq!(decoded.iter().collect::<Vec<_>>(), references);
        let retained = decoded.back().unwrap().part_bytes(0).unwrap();
        drop(decoded);
        drop(received);
        drop(packed);
        drop(messages);
        drop(views);
        assert_eq!(metadata.available(), 0);
        assert_eq!(retained.as_ref(), &[7; 256]);
        drop(retained);
        assert_eq!(metadata.available(), 1);
        assert_eq!(sources.available(), 3);
    }
}

#[test]
fn copied_batch_children_hold_the_shared_lease_until_the_final_byte_view() {
    let metadata = payload_pool([(1024, 1)]).unwrap();
    let messages: Vec<_> = [16, 128, 256]
        .into_iter()
        .map(|size| Message::with_group("g", vec![7; size]))
        .collect();
    let packed = prepare_batch(&metadata, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let table = table(1);
    let reference = publish(&packed, &table, 0, GENERATION_STEP, wire::COPIED_BATCH);
    let views = Views::new(1);
    let body = borrow(&packed, &table, reference, &views);
    let received = Message::with_prefix(b"g".as_slice().into(), Message::from(body));
    let mut decoded = VecDeque::new();
    batch::decode(&received, None, &views, &mut decoded).unwrap();
    assert_eq!(decoded.into_iter().collect::<Vec<_>>(), messages);
    let mut decoded = VecDeque::new();
    batch::decode(&received, None, &views, &mut decoded).unwrap();
    let bytes = decoded.back().unwrap().part_bytes(1).unwrap();
    drop(packed);
    drop(received);
    drop(views);
    drop(decoded);
    table.close_reader();
    assert_eq!(metadata.available(), 0);
    assert_eq!(bytes.as_ref(), &[7; 256]);
    drop(bytes);
    assert_eq!(metadata.available(), 1);
}

#[test]
fn shared_batch_byte_slices_survive_concurrent_final_drops() {
    for referenced in [false, true] {
        let sources = payload_pool([(128, 3)]).unwrap();
        let metadata = payload_pool([(1024, 1)]).unwrap();
        let messages: Vec<_> = (0..3)
            .map(|value| sources.message(128, |body| body.fill(value)).unwrap())
            .collect();
        let bodies: Vec<_> = messages.iter().collect();
        let packed = if referenced {
            prepare_reference_batch(&metadata, &bodies)
        } else {
            prepare_batch(&metadata, &bodies)
        }
        .unwrap()
        .unwrap();
        let slot = find_slot(packed.part_slice(0).unwrap()).unwrap();
        let regions = slot.state.lock().unwrap().regions.clone();
        let table = table(1);
        let kind = if referenced {
            wire::REFERENCE_BATCH
        } else {
            wire::COPIED_BATCH
        };
        let reference = publish(&packed, &table, 0, GENERATION_STEP, kind);
        let views = Views::new(1);
        let received = Message::from(borrow(&packed, &table, reference, &views));
        let mut decoded = VecDeque::new();
        if referenced {
            batch::decode_references(
                &received,
                None,
                &views,
                |id, token| {
                    regions
                        .iter()
                        .find(|region| region.id == id && region.token == token)
                        .cloned()
                },
                &mut decoded,
            )
            .unwrap();
        } else {
            batch::decode(&received, None, &views, &mut decoded).unwrap();
        }
        assert_eq!(decoded.iter().collect::<Vec<_>>(), bodies);
        assert!(
            decoded
                .iter()
                .all(|message| message.last_payload().unwrap().as_chunk().is_some())
        );
        let retained = decoded[1].part_bytes(0).unwrap().slice(3..19);
        let bytes: Vec<_> = decoded
            .iter()
            .map(|message| message.part_bytes(0).unwrap())
            .collect();
        drop(decoded);
        drop(received);
        drop(packed);
        drop(messages);
        drop(slot);
        drop(regions);
        drop(views);
        table.close_reader();
        assert_eq!(metadata.available(), 0);
        let handles: Vec<_> = bytes
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                std::thread::spawn(move || assert_eq!(bytes.as_ref(), vec![index as u8; 128]))
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(metadata.available(), 0);
        assert_eq!(retained.as_ref(), &[1; 16]);
        drop(retained);
        assert_eq!(metadata.available(), 1);
        assert_eq!(sources.available(), 3);
    }
}

#[test]
fn reference_batch_gaps_remain_writable_while_neighbor_views_are_alive() {
    for size in [64, 128] {
        let sources = payload_pool([(128, 3)]).unwrap();
        let metadata = payload_pool([(256, 1)]).unwrap();
        let first = sources.message(size, |body| body.fill(1)).unwrap();
        let gap = sources.message(128, |body| body.fill(2)).unwrap();
        let last = sources.message(size, |body| body.fill(3)).unwrap();
        let packed = prepare_reference_batch(&metadata, &[&first, &last])
            .unwrap()
            .unwrap();
        let slot = find_slot(packed.part_slice(0).unwrap()).unwrap();
        let regions = slot.state.lock().unwrap().regions.clone();
        let table = table(1);
        let reference = publish(&packed, &table, 0, GENERATION_STEP, wire::REFERENCE_BATCH);
        let views = Views::new(1);
        let received = Message::from(borrow(&packed, &table, reference, &views));
        let mut decoded = VecDeque::new();
        batch::decode_references(
            &received,
            None,
            &views,
            |id, token| {
                regions
                    .iter()
                    .find(|region| region.id == id && region.token == token)
                    .cloned()
            },
            &mut decoded,
        )
        .unwrap();
        assert!(
            decoded
                .iter()
                .all(|message| message.last_payload().unwrap().as_chunk().is_none())
        );
        drop(first);
        drop(last);
        drop(packed);
        drop(received);
        drop(gap);
        let reused = sources
            .try_message(128, |body| body.fill(9))
            .unwrap()
            .unwrap();
        assert_eq!(reused.part_slice(0).unwrap(), &[9; 128]);
        assert_eq!(decoded[0].part_slice(0).unwrap(), vec![1; size]);
        assert_eq!(decoded[1].part_slice(0).unwrap(), vec![3; size]);
        drop(reused);
        drop(decoded);
        assert_eq!(sources.available(), 3);
        assert_eq!(metadata.available(), 1);
    }
}

#[test]
fn reference_run_validation_checks_every_body_and_retains_empty_views() {
    #[derive(Debug)]
    struct EditedBatch {
        bytes: Vec<u8>,
        _source: Message,
    }
    impl AsRef<[u8]> for EditedBatch {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    let sources = payload_pool([(128, 4)]).unwrap();
    let metadata = payload_pool([(256, 1)]).unwrap();
    let messages: Vec<_> = (0..4)
        .map(|value| sources.message(128, |body| body.fill(value)).unwrap())
        .collect();
    let packed = prepare_reference_batch(&metadata, &messages.iter().collect::<Vec<_>>())
        .unwrap()
        .unwrap();
    let slot = find_slot(packed.part_slice(0).unwrap()).unwrap();
    let regions = slot.state.lock().unwrap().regions.clone();
    let views = Views::new(1);
    let lookup = |id, token| {
        regions
            .iter()
            .find(|region| region.id == id && region.token == token)
            .cloned()
    };
    let mut invalid = packed.part_slice(0).unwrap().to_vec();
    invalid[batch::COUNT_BYTES + batch::SIZE_BYTES..batch::COUNT_BYTES + 2 * batch::SIZE_BYTES]
        .copy_from_slice(&256_u64.to_le_bytes());
    let mut decoded = VecDeque::new();
    assert!(
        batch::decode_references(
            &Message::from_slice(&invalid),
            Some(128),
            &views,
            lookup,
            &mut decoded
        )
        .is_err()
    );
    assert!(
        decoded.is_empty(),
        "validate a whole run before borrowing its span"
    );
    let mut bytes = packed.part_slice(0).unwrap().to_vec();
    bytes[batch::COUNT_BYTES + batch::SIZE_BYTES..batch::COUNT_BYTES + 2 * batch::SIZE_BYTES]
        .fill(0);
    let edited = Message::from(bytes::Bytes::from_owner(EditedBatch {
        bytes,
        _source: packed.clone(),
    }));
    batch::decode_references(&edited, None, &views, lookup, &mut decoded).unwrap();
    assert_eq!(decoded[1].part_slice(0).unwrap(), []);
    assert!(decoded[1].last_payload().unwrap().as_chunk().is_none());
    assert!(decoded[2].last_payload().unwrap().as_chunk().is_some());
    let empty = decoded[1].part_bytes(0).unwrap();
    drop(decoded);
    drop(edited);
    drop(packed);
    drop(messages);
    drop(slot);
    drop(regions);
    drop(views);
    assert!(empty.is_empty());
    assert_eq!(sources.available(), 0);
    drop(empty);
    assert_eq!(sources.available(), 4);
    assert_eq!(metadata.available(), 1);
}

#[test]
fn referenced_batches_retain_every_source_until_all_recipient_views_release() {
    let sources = payload_pool([(256, 2)]).unwrap();
    let metadata = payload_pool([(256, 1)]).unwrap();
    let messages = [grouped(&sources, 128, 7), grouped(&sources, 256, 9)];
    let packed = prepare_reference_batch(&metadata, &[&messages[0], &messages[1]])
        .unwrap()
        .unwrap();
    let slot = find_slot(packed.part_slice(1).unwrap()).unwrap();
    let regions = slot.state.lock().unwrap().regions.clone();
    let tables = [table(1), table(1)];
    let views = Views::new(1);
    let mut recipients = Vec::new();
    for table in &tables {
        let reference = publish(&packed, table, 0, GENERATION_STEP, wire::REFERENCE_BATCH);
        let body = borrow(&packed, table, reference, &views);
        let received = Message::with_prefix(b"g".as_slice().into(), Message::from(body));
        let mut decoded = VecDeque::new();
        batch::decode_references(
            &received,
            None,
            &views,
            |id, token| {
                regions
                    .iter()
                    .find(|region| region.id == id && region.token == token)
                    .cloned()
            },
            &mut decoded,
        )
        .unwrap();
        assert_eq!(decoded[0].part_slice(1).unwrap(), &[7; 128]);
        assert_eq!(decoded[1].part_slice(1).unwrap(), &[9; 256]);
        recipients.push(decoded);
    }
    let bytes = recipients[1][0].part_bytes(1).unwrap();
    drop(messages);
    drop(packed);
    drop(slot);
    drop(regions);
    drop(metadata);
    drop(views);
    for table in &tables {
        table.close_reader();
    }
    drop(recipients);
    assert_eq!(sources.available(), 0);
    assert_eq!(bytes.as_ref(), &[7; 128]);
    drop(bytes);
    assert_eq!(sources.available(), 2);
}

#[test]
fn malformed_batches_and_reference_records_never_dereference_invalid_coordinates() {
    let views = Views::new(1);
    for length in 0..80 {
        let bytes = vec![0; length];
        let message = Message::with_group("g", bytes.clone());
        assert!(batch::decode(&message, None, &views, &mut VecDeque::new()).is_err());
        assert!(batch::references(&bytes).is_err());
        if length != wire::REFERENCE_SIZE {
            assert!(wire::Reference::decode(&bytes, wire::BODY).is_err());
        }
    }
    let sources = payload_pool([(128, 2)]).unwrap();
    let metadata = payload_pool([(256, 1)]).unwrap();
    let messages = [grouped(&sources, 128, 7), grouped(&sources, 128, 9)];
    let packed = prepare_reference_batch(&metadata, &[&messages[0], &messages[1]])
        .unwrap()
        .unwrap();
    assert!(
        batch::decode_references(&packed, None, &views, |_, _| None, &mut VecDeque::new()).is_err()
    );
    for index in 0..2 {
        let mut bytes = packed.part_slice(1).unwrap().to_vec();
        let size = batch::COUNT_BYTES + index * batch::SIZE_BYTES;
        bytes[size..size + batch::SIZE_BYTES].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(batch::references(&bytes).is_err());
    }
}

#[test]
fn concurrent_view_clones_keep_pool_slots_read_only_until_final_release() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let message = pool.message(128, |body| body.fill(7)).unwrap();
    let table = table(1);
    let views = Views::new(1);
    let reference = publish(&message, &table, 0, GENERATION_STEP, wire::BODY);
    let received = borrow(&message, &table, reference, &views);
    let clones = [received.clone(), received.clone()];
    drop(message);
    drop(received);
    std::thread::scope(|scope| {
        for clone in clones {
            scope.spawn(move || {
                assert_eq!(clone.as_slice(), &[7; 128]);
                drop(clone);
            });
        }
        assert!(
            pool.try_message(128, |body| body.fill(9))
                .unwrap()
                .is_none_or(|message| message.part_slice(0).unwrap() == [9; 128])
        );
    });
    assert_eq!(pool.available(), 1);
}

#[test]
fn automatic_memory_limits_follow_retained_mapping_owners() {
    let pool = SendPool::new(1, Some(HEADER + 128));
    let first = pool
        .prepare(&Message::from_slice(&[7; 128]))
        .unwrap()
        .unwrap();
    let bytes = first.part_bytes(0).unwrap();
    assert_eq!(pool.memory_used(), HEADER + 128);
    drop(first);
    assert!(
        pool.prepare(&Message::from_slice(&[9; 256]))
            .unwrap()
            .is_none()
    );
    assert_eq!(bytes.as_ref(), &[7; 128]);
    drop(bytes);
    assert!(
        pool.prepare(&Message::from_slice(&[9; 128]))
            .unwrap()
            .is_some()
    );
    assert_eq!(pool.memory_used(), HEADER + 128);
    assert!(payload_pool([(usize::MAX, 2)]).is_err());
}

#[test]
fn inline_copied_batch_children_do_not_pin_shared_storage() {
    let pool = payload_pool([(128, 1)]).unwrap();
    let messages = [
        Message::with_group("g", vec![1; 16]),
        Message::with_group("g", vec![2; 16]),
    ];
    let packed = prepare_batch(&pool, &[&messages[0], &messages[1]])
        .unwrap()
        .unwrap();
    let table = table(1);
    let views = Views::new(1);
    let reference = publish(&packed, &table, 0, GENERATION_STEP, wire::COPIED_BATCH);
    let received = Message::with_prefix(
        b"g".as_slice().into(),
        Message::from(borrow(&packed, &table, reference, &views)),
    );
    let mut decoded = VecDeque::new();
    batch::decode(&received, None, &views, &mut decoded).unwrap();
    drop(packed);
    drop(received);
    drop(views);
    assert_eq!(pool.available(), 1);
    assert_eq!(decoded.into_iter().collect::<Vec<_>>(), messages);
}

#[test]
fn rejected_batch_creates_no_views_and_releases_its_storage() {
    let pool = payload_pool([(512, 1)]).unwrap();
    let messages = [
        Message::with_group("g", vec![1; 64]),
        Message::with_group("g", vec![2; 128]),
    ];
    let packed = prepare_batch(&pool, &[&messages[0], &messages[1]])
        .unwrap()
        .unwrap();
    let table = table(1);
    let views = Views::new(1);
    let reference = publish(&packed, &table, 0, GENERATION_STEP, wire::COPIED_BATCH);
    let received = Message::with_prefix(
        b"g".as_slice().into(),
        Message::from(borrow(&packed, &table, reference, &views)),
    );
    let mut decoded = VecDeque::new();
    assert!(batch::decode(&received, Some(200), &views, &mut decoded).is_err());
    assert!(decoded.is_empty());
    drop(packed);
    drop(received);
    assert_eq!(pool.available(), 1);
}

#[test]
fn malformed_counts_padding_and_lengths_do_not_create_shared_views() {
    let views = Views::new(1);
    for count in [0_u16, 1, (BATCH_MAX_MESSAGES + 1) as u16, u16::MAX] {
        let mut bytes = vec![0; 128];
        bytes[..batch::COUNT_BYTES].copy_from_slice(&count.to_le_bytes());
        assert!(batch::references(&bytes).is_err());
        assert!(
            batch::decode(
                &Message::with_group("g", bytes),
                None,
                &views,
                &mut VecDeque::new()
            )
            .is_err()
        );
    }
    let pool = payload_pool([(256, 1)]).unwrap();
    let messages = [
        Message::with_group("g", vec![7; 1]),
        Message::with_group("g", vec![9; 1]),
    ];
    let packed = prepare_batch(&pool, &[&messages[0], &messages[1]])
        .unwrap()
        .unwrap();
    let bytes = packed.part_slice(1).unwrap();
    for length in 0..bytes.len() {
        assert!(
            batch::decode(
                &Message::with_group("g", bytes[..length].to_vec()),
                None,
                &views,
                &mut VecDeque::new()
            )
            .is_err()
        );
    }
    let mut bad = bytes.to_vec();
    *bad.last_mut().unwrap() = 1;
    assert!(
        batch::decode(
            &Message::with_group("g", bad),
            None,
            &views,
            &mut VecDeque::new()
        )
        .is_err()
    );
}

#[test]
fn preface_and_reference_roundtrips_check_identity_and_truncation() {
    for count in [1, MAX_LEASES] {
        let bytes = wire::Preface {
            id: 7,
            count,
            token: u64::MAX,
        }
        .encode();
        let preface = wire::Preface::decode(&bytes).unwrap();
        assert_eq!(
            (preface.id, preface.count, preface.token),
            (7, count, u64::MAX)
        );
    }
    for count in [0, MAX_LEASES + 1] {
        let bytes = wire::Preface {
            id: 7,
            count,
            token: 1,
        }
        .encode();
        assert!(wire::Preface::decode(&bytes).is_err());
    }
    let mut bytes = wire::Preface {
        id: 7,
        count: 1,
        token: 1,
    }
    .encode();
    bytes[0] ^= 1;
    assert!(wire::Preface::decode(&bytes).is_err());
    let reference = wire::Reference {
        kind: wire::BODY,
        id: 7,
        token: 9,
        offset: HEADER,
        length: 128,
        lease: 0,
        generation: GENERATION_STEP,
    };
    let bytes = reference.encode();
    let decoded = wire::Reference::decode(&bytes, wire::BODY).unwrap();
    assert_eq!(decoded.coordinates(), reference.coordinates());
    assert_eq!(decoded.generation, reference.generation);
    for length in 0..bytes.len() {
        assert!(wire::Reference::decode(&bytes[..length], wire::BODY).is_err());
    }
    assert!(wire::Reference::decode(&bytes, wire::COPIED_BATCH).is_err());
}

#[test]
fn unpublished_reference_batch_releases_sources_after_final_local_owner() {
    let sources = payload_pool([(128, 2)]).unwrap();
    let metadata = payload_pool([(256, 1)]).unwrap();
    let messages = [grouped(&sources, 128, 7), grouped(&sources, 128, 9)];
    let packed = prepare_reference_batch(&metadata, &[&messages[0], &messages[1]])
        .unwrap()
        .unwrap();
    let local = packed.part_bytes(1).unwrap();
    let clone = packed.clone();
    drop(messages);
    drop(packed);
    assert_eq!(sources.available(), 0);
    drop(clone);
    assert_eq!(sources.available(), 0);
    drop(local);
    assert_eq!(sources.available(), 2);
    assert_eq!(metadata.available(), 1);
}

#[test]
fn failed_storage_allocation_returns_its_memory_reservation() {
    let budget = crate::memory::Budget::new(Some(128));
    for size in [usize::MAX, isize::MAX as usize] {
        let reservation = crate::memory::Budget::reserve(&budget, 64).unwrap();
        assert_eq!(budget.used(), 64);
        assert!(pool_storage(size, 1, Some(reservation)).is_err());
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn carrier_empty_buffers_and_iovec_limits_preserve_every_nonempty_byte() {
    use std::io::IoSlice;
    let carrier = Carrier::testing(1);
    let mapping = Mapping::create(Carrier::length(1), LEASES).unwrap();
    assert_eq!(carrier.write(&mapping, &[]).unwrap(), 0);
    assert_eq!(carrier.read(&mapping, &mut []).unwrap(), 0);
    assert_eq!(
        carrier.read(&mapping, &mut [0]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let limit = crate::carrier::MAX_SLICES;
    let mut slices = vec![IoSlice::new(&[]); limit + 1];
    slices.extend((0..=limit).map(|_| IoSlice::new(b"x")));
    assert_eq!(carrier.write(&mapping, &slices).unwrap(), limit);
    let mut output = vec![0; limit];
    assert_eq!(carrier.read(&mapping, &mut output).unwrap(), limit);
    assert_eq!(output, vec![b'x'; limit]);
    assert_eq!(carrier.write(&mapping, &[IoSlice::new(b"x")]).unwrap(), 1);
    assert_eq!(carrier.read(&mapping, &mut output).unwrap(), 1);
    assert_eq!(output[0], b'x');
}

#[test]
fn lazy_batch_cursors_and_retained_children_keep_the_lease_across_threads() {
    for referenced in [false, true] {
        let sources = payload_pool([(128, 4)]).unwrap();
        let messages: Vec<_> = (0..4_u8)
            .map(|seq| sources.message(128, |body| body.fill(seq)).unwrap())
            .collect();
        let (mut cursor, metadata, _table) = received_cursor(&messages, referenced);
        let mut prefix = cursor.take_prefix(2);
        drop(messages);
        let retained = prefix
            .pop_front()
            .unwrap()
            .part_bytes(0)
            .unwrap()
            .slice(17..97);
        std::thread::spawn(move || {
            assert_eq!(
                prefix.pop_front().unwrap().part_slice(0).unwrap(),
                &[1; 128]
            );
            assert!(prefix.pop_front().is_none());
        })
        .join()
        .unwrap();
        assert_eq!(
            cursor.pop_front().unwrap().part_slice(0).unwrap(),
            &[2; 128]
        );
        assert_eq!(
            cursor.pop_front().unwrap().part_slice(0).unwrap(),
            &[3; 128]
        );
        drop(cursor);
        assert_eq!(metadata.available(), 0);
        assert_eq!(retained.as_ref(), &[0; 80]);
        drop(retained);
        assert_eq!(metadata.available(), 1);
        assert_eq!(sources.available(), 4);
    }
}

fn received_cursor(
    messages: &[Message],
    referenced: bool,
) -> (ReceivedBatch, omq_proto::PayloadPool, Arc<Table>) {
    let metadata = payload_pool([(8192, 1)]).unwrap();
    let refs: Vec<_> = messages.iter().collect();
    let packed = if referenced {
        prepare_reference_batch(&metadata, &refs)
    } else {
        prepare_batch(&metadata, &refs)
    }
    .unwrap()
    .unwrap();
    let slot = find_slot(crate::body(&packed).unwrap()).unwrap();
    let regions = slot.state.lock().unwrap().regions.clone();
    let table = table(1);
    let kind = if referenced {
        wire::REFERENCE_BATCH
    } else {
        wire::COPIED_BATCH
    };
    let reference = publish(&packed, &table, 0, GENERATION_STEP, kind);
    let views = Views::new(1);
    let mut received = packed.clone();
    received.replace_last_payload(borrow(&packed, &table, reference, &views));
    let cursor = if referenced {
        ReceivedBatch::referenced(&received, None, &views, |id, token| {
            regions
                .iter()
                .find(|region| region.id == id && region.token == token)
                .cloned()
        })
    } else {
        ReceivedBatch::copied(&received, None, &views)
    }
    .unwrap();
    (cursor, metadata, table)
}

#[test]
fn variable_batch_cursors_preserve_split_offsets_routes_and_final_views() {
    for referenced in [false, true] {
        for grouped in [false, true] {
            let sizes = if referenced {
                [56, 64, 256, 257, 511, 512, 1024, 1025]
            } else {
                [0, 1, 55, 56, 64, 256, 257, 1024]
            };
            let sources = payload_pool([(2048, sizes.len())]).unwrap();
            let messages: Vec<_> = sizes
                .iter()
                .enumerate()
                .map(|(index, &size)| {
                    let body = sources
                        .message(size, |body| body.fill(index as u8))
                        .unwrap();
                    let message = if grouped {
                        Message::with_prefix(b"g".as_slice().into(), body)
                    } else {
                        body
                    };
                    message.with_routing_id(7)
                })
                .collect();
            let (mut cursor, metadata, table) = received_cursor(&messages, referenced);
            cursor.set_routing_id(99);
            drop(messages);
            let check = move |message: &Message, index: usize| {
                assert_eq!(message.len(), 1 + usize::from(grouped));
                assert_eq!(message.routing_id(), Some(99));
                assert_eq!(
                    crate::body(message).unwrap(),
                    vec![index as u8; sizes[index]]
                );
                if grouped {
                    assert_eq!(message.part_slice(0).unwrap(), b"g");
                }
            };
            assert!(cursor.take_prefix(0).is_empty());
            check(&cursor.pop_front().unwrap(), 0);
            let mut prefix = cursor.take_prefix(3);
            let mut nested = prefix.take_prefix(1);
            check(&nested.pop_front().unwrap(), 1);
            assert!(nested.pop_front().is_none());
            drop(nested);
            let mut out = Vec::new();
            assert_eq!(prefix.drain_into(&mut out, usize::MAX, |_| false), 1);
            assert_eq!(prefix.drain_into(&mut out, usize::MAX, |_| true), 1);
            check(&out[0], 2);
            check(&out[1], 3);
            out.clear();
            drop(prefix);
            let mut clone = cursor.clone();
            std::thread::spawn(move || {
                for index in 4..sizes.len() {
                    check(&clone.pop_front().unwrap(), index);
                }
                assert!(clone.pop_front().is_none());
            })
            .join()
            .unwrap();
            let first = cursor.pop_front().unwrap();
            let held = first.part_bytes(first.len() - 1).unwrap();
            drop(first);
            let mut tail = cursor.take_prefix(usize::MAX);
            assert!(cursor.is_empty());
            drop(cursor);
            assert_eq!(tail.drain_into(&mut out, usize::MAX, |_| true), 3);
            for (index, message) in out.iter().enumerate() {
                check(message, index + 5);
            }
            drop(out);
            drop(tail);
            table.close_reader();
            assert_eq!(metadata.available(), 0);
            assert_eq!(held.as_ref(), vec![4; sizes[4]]);
            drop(held);
            assert_eq!(metadata.available(), 1);
            assert_eq!(sources.available(), sizes.len());
        }
    }
}
