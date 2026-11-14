use super::*;

#[test]
fn maximum_message_and_part_counts_preserve_empty_multipart_messages() {
    let mut body = Vec::new();
    for _ in 0..MAX_ARENA_MESSAGES {
        for _ in 0..15 {
            body.extend_from_slice(&[FLAG_MORE, 0]);
        }

        body.extend_from_slice(&[0, 0]);
    }

    let body = Bytes::from(body);
    let limit = 16 * size_of::<Payload>();
    let limits = |size, parts| ArenaLimits {
        size,
        payload_slots: true,
        parts,
    };

    let mut arena = DecodedArena::new(
        body.clone(),
        body.len(),
        MAX_ARENA_MESSAGES,
        limits(Some(limit), Some(16)),
    )
    .unwrap();

    for _ in 0..MAX_ARENA_MESSAGES {
        let message = arena.next().unwrap();
        assert_eq!(message.len(), 16);
        assert_eq!(message.byte_len(), 0);
    }

    assert!(arena.is_empty());
    assert!(arena.next().is_none());

    assert!(matches!(
        DecodedArena::new(
            body.clone(),
            body.len(),
            MAX_ARENA_MESSAGES,
            limits(Some(limit - 1), None),
        ),
        Err(Error::MessageTooLarge { .. })
    ));

    // One part over the per-message part limit.
    assert!(matches!(
        DecodedArena::new(
            body.clone(),
            body.len(),
            MAX_ARENA_MESSAGES,
            limits(None, Some(15)),
        ),
        Err(Error::Protocol(_))
    ));

    let mut excessive = body.to_vec();
    excessive.splice(0..0, [FLAG_MORE, 0]);

    assert!(
        DecodedArena::new(
            Bytes::from(excessive),
            body.len() + 2,
            MAX_ARENA_MESSAGES,
            ArenaLimits::default(),
        )
        .is_err()
    );
}

#[test]
fn full_size_long_frame_rejects_every_truncation_and_overflow_length() {
    let payload_size = MAX_ARENA_BYTES - 9;
    let mut body = vec![FLAG_LONG];
    body.extend_from_slice(&u64::try_from(payload_size).unwrap().to_be_bytes());
    body.extend((0..payload_size).map(|index| index.to_le_bytes()[0]));
    let body = Bytes::from(body);

    for length in 0..body.len() {
        assert!(
            DecodedArena::new(body.slice(..length), body.len(), 1, ArenaLimits::default()).is_err()
        );
    }

    let limits = ArenaLimits {
        size: Some(payload_size),
        ..ArenaLimits::default()
    };
    let mut arena = DecodedArena::new(body.clone(), body.len(), 1, limits).unwrap();
    let message = arena.next().unwrap();
    assert_eq!(message.part_slice(0).unwrap(), &body[9..]);
    assert!(arena.is_empty());
    let mut overflow = vec![FLAG_LONG];
    overflow.extend_from_slice(&u64::MAX.to_be_bytes());
    assert!(DecodedArena::new(Bytes::from(overflow), 9, 1, ArenaLimits::default()).is_err());
}
