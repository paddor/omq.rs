use super::{ALIGN_LIMIT, PAYLOAD_ALIGN, aligned_if_small, line_aligned};
use bytes::Bytes;

#[test]
fn line_aligned_bodies_start_on_their_own_cache_line_pair() {
    for size in [16, 64, 100, 128, 4096] {
        let body: Vec<u8> = (0..size).map(|i| i as u8).collect();
        let aligned = line_aligned(&body);

        assert_eq!(aligned, body);
        assert_eq!(aligned.as_ptr() as usize % PAYLOAD_ALIGN, 0);
    }
}

#[test]
fn bodies_below_the_align_limit_are_line_aligned() {
    let body = Bytes::from(vec![b'x'; ALIGN_LIMIT - 1]);
    let aligned = aligned_if_small(body.clone());

    assert_eq!(aligned, body);
    assert_eq!(aligned.as_ptr() as usize % PAYLOAD_ALIGN, 0);
}

#[test]
fn bodies_from_the_align_limit_keep_their_allocation() {
    let body = Bytes::from(vec![b'x'; ALIGN_LIMIT]);
    let ptr = body.as_ptr();

    assert_eq!(aligned_if_small(body).as_ptr(), ptr);
}
