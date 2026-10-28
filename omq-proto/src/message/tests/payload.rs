use crate::message::{MAX_INLINE_PAYLOAD, Payload};
use bytes::Bytes;

#[test]
fn payload_empty() {
    let p = Payload::new();
    assert_eq!(p.len(), 0);
    assert!(p.is_empty());
    assert_eq!(p.as_bytes(), Bytes::new());
    assert_eq!(p.as_slice(), &[][..]);
}

#[test]
fn payload_single_chunk_as_bytes_is_zero_copy() {
    let b = Bytes::from_static(b"hello");
    let p = Payload::from_bytes(b.clone());
    assert_eq!(p.len(), 5);
    let got = p.as_bytes();
    assert_eq!(got, b);
    assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
}

#[test]
fn payload_from_static_str() {
    let p: Payload = "hello".into();
    assert_eq!(p.len(), 5);
    assert_eq!(p.as_bytes(), &b"hello"[..]);
}

#[test]
fn payload_as_chunk_single_chunk_returns_some() {
    let b = Bytes::from_static(b"hello");
    let p = Payload::from_bytes(b.clone());
    let got = p.as_chunk().expect("single chunk");
    assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
}

#[test]
fn payload_as_chunk_empty_returns_none() {
    let p = Payload::new();
    assert!(p.as_chunk().is_none());
}

#[test]
fn payload_as_bytes_empty_returns_empty() {
    let p = Payload::new();
    assert_eq!(p.as_bytes(), Bytes::new());
}

#[test]
fn payload_as_bytes_single_chunk_is_zero_copy() {
    let b = Bytes::from_static(b"hello");
    let p = Payload::from_bytes(b.clone());
    let got = p.as_bytes();
    assert_eq!(got, b);
    assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
}

#[test]
fn payload_as_slice_empty_returns_empty_slice() {
    let p = Payload::new();
    assert_eq!(p.as_slice(), &[][..]);
}

#[test]
fn payload_as_slice_single_chunk_borrows() {
    let b = Bytes::from_static(b"world");
    let p = Payload::from_bytes(b.clone());
    assert_eq!(p.as_slice(), &b"world"[..]);
}

#[test]
fn payload_into_bytes_via_from() {
    let b = Bytes::from_static(b"roundtrip");
    let p = Payload::from_bytes(b.clone());
    let got: Bytes = p.into();
    assert_eq!(got, b);
}

#[test]
fn payload_into_bytes_moves_heap_chunks() {
    let b = Bytes::from(vec![7u8; 128]);
    let ptr = b.as_ptr();
    let got = Payload::from_bytes(b).into_bytes();
    assert!(std::ptr::addr_eq(got.as_ptr(), ptr));

    let accounted = Payload::from_slice(&[8u8; 128]);
    let ptr = accounted.as_slice().as_ptr();
    let got = accounted.into_bytes();
    assert!(std::ptr::addr_eq(got.as_ptr(), ptr));
    assert_eq!(got, vec![8u8; 128]);
}

#[test]
fn payload_into_bytes_copies_inline() {
    assert_eq!(Payload::inline(b"hello").into_bytes(), &b"hello"[..]);
    assert_eq!(Payload::new().into_bytes(), Bytes::new());
}

#[test]
fn payload_size_of() {
    assert_eq!(std::mem::size_of::<Payload>(), 64);
}

#[test]
fn payload_inline_basic() {
    let p = Payload::inline(b"hello");
    assert_eq!(p.len(), 5);
    assert!(!p.is_empty());
    assert_eq!(p.as_slice(), b"hello");
    assert_eq!(p.as_bytes(), &b"hello"[..]);
    assert!(p.as_chunk().is_none());
}

#[test]
fn payload_inline_empty_becomes_empty() {
    let p = Payload::inline(b"");
    assert!(p.is_empty());
    assert_eq!(p.len(), 0);
}

#[test]
fn payload_inline_clone() {
    let p = Payload::inline(b"data");
    let p2 = p.clone();
    assert_eq!(p, p2);
}

#[test]
fn payload_inline_max_size() {
    let data = [0xAA; MAX_INLINE_PAYLOAD];
    let p = Payload::inline(&data);
    assert_eq!(p.len(), MAX_INLINE_PAYLOAD);
    assert_eq!(p.as_slice(), &data[..]);
}

#[test]
fn payload_from_slice_inline() {
    let p = Payload::from_slice(b"small");
    assert_eq!(p.len(), 5);
    assert_eq!(p.as_slice(), b"small");
}

#[test]
fn payload_from_slice_heap() {
    let data = [0xBB; MAX_INLINE_PAYLOAD + 1];
    let p = Payload::from_slice(&data);
    assert_eq!(p.len(), MAX_INLINE_PAYLOAD + 1);
    assert_eq!(p.as_slice(), &data[..]);
}
