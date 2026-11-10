use super::*;

#[test]
fn final_view_release_recycles_once_and_retains_the_original_storage() {
    let views = Views::new(1);
    let batch = Payload::from_shared_owner(Arc::new(BytesOwner(vec![7; 128])));
    let first = views.copied(batch, 8, 64);
    let clone = first.clone();
    let bytes = first.as_bytes();
    assert_eq!(views.0.free.len(), 0);

    drop(first);
    drop(clone);
    assert_eq!(views.0.free.len(), 0);
    assert_eq!(bytes.as_ref(), &[7; 64]);

    drop(bytes);
    assert_eq!(views.0.free.len(), 1);

    let next = views.copied(Payload::from_slice(&[9; 128]), 32, 32);
    assert_eq!(next.as_slice(), &[9; 32]);
}

#[test]
fn concurrent_last_clones_recycle_without_losing_headers() {
    let views = Views::new(1);
    for _ in 0..64 {
        let payload = views.copied(Payload::from_slice(&[7; 128]), 0, 128);
        let clones: Vec<_> = (0..8).map(|_| payload.clone()).collect();
        drop(payload);
        let barrier = std::sync::Barrier::new(clones.len());

        std::thread::scope(|scope| {
            for clone in clones {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    assert_eq!(clone.as_slice(), &[7; 128]);
                    drop(clone);
                });
            }
        });

        assert_eq!(views.0.free.len(), 1);
    }
}

#[test]
fn exhaustion_and_cache_destruction_preserve_live_views() {
    let views = Views::new(1);
    let first = views.copied(Payload::from_slice(&[7; 128]), 0, 64);
    let second = views.copied(Payload::from_slice(&[9; 128]), 64, 64);

    drop(first);
    assert_eq!(views.0.free.len(), 1);

    drop(views);
    assert_eq!(second.as_slice(), &[9; 64]);
    drop(second);
}

#[derive(Debug)]
struct BytesOwner(Vec<u8>);

impl AsRef<[u8]> for BytesOwner {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl PayloadOwner for BytesOwner {}
