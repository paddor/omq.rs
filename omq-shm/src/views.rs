//! Recycled local view headers; payload bytes remain in the sender's mapping.
#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex, Weak};

use concurrent_queue::ConcurrentQueue;
use omq_proto::{Payload, message::PayloadOwner};

use crate::{lease::Borrowed, mapping::Mapping};

#[derive(Debug)]
pub(super) struct Views(Arc<Cache>);

#[derive(Debug)]
struct Cache {
    free: ConcurrentQueue<Arc<View>>,
    release: Mutex<()>,
}

#[derive(Debug)]
struct View {
    data: Option<Data>,
    cache: Weak<Cache>,
}

#[derive(Debug)]
enum Data {
    Borrowed(Borrowed),
    Copied {
        batch: Payload,
        offset: usize,
        length: usize,
    },
    Referenced {
        mapping: Arc<Mapping>,
        offset: usize,
        length: usize,
        _batch: Payload,
    },
}

impl Views {
    pub(super) fn new(capacity: usize) -> Self {
        let cache = Arc::new(Cache {
            free: ConcurrentQueue::bounded(capacity),
            release: Mutex::new(()),
        });
        for _ in 0..capacity {
            cache
                .free
                .push(Arc::new(View {
                    data: None,
                    cache: Arc::downgrade(&cache),
                }))
                .expect("view cache capacity");
        }
        Self(cache)
    }

    pub(super) fn borrowed(&self, data: Borrowed) -> Payload {
        self.take(Data::Borrowed(data))
    }

    pub(super) fn copied(&self, batch: Payload, offset: usize, length: usize) -> Payload {
        self.take(Data::Copied {
            batch,
            offset,
            length,
        })
    }

    pub(super) fn referenced(
        &self,
        mapping: Arc<Mapping>,
        offset: usize,
        length: usize,
        batch: Payload,
    ) -> Payload {
        self.take(Data::Referenced {
            mapping,
            offset,
            length,
            _batch: batch,
        })
    }

    fn take(&self, data: Data) -> Payload {
        let mut view = self.0.free.pop().unwrap_or_else(|_| {
            Arc::new(View {
                data: None,
                cache: Arc::downgrade(&self.0),
            })
        });
        Arc::get_mut(&mut view).expect("unique cached view").data = Some(data);
        Payload::from_shared_owner(view)
    }
}

impl AsRef<[u8]> for View {
    fn as_ref(&self) -> &[u8] {
        match self.data.as_ref().expect("leased view") {
            Data::Borrowed(data) => data.as_ref(),
            Data::Copied {
                batch,
                offset,
                length,
            } => &batch.as_slice()[*offset..*offset + *length],
            Data::Referenced {
                mapping,
                offset,
                length,
                ..
            } => mapping.bytes(*offset, *length),
        }
    }
}

impl PayloadOwner for View {
    fn release(self: Arc<Self>) {
        let Some(cache) = self.cache.upgrade() else {
            return;
        };
        let Some(mut view) = crate::coordination::unique_release(self, &cache.release) else {
            return;
        };
        // A unique view needs no lock. Release its lease before publishing
        // the empty header; nested owner callbacks must run outside the gate.
        Arc::get_mut(&mut view).expect("unique released view").data = None;
        let _ = cache.free.push(view);
    }
}

#[cfg(test)]
mod tests {
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
}
