//! Inline native admission and a bounded fallback for eager asynchronous sends.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Wake, Waker};

use omq_tokio::{Message, Socket, SocketType, TrySendError};

pub(crate) struct SendQueue {
    producer: Mutex<Option<yring::AsyncProducer<Message>>>,
    // Includes the message currently awaiting native admission. Empty ring
    // length alone cannot authorize a later inline send.
    queued: AtomicUsize,
    inline: AtomicBool,
    capacity: usize,
    space_waker: Waker,
    pub(crate) protocol_pending: AtomicBool,
}

impl SendQueue {
    pub(crate) fn new(
        capacity: usize,
        signal: Arc<dyn Fn() + Send + Sync>,
    ) -> (Arc<Self>, yring::AsyncConsumer<Message>) {
        let (producer, consumer) = yring::async_spsc(capacity);
        (
            Arc::new(Self {
                producer: Mutex::new(Some(producer)),
                queued: AtomicUsize::new(0),
                inline: AtomicBool::new(false),
                capacity,
                space_waker: Waker::from(Arc::new(SpaceWake(signal))),
                protocol_pending: AtomicBool::new(false),
            }),
            consumer,
        )
    }

    pub(crate) fn try_send(&self, socket: &Socket, message: Message) -> Result<(), TrySendError> {
        {
            let mut publication = self.producer.lock().unwrap();
            let Some(producer) = publication.as_mut() else {
                return Err(TrySendError::Closed);
            };
            let latency = matches!(
                socket.socket_type(),
                SocketType::Req | SocketType::Rep | SocketType::Pair | SocketType::Channel
            );
            if (!latency && self.capacity < 256)
                || self.queued.load(Ordering::Acquire) > 0
                || self.inline.load(Ordering::Acquire)
            {
                if matches!(socket.socket_type(), SocketType::Req | SocketType::Rep) {
                    return Err(TrySendError::Error(omq_proto::Error::Protocol(
                        "request/reply send already pending".into(),
                    )));
                }
                let result = self.enqueue(producer, message, false);
                drop(publication);
                return Self::queued_result(result);
            }
            self.inline.store(true, Ordering::Release);
        }
        // Native admission can release a Python exporter. No binding lock or
        // cloned payload owner surrounds that call. Reentrant sends see inline
        // occupied and publish to the bounded fallback after this admission.
        let result = match socket.try_send(message) {
            Err(TrySendError::Full(message)) => {
                let result = {
                    let mut publication = self.producer.lock().unwrap();
                    match publication.as_mut() {
                        Some(producer) => self.enqueue(
                            producer,
                            message,
                            matches!(socket.socket_type(), SocketType::Req | SocketType::Rep),
                        ),
                        None => Err((message, true)),
                    }
                };
                Self::queued_result(result)
            }
            result => result,
        };
        self.inline.store(false, Ordering::Release);
        result
    }

    fn enqueue(
        &self,
        producer: &mut yring::AsyncProducer<Message>,
        message: Message,
        request_reply: bool,
    ) -> Result<(), (Message, bool)> {
        if producer.is_consumer_dropped() {
            return Err((message, true));
        }
        // Increment before publication: the worker may consume immediately.
        self.queued.fetch_add(1, Ordering::Relaxed);
        if request_reply {
            self.protocol_pending.store(true, Ordering::Release);
        }
        let result = match producer.push_and_flush(message) {
            Err(message) => {
                let mut cx = Context::from_waker(&self.space_waker);
                if producer.poll_ready(&mut cx).is_ready() {
                    producer.push_and_flush(message)
                } else {
                    Err(message)
                }
            }
            result => result,
        };
        match result {
            Ok(()) => Ok(()),
            Err(message) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                if request_reply {
                    self.protocol_pending.store(false, Ordering::Release);
                }
                Err((message, producer.is_consumer_dropped()))
            }
        }
    }

    fn queued_result(result: Result<(), (Message, bool)>) -> Result<(), TrySendError> {
        result.map_err(|(message, closed)| {
            if closed {
                // A Python exporter can reenter close on this drop.
                drop(message);
                TrySendError::Closed
            } else {
                TrySendError::Full(message)
            }
        })
    }

    /// Run after the worker releases its consumed ring slot.
    pub(crate) fn complete(&self) {
        // Clear before opening inline admission: a new queued REQ/REP send
        // must not have its pending flag overwritten by this completion.
        self.protocol_pending.store(false, Ordering::Release);
        self.queued.fetch_sub(1, Ordering::Release);
    }

    pub(crate) fn close(&self) {
        let producer = self.producer.lock().unwrap().take();
        drop(producer);
    }
}

struct SpaceWake(Arc<dyn Fn() + Send + Sync>);

impl Wake for SpaceWake {
    fn wake(self: Arc<Self>) {
        (self.0)();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        (self.0)();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, StreamExt};

    #[test]
    fn closed_fallback_releases_exporters_outside_publication_lock() {
        struct Export {
            bytes: [u8; 128],
            queue: std::sync::Weak<SendQueue>,
        }
        impl AsRef<[u8]> for Export {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl Drop for Export {
            fn drop(&mut self) {
                let queue = self.queue.upgrade().unwrap();
                assert!(queue.producer.try_lock().is_ok());
            }
        }
        let ctx = omq_tokio::Context::new();
        let socket = ctx.blocking_socket(SocketType::Push, omq_tokio::Options::default());
        let (queue, consumer) = SendQueue::new(8, Arc::new(|| {}));
        drop(consumer);
        let bytes = bytes::Bytes::from_owner(Export {
            bytes: [0; 128],
            queue: Arc::downgrade(&queue),
        });
        assert!(matches!(
            queue.try_send(&socket.into_async(), Message::single(bytes)),
            Err(TrySendError::Closed)
        ));
        ctx.term();
    }

    #[test]
    fn popped_fallback_remains_a_fifo_barrier_until_native_admission() {
        let ctx = omq_tokio::Context::new();
        let push = ctx.blocking_socket(SocketType::Push, omq_tokio::Options::default());
        push.bind("inproc://binding-inflight-fifo".parse().unwrap())
            .unwrap();
        let socket = push.clone().into_async();
        let (queue, mut consumer) = SendQueue::new(256, Arc::new(|| {}));
        queue.try_send(&socket, Message::single("first")).unwrap();
        let first = consumer.next().now_or_never().flatten().unwrap();
        let pull = ctx.blocking_socket(SocketType::Pull, omq_tokio::Options::default());
        pull.connect("inproc://binding-inflight-fifo".parse().unwrap())
            .unwrap();
        push.wait_connected(1, std::time::Duration::from_secs(2))
            .unwrap();
        // Native admission is available now, but the worker still owns first.
        queue.try_send(&socket, Message::single("second")).unwrap();
        assert!(matches!(pull.try_recv(), Err(omq_proto::Error::WouldBlock)));
        socket.try_send(first).unwrap();
        consumer.release();
        queue.complete();
        let second = consumer.next().now_or_never().flatten().unwrap();
        socket.try_send(second).unwrap();
        consumer.release();
        queue.complete();
        assert_eq!(pull.recv().unwrap().part_slice(0), Some(&b"first"[..]));
        assert_eq!(pull.recv().unwrap().part_slice(0), Some(&b"second"[..]));
        queue.close();
        ctx.term();
    }
}
