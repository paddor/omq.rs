//! Application-owned receive polling with a ready-queue fast path.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::{Stream, stream::FusedStream};
use omq_proto::{Error, Message, Result, SocketType, flow::DrainBudget};
use pin_project_lite::pin_project;

use super::Socket;

pin_project! {
    #[derive(Debug)]
    pub(super) struct MessageStream<'a, F, Fut> {
        socket: &'a Socket,
        recv: F,
        budget: DrainBudget,
        terminated: bool,
        #[pin]
        future: Option<Fut>,
    }
}

impl<'a, F, Fut> MessageStream<'a, F, Fut>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Message>>,
{
    pub(super) fn new(socket: &'a Socket, recv: F) -> Self {
        Self {
            socket,
            recv,
            budget: DrainBudget::WORKER,
            terminated: false,
            future: None,
        }
    }
}

impl<F, Fut> Stream for MessageStream<'_, F, Fut>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Message>>,
{
    type Item = Result<Message>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.terminated {
            return Poll::Ready(None);
        }
        if this.budget.exhausted() {
            this.budget.reset();
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let result = if let Some(future) = this.future.as_mut().as_pin_mut() {
            ready!(future.poll(cx))
        } else if matches!(this.socket.socket_type(), SocketType::Req | SocketType::Rep) {
            this.future.set(Some((this.recv)()));
            ready!(
                this.future
                    .as_mut()
                    .as_pin_mut()
                    .expect("receive future")
                    .poll(cx)
            )
        } else {
            match this.socket.try_recv() {
                Err(Error::WouldBlock) => {
                    this.future.set(Some((this.recv)()));
                    ready!(
                        this.future
                            .as_mut()
                            .as_pin_mut()
                            .expect("receive future")
                            .poll(cx)
                    )
                }
                result => result,
            }
        };
        this.future.set(None);
        match result {
            Ok(message) => {
                let _ = this.budget.account(message.byte_len());
                Poll::Ready(Some(Ok(message)))
            }
            Err(Error::Closed) => {
                *this.terminated = true;
                Poll::Ready(None)
            }
            Err(error) => {
                *this.terminated = true;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

impl<F, Fut> FusedStream for MessageStream<'_, F, Fut>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Message>>,
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
