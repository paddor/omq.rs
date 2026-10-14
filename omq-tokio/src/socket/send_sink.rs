//! One pending application send with a synchronous submission fast path.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::Sink;
use omq_proto::{Error, Message, Result, SocketType, TrySendError, flow::DrainBudget};
use pin_project_lite::pin_project;

use super::Socket;

pin_project! {
    #[derive(Debug)]
    pub(super) struct MessageSink<'a, F, Fut> {
        socket: &'a Socket,
        send: F,
        pending: Option<Message>,
        pending_bytes: usize,
        budget: DrainBudget,
        #[pin]
        future: Option<Fut>,
    }
}

impl<'a, F, Fut> MessageSink<'a, F, Fut>
where
    F: FnMut(Message) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    #[inline]
    pub(super) fn new(socket: &'a Socket, send: F) -> Self {
        Self {
            socket,
            send,
            pending: None,
            pending_bytes: 0,
            budget: DrainBudget::WORKER,
            future: None,
        }
    }
}

impl<F, Fut> Sink<Message> for MessageSink<'_, F, Fut>
where
    F: FnMut(Message) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    type Error = Error;

    #[inline]
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
    }

    #[inline]
    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<()> {
        let this = self.project();
        if this.pending.is_some() || this.future.is_some() {
            return Err(Error::Protocol(
                "send sink must be ready before sending".into(),
            ));
        }
        *this.pending = Some(message);
        Ok(())
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();
        if this.future.is_none() {
            if this.pending.is_none() {
                return Poll::Ready(Ok(()));
            }
            if this.budget.exhausted() {
                this.budget.reset();
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let message = this.pending.take().expect("pending send");
            *this.pending_bytes = message.byte_len();
            // Preserve send's REQ retry and REP envelope cancellation guards.
            let message = if matches!(this.socket.socket_type(), SocketType::Req | SocketType::Rep)
            {
                message
            } else {
                match this.socket.try_send(message) {
                    Ok(()) => {
                        let _ = this.budget.account(*this.pending_bytes);
                        return Poll::Ready(Ok(()));
                    }
                    Err(TrySendError::Full(message)) => message,
                    Err(TrySendError::Closed) => return Poll::Ready(Err(Error::Closed)),
                    Err(TrySendError::Error(error)) => return Poll::Ready(Err(error)),
                }
            };
            this.future.set(Some((this.send)(message)));
        }
        let result = ready!(
            this.future
                .as_mut()
                .as_pin_mut()
                .expect("pending send future")
                .poll(cx)
        );
        this.future.set(None);
        if result.is_ok() {
            let _ = this.budget.account(*this.pending_bytes);
        }
        Poll::Ready(result)
    }

    #[inline]
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
    }
}
