use super::Socket;
use crate::socket::actor::{CloseLinger, SocketCommand};
use futures::channel::oneshot;
use omq_proto::error::Result;

impl Socket {
    /// Graceful close. Stops accepting new app work, drains pending sends up
    /// to `options.linger`, then cancels the driver. Non-zero linger keeps
    /// bind/connect endpoints alive while draining, so late peers can receive
    /// queued connect-side pre-ready sends before the deadline. Zero linger
    /// cancels endpoints and drops queued sends immediately. It also ends a
    /// close that another clone started with a longer linger, and it
    /// returns without waiting for the driver to release the endpoints.
    ///
    /// Consumes the handle; other clones remain valid until they also drop
    /// (subsequent calls on them return `Error::Closed`).
    pub async fn close(self) -> Result<()> {
        self.close_inner(CloseLinger::Configured).await
    }

    /// Graceful close with a one-shot linger override.
    ///
    /// `None` waits forever; `Some(Duration::ZERO)` drops immediately. This
    /// override only applies to this close call. It is mainly for compatibility
    /// layers whose close call accepts a per-call linger value.
    pub async fn close_with_linger(self, linger: Option<std::time::Duration>) -> Result<()> {
        self.close_inner(CloseLinger::Override(linger)).await
    }

    async fn close_inner(self, linger: CloseLinger) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        let effective_linger = linger.resolve(self.inner.linger);
        let close = SocketCommand::Close {
            ack: Some(ack),
            linger,
        };
        let zero_linger = matches!(effective_linger, Some(std::time::Duration::ZERO));

        let ack = if zero_linger {
            // A full inbox cannot take the command. A closed one means the
            // close of another clone is lingering. Zero linger ends both.
            if self.inner.cmd_tx.try_send(close).is_err() {
                self.inner.cancel.cancel();
            }

            tokio::select! {
                biased;
                res = rx => Some(res),
                () = tokio::task::yield_now() => {
                    self.inner.cancel.cancel();
                    None
                }
            }
        } else {
            let complete = async {
                let _ = self.inner.cmd_tx.send(close).await;
                rx.await
            };

            // Include admission: a full protocol inbox can block the actor's
            // current command before it can start its own linger timer.
            if let Some(duration) = effective_linger {
                if let Ok(result) = tokio::time::timeout(duration, complete).await {
                    Some(result)
                } else {
                    self.inner.cancel.cancel();
                    None
                }
            } else {
                Some(complete.await)
            }
        };

        let actor_task = self.inner.actor_task.lock().unwrap().take();
        let res = match ack {
            Some(Ok(res)) => res,
            // No answer: the close of another clone owns the drain.
            Some(Err(_)) if !zero_linger => {
                self.join_other_close(actor_task, effective_linger).await;
                return Ok(());
            }
            Some(Err(_)) | None => Ok(()),
        };

        self.send_submitter.shutdown();
        self.inner.recv_rx.shutdown();

        if let Some(task) = actor_task
            && !zero_linger
        {
            let _ = task.await;
        }

        res
    }

    /// Wait for a close that another clone started, at most `linger`.
    /// Shutting the send side down earlier would discard the queue that
    /// close still drains.
    async fn join_other_close(
        &self,
        actor_task: Option<tokio::task::JoinHandle<()>>,
        linger: Option<std::time::Duration>,
    ) {
        // Without the task, the clone that joins the actor finishes up.
        let Some(task) = actor_task else {
            return;
        };
        match linger {
            Some(duration) => {
                if tokio::time::timeout(duration, task).await.is_err() {
                    self.inner.cancel.cancel();
                }
            }
            None => {
                let _ = task.await;
            }
        }
        self.send_submitter.shutdown();
        self.inner.recv_rx.shutdown();
    }
}
