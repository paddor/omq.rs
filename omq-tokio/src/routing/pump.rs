//! Shared per-peer pump used by `RoundRobinSend` (N pumps, shared queue)
//! and `FanOutSend` (one pump per peer, per-peer queue). The batch-and-
//! yield shape is identical; only the queue ownership differs.

use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::message::Message;

/// Max messages one pump forwards before yielding.
pub(crate) const MAX_BATCH_MSGS: usize = 256;

/// Max bytes one pump forwards before yielding.
pub(crate) const MAX_BATCH_BYTES: usize = 512 * 1024;

/// Drive messages from `rx` to `peer.inbox` with per-batch fairness caps.
/// Returns when the channel is closed, the peer is gone, or `cancel` fires.
pub(crate) async fn drain(
    rx: flume::Receiver<Message>,
    peer: DriverHandle,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            res = rx.recv_async() => {
                let Ok(mut msg) = res else { return; };
                let mut count = 0usize;
                let mut bytes = 0usize;
                loop {
                    let m_bytes = msg.byte_len();
                    if peer.inbox.send(DriverCommand::SendMessage(msg)).await.is_err() {
                        return;
                    }
                    count += 1;
                    bytes += m_bytes;
                    if count >= MAX_BATCH_MSGS || bytes >= MAX_BATCH_BYTES {
                        break;
                    }
                    match rx.try_recv() {
                        Ok(next) => msg = next,
                        Err(_) => break,
                    }
                }
                yield_now().await;
            }
        }
    }
}
