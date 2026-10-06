//! Exponential backoff helper for reconnect loops.
//!
//! [`dial_with_backoff`] retries a `Transport::connect` until it succeeds or
//! the caller cancels. Each retry waits longer than the last per
//! [`ReconnectPolicy`], with a small random jitter to stagger thundering
//! herds.

use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use omq_proto::backoff::next_delay;
use omq_proto::error::Result;
use omq_proto::options::ReconnectPolicy;

#[cfg(test)]
mod tests;

/// Outcome of a canceled backoff loop.
#[derive(Debug)]
pub enum Canceled {
    /// The cancellation token fired before we connected.
    Token,
    /// The policy is [`ReconnectPolicy::Disabled`] and we exhausted the
    /// single attempt.
    PolicyDisabled,
    /// The dial failed with `ECONNREFUSED` and
    /// `reconnect_stop_conn_refused` was set.
    StoppedConnRefused,
}

/// Keep trying to connect. Returns the established stream on success, or
/// [`Canceled`] if the cancellation token fired (or the policy was
/// [`ReconnectPolicy::Disabled`] and we failed once).
///
/// The first attempt happens immediately; subsequent attempts wait per the
/// policy. Reports per-attempt delays through `on_delay` so callers can emit
/// `ConnectDelayed` monitor events.
///
/// The `dial` closure performs one connection attempt; each call builds a
/// fresh future so no state leaks across retries. Cancellation drops a pending
/// attempt; the future must release any partially established transport on drop.
pub async fn dial_with_backoff<F, Fut, S>(
    dial: F,
    policy: ReconnectPolicy,
    stop_conn_refused: bool,
    cancel: &CancellationToken,
    on_delay: impl FnMut(Duration, u32),
) -> std::result::Result<S, Canceled>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<S>>,
{
    dial_with_backoff_from(dial, policy, stop_conn_refused, cancel, on_delay, 0).await
}

/// Resume after a driver failure using the same delay policy as a failed dial.
pub(crate) async fn dial_with_backoff_from<F, Fut, S>(
    mut dial: F,
    policy: ReconnectPolicy,
    stop_conn_refused: bool,
    cancel: &CancellationToken,
    mut on_delay: impl FnMut(Duration, u32),
    mut attempt: u32,
) -> std::result::Result<S, Canceled>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<S>>,
{
    loop {
        if cancel.is_cancelled() {
            return Err(Canceled::Token);
        }
        if attempt > 0 {
            let Some(delay) = next_delay(&policy, attempt) else {
                return Err(Canceled::PolicyDisabled);
            };
            on_delay(delay, attempt);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(Canceled::Token),
                () = sleep(delay) => {}
            }
        }
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(Canceled::Token),
            result = dial() => result,
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                if stop_conn_refused && err.is_connection_refused() {
                    return Err(Canceled::StoppedConnRefused);
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}
