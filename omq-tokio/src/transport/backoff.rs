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

/// Outcome of a cancelled backoff loop.
#[derive(Debug)]
pub enum Cancelled {
    /// The cancellation token fired before we connected.
    Token,
    /// The policy is [`ReconnectPolicy::Disabled`] and we exhausted the
    /// single attempt.
    PolicyDisabled,
}

/// Keep trying to connect. Returns the established stream on success, or
/// [`Cancelled`] if the cancellation token fired (or the policy was
/// [`ReconnectPolicy::Disabled`] and we failed once).
///
/// The first attempt happens immediately; subsequent attempts wait per the
/// policy. Reports per-attempt delays through `on_delay` so callers can emit
/// `ConnectDelayed` monitor events.
///
/// The `dial` closure performs one connection attempt; each call builds a
/// fresh future so no state leaks across retries.
pub async fn dial_with_backoff<F, Fut, S>(
    mut dial: F,
    policy: ReconnectPolicy,
    cancel: &CancellationToken,
    mut on_delay: impl FnMut(Duration, u32),
) -> std::result::Result<S, Cancelled>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<S>>,
{
    let mut attempt: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(Cancelled::Token);
        }
        match dial().await {
            Ok(stream) => return Ok(stream),
            Err(_err) => {
                attempt = attempt.saturating_add(1);
                let Some(delay) = next_delay(&policy, attempt) else {
                    return Err(Cancelled::PolicyDisabled);
                };
                on_delay(delay, attempt);
                tokio::select! {
                    () = cancel.cancelled() => return Err(Cancelled::Token),
                    () = sleep(delay) => {}
                }
            }
        }
    }
}
