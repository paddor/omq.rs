use std::cell::Cell;
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_pending_attempt() {
    let cancel = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut started_tx = Some(started_tx);
    let attempt = dial_with_backoff(
        || {
            let guard = Dropped(dropped.clone());
            let started = started_tx.take().unwrap();
            async move {
                let _guard = guard;
                started.send(()).unwrap();
                pending::<Result<()>>().await
            }
        },
        ReconnectPolicy::default(),
        false,
        &cancel,
        |_, _| panic!("canceled attempt must not schedule another retry"),
    );
    let stop = async {
        started_rx.await.unwrap();
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(tokio::time::timeout(Duration::from_secs(1), attempt), stop);
    assert!(matches!(result, Ok(Err(Canceled::Token))), "{result:?}");
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn canceled_before_attempt_never_dials() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let attempted = Cell::new(false);
    let result = dial_with_backoff(
        || {
            attempted.set(true);
            pending::<Result<()>>()
        },
        ReconnectPolicy::default(),
        false,
        &cancel,
        |_, _| panic!("must not retry after cancellation"),
    )
    .await;
    assert!(matches!(result, Err(Canceled::Token)));
    assert!(!attempted.get());
}

#[tokio::test]
async fn cancellation_wins_when_attempt_also_becomes_ready() {
    let cancel = CancellationToken::new();
    let (complete, incoming) = tokio::sync::oneshot::channel();
    let mut incoming = Some(incoming);
    let attempt = dial_with_backoff(
        || {
            let incoming = incoming.take().unwrap();
            async move { Ok(incoming.await.unwrap()) }
        },
        ReconnectPolicy::default(),
        false,
        &cancel,
        |_, _| panic!("must not retry"),
    );
    tokio::pin!(attempt);
    std::future::poll_fn(|cx| {
        assert!(attempt.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    complete.send(42).unwrap();
    cancel.cancel();
    assert!(matches!(attempt.await, Err(Canceled::Token)));
}

#[tokio::test(start_paused = true)]
async fn failed_attempt_retries_and_returns_connection() {
    let attempts = Cell::new(0);
    let mut delays = Vec::new();
    let result = dial_with_backoff(
        || {
            attempts.set(attempts.get() + 1);
            let attempt = attempts.get();
            async move {
                if attempt == 1 {
                    Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into())
                } else {
                    Ok(42)
                }
            }
        },
        ReconnectPolicy::Fixed(Duration::from_millis(10)),
        false,
        &CancellationToken::new(),
        |delay, attempt| delays.push((delay, attempt)),
    )
    .await;
    assert!(matches!(result, Ok(42)));
    assert_eq!(attempts.get(), 2);
    assert_eq!(delays.len(), 1);
    assert_eq!(delays[0].1, 1);
}

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_retry_sleep() {
    let cancel = CancellationToken::new();
    let result = dial_with_backoff(
        || async { Err::<(), _>(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into()) },
        ReconnectPolicy::Fixed(Duration::from_secs(30)),
        false,
        &cancel,
        |_, _| cancel.cancel(),
    )
    .await;
    assert!(matches!(result, Err(Canceled::Token)));
}

#[tokio::test]
async fn disabled_and_refused_policies_stop_after_one_attempt() {
    for (policy, stop_refused) in [
        (ReconnectPolicy::Disabled, false),
        (ReconnectPolicy::default(), true),
    ] {
        let attempts = Cell::new(0);
        let result = dial_with_backoff(
            || {
                attempts.set(attempts.get() + 1);
                async {
                    Err::<(), _>(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into())
                }
            },
            policy,
            stop_refused,
            &CancellationToken::new(),
            |_, _| panic!("policy forbids retry"),
        )
        .await;
        assert!(matches!(
            (stop_refused, result),
            (true, Err(Canceled::StoppedConnRefused)) | (false, Err(Canceled::PolicyDisabled))
        ));
        assert_eq!(attempts.get(), 1);
    }
}
