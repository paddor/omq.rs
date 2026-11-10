use std::sync::Arc;

use tokio::time::{Duration, timeout};

use super::*;

#[test]
fn any_changed_observes_changes_before_first_poll_and_after_cancellation() {
    let signal = Arc::new(StateSignal::new());
    let mut wait = std::pin::pin!(any_changed([signal.clone()]).wait());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    signal.notify_changed();
    assert!(wait.as_mut().poll(&mut cx).is_ready());

    let mut cancelled = Box::pin(any_changed([signal.clone()]).wait());
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(cancelled);
    let mut wait = std::pin::pin!(any_changed([signal.clone()]).wait());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    signal.notify_changed();
    assert!(wait.as_mut().poll(&mut cx).is_ready());
    assert!(
        std::pin::pin!(any_changed([]).wait())
            .as_mut()
            .poll(&mut cx)
            .is_pending()
    );
}

#[test]
fn any_changed_registers_every_signal_and_wakes_the_task() {
    struct Count(AtomicUsize);
    impl std::task::Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    for count in [1, 4, 8] {
        let signals: Vec<_> = (0..count).map(|_| Arc::new(StateSignal::new())).collect();
        let mut wait = std::pin::pin!(any_changed(signals.iter().cloned()).wait());
        let counter = Arc::new(Count(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(counter.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        signals.last().unwrap().notify_changed();
        assert!(counter.0.load(Ordering::Relaxed) != 0);
        assert!(wait.as_mut().poll(&mut cx).is_ready());
    }
}

#[cfg(feature = "dart")]
#[test]
fn dart_forwarded_waker_can_register_another_endpoint() {
    struct RegisterEndpoint {
        space: Arc<StateSignal>,
        endpoint: Arc<DataSignal>,
    }

    impl std::task::Wake for RegisterEndpoint {
        fn wake(self: Arc<Self>) {
            assert!(self.space.endpoints.get().unwrap().try_lock().is_ok());
            self.space.dart_forward_to(&self.endpoint);
        }
    }

    let space = Arc::new(StateSignal::new());
    let endpoint = Arc::new(DataSignal::new());
    let extra = Arc::new(DataSignal::new());
    space.dart_forward_to(&endpoint);
    let waker = std::task::Waker::from(Arc::new(RegisterEndpoint {
        space: space.clone(),
        endpoint: extra.clone(),
    }));
    let mut cx = std::task::Context::from_waker(&waker);

    let mut wait = std::pin::pin!(endpoint.ready());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    space.notify_changed();
    assert!(wait.as_mut().poll(&mut cx).is_ready());

    space.notify_changed();
    assert!(!extra.is_idle());
}

#[cfg(feature = "dart")]
#[test]
fn dart_space_signal_supports_many_endpoints_and_prunes_dropped_ones() {
    let space = StateSignal::new();
    let dropped = Arc::new(DataSignal::new());
    space.dart_forward_to(&dropped);
    drop(dropped);
    let endpoints: Vec<_> = (0..129).map(|_| Arc::new(DataSignal::new())).collect();

    for endpoint in &endpoints {
        space.dart_forward_to(endpoint);
        space.dart_forward_to(endpoint);
    }

    assert_eq!(
        space.endpoints.get().unwrap().lock().unwrap().len(),
        endpoints.len()
    );
    space.notify_changed();
    assert!(endpoints.iter().all(|endpoint| !endpoint.is_idle()));
}

#[tokio::test]
async fn first_mark_wakes() {
    let sig = Arc::new(DataSignal::new());
    let s = sig.clone();
    let handle = tokio::spawn(async move { timeout(Duration::from_secs(1), s.notified()).await });
    tokio::task::yield_now().await;
    sig.mark();
    assert!(handle.await.unwrap().is_ok());
}

#[tokio::test]
async fn second_mark_coalesces() {
    let sig = Arc::new(DataSignal::new());
    sig.mark();
    sig.mark();
    timeout(Duration::from_secs(1), sig.ready())
        .await
        .expect("pending signal should be ready");

    sig.begin_drain();
    sig.clear_after(true);
    let s = sig.clone();
    let handle = tokio::spawn(async move { timeout(Duration::from_millis(20), s.ready()).await });
    assert!(
        handle.await.unwrap().is_err(),
        "no wake after clear without new mark",
    );
}

#[tokio::test]
async fn rearm_fires_when_nonempty() {
    let sig = Arc::new(DataSignal::new());
    sig.mark();
    let s = sig.clone();
    let _ = timeout(Duration::from_secs(1), s.notified()).await;
    sig.begin_drain();
    assert!(sig.clear_after(false));

    let s2 = sig.clone();
    let handle = tokio::spawn(async move { timeout(Duration::from_secs(1), s2.notified()).await });
    assert!(handle.await.unwrap().is_ok());
}

#[tokio::test]
async fn rearm_silent_when_empty() {
    let sig = Arc::new(DataSignal::new());
    sig.mark();
    let s = sig.clone();
    let _ = timeout(Duration::from_secs(1), s.notified()).await;
    sig.begin_drain();
    assert!(!sig.clear_after(true));

    let s2 = sig.clone();
    let handle =
        tokio::spawn(async move { timeout(Duration::from_millis(20), s2.notified()).await });
    assert!(
        handle.await.unwrap().is_err(),
        "rearm with is_empty=true must not wake",
    );
}

#[tokio::test]
async fn clear_after_rearms_when_marked_during_drain_even_if_empty_stale() {
    let sig = Arc::new(DataSignal::new());
    sig.mark();
    sig.begin_drain();
    sig.mark();
    assert!(sig.clear_after(true));
    timeout(Duration::from_secs(1), sig.ready())
        .await
        .expect("dirty drain should preserve readiness");
}

#[tokio::test]
async fn wake_all_wakes_multiple() {
    let sig = Arc::new(DataSignal::new());
    let mut handles = Vec::new();

    for _ in 0..3 {
        let s = sig.clone();
        handles.push(tokio::spawn(async move {
            timeout(Duration::from_secs(1), s.notified()).await
        }));
    }

    tokio::task::yield_now().await;
    sig.wake_all();

    for h in handles {
        assert!(h.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn ready_observes_pending_after_cancelled_waiter() {
    let sig = DataSignal::new();
    let mut notified = Box::pin(sig.notified());
    notified.as_mut().enable();

    sig.mark();
    drop(notified);

    timeout(Duration::from_secs(1), sig.ready())
        .await
        .expect("pending flag should keep readiness visible");
}

#[tokio::test]
async fn state_signal_observes_change_after_waiter_creation() {
    let sig = StateSignal::new();
    let seen = sig.generation();
    let wait = sig.changed_after(seen);
    tokio::pin!(wait);
    sig.notify_changed();
    timeout(Duration::from_secs(1), wait)
        .await
        .expect("generation change should wake waiter");
}

#[tokio::test]
async fn state_signal_observes_change_before_await() {
    let sig = StateSignal::new();
    let seen = sig.generation();
    sig.notify_changed();
    timeout(Duration::from_secs(1), sig.changed_after(seen))
        .await
        .expect("generation change should be stateful");
}
