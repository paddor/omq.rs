use super::*;

#[test]
fn failed_listener_reservation_releases_socket_credit() {
    let socket = Admission::new(2);
    let listener = Admission::new(1);

    let first = PendingHandshake::acquire(&socket, Some(&listener), None).unwrap();
    assert!(PendingHandshake::acquire(&socket, Some(&listener), None).is_none());

    let second = PendingHandshake::acquire(&socket, None, None).unwrap();
    assert!(PendingHandshake::acquire(&socket, None, None).is_none());

    drop(first);
    let third = PendingHandshake::acquire(&socket, Some(&listener), None).unwrap();

    drop((second, third));
    assert_eq!(socket.used.load(Ordering::Relaxed), 0);
    assert_eq!(listener.used.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn waiting_for_admission_wakes_and_carries_one_reservation() {
    let admission = Admission::new(1);
    let first = admission.try_acquire().unwrap();

    let waiting = tokio::spawn({
        let admission = admission.clone();
        async move { admission.acquire().await }
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    drop(first);
    let permit = waiting.await.unwrap();
    assert!(admission.try_acquire().is_none());

    let setup = DialSetup {
        admission: Some(admission.clone()),
        timeout: None,
        cancel: CancellationToken::new(),
    };
    let ((), state) = setup
        .run_until(None, Some(PendingHandshake::from_socket(permit)), async {
            Ok(())
        })
        .await
        .unwrap();

    assert!(admission.try_acquire().is_none());
    drop(state);
    assert!(admission.try_acquire().is_some());
}

#[tokio::test(start_paused = true)]
async fn supplied_deadline_does_not_restart_after_dns() {
    let admission = Admission::new(1);
    let setup = DialSetup {
        admission: Some(admission.clone()),
        timeout: Some(Duration::from_secs(1)),
        cancel: CancellationToken::new(),
    };
    let deadline = Instant::now() + Duration::from_millis(100);
    let result = setup
        .run_until(Some(deadline), None, async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(Error::HandshakeFailed(_))));
    assert!(admission.try_acquire().is_some());
}

#[tokio::test]
async fn deadline_and_cancellation_release_but_success_transfers_admission() {
    let admission = Admission::new(1);
    let setup = DialSetup {
        admission: Some(admission.clone()),
        timeout: Some(Duration::from_millis(10)),
        cancel: CancellationToken::new(),
    };
    assert!(
        setup
            .run_until(None, None, std::future::pending::<Result<()>>())
            .await
            .is_err()
    );
    assert!(admission.try_acquire().is_some());

    let mut pending = Box::pin(setup.run_until(None, None, std::future::pending::<Result<()>>()));

    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;

    assert!(admission.try_acquire().is_none());
    drop(pending);
    assert!(admission.try_acquire().is_some());

    let ((), state) = setup.run_until(None, None, async { Ok(()) }).await.unwrap();
    assert!(admission.try_acquire().is_none());
    drop(state);
    assert!(admission.try_acquire().is_some());
}

#[test]
fn one_remote_host_holds_at_most_its_share_of_inbound_slots() {
    let admission = Admission::new(16);
    let host: IpAddr = "192.0.2.1".parse().unwrap();
    let same_prefix: IpAddr = "2001:db8::1".parse().unwrap();

    let first = admission.try_acquire_from(Some(host)).unwrap();
    let _second = admission.try_acquire_from(Some(host)).unwrap();
    assert!(admission.try_acquire_from(Some(host)).is_none());

    // Another host still gets slots; one IPv6 /64 counts as one host.
    let other = admission.try_acquire_from(Some("192.0.2.2".parse().unwrap()));
    assert!(other.is_some());
    let _v6 = admission.try_acquire_from(Some(same_prefix)).unwrap();
    let _v6b = admission
        .try_acquire_from(Some("2001:db8::2".parse().unwrap()))
        .unwrap();
    assert!(
        admission
            .try_acquire_from(Some("2001:db8::3".parse().unwrap()))
            .is_none()
    );

    // Releasing a slot returns it to that host.
    drop(first);
    assert!(admission.try_acquire_from(Some(host)).is_some());

    // Loopback peers are local and use only the socket limit.
    let loopback: IpAddr = "127.0.0.1".parse().unwrap();
    let held: Vec<_> = (0..8)
        .map(|_| admission.try_acquire_from(Some(loopback)).unwrap())
        .collect();
    assert_eq!(held.len(), 8);
}
