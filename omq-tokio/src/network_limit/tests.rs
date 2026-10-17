use super::*;

fn bucket(rate: u64, burst: u64, lanes: usize) -> Bucket {
    Allocator::new(
        NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(rate, burst)),
        },
        lanes,
    )
    .egress
    .unwrap()
}

#[test]
fn outstanding_grants_still_occupy_burst_capacity() {
    let mut b = bucket(100, 100, 2);
    let start = b.updated;
    for account in &b.accounts {
        account.mailbox.reports.demand.store(1, Ordering::Release);
    }
    for _ in 0..8 {
        b.service(start);
    }
    assert_eq!(b.available, 0);
    assert_eq!(b.held, 100);
    b.service(start + Duration::from_secs(60));
    assert_eq!(b.held, 100);
    // The first lane reports old spending after a minute. No historical
    // refill can be applied to capacity that was still reserved until now.
    let spent = b.accounts[0].totals.held;
    b.accounts[0]
        .mailbox
        .reports
        .spent
        .store(spent, Ordering::Release);
    b.service(start + Duration::from_secs(60));
    assert_eq!(b.available, 0);
    assert_eq!(b.held, 100 - spent);
    b.service(start + Duration::from_secs(60) + Duration::from_millis(10));
    assert_eq!(b.held, 101 - spent);
}

#[test]
fn returned_credit_is_transferred_not_refilled() {
    let mut b = bucket(100, 100, 1);
    let start = b.updated;
    b.accounts[0]
        .mailbox
        .reports
        .demand
        .store(100, Ordering::Release);
    b.service(start);
    let granted = b.accounts[0].totals.held;
    b.accounts[0]
        .mailbox
        .reports
        .demand
        .store(0, Ordering::Release);
    b.accounts[0]
        .mailbox
        .reports
        .returned
        .store(granted, Ordering::Release);
    b.service(start);
    assert_eq!(b.available, 100);
    assert_eq!(b.held, 0);
}

#[test]
fn fractional_refill_and_large_rates_do_not_overflow() {
    let mut b = bucket(3, 10, 1);
    b.available = 0;
    let start = b.updated;
    for step in 1..=10 {
        b.refill(start + Duration::from_millis(step * 100));
    }
    assert_eq!(b.available, 3);
    let mut huge = bucket(u64::MAX, u64::MAX, 1);
    huge.available = 0;
    huge.refill(huge.updated + Duration::from_secs(10));
    assert_eq!(huge.available, u64::MAX);
}

#[test]
fn one_byte_burst_rotates_between_waiting_lanes() {
    let mut b = bucket(1, 1, 4);
    let start = b.updated;
    for account in &b.accounts {
        account.mailbox.reports.demand.store(1, Ordering::Release);
    }
    for second in 0..12 {
        b.service(start + Duration::from_secs(second));
        for account in &b.accounts {
            account
                .mailbox
                .reports
                .spent
                .store(account.totals.granted, Ordering::Release);
        }
        b.service(start + Duration::from_secs(second));
    }
    assert!(b.accounts.iter().all(|a| a.totals.granted != 0));
}

#[tokio::test(start_paused = true)]
async fn cancelled_waiter_releases_the_next_connection() {
    let allocator = Allocator::new(
        NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(100, 10)),
        },
        1,
    );
    let lane = allocator.lanes()[0].clone();
    let cancel = CancellationToken::new();
    let allocator_task = tokio::spawn(allocator.run(cancel.clone()));
    let lane_task = tokio::spawn(lane.clone().run(cancel.clone()));
    let mut first = lane.egress().unwrap();
    let mut second = lane.egress().unwrap();
    let cx = Context::from_waker(Waker::noop());
    assert!(first.poll(&cx, 10, 10).is_pending());
    assert!(second.poll(&cx, 1, 1).is_pending());
    drop(first);
    let n = tokio::time::timeout(Duration::from_secs(1), second.acquire(1))
        .await
        .unwrap();
    second.finish(n, 1);
    cancel.cancel();
    allocator_task.await.unwrap();
    lane_task.await.unwrap();
}

#[test]
fn whole_datagram_grants_do_not_fragment_a_small_burst() {
    let mut b = bucket(1200, 1500, 4);
    let start = b.updated;
    for a in &b.accounts {
        a.mailbox.reports.demand.store(1200, Ordering::Release);
    }
    for second in 0..8 {
        b.service(start + Duration::from_secs(second));
        assert_eq!(b.accounts.iter().filter(|a| a.totals.held != 0).count(), 1);
        assert!(
            b.accounts
                .iter()
                .all(|a| a.totals.held == 0 || a.totals.held >= 1200)
        );
        for a in &b.accounts {
            a.mailbox
                .reports
                .spent
                .store(a.totals.granted, Ordering::Release);
        }
        b.service(start + Duration::from_secs(second));
    }
    assert!(b.accounts.iter().all(|a| a.totals.granted >= 2400));
}

#[test]
fn a_large_datagram_is_not_starved_by_stream_grants() {
    let mut b = bucket(1000, 100, 2);
    let start = b.updated;
    b.available = 0;
    b.accounts[0]
        .mailbox
        .reports
        .demand
        .store(100, Ordering::Release);
    b.accounts[1]
        .mailbox
        .reports
        .demand
        .store(1, Ordering::Release);
    for tick in 0..=500 {
        let now = start + Duration::from_millis(tick);
        b.service(now);
        for account in &b.accounts {
            account
                .mailbox
                .reports
                .spent
                .store(account.totals.granted, Ordering::Release);
        }
        b.service(now);
    }
    assert!(
        b.accounts[0].totals.granted >= 100,
        "the datagram never received enough credit"
    );
    assert!(
        b.accounts[1].totals.granted > 0,
        "the stream must also make progress"
    );
}

#[test]
fn delayed_batched_reports_preserve_the_aggregate_envelope() {
    let mut b = bucket(1000, 100, 4);
    let start = b.updated;
    let mut spent = [0u64; 4];
    let mut returned = [0u64; 4];
    let mut samples = Vec::new();
    for tick in 0..1000u64 {
        for (index, a) in b.accounts.iter().enumerate() {
            let credit = a.totals.granted - spent[index] - returned[index];
            if tick % (index as u64 + 2) == 0 {
                spent[index] += credit;
            } else if tick % 7 == 0 {
                returned[index] += credit;
            }
            if tick % 3 == 0 {
                a.mailbox
                    .reports
                    .spent
                    .store(spent[index], Ordering::Release);
                a.mailbox
                    .reports
                    .returned
                    .store(returned[index], Ordering::Release);
            }
            a.mailbox.reports.demand.store(1, Ordering::Release);
        }
        b.service(start + Duration::from_millis(tick));
        assert!(b.available + b.held <= 100);
        let total = spent.iter().sum::<u64>();
        assert!(total <= 100 + tick);
        for &(previous, amount) in &samples {
            assert!(total - amount <= 100 + tick - previous);
        }
        if tick % 10 == 0 {
            samples.push((tick, total));
        }
    }
}

#[cfg(feature = "dart")]
#[test]
fn datagram_refunds_and_repair_debt_are_applied_once() {
    let mut b = bucket(100, 100, 1);
    let start = b.updated;
    b.available = 0;
    b.accounts[0]
        .mailbox
        .reports
        .refunded
        .store(80, Ordering::Release);
    b.accounts[0]
        .mailbox
        .reports
        .debt
        .store(100, Ordering::Release);
    b.service(start);
    assert_eq!((b.available, b.debt), (0, 20));
    b.service(start);
    assert_eq!((b.available, b.debt), (0, 20));
    b.service(start + Duration::from_millis(300));
    assert_eq!((b.available, b.debt), (10, 0));
}

#[test]
fn fractional_refill_repays_debt_even_with_all_burst_credit_held() {
    let mut b = bucket(1, 10, 1);
    b.available = 0;
    b.held = 10;
    b.debt = 1;
    let start = b.updated;
    for tick in 1..=10 {
        b.refill(start + Duration::from_millis(tick * 100));
    }
    assert_eq!(b.debt, 0);
    assert_eq!(b.held, 10);
    assert_eq!(b.available, 0);
}

#[tokio::test(start_paused = true)]
async fn short_and_pending_writes_return_unused_credit() {
    use tokio::io::AsyncWriteExt;
    let allocator = Allocator::new(
        NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(1, 10)),
        },
        1,
    );
    let lane = allocator.lanes()[0].clone();
    let cancel = CancellationToken::new();
    let allocator_task = tokio::spawn(allocator.run(cancel.clone()));
    let lane_task = tokio::spawn(lane.clone().run(cancel.clone()));
    let (writer, _reader) = tokio::io::duplex(1);
    let mut writer = PacedWrite::new(writer, lane.egress());
    assert_eq!(writer.write(&[1; 10]).await.unwrap(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), writer.write(&[2; 10]))
            .await
            .is_err()
    );
    writer.cancel_wait();
    let mut other = lane.egress().unwrap();
    let grant = tokio::time::timeout(Duration::from_millis(10), other.acquire(9))
        .await
        .unwrap();
    assert_eq!(grant, 9);
    other.finish(grant, 9);
    cancel.cancel();
    allocator_task.await.unwrap();
    lane_task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn lane_shutdown_does_not_reissue_previous_grants() {
    let allocator = Allocator::new(
        NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(1, 8)),
        },
        1,
    );
    let lane = allocator.lanes()[0].clone();
    let stop_allocator = CancellationToken::new();
    let stop_lane = CancellationToken::new();
    let allocator_task = tokio::spawn(allocator.run(stop_allocator.clone()));
    let lane_task = tokio::spawn(lane.clone().run(stop_lane.clone()));
    let mut gate = lane.egress().unwrap();
    let reserved = gate.acquire(8).await;
    gate.finish(reserved, 8);
    gate.cancel_wait();
    stop_lane.cancel();
    lane_task.await.unwrap();
    // A connection may still get polled while its runtime is shutting down.
    // Its already-consumed cumulative mailbox total is not a new grant.
    assert!(
        gate.poll(&Context::from_waker(Waker::noop()), 1, 1)
            .is_pending()
    );
    stop_allocator.cancel();
    allocator_task.await.unwrap();
}
