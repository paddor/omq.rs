use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};
use omq_proto::frame_buffer::FrameBuffer;
#[cfg(feature = "lz4")]
use omq_proto::options::Options;
#[cfg(feature = "lz4")]
use omq_proto::proto::transform::CompressionKind;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::engine::control_queue::ControlQueue;
use crate::engine::transmit_slot::SlotConfig;
use crate::routing::subscription::SubscriptionSet;

use super::{
    FanOutLaneState, FanOutLanes, FanOutMode, FanOutMutePolicy, LaneAdmission, LaneControl,
    LaneData, LaneDispatch, LaneDistributor, LaneEndpoint, LaneInput, LanePeer, LanePeerAdd,
    LaneWorker,
};

fn paused_lanes(count: usize) -> (FanOutLanes, Vec<yring::Consumer<LaneData>>) {
    let lanes = test_lanes(count);
    let mut receivers = Vec::new();
    for (index, input) in lanes
        .distributor
        .lock()
        .unwrap()
        .lanes
        .iter_mut()
        .enumerate()
    {
        let (tx, rx) = yring::spsc(1);
        input.tx = tx;
        lanes.active_flags[index].store(true, std::sync::atomic::Ordering::Release);
        receivers.push(rx);
    }
    (lanes, receivers)
}

fn pop_dispatch(receiver: &mut yring::Consumer<LaneData>) -> Option<omq_proto::Message> {
    receiver.prefetch();
    let result = receiver
        .pop()
        .map(|LaneData::Dispatch(dispatch)| dispatch.msg);
    receiver.release();
    result
}

#[test]
fn strict_admission_is_atomic_across_lanes_but_lossy_dispatch_skips_full_lanes() {
    for full_lane in [0, 1] {
        let (lanes, mut receivers) = paused_lanes(2);
        lanes
            .try_dispatch(test_dispatch("first"), LaneAdmission::Backpressure)
            .unwrap();
        let available_lane = 1 - full_lane;
        assert_eq!(
            pop_dispatch(&mut receivers[available_lane]),
            Some("first".into())
        );

        let Err(omq_proto::TrySendError::Full(message)) =
            lanes.try_dispatch(test_dispatch("retry"), LaneAdmission::Backpressure)
        else {
            panic!("one full lane must reject the entire publication");
        };
        assert_eq!(message, "retry".into());
        assert_eq!(pop_dispatch(&mut receivers[available_lane]), None);

        lanes
            .try_dispatch(test_dispatch("lossy"), LaneAdmission::Drop)
            .unwrap();
        assert_eq!(
            pop_dispatch(&mut receivers[available_lane]),
            Some("lossy".into())
        );
        assert_eq!(
            pop_dispatch(&mut receivers[full_lane]),
            Some("first".into())
        );
        lanes
            .try_dispatch(
                super::LaneDispatch { msg: message },
                LaneAdmission::Backpressure,
            )
            .unwrap();
        for receiver in &mut receivers {
            assert_eq!(pop_dispatch(receiver), Some("retry".into()));
            assert_eq!(pop_dispatch(receiver), None);
        }
    }
}

#[tokio::test]
async fn native_readiness_observes_inactive_lanes_worker_exit_and_close() {
    use std::future::Future;

    #[derive(Default)]
    struct CountWake(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    for event in ["remove", "exit", "close"] {
        let (sender, _receivers) = full_radio_sender();
        let submitter = sender.submitter();
        let wake = Arc::new(CountWake::default());
        let waker = std::task::Waker::from(wake.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut wait = Box::pin(submitter.wait_send_ready());
        assert!(wait.as_mut().poll(&mut cx).is_pending());

        for lane in 0..2 {
            release_lane(&sender, event, lane);
            if lane == 0 || event != "close" {
                assert!(
                    wake.0.swap(0, std::sync::atomic::Ordering::SeqCst) > 0,
                    "{event}: no wake"
                );
            }
            if lane == 0 && event != "close" {
                assert!(wait.as_mut().poll(&mut cx).is_pending());
            }
        }
        assert!(wait.as_mut().poll(&mut cx).is_ready(), "{event}");
        assert!(submitter.send_ready());
        let result = submitter.try_send(omq_proto::Message::with_group("group", "after"));
        if event == "close" {
            assert!(matches!(result, Err(omq_proto::TrySendError::Closed)));
            // Also cover closure after the submitter's initial open check.
            assert!(matches!(
                sender
                    .lanes
                    .try_dispatch(test_dispatch("race"), LaneAdmission::Backpressure),
                Err(omq_proto::TrySendError::Closed)
            ));
        } else {
            result.unwrap();
        }
    }
}

/// A RADIO sender over two paused lanes whose one-slot rings are full.
fn full_radio_sender() -> (
    crate::routing::fan_out::FanOutSend,
    Vec<yring::Consumer<LaneData>>,
) {
    let mut sender = crate::routing::fan_out::FanOutSend::new(
        omq_proto::SocketType::Radio,
        &omq_proto::Options::default(),
        FanOutMode::Group,
        &crate::context::IoPoolHandle::none(),
    );
    let (lanes, receivers) = paused_lanes(2);
    sender.lanes = Arc::new(lanes);
    sender
        .lane_peer_count
        .store(2, std::sync::atomic::Ordering::Release);

    let submitter = sender.submitter();
    let message = omq_proto::Message::with_group("group", "body");
    submitter.try_send(message.clone()).unwrap();
    assert!(matches!(
        submitter.try_send(message),
        Err(omq_proto::TrySendError::Full(_))
    ));
    assert!(!submitter.send_ready());
    (sender, receivers)
}

/// Free `lane` the way `event` names: remove its peer, mark its worker
/// exited, or stop admission for all lanes.
fn release_lane(sender: &crate::routing::fan_out::FanOutSend, event: &str, lane: usize) {
    match event {
        "remove" => sender.lanes.remove_peer(lane, 100),
        "exit" => {
            let dist = sender.lanes.distributor.lock().unwrap();
            dist.lanes[lane]
                .exited
                .store(true, std::sync::atomic::Ordering::Release);
            dist.space.notify_changed();
        }
        "close" => sender.stop_admission(),
        _ => unreachable!(),
    }
}

#[test]
fn add_peer_uses_supplied_lane_and_marks_it_active() {
    let lanes = test_lanes(3);

    let assigned = lanes.add_lane_peer(
        2,
        LanePeerAdd {
            peer_id: 7,
            slot: test_slot(7),
            any_groups: false,
        },
    );

    assert_eq!(assigned, Some(2));
    assert!(!lanes.active_flags[0].load(std::sync::atomic::Ordering::Acquire));
    assert!(!lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
    assert!(lanes.active_flags[2].load(std::sync::atomic::Ordering::Acquire));
    let state = lanes.state.lock().expect("lanes poisoned");
    assert_eq!(state.endpoints[2].peer_count, 1);
}

#[test]
fn remove_peer_clears_lane_when_last_peer_leaves() {
    let lanes = test_lanes(2);
    lanes.add_lane_peer(
        1,
        LanePeerAdd {
            peer_id: 11,
            slot: test_slot(11),
            any_groups: false,
        },
    );
    lanes.add_lane_peer(
        1,
        LanePeerAdd {
            peer_id: 12,
            slot: test_slot(12),
            any_groups: false,
        },
    );

    lanes.remove_peer(1, 11);
    assert!(lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
    lanes.remove_peer(1, 12);
    assert!(!lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
    let state = lanes.state.lock().expect("lanes poisoned");
    assert_eq!(state.endpoints[1].peer_count, 0);
}

#[test]
fn linger_counts_distributor_and_secondary_worker_owned_batches() {
    let lanes = test_lanes(2);
    lanes.stop_admission();
    assert!(lanes.is_empty());
    let distributor = lanes.distributor.lock().unwrap().lanes[0].signal.clone();
    distributor.mark();
    distributor.begin_drain();
    assert!(
        !lanes.is_empty(),
        "ring release must not finish a worker batch"
    );
    distributor.clear_after(true);
    assert!(lanes.is_empty());
    let secondary = lanes.state.lock().unwrap().endpoints[1].data_signal.clone();
    secondary.mark();
    assert!(!lanes.is_empty(), "secondary ring was missed");
    secondary.begin_drain();
    assert!(!lanes.is_empty(), "secondary worker batch was missed");
    secondary.clear_after(true);
    assert!(lanes.is_empty());
}

#[tokio::test]
async fn dispatch_waits_for_space_signal_when_nodrop_ring_full() {
    let (data_tx, mut data_rx) = yring::spsc::<LaneData>(1);
    let data_space = Arc::new(crate::engine::signal::StateSignal::new());
    let lanes = FanOutLanes {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
        active_flags: Arc::new(vec![AtomicBool::new(true)]),
        distributor: Mutex::new(LaneDistributor {
            lanes: vec![LaneInput {
                tx: data_tx,
                signal: Arc::new(crate::engine::signal::DataSignal::new()),
                exited: Arc::new(AtomicBool::new(false)),
            }],
            space: data_space.clone(),
        }),
        admission_closed: AtomicBool::new(false),
        mute_policy: FanOutMutePolicy::Block,
    };

    lanes
        .try_dispatch(test_dispatch("one"), super::LaneAdmission::Backpressure)
        .unwrap();

    let send_second = lanes.dispatch(test_dispatch("two"));
    tokio::pin!(send_second);
    tokio::select! {
        () = &mut send_second => panic!("dispatch completed while ring stayed full"),
        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
    }

    data_rx.prefetch();
    let first = data_rx.pop().expect("first dispatch present");
    let LaneData::Dispatch(first) = first;
    assert_eq!(first.msg.part_bytes(0).unwrap().as_ref(), b"one");
    data_rx.release();
    data_space.notify_changed();

    tokio::time::timeout(std::time::Duration::from_secs(1), send_second)
        .await
        .expect("dispatch did not wake after space signal");
}

#[tokio::test]
async fn nodrop_dispatch_returns_when_worker_exits_before_dropping_ring() {
    let (data_tx, data_rx) = yring::spsc::<LaneData>(1);
    let data_space = Arc::new(crate::engine::signal::StateSignal::new());
    let lanes = FanOutLanes {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
        active_flags: Arc::new(vec![AtomicBool::new(true)]),
        distributor: Mutex::new(LaneDistributor {
            lanes: vec![LaneInput {
                tx: data_tx,
                signal: Arc::new(crate::engine::signal::DataSignal::new()),
                exited: Arc::new(AtomicBool::new(false)),
            }],
            space: data_space.clone(),
        }),
        admission_closed: AtomicBool::new(false),
        mute_policy: FanOutMutePolicy::Block,
    };
    lanes
        .try_dispatch(test_dispatch("one"), super::LaneAdmission::Backpressure)
        .unwrap();

    let send_second = lanes.dispatch(test_dispatch("two"));
    tokio::pin!(send_second);
    tokio::select! {
        () = &mut send_second => panic!("dispatch completed while ring stayed full"),
        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
    }

    // The worker's exit: flag, then the final space wake. The ring stays
    // full because nothing drains it anymore.
    lanes.distributor.lock().unwrap().lanes[0]
        .exited
        .store(true, std::sync::atomic::Ordering::Release);
    data_space.notify_changed();
    tokio::time::timeout(std::time::Duration::from_secs(1), send_second)
        .await
        .expect("dispatch kept waiting on a ring nothing drains");
    assert!(
        lanes.is_empty(),
        "an exited worker leaves nothing to linger on"
    );
    drop(data_rx);
}

#[tokio::test]
async fn nodrop_dispatch_returns_if_worker_exited_before_send() {
    use futures::FutureExt;

    for drop_ring in [false, true] {
        let (data_tx, data_rx) = yring::spsc::<LaneData>(1);
        let mut data_rx = Some(data_rx);
        let data_space = Arc::new(crate::engine::signal::StateSignal::new());
        let lanes = FanOutLanes {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
            active_flags: Arc::new(vec![AtomicBool::new(true)]),
            distributor: Mutex::new(LaneDistributor {
                lanes: vec![LaneInput {
                    tx: data_tx,
                    signal: Arc::new(crate::engine::signal::DataSignal::new()),
                    exited: Arc::new(AtomicBool::new(false)),
                }],
                space: data_space.clone(),
            }),
            admission_closed: AtomicBool::new(false),
            mute_policy: FanOutMutePolicy::Block,
        };
        lanes
            .try_dispatch(test_dispatch("one"), super::LaneAdmission::Backpressure)
            .unwrap();

        // Stop can publish its final wake before an in-flight sender
        // tries to push. No later space notification will arrive.
        lanes.distributor.lock().unwrap().lanes[0]
            .exited
            .store(true, std::sync::atomic::Ordering::Release);
        data_space.notify_changed();
        if drop_ring {
            drop(data_rx.take());
        }
        assert!(
            lanes
                .dispatch(test_dispatch("two"))
                .now_or_never()
                .is_some(),
            "dispatch missed the final worker wake (drop_ring={drop_ring})"
        );
    }
}

#[tokio::test]
async fn empty_data_drain_clears_stale_signal() {
    let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
    let data_signal = Arc::new(crate::engine::signal::DataSignal::new());
    let worker = LaneWorker {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        data_rx,
        ctrl: Arc::new(ControlQueue::new()),
        ctrl_batch: VecDeque::new(),
        data_signal: data_signal.clone(),
        data_space: Arc::new(crate::engine::signal::StateSignal::new()),
        mode: FanOutMode::SubscriptionPrefix,
        mute_policy: FanOutMutePolicy::DropNewest,
        peers: FxHashMap::default(),
        subscribe_all_count: 0,
        eq: FrameBuffer::one_shot(),
        chunks: Vec::new(),
        codec_groups: test_plain_groups(),
        exited: Arc::new(AtomicBool::new(false)),
    };

    data_signal.mark();
    data_signal.begin_drain();

    assert!(!worker.finish_data_drain(false));
    tokio::time::timeout(std::time::Duration::from_secs(1), data_signal.ready())
        .await
        .expect("stale notify permit should be consumable once");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), data_signal.ready())
            .await
            .is_err(),
        "empty drain must clear stale readiness"
    );
}

#[test]
fn drop_oldest_lossy_dispatch_keeps_newest_per_peer_frames() {
    let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
    let slot = test_slot_with_msg_cap(7, 2);
    let mut subscriptions = SubscriptionSet::new();
    subscriptions.add(b"");
    let mut peers = FxHashMap::default();
    peers.insert(
        7,
        LanePeer {
            subscriptions,
            groups: FxHashSet::default(),
            any_groups: false,
            slot: slot.clone(),
            dict_shipped: false,
            codec_group: 0,
            needs_flush: false,
        },
    );
    let mut worker = LaneWorker {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        data_rx,
        ctrl: Arc::new(ControlQueue::new()),
        ctrl_batch: VecDeque::new(),
        data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        data_space: Arc::new(crate::engine::signal::StateSignal::new()),
        mode: FanOutMode::SubscriptionPrefix,
        mute_policy: FanOutMutePolicy::DropOldest,
        peers,
        subscribe_all_count: 1,
        eq: FrameBuffer::one_shot(),
        chunks: Vec::new(),
        codec_groups: test_plain_groups(),
        exited: Arc::new(AtomicBool::new(false)),
    };
    let mut touched = SmallVec::new();

    for body in ["first", "second", "third"] {
        worker.dispatch_lossy(&test_dispatch(body), &mut touched);
    }

    let mut actual = Vec::new();
    slot.drain(&mut actual, 1024);
    assert_eq!(
        actual,
        vec![encoded_dispatch("second"), encoded_dispatch("third")]
    );
}

#[cfg(feature = "lz4")]
#[test]
fn drop_oldest_lossy_dispatch_keeps_dict_with_first_payload() {
    let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
    let slot = test_slot_with_msg_cap(7, 1);
    let options = Options::default().compression_dict(Bytes::from_static(b"shared-dict"));
    let mut codec_groups = std::array::from_fn(|_| None);
    codec_groups[0] = Some(
        super::CodecGroup::new(Some(crate::engine::codec::CodecProfile::new(
            CompressionKind::Lz4,
            &options,
        )))
        .unwrap(),
    );
    let mut worker = single_peer_worker(data_rx, &slot, FanOutMutePolicy::DropOldest, codec_groups);
    let mut touched = SmallVec::new();

    let payload = "shared-dict".repeat(16);
    worker.dispatch_lossy(&test_dispatch(&payload), &mut touched);
    worker.dispatch_lossy(&test_dispatch(&payload), &mut touched);

    let mut actual = Vec::new();
    slot.drain(&mut actual, 1024);
    assert_eq!(actual.len(), 1);
    let bytes = actual.concat();
    assert!(
        bytes.windows(4).any(|window| window == b"LZ4D"),
        "dict shipment must stay queued"
    );
    assert!(
        !bytes.windows(4).any(|window| window == b"LZ4B"),
        "compressed payload must not evict or share the protected dict slot"
    );

    worker.dispatch_lossy(&test_dispatch(&payload), &mut touched);
    let mut after_dict = Vec::new();
    slot.drain(&mut after_dict, 1024);
    assert_eq!(after_dict.len(), 1);
    assert!(
        after_dict
            .concat()
            .windows(4)
            .any(|window| window == b"LZ4B"),
        "payload may queue after protected dict drains"
    );
}

/// A worker with one subscribe-all peer 7 on codec group 0.
#[cfg(feature = "lz4")]
fn single_peer_worker(
    data_rx: yring::Consumer<LaneData>,
    slot: &Arc<crate::engine::transmit_slot::PeerTransmitSlot>,
    mute_policy: FanOutMutePolicy,
    codec_groups: [Option<super::CodecGroup>; super::MAX_CODEC_GROUPS],
) -> LaneWorker {
    let mut subscriptions = SubscriptionSet::new();
    subscriptions.add(b"");
    let mut peers = FxHashMap::default();
    peers.insert(
        7,
        LanePeer {
            subscriptions,
            groups: FxHashSet::default(),
            any_groups: false,
            slot: slot.clone(),
            dict_shipped: false,
            codec_group: 0,
            needs_flush: false,
        },
    );

    LaneWorker {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        data_rx,
        ctrl: Arc::new(ControlQueue::new()),
        ctrl_batch: VecDeque::new(),
        data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        data_space: Arc::new(crate::engine::signal::StateSignal::new()),
        mode: FanOutMode::SubscriptionPrefix,
        mute_policy,
        peers,
        subscribe_all_count: 1,
        eq: FrameBuffer::one_shot(),
        chunks: Vec::new(),
        codec_groups,
        exited: Arc::new(AtomicBool::new(false)),
    }
}

#[test]
fn drop_newest_lossy_dispatch_keeps_oldest_per_peer_frames() {
    let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
    let slot = test_slot_with_msg_cap(7, 2);
    let mut subscriptions = SubscriptionSet::new();
    subscriptions.add(b"");
    let mut peers = FxHashMap::default();
    peers.insert(
        7,
        LanePeer {
            subscriptions,
            groups: FxHashSet::default(),
            any_groups: false,
            slot: slot.clone(),
            dict_shipped: false,
            codec_group: 0,
            needs_flush: false,
        },
    );
    let mut worker = LaneWorker {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        data_rx,
        ctrl: Arc::new(ControlQueue::new()),
        ctrl_batch: VecDeque::new(),
        data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        data_space: Arc::new(crate::engine::signal::StateSignal::new()),
        mode: FanOutMode::SubscriptionPrefix,
        mute_policy: FanOutMutePolicy::DropNewest,
        peers,
        subscribe_all_count: 1,
        eq: FrameBuffer::one_shot(),
        chunks: Vec::new(),
        codec_groups: test_plain_groups(),
        exited: Arc::new(AtomicBool::new(false)),
    };
    let mut touched = SmallVec::new();

    for body in ["first", "second", "third"] {
        worker.dispatch_lossy(&test_dispatch(body), &mut touched);
    }

    assert!(!slot.fanout_active());
    let mut actual = Vec::new();
    slot.drain(&mut actual, 1024);
    assert_eq!(actual, vec![encoded_dispatches(&["first", "second"])]);
}

fn batch_worker(
    mute_policy: FanOutMutePolicy,
    slots: &[Arc<crate::engine::transmit_slot::PeerTransmitSlot>],
) -> (LaneWorker, Arc<ControlQueue<LaneControl>>) {
    let (_data_tx, data_rx) = yring::spsc(4);
    let ctrl = Arc::new(ControlQueue::new());
    let peers = slots
        .iter()
        .map(|slot| {
            let mut subscriptions = SubscriptionSet::new();
            subscriptions.add(b"");
            (
                slot.peer_id,
                LanePeer {
                    subscriptions,
                    groups: FxHashSet::default(),
                    any_groups: false,
                    slot: slot.clone(),
                    dict_shipped: false,
                    codec_group: 0,
                    needs_flush: false,
                },
            )
        })
        .collect();
    (
        LaneWorker {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            data_rx,
            ctrl: ctrl.clone(),
            ctrl_batch: VecDeque::new(),
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            mode: FanOutMode::SubscriptionPrefix,
            mute_policy,
            peers,
            subscribe_all_count: slots.len(),
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: test_plain_groups(),
            exited: Arc::new(AtomicBool::new(false)),
        },
        ctrl,
    )
}

fn batch_wire(worker: &LaneWorker) -> (FrameBuffer, SmallVec<[usize; 8]>) {
    let batch: Vec<_> = ["0", "1", "2", "3", "4", "5", "6", "7", "8"]
        .into_iter()
        .map(|body| LaneData::Dispatch(test_dispatch(body)))
        .collect();
    let mut wire = FrameBuffer::new();
    let mut ends = SmallVec::new();
    assert_eq!(worker.encode_plain_batch(&batch, &mut wire, &mut ends), 8);
    (wire, ends)
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shm_dispatch_keeps_ready_peers_progressing_under_both_mute_policies() {
    for policy in [FanOutMutePolicy::Block, FanOutMutePolicy::DropNewest] {
        for size in [16, 256] {
            let (first, second) = std::os::unix::net::UnixStream::pair().unwrap();
            let slow = test_slot_with_msg_cap(7, 1);
            let fast = test_slot(8);
            for (slot, stream) in [(&slow, &first), (&fast, &second)] {
                let state = omq_shm::Connection::new(stream, 8, None).unwrap();
                slot.set_shm(Arc::new(Mutex::new(state)));
                slot.handshake_done
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            let (mut worker, _control) = batch_worker(policy, &[slow.clone(), fast.clone()]);
            let dispatch = LaneDispatch {
                msg: omq_proto::Message::with_group("group", vec![7; size]),
            };
            let mut touched = SmallVec::new();
            let mut pending = SmallVec::new();
            for _ in 0..2 {
                worker.dispatch_shm_group(
                    &[7, 8],
                    &dispatch,
                    &mut touched,
                    (policy == FanOutMutePolicy::Block).then_some(&mut pending),
                );
            }
            assert_eq!(
                pending.len(),
                usize::from(policy == FanOutMutePolicy::Block)
            );
            assert_eq!(slow.fanout_active(), policy == FanOutMutePolicy::Block);
            worker.flush_touched(&mut touched);
            assert!(!fast.data_signal.is_idle());
            let mut fast_frames = Vec::new();
            let mut slow_frames = Vec::new();
            fast.drain(&mut fast_frames, 1024);
            slow.drain(&mut slow_frames, 1024);
            assert_ne!(slow_frames, [] as [bytes::Bytes; 0]);
            assert_eq!(fast_frames.concat().len(), slow_frames.concat().len() * 2);
            if policy == FanOutMutePolicy::Block {
                assert!(
                    !tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        worker.finish_pending(&mut pending, &mut touched)
                    )
                    .await
                    .unwrap()
                );
                assert!(pending.is_empty());
                let mut retried = Vec::new();
                slow.drain(&mut retried, 1024);
                assert_eq!(retried.concat().len(), slow_frames.concat().len());
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn shm_reference_batches_do_not_charge_application_payload_bytes() {
    const SIZE: usize = 1024 * 1024;
    let (stream, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let state = omq_shm::Connection::new(&stream, 16, None).unwrap();
    let slot = test_slot(1);
    slot.set_shm(Arc::new(Mutex::new(state)));
    let (mut worker, _control) = batch_worker(FanOutMutePolicy::Block, &[slot]);
    worker.mode = FanOutMode::Group;
    let pool = omq_shm::payload_pool([(SIZE, 9)]).unwrap();
    let batch: Vec<_> = (0..9)
        .map(|_| {
            let body = pool.message(SIZE, |bytes| bytes.fill(7)).unwrap();
            LaneData::Dispatch(LaneDispatch {
                msg: omq_proto::Message::with_prefix(Bytes::from_static(b"group"), body),
            })
        })
        .collect();
    let (_, count, referenced) = worker.prepare_shm_batch(&batch).unwrap();
    assert!(referenced);
    assert_eq!(count, batch.len());
}

fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

#[tokio::test]
async fn plain_batch_retries_only_blocked_suffix_and_wakes_ready_peers() {
    let slow = test_slot_with_msg_cap(7, 2);
    let fast = test_slot(8);
    let (mut worker, _control) =
        batch_worker(FanOutMutePolicy::Block, &[slow.clone(), fast.clone()]);
    let (wire, ends) = batch_wire(&worker);
    let mut touched = SmallVec::new();
    let dispatch = worker.dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched);
    tokio::pin!(dispatch);
    assert!(poll_once(dispatch.as_mut()).is_pending());
    assert!(
        !fast.data_signal.is_idle(),
        "ready peers must wake before waiting for slow peers"
    );
    assert!(
        !slow.data_signal.is_idle(),
        "accepted prefix must wake its driver"
    );
    let mut fast_received = Vec::new();
    fast.drain(&mut fast_received, 1024);
    assert_eq!(fast_received.concat(), wire.uncommitted_arena());

    let mut slow_received = Vec::new();
    for turn in 0..3 {
        assert!(slow.drain(&mut slow_received, 1024).space_available);
        slow.space_available.notify_changed();
        let result = poll_once(dispatch.as_mut());
        if turn < 2 {
            assert!(result.is_pending());
        } else {
            assert_eq!(result, std::task::Poll::Ready(false));
        }
    }
    slow.drain(&mut slow_received, 1024);
    assert_eq!(
        slow_received.concat(),
        wire.uncommitted_arena(),
        "no repeated or missing publications"
    );
    assert!(fast.is_empty());
}

#[tokio::test]
async fn plain_batch_services_shutdown_while_peer_stays_full() {
    let slow = test_slot_with_msg_cap(7, 1);
    let (mut worker, control) = batch_worker(FanOutMutePolicy::Block, std::slice::from_ref(&slow));
    let (wire, ends) = batch_wire(&worker);
    let mut touched = SmallVec::new();
    let dispatch = worker.dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched);
    tokio::pin!(dispatch);
    assert!(poll_once(dispatch.as_mut()).is_pending());
    control.push(LaneControl::Shutdown);
    assert_eq!(poll_once(dispatch.as_mut()), std::task::Poll::Ready(true));
    assert!(
        !slow.data_signal.is_idle(),
        "shutdown must not hide accepted data"
    );
}

#[tokio::test]
async fn plain_batch_muted_peer_does_not_starve_ready_peer() {
    let slow = test_slot_with_msg_cap(7, 1);
    let fast = test_slot(8);
    let (mut worker, _control) =
        batch_worker(FanOutMutePolicy::DropNewest, &[slow.clone(), fast.clone()]);
    let (wire, ends) = batch_wire(&worker);
    let mut touched = SmallVec::new();
    assert!(
        !worker
            .dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched)
            .await
    );
    assert!(!slow.fanout_active());
    // The slow peer remains full during the next batch.
    assert!(
        !worker
            .dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched)
            .await
    );
    worker.flush_touched(&mut touched);
    assert!(!fast.data_signal.is_idle());
    let mut received = Vec::new();
    fast.drain(&mut received, 1024);
    assert_eq!(received.concat(), wire.uncommitted_arena().repeat(2));
    let mut prefix = Vec::new();
    slow.drain(&mut prefix, 1024);
    assert_eq!(prefix.concat(), &wire.uncommitted_arena()[..ends[0]]);
    assert!(
        slow.fanout_active(),
        "draining below LWM must reactivate the peer"
    );
}

fn test_plain_groups() -> [Option<super::CodecGroup>; super::MAX_CODEC_GROUPS] {
    let mut groups = std::array::from_fn(|_| None);
    groups[0] = Some(super::CodecGroup::new(None).unwrap());
    groups
}

fn test_lanes(count: usize) -> FanOutLanes {
    let endpoints = test_endpoints(count);
    let inputs = endpoints
        .iter()
        .map(|endpoint| {
            let (tx, _rx) = yring::spsc(4);
            LaneInput {
                tx,
                signal: endpoint.data_signal.clone(),
                exited: endpoint.exited.clone(),
            }
        })
        .collect();
    FanOutLanes {
        #[cfg(target_os = "linux")]
        shm_pool: omq_shm::SendPool::new(16, None),
        state: std::sync::Mutex::new(FanOutLaneState { endpoints }),
        active_flags: Arc::new(
            (0..count)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        ),
        distributor: Mutex::new(LaneDistributor {
            lanes: inputs,
            space: Arc::new(crate::engine::signal::StateSignal::new()),
        }),
        admission_closed: AtomicBool::new(false),
        mute_policy: FanOutMutePolicy::DropNewest,
    }
}

fn test_endpoints(count: usize) -> Vec<LaneEndpoint> {
    (0..count).map(|_| test_endpoint()).collect()
}

fn test_endpoint() -> LaneEndpoint {
    LaneEndpoint {
        ctrl: Arc::new(ControlQueue::new()),
        data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        exited: Arc::new(AtomicBool::new(false)),
        peer_count: 0,
        codec_groups: std::array::from_fn(|_| None),
        peer_groups: FxHashMap::default(),
    }
}

fn test_slot(peer_id: u64) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
    test_slot_with_msg_cap(peer_id, 16)
}

fn test_slot_with_msg_cap(
    peer_id: u64,
    msg_cap: usize,
) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
    crate::engine::transmit_slot::PeerTransmitSlot::with_config(
        peer_id,
        SlotConfig {
            arena_threshold: 4096,
            arena_cap: 16 * 1024,
            cap: 64 * 1024,
            msg_cap,
            ..SlotConfig::default()
        },
    )
}

fn test_dispatch(body: &str) -> LaneDispatch {
    let msg = omq_proto::message::Message::from_slice(body.as_bytes());
    LaneDispatch { msg }
}

fn encoded_dispatch(body: &str) -> Bytes {
    let msg = omq_proto::message::Message::from_slice(body.as_bytes());
    let mut eq = FrameBuffer::one_shot();
    let mut chunks = Vec::new();
    let frame = build_fan_out_frame(&mut eq, &msg, &mut chunks, 1, 8 * 1024);
    let bytes = match frame {
        omq_proto::fan_out_frame::FanOutFrame::Arena(raw) => Bytes::copy_from_slice(raw),
        omq_proto::fan_out_frame::FanOutFrame::Chunks(chunks) if chunks.len() == 1 => {
            chunks[0].clone()
        }
        omq_proto::fan_out_frame::FanOutFrame::Chunks(chunks) => {
            let len = chunks.iter().map(Bytes::len).sum();
            let mut buf = bytes::BytesMut::with_capacity(len);
            for chunk in chunks {
                buf.extend_from_slice(chunk);
            }
            buf.freeze()
        }
    };
    clear_fan_out_frame(&mut eq, &mut chunks);
    bytes
}

fn encoded_dispatches(bodies: &[&str]) -> Bytes {
    let mut bytes = bytes::BytesMut::new();
    for body in bodies {
        let encoded = encoded_dispatch(body);
        bytes.extend_from_slice(&encoded);
    }
    bytes.freeze()
}
#[test]
fn dropping_an_unstopped_worker_releases_lane_space_waiters() {
    let (worker, _control) = batch_worker(FanOutMutePolicy::Block, &[]);
    let exited = worker.exited.clone();
    let space = worker.data_space.clone();
    let seen = space.generation();
    drop(worker);
    assert!(exited.load(std::sync::atomic::Ordering::Acquire));
    assert_ne!(space.generation(), seen);
}
