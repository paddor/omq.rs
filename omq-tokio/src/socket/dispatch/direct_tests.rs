use super::*;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::write_ownership::WriteOwnership;
use omq_proto::Message;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct ScriptedWrite {
    steps: VecDeque<io::Result<usize>>,
    bytes: Vec<u8>,
    calls: usize,
}

impl Write for ScriptedWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        let count = self
            .steps
            .pop_front()
            .unwrap_or(Ok(usize::MAX))?
            .min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn fixture(step: io::Result<usize>) -> (DirectWriteState<ScriptedWrite>, Arc<PeerTransmitSlot>) {
    let state = DirectWriteState {
        stream: ScriptedWrite {
            steps: VecDeque::from([step]),
            bytes: Vec::new(),
            calls: 0,
        },
        ownership: WriteOwnership::new(),
    };
    let slot = PeerTransmitSlot::new(
        1,
        false,
        None,
        None,
        4096,
        4096,
        8192,
        4,
        crate::engine::framing::WireFraming::Zmtp,
    );
    slot.handshake_done.store(true, Ordering::Release);
    (state, slot)
}

#[test]
fn short_write_and_eagain_keep_tail_ahead_of_later_send() {
    for step in [Ok(1), Ok(4), Err(io::ErrorKind::WouldBlock.into())] {
        let (mut state, slot) = fixture(step);
        let first = Message::single("first");
        let second = Message::single("second");
        assert_eq!(state.try_send(&slot, &first), TryFrameResult::Ineligible);
        state.ownership.publish_idle(|| true);
        assert_eq!(state.try_send(&slot, &first), TryFrameResult::Ok);
        assert_eq!(state.stream.calls, 1, "one nonblocking write per admission");
        assert!(!slot.is_empty());
        assert_eq!(state.try_send(&slot, &second), TryFrameResult::Ineligible);
        let mut pending = Vec::new();
        slot.try_drain_arena_only(&mut pending).unwrap();
        assert!(slot.is_empty());
        // Slot empty does not mean TCP idle: driver still owns the staged tail.
        state.ownership.publish_idle(|| pending.is_empty());
        assert_eq!(state.try_send(&slot, &second), TryFrameResult::Ineligible);
        state.stream.bytes.extend_from_slice(&pending);
        pending.clear();
        state.ownership.publish_idle(|| pending.is_empty());
        assert_eq!(state.try_send(&slot, &second), TryFrameResult::Ok);
        assert_eq!(state.stream.bytes, b"\0\x05first\0\x06second");
        assert!(slot.is_empty());
    }
}

#[test]
fn write_failure_accepts_once_and_closed_state_cannot_be_reopened() {
    for step in [Ok(0), Err(io::ErrorKind::BrokenPipe.into())] {
        let (mut state, slot) = fixture(step);
        state.ownership.publish_idle(|| true);
        assert_eq!(
            state.try_send(&slot, &Message::single("accepted")),
            TryFrameResult::Ok
        );
        assert!(state.is_closed());
        assert!(!slot.data_signal.is_idle());
        state.ownership.publish_idle(|| true);
        assert_eq!(
            state.try_send(&slot, &Message::single("retry")),
            TryFrameResult::Dead
        );
        assert_eq!(state.stream.calls, 1);
    }
}
