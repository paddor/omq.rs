//! SHM output staging and completion/process waits for the connection driver.

use omq_proto::Result;
use tokio::task::JoinHandle;

use super::PeerTransmitSlot;
#[cfg(target_os = "linux")]
use crate::transport::shm::ShmState;

#[cfg(target_os = "linux")]
use {
    super::OUTBOUND_BATCH_TIME,
    crate::routing::OUTBOUND_BATCH_MAX_MSGS,
    omq_proto::{
        Message,
        flow::{DrainBudget, max_batch_bytes},
        frame_buffer::FrameBuffer,
    },
    std::{collections::VecDeque, time::Instant},
};

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(super) struct Outbound {
    state: ShmState,
    pending: VecDeque<Message>,
}

#[cfg(target_os = "linux")]
impl Outbound {
    pub(super) fn new(state: ShmState) -> Self {
        Self {
            state,
            pending: VecDeque::new(),
        }
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn stage(
        &mut self,
        first: &Message,
        mut try_recv: impl FnMut() -> Option<Message>,
        max_msgs: usize,
        frames: &mut FrameBuffer,
    ) -> Result<usize> {
        let started = Instant::now();
        let mut budget = DrainBudget::new(max_msgs, max_batch_bytes());
        self.pending.push_back(first.clone());
        let _ = budget.account(first.byte_len());
        while !budget.exhausted() && started.elapsed() < OUTBOUND_BATCH_TIME {
            let Some(message) = try_recv() else {
                break;
            };
            let _ = budget.account(message.byte_len());
            self.pending.push_back(message);
        }
        let count = budget.msgs();
        self.drain(frames)?;
        Ok(count)
    }

    pub(super) fn drain(&mut self, frames: &mut FrameBuffer) -> Result<()> {
        let mut state = self.state.lock().expect("SHM connection");
        let mut budget = DrainBudget::new(OUTBOUND_BATCH_MAX_MSGS, max_batch_bytes());
        while !budget.exhausted() {
            let Some(message) = self.pending.front() else {
                break;
            };
            if !state.encode_and_frame(message, frames)? {
                break;
            }
            let _ = budget.account(message.byte_len());
            self.pending.pop_front();
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Driver {
    #[cfg(target_os = "linux")]
    state: Option<ShmState>,
    credit: Option<JoinHandle<()>>,
}

pub(super) async fn peer_death(#[cfg(target_os = "linux")] state: Option<ShmState>) -> Result<()> {
    #[cfg(target_os = "linux")]
    if let Some(state) = state {
        let descriptor = state.lock().expect("SHM connection").peer_descriptor()?;
        if let Some(descriptor) = descriptor {
            let registered = tokio::io::unix::AsyncFd::new(descriptor)?;
            let _ = registered.readable().await?;
        }
        state.lock().expect("SHM connection").peer_exited();
        return Ok(());
    }
    std::future::pending::<Result<()>>().await
}

impl Driver {
    pub(super) fn new(#[cfg(target_os = "linux")] state: Option<ShmState>) -> Self {
        Self {
            #[cfg(target_os = "linux")]
            state,
            credit: None,
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(super) fn arm_credit_wait(&mut self, pending: bool, slot: Option<&PeerTransmitSlot>) {
        #[cfg(target_os = "linux")]
        if self.credit.is_none()
            && (pending || slot.is_some_and(PeerTransmitSlot::shm_credit_blocked))
            && let Some(state) = &self.state
        {
            let wait = state.lock().expect("SHM connection").credit_wait();
            self.credit = Some(tokio::task::spawn_blocking(move || wait.wait()));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (pending, slot);
    }

    pub(super) fn waiting_for_credit(&self) -> bool {
        self.credit.is_some()
    }

    pub(super) async fn credit_returned(&mut self, slot: Option<&PeerTransmitSlot>) {
        if let Some(wait) = &mut self.credit {
            let _ = wait.await;
        } else {
            std::future::pending::<()>().await;
        }
        self.credit = None;
        if let Some(slot) = slot {
            slot.shm_credit_returned();
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(super) fn receive_polling(&self, polling: bool) {
        #[cfg(target_os = "linux")]
        if let Some(state) = &self.state {
            state
                .lock()
                .expect("SHM connection")
                .carrier_receive_polling(polling);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = polling;
    }
}
