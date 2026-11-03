//! SHM output staging and completion/process waits for the connection
//! driver. The carrier work lives in `linux`; off Linux the same names
//! exist over uninhabited types and never run.

use omq_proto::Result;
use tokio::task::JoinHandle;

use super::PeerTransmitSlot;
use crate::engine::shm_state::ShmState;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(not(target_os = "linux"))]
use stub as platform;

pub(super) use platform::{Outbound, poll_received};

#[derive(Debug)]
pub(super) struct Driver {
    state: Option<ShmState>,
    credit: Option<JoinHandle<()>>,
}

pub(super) async fn peer_death(state: Option<ShmState>) -> Result<()> {
    if let Some(state) = state {
        return platform::peer_exited(state).await;
    }
    std::future::pending::<Result<()>>().await
}

impl Driver {
    pub(super) fn new(state: Option<ShmState>) -> Self {
        Self {
            state,
            credit: None,
        }
    }

    pub(super) fn arm_credit_wait(&mut self, pending: bool, slot: Option<&PeerTransmitSlot>) {
        if let Some(state) = &self.state
            && self.credit.is_none()
            && (pending || slot.is_some_and(PeerTransmitSlot::shm_credit_blocked))
        {
            self.credit = Some(platform::spawn_credit_wait(state));
        }
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

    pub(super) fn receive_polling(&self, polling: bool) {
        if let Some(state) = &self.state {
            platform::set_receive_polling(state, polling);
        }
    }
}

/// No SHM off Linux: every function takes a value of an uninhabited type.
#[cfg(not(target_os = "linux"))]
mod stub {
    use omq_proto::frame_buffer::FrameBuffer;
    use omq_proto::proto::Connection;
    use omq_proto::{Message, Result};
    use tokio::task::JoinHandle;

    use crate::engine::shm_state::{ShmConnection, ShmState};

    #[derive(Debug)]
    pub(in crate::engine::driver) enum Outbound {}

    impl Outbound {
        #[expect(clippy::needless_pass_by_value, reason = "mirrors the Linux signature")]
        pub(in crate::engine::driver) fn new(state: ShmState) -> Self {
            match state {}
        }

        pub(in crate::engine::driver) fn pending_len(&self) -> usize {
            match *self {}
        }

        pub(in crate::engine::driver) fn has_pending(&self) -> bool {
            match *self {}
        }

        pub(in crate::engine::driver) fn stage(
            &mut self,
            _first: &Message,
            _try_recv: impl FnMut() -> Option<Message>,
            _max_msgs: usize,
            _frames: &mut FrameBuffer,
        ) -> Result<usize> {
            match *self {}
        }

        pub(in crate::engine::driver) fn drain(&mut self, _frames: &mut FrameBuffer) -> Result<()> {
            match *self {}
        }
    }

    pub(in crate::engine::driver) fn poll_received(
        _connection: &mut Connection,
        state: &mut ShmConnection,
    ) -> Result<Option<Message>> {
        match *state {}
    }

    #[expect(clippy::unused_async, reason = "mirrors the Linux signature")]
    pub(super) async fn peer_exited(state: ShmState) -> Result<()> {
        match state {}
    }

    pub(super) fn spawn_credit_wait(state: &ShmState) -> JoinHandle<()> {
        match *state {}
    }

    pub(super) fn set_receive_polling(state: &ShmState, _polling: bool) {
        match *state {}
    }
}
