//! SHM carrier state as the engine carries it. Off Linux there is no SHM:
//! these types have no values, so every `Option` of them is `None` and the
//! code that passes them along needs no platform cfg.

#[cfg(target_os = "linux")]
mod imp {
    pub(crate) use crate::transport::shm::ShmState;
    pub(crate) use omq_shm::Connection as ShmConnection;

    /// A locked SHM connection.
    pub(crate) type ShmGuard<'a> = std::sync::MutexGuard<'a, ShmConnection>;

    #[inline]
    pub(crate) fn lock(state: &ShmState) -> ShmGuard<'_> {
        state.lock().expect("SHM connection")
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    /// SHM carrier state. There is none off Linux.
    #[derive(Clone, Debug)]
    pub(crate) enum ShmState {}

    /// SHM connection. There is none off Linux.
    #[derive(Debug)]
    pub(crate) enum ShmConnection {}

    /// A locked SHM connection. There is none off Linux.
    pub(crate) enum ShmGuard {}

    impl std::ops::Deref for ShmGuard {
        type Target = ShmConnection;

        fn deref(&self) -> &ShmConnection {
            match *self {}
        }
    }

    impl std::ops::DerefMut for ShmGuard {
        fn deref_mut(&mut self) -> &mut ShmConnection {
            match *self {}
        }
    }

    pub(crate) fn lock(state: &ShmState) -> ShmGuard {
        match *state {}
    }
}

pub(crate) use imp::{ShmConnection, ShmState, lock};
