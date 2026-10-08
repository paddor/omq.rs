use super::{Phase, Ready};

/// Confirmation state, independent of addresses and runtime clocks. IDs are
/// supplied by the runtime and remain stable across retries of this attempt.
#[derive(Clone, Debug)]
pub struct Handshake {
    local: u64,
    remote: u64,
    connector: bool,
    confirmed: bool,
    pending: Option<Phase>,
}

impl Handshake {
    pub fn connector(local: u64) -> Self {
        assert_ne!(local, 0);
        Self {
            local,
            remote: 0,
            connector: true,
            confirmed: false,
            pending: Some(Phase::Hello),
        }
    }

    pub fn listener(local: u64, remote: u64) -> Self {
        assert!(local != 0 && remote != 0);
        Self {
            local,
            remote,
            connector: false,
            confirmed: false,
            pending: Some(Phase::Welcome),
        }
    }

    pub const fn local(&self) -> u64 {
        self.local
    }
    pub const fn remote(&self) -> u64 {
        self.remote
    }
    pub const fn confirmed(&self) -> bool {
        self.confirmed
    }
    pub const fn pending(&self) -> Option<Phase> {
        self.pending
    }

    /// Reject unknown challenges and changed sessions without changing state.
    /// The owner may start a fresh attempt after observing a peer restart.
    pub fn receive(&mut self, ready: Ready<'_>) -> bool {
        match ready.phase {
            Phase::Hello if !self.connector && ready.session == self.remote && ready.echo == 0 => {
                self.pending = Some(Phase::Welcome);
            }
            Phase::Welcome
                if self.connector
                    && ready.echo == self.local
                    && (self.remote == 0 || self.remote == ready.session) =>
            {
                self.remote = ready.session;
                self.confirmed = true;
                self.pending = Some(Phase::Confirm);
            }
            Phase::Confirm
                if !self.connector && ready.echo == self.local && ready.session == self.remote =>
            {
                self.confirmed = true;
                self.pending = None;
            }
            _ => return false,
        }
        true
    }

    pub fn retry(&mut self) {
        self.pending = Some(if self.connector {
            if self.confirmed {
                Phase::Confirm
            } else {
                Phase::Hello
            }
        } else {
            Phase::Welcome
        });
    }

    pub fn committed(&mut self) {
        self.pending = None;
    }
}
