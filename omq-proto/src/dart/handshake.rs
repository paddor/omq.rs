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
    /// Start a connector attempt with a pending HELLO.
    ///
    /// # Panics
    /// Panics if the local session ID is zero.
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

    /// Start a listener attempt with a pending WELCOME.
    ///
    /// # Panics
    /// Panics if either session ID is zero.
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

    /// Return the local receiver session ID.
    pub const fn local(&self) -> u64 {
        self.local
    }
    /// Return the remote receiver session ID, or zero before WELCOME.
    pub const fn remote(&self) -> u64 {
        self.remote
    }
    /// Whether the remote receiver session has been confirmed.
    pub const fn confirmed(&self) -> bool {
        self.confirmed
    }
    /// Return the next handshake phase awaiting transmission.
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

    /// Schedule retransmission of the current handshake phase.
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

    /// Clear the pending phase after successful transmission.
    pub fn committed(&mut self) {
        self.pending = None;
    }
}
