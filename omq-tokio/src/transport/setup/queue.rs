//! Pending setups of WS and QUIC listeners.

use std::net::SocketAddr;
use std::time::Instant;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;

use omq_proto::endpoint::Endpoint;
use omq_proto::flow::DrainBudget;
use omq_proto::{Error, Result};

use super::{AcceptSetup, Admission, PendingHandshake, SetupState};

/// A finished setup: the transport with its admission, or why it failed.
type Finished<T> = (SocketAddr, Result<(T, SetupState)>);

/// Why a listener refused a peer before its setup started.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Refusal {
    /// Socket or listener pending-handshake slots are taken.
    Full,
    /// The setup timeout overflows the clock.
    Clock,
}

impl Refusal {
    fn reason(self) -> &'static str {
        match self {
            Self::Full => "pending-handshake limit reached",
            Self::Clock => "transport setup timeout exceeds clock range",
        }
    }
}

/// Listener setups in flight. Each holds socket and listener admission and
/// one absolute deadline until it hands off or fails.
pub(crate) struct SetupQueue<T> {
    setup: AcceptSetup,
    monitor_endpoint: Endpoint,
    listener_admission: Admission,
    pending: FuturesUnordered<BoxFuture<'static, Finished<T>>>,
}

impl<T> SetupQueue<T> {
    pub(crate) fn new(
        setup: AcceptSetup,
        monitor_endpoint: Endpoint,
        listener_limit: usize,
    ) -> Self {
        Self {
            setup,
            monitor_endpoint,
            listener_admission: Admission::new(listener_limit),
            pending: FuturesUnordered::new(),
        }
    }

    pub(crate) fn set_monitor_endpoint(&mut self, endpoint: Endpoint) {
        self.monitor_endpoint = endpoint;
    }

    /// Setups in flight.
    #[cfg_attr(
        not(feature = "quic"),
        allow(dead_code, reason = "QUIC debug output and WS tests")
    )]
    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }

    /// Listener setup slots in use.
    #[cfg(feature = "quic")]
    pub(crate) fn listener_in_use(&self) -> usize {
        self.listener_admission.in_use()
    }

    #[cfg(feature = "quic")]
    pub(crate) fn io_pool(&self) -> &crate::context::IoPoolHandle {
        &self.setup.io_pool
    }

    /// Reserve socket and listener slots for a peer at `addr` and fix its
    /// setup deadline. A refusal is reported to the monitor.
    pub(crate) fn admit(
        &self,
        addr: SocketAddr,
    ) -> core::result::Result<(SetupState, Instant), Refusal> {
        self.try_admit(addr).inspect_err(|refusal| {
            self.reject(addr, refusal.reason().into());
        })
    }

    fn try_admit(&self, addr: SocketAddr) -> core::result::Result<(SetupState, Instant), Refusal> {
        let admission = PendingHandshake::acquire(
            &self.setup.admission,
            Some(&self.listener_admission),
            Some(addr.ip()),
        )
        .ok_or(Refusal::Full)?;
        let deadline = Instant::now()
            .checked_add(self.setup.timeout)
            .ok_or(Refusal::Clock)?;
        let state = SetupState {
            deadline: Some(deadline),
            cancel: self.setup.cancel.clone(),
            admission,
        };
        Ok((state, deadline))
    }

    pub(crate) fn push(&mut self, setup: impl Future<Output = Finished<T>> + Send + 'static) {
        self.pending.push(Box::pin(setup));
    }

    /// The next finished setup. `Ok(None)` means one failed and was reported
    /// to the monitor; `Err(Error::Closed)` means the socket closed.
    pub(crate) async fn next_ready(&mut self) -> Result<Option<(T, SocketAddr, SetupState)>> {
        tokio::select! {
            biased;
            () = self.setup.cancel.cancelled() => Err(Error::Closed),
            Some((addr, result)) = self.pending.next(), if !self.pending.is_empty() => {
                match result {
                    Ok((accepted, setup)) => Ok(Some((accepted, addr, setup))),
                    Err(error) => {
                        self.reject(addr, error.to_string());
                        Ok(None)
                    }
                }
            }
        }
    }

    fn reject(&self, addr: SocketAddr, reason: String) {
        self.setup
            .monitor
            .publish(omq_proto::MonitorEvent::HandshakeFailed {
                endpoint: self.monitor_endpoint.clone(),
                peer_ident: omq_proto::monitor::PeerIdent::Socket(addr),
                reason,
            });
    }
}

/// Work budget of one listener `accept` call. Every finished setup consumed
/// at most one bounded header, so raw-accept and rejection floods still
/// yield after 32 passes or 128 KiB, even when every future is ready.
pub(crate) struct AcceptBudget(DrainBudget);

impl AcceptBudget {
    pub(crate) fn new() -> Self {
        Self(Self::fresh())
    }

    fn fresh() -> DrainBudget {
        DrainBudget::new(32, 128 * 1024)
    }

    /// Charge one loop pass; yield to the runtime once the budget runs out.
    pub(crate) async fn charge(&mut self) {
        if !self.0.account(4096) {
            tokio::task::yield_now().await;
            self.0 = Self::fresh();
        }
    }
}
