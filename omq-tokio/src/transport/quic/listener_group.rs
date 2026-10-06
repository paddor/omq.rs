//! Listener UDP ownership and reuse within one socket-control runtime.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use omq_proto::{Error, Options, Result};
use tokio_util::sync::CancellationToken;

use super::ListenerEndpoint;

type Registry = HashMap<(tokio::runtime::Id, SocketAddr), Weak<ListenerGroup>>;
static GROUPS: OnceLock<Mutex<Registry>> = OnceLock::new();
static BINDS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// All reuseport members must outlive every accepted connection: removing
/// even an idle member changes the kernel's socket indices. An unbound group
/// can accept a new listener without rebinding its still-live UDP sockets.
#[derive(Debug)]
pub(super) struct ListenerGroup {
    endpoints: Vec<ListenerEndpoint>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    generation: u64,
    cancel: Option<CancellationToken>,
}

impl ListenerGroup {
    pub(super) async fn bind_guard() -> tokio::sync::MutexGuard<'static, ()> {
        // The plain port probe and reuseport group creation must be one
        // operation across contexts; otherwise two fresh groups can merge.
        BINDS.lock().await
    }

    pub(super) fn cached(addr: SocketAddr) -> Option<Arc<Self>> {
        let mut groups = GROUPS
            .get_or_init(Mutex::default)
            .lock()
            .expect("QUIC groups");
        groups.retain(|_, group| group.strong_count() > 0);
        groups
            .get(&(tokio::runtime::Handle::current().id(), addr))?
            .upgrade()
    }

    pub(super) fn new(
        addr: SocketAddr,
        endpoints: Vec<ListenerEndpoint>,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let group = Arc::new(Self {
            endpoints,
            state: Mutex::new(State {
                generation: 1,
                cancel: Some(cancel),
            }),
        });
        GROUPS
            .get_or_init(Mutex::default)
            .lock()
            .expect("QUIC groups")
            .insert(
                (tokio::runtime::Handle::current().id(), addr),
                Arc::downgrade(&group),
            );
        group
    }

    pub(super) fn claim(
        &self,
        server: &quinn::ServerConfig,
        cancel: CancellationToken,
        options: &Options,
    ) -> Result<u64> {
        let mut state = self.state.lock().expect("QUIC listener state");
        if state
            .cancel
            .as_ref()
            .is_some_and(|token| !token.is_cancelled())
        {
            return Err(Error::Io(std::io::ErrorKind::AddrInUse.into()));
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Config("QUIC listener generation exhausted".into()))?;
        for endpoint in &self.endpoints {
            endpoint.udp.set_server_config(Some(server.clone()));
            endpoint
                .udp
                .raise_buffers(options.recv_buffer_size, options.send_buffer_size);
        }
        state.cancel = Some(cancel);
        Ok(state.generation)
    }

    pub(super) fn release(&self, generation: u64) {
        let mut state = self.state.lock().expect("QUIC listener state");
        // An unbind acknowledges cancellation before its task exits. Its
        // eventual drop must not disable a newer listener on this group.
        if state.generation == generation {
            for endpoint in &self.endpoints {
                endpoint.udp.set_server_config(None);
            }
            state.cancel = None;
        }
    }
}

impl std::ops::Deref for ListenerGroup {
    type Target = [ListenerEndpoint];

    fn deref(&self) -> &Self::Target {
        &self.endpoints
    }
}
