//! Explicit payload-pool configuration, sealed before endpoint setup.

use std::sync::Mutex;

use omq_proto::{Error, Options, PayloadPool, Result, SocketType};

/// Application send storage and installed receive storage for a socket.
/// Unsupported application directions are `None`. Pool clones share storage.
#[derive(Clone, Debug)]
pub struct SocketPools {
    /// Construct outgoing messages without a socket lifecycle dependency.
    pub send: Option<PayloadPool>,
    /// Storage shared by the socket's receive drivers.
    pub recv: Option<PayloadPool>,
}

#[derive(Debug)]
pub(crate) struct Configuration {
    state: Mutex<State>,
    socket_type: SocketType,
    send_hwm: u32,
    recv_hwm: u32,
}

#[derive(Debug)]
struct State {
    started: bool,
    send: Option<PayloadPool>,
    recv: Option<PayloadPool>,
}

impl Configuration {
    pub(crate) fn new(socket_type: SocketType, options: &Options) -> Self {
        Self {
            state: Mutex::new(State {
                started: false,
                send: None,
                recv: options.recv_payload_pool.clone(),
            }),
            socket_type,
            send_hwm: options.send_hwm,
            recv_hwm: options.recv_hwm,
        }
    }

    pub(crate) fn freeze(&self) {
        self.state
            .lock()
            .expect("payload pool configuration")
            .started = true;
    }

    pub(crate) fn receive(&self) -> Option<PayloadPool> {
        self.state
            .lock()
            .expect("payload pool configuration")
            .recv
            .clone()
    }

    pub(crate) fn set_receive(&self, pool: PayloadPool) -> Result<()> {
        let mut state = self.state.lock().expect("payload pool configuration");
        ensure_mutable(&state)?;
        state.recv = Some(pool);
        Ok(())
    }

    pub(crate) fn initialize(&self) -> Result<SocketPools> {
        let mut state = self.state.lock().expect("payload pool configuration");
        ensure_mutable(&state)?;
        let sends = !matches!(
            self.socket_type,
            SocketType::Pull | SocketType::Gather | SocketType::Sub | SocketType::Dish
        );
        let receives = !matches!(
            self.socket_type,
            SocketType::Push | SocketType::Scatter | SocketType::Pub | SocketType::Radio
        );
        let send = if sends && state.send.is_none() {
            Some(default_pool(self.send_hwm)?)
        } else if sends {
            state.send.clone()
        } else {
            None
        };
        let recv = if receives && state.recv.is_none() {
            Some(default_pool(self.recv_hwm)?)
        } else if receives {
            state.recv.clone()
        } else {
            None
        };
        state.send.clone_from(&send);
        if receives {
            state.recv.clone_from(&recv);
        }
        Ok(SocketPools { send, recv })
    }
}

fn ensure_mutable(state: &State) -> Result<()> {
    if state.started {
        return Err(Error::Config(
            "receive payload pools must be configured before bind/connect".into(),
        ));
    }
    Ok(())
}

fn default_pool(hwm: u32) -> Result<PayloadPool> {
    let slots = hwm.max(1) as usize;
    PayloadPool::new([(if hwm >= 8192 { 2048 } else { 4096 }, slots)])
}
