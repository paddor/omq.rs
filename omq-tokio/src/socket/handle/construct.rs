use super::{Inner, ReadyPeers, ReqAwaitingReply, Socket};
use crate::routing::{SendCompatibility, SendStrategy};
use crate::socket::actor::{SocketDriver, spawn_driver};
use crate::socket::monitor::MonitorPublisher;
use crate::socket::recv::{SpscAwareRecv, SpscHandles};
use crate::transport::inproc::InprocRegistry;
use omq_proto::{
    error::{Error, Result},
    options::Options,
    proto::SocketType,
    type_state::TypeState,
};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

impl Socket {
    /// Create a new socket of the given type with the given options. Spawns
    /// the driver task on the current tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if the options are invalid for the protocol or socket type.
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        Self::try_new(socket_type, options).expect("invalid socket options")
    }

    /// Create a socket on the current Tokio runtime.
    ///
    /// # Errors
    /// Returns an error if the options are invalid for the protocol or socket type.
    pub fn try_new(socket_type: SocketType, options: Options) -> Result<Self> {
        Self::new_inner(
            socket_type,
            options,
            None,
            &crate::context::IoPoolHandle::none(),
            crate::transport::inproc::standalone_registry(),
            SendCompatibility::Native,
        )
    }

    pub(crate) fn new_with_io_pool(
        socket_type: SocketType,
        options: Options,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
    ) -> Self {
        Self::new_inner(
            socket_type,
            options,
            None,
            io_pool,
            inproc_registry,
            SendCompatibility::Native,
        )
        .expect("invalid socket options")
    }

    /// Like [`Socket::new`], but installs a `RecvSinkConfig` that the
    /// actor will use for the first peer's driver (and refill on
    /// disconnect). Used by omq-libzmq to bypass the recv-pump relay.
    pub fn new_with_recv_sink_config(
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
    ) -> Self {
        Self::try_new_with_recv_sink_config(socket_type, options, config)
            .expect("invalid socket options")
    }

    /// Create a socket with a receive sink on the current Tokio runtime.
    ///
    /// The actor uses this sink for the first peer's driver and refills it
    /// after disconnects.
    ///
    /// # Errors
    /// Returns an error if the options are invalid for the protocol or socket type.
    pub fn try_new_with_recv_sink_config(
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
    ) -> Result<Self> {
        Self::new_inner(
            socket_type,
            options,
            Some(config),
            &crate::context::IoPoolHandle::none(),
            crate::transport::inproc::standalone_registry(),
            SendCompatibility::Native,
        )
    }

    pub(crate) fn new_with_recv_sink_config_and_io_pool(
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
    ) -> Self {
        Self::new_inner(
            socket_type,
            options,
            Some(config),
            io_pool,
            inproc_registry,
            SendCompatibility::Native,
        )
        .expect("invalid socket options")
    }

    pub(crate) fn new_for_libzmq(
        socket_type: SocketType,
        options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
    ) -> Self {
        Self::new_inner(
            socket_type,
            options,
            recv_sink_config,
            io_pool,
            inproc_registry,
            SendCompatibility::Libzmq,
        )
        .expect("invalid socket options")
    }

    fn new_inner(
        socket_type: SocketType,
        mut options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
        compatibility: SendCompatibility,
    ) -> Result<Self> {
        if socket_type == SocketType::Peer && options.max_message_size.is_none() {
            options.max_message_size = Some(omq_proto::options::DEFAULT_PEER_MAX_BYTES);
        }
        options.validate()?;
        let latency_profile = options.workload_profile.unwrap_or(
            if matches!(socket_type, SocketType::Req | SocketType::Rep) {
                omq_proto::WorkloadProfile::Latency
            } else {
                omq_proto::WorkloadProfile::Throughput
            },
        ) == omq_proto::WorkloadProfile::Latency
            && !options.mechanism.has_frame_transform();
        if options.conflate && !crate::routing::supports_conflate(socket_type) {
            return Err(Error::Config(format!(
                "Options::conflate(true) is not supported for socket type {socket_type:?}"
            )));
        }
        let cancel = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(options.send_hwm.max(16) as usize);
        let recv_hwm = options.recv_hwm.max(16) as usize;
        let driver_linger = options.linger;
        let recv_batching = options.recv_batching && supports_recv_batching(socket_type);
        let recv_spin = options.recv_spin;
        let blocking_recv_waker = crate::socket::recv::BlockingRecvWaker::new();
        let (recv_tx, recv_consumer, recv_pipe_notify, recv_pipe_space) =
            crate::socket::recv::recv_pipe(recv_hwm, blocking_recv_waker.clone());
        let monitor = MonitorPublisher::new();
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options, io_pool)
            .with_compatibility(compatibility);
        let send_submitter = send_strategy.submitter();
        let mut spsc = recv_handles(
            socket_type,
            &options,
            recv_sink_config.is_none(),
            blocking_recv_waker,
        );
        let receive_handle = Arc::new(());
        spsc.set_receive_handles(&receive_handle);
        let peer_recv_routes = (socket_type == SocketType::Peer && recv_sink_config.is_none())
            .then(|| spsc.init_peer_recv(recv_hwm, &options));
        let type_state = Arc::new(Mutex::new(TypeState::new()));
        let rep_current = Arc::new(Mutex::new(None));
        let req_awaiting_reply = Arc::new(ReqAwaitingReply::default());
        let subscribe_count = Arc::new(AtomicU64::new(0));
        let ready_peer_count = Arc::new(ReadyPeers::default());
        let mut driver = SocketDriver::new(
            socket_type,
            options,
            cmd_rx,
            recv_tx,
            cancel.clone(),
            monitor.clone(),
            send_strategy,
            spsc.clone(),
            type_state,
            req_awaiting_reply.clone(),
            recv_sink_config,
            subscribe_count.clone(),
            ready_peer_count.clone(),
            io_pool.clone(),
            inproc_registry,
        );
        driver.peer_recv_routes = peer_recv_routes;
        #[cfg(feature = "dart")]
        let dart = driver.dart.clone();
        let payload_pools = driver.payload_pools.clone();
        let actor_task = spawn_driver(driver, io_pool);
        Ok(Self {
            send_submitter: send_submitter.clone(),
            receive_handle,
            inner: Arc::new(Inner {
                socket_type,
                payload_pools,
                #[cfg(feature = "dart")]
                dart,
                cmd_tx,
                cancel,
                linger: driver_linger,
                recv_rx: SpscAwareRecv::new(
                    recv_consumer,
                    recv_pipe_notify,
                    recv_pipe_space,
                    spsc,
                    // REP admits one complete request per receive call.
                    latency_profile || socket_type == SocketType::Rep,
                    recv_batching,
                    recv_spin,
                ),
                monitor,
                send_submitter,
                rep_current,
                req_awaiting_reply,
                subscribe_count,
                ready_peer_count,
                last_bound_endpoint: RwLock::new(None),
                actor_task: Mutex::new(Some(actor_task)),
            }),
        })
    }
}

fn recv_handles(
    socket_type: SocketType,
    options: &Options,
    native: bool,
    blocking_recv_waker: Arc<crate::socket::recv::BlockingRecvWaker>,
) -> SpscHandles {
    let conflate_recv = options.conflate
        && matches!(
            socket_type,
            SocketType::Pull
                | SocketType::Sub
                | SocketType::XSub
                | SocketType::Dish
                | SocketType::Dealer
                | SocketType::Gather
        );
    let mut spsc = SpscHandles::new(blocking_recv_waker, conflate_recv);
    if supports_recv_batching(socket_type) && !conflate_recv && native {
        let source_aware = matches!(socket_type, SocketType::Pull | SocketType::Gather);
        let constructor = if source_aware {
            crate::socket::fanin::Fanin::new_source_aware
        } else {
            crate::socket::fanin::Fanin::new
        };
        spsc.fanin = Some(constructor(
            options.recv_hwm.max(16) as usize,
            spsc.recv_signal.clone(),
            spsc.blocking_recv_waker.clone(),
        ));
    }
    spsc
}

fn supports_recv_batching(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull
            | SocketType::Gather
            | SocketType::Sub
            | SocketType::XSub
            | SocketType::Dish
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_new_returns_invalid_options() {
        assert!(matches!(
            Socket::try_new(SocketType::Pair, Options::default().identity(vec![0])),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            Socket::try_new(SocketType::Pair, Options::default().conflate(true)),
            Err(Error::Config(_))
        ));
    }
}
