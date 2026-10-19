use super::{Inner, Socket};
use crate::routing::{SendCompatibility, SendStrategy};
use crate::socket::actor::{DriverEnv, HandleLink, HandleShared, SocketDriver, spawn_driver};
use crate::socket::monitor::MonitorPublisher;
use crate::socket::peer_recv::PeerRecvRoutes;
use crate::socket::recv::{
    BlockingRecvWaker, RecvPipeEnds, RecvTuning, SharedRecvPipe, SpscAwareRecv, SpscHandles,
};
use crate::transport::inproc::InprocRegistry;
use omq_proto::{
    error::{Error, Result},
    options::Options,
    proto::SocketType,
};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Where a new socket runs: its IO pool and inproc namespace.
struct Placement<'a> {
    io_pool: &'a crate::context::IoPoolHandle,
    inproc_registry: Arc<InprocRegistry>,
}

impl Placement<'_> {
    /// Standalone socket on the current Tokio runtime.
    fn standalone(io_pool: &crate::context::IoPoolHandle) -> Placement<'_> {
        Placement {
            io_pool,
            inproc_registry: crate::transport::inproc::standalone_registry(),
        }
    }
}

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
            Placement::standalone(&crate::context::IoPoolHandle::none()),
            SendCompatibility::Native,
        )
    }

    pub(crate) fn new_with_io_pool(
        socket_type: SocketType,
        options: Options,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
    ) -> Self {
        let placement = Placement {
            io_pool,
            inproc_registry,
        };
        Self::new_inner(
            socket_type,
            options,
            None,
            placement,
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
            Placement::standalone(&crate::context::IoPoolHandle::none()),
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
        let placement = Placement {
            io_pool,
            inproc_registry,
        };
        Self::new_inner(
            socket_type,
            options,
            Some(config),
            placement,
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
        let placement = Placement {
            io_pool,
            inproc_registry,
        };
        Self::new_inner(
            socket_type,
            options,
            recv_sink_config,
            placement,
            SendCompatibility::Libzmq,
        )
        .expect("invalid socket options")
    }

    fn new_inner(
        socket_type: SocketType,
        mut options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
        placement: Placement<'_>,
        compatibility: SendCompatibility,
    ) -> Result<Self> {
        prepare_options(socket_type, &mut options)?;
        let tuning = recv_tuning(socket_type, &options);

        let cancel = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(options.send_queue_capacity().max(16));
        let driver_linger = options.linger;
        let monitor = MonitorPublisher::new();
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options, placement.io_pool)
            .with_compatibility(compatibility);
        let send_submitter = send_strategy.submitter();
        // A caller-provided sink replaces the native receive fast paths.
        let recv = ReceiveParts::new(socket_type, &options, recv_sink_config.is_none());
        let shared = HandleShared::new();

        let link = HandleLink {
            cmd_rx,
            recv_tx: recv.recv_tx,
            cancel: cancel.clone(),
            monitor: monitor.clone(),
            spsc: recv.spsc.clone(),
            peer_recv_routes: recv.peer_recv_routes,
            shared: shared.clone(),
        };
        let env = DriverEnv {
            io_pool: placement.io_pool.clone(),
            inproc_registry: placement.inproc_registry,
            recv_sink_config,
        };
        let driver = SocketDriver::new(socket_type, options, send_strategy, link, env);
        #[cfg(feature = "dart")]
        let dart = driver.dart.clone();
        let payload_pools = driver.payload_pools.clone();
        let actor_task = spawn_driver(driver, placement.io_pool);

        Ok(Self {
            send_submitter: send_submitter.clone(),
            receive_handle: recv.receive_handle,
            inner: Arc::new(Inner {
                socket_type,
                payload_pools,
                #[cfg(feature = "dart")]
                dart,
                cmd_tx,
                cancel,
                linger: driver_linger,
                recv_rx: SpscAwareRecv::new(recv.ends, recv.spsc, tuning),
                monitor,
                send_submitter,
                rep_current: Arc::new(Mutex::new(None)),
                req_awaiting_reply: shared.req_awaiting_reply,
                subscribe_count: shared.subscribe_count,
                ready_peer_count: shared.ready_peers,
                last_bound_endpoint: RwLock::new(None),
                actor_task: Mutex::new(Some(actor_task)),
            }),
        })
    }
}

/// Receive-side pieces, split between the socket handle and its actor.
struct ReceiveParts {
    /// Actor end of the shared receive pipe.
    recv_tx: Arc<SharedRecvPipe>,
    /// Handle end of the shared receive pipe.
    ends: RecvPipeEnds,
    spsc: SpscHandles,
    /// Receive handles stay valid while the socket holds this token.
    receive_handle: Arc<()>,
    peer_recv_routes: Option<PeerRecvRoutes>,
}

impl ReceiveParts {
    fn new(socket_type: SocketType, options: &Options, native: bool) -> Self {
        let recv_hwm = options.recv_queue_capacity().max(16);
        let blocking_recv_waker = BlockingRecvWaker::new();
        let (recv_tx, consumer, notify, space) =
            crate::socket::recv::recv_pipe(recv_hwm, blocking_recv_waker.clone());

        let mut spsc = recv_handles(socket_type, options, native, blocking_recv_waker);
        let receive_handle = Arc::new(());
        spsc.set_receive_handles(&receive_handle);
        let peer_recv_routes = (socket_type == SocketType::Peer && native)
            .then(|| spsc.init_peer_recv(recv_hwm, options));

        Self {
            recv_tx,
            ends: RecvPipeEnds {
                consumer,
                notify,
                space,
            },
            spsc,
            receive_handle,
            peer_recv_routes,
        }
    }
}

/// Apply socket-type defaults, then validate.
fn prepare_options(socket_type: SocketType, options: &mut Options) -> Result<()> {
    if socket_type == SocketType::Peer && options.max_message_size.is_none() {
        options.max_message_size = Some(omq_proto::options::DEFAULT_PEER_MAX_BYTES);
    }
    options.validate()?;

    if options.conflate && !crate::routing::supports_conflate(socket_type) {
        return Err(Error::Config(format!(
            "Options::conflate(true) is not supported for socket type {socket_type:?}"
        )));
    }
    Ok(())
}

/// REQ/REP default to the latency profile. Frame transforms (compression)
/// always use the throughput receive path.
fn recv_tuning(socket_type: SocketType, options: &Options) -> RecvTuning {
    let default = if matches!(socket_type, SocketType::Req | SocketType::Rep) {
        omq_proto::WorkloadProfile::Latency
    } else {
        omq_proto::WorkloadProfile::Throughput
    };
    let latency = options.workload_profile.unwrap_or(default)
        == omq_proto::WorkloadProfile::Latency
        && !options.mechanism.has_frame_transform();

    RecvTuning {
        // REP admits one complete request per receive call.
        latency_profile: latency || socket_type == SocketType::Rep,
        batching: options.recv_batching && supports_recv_batching(socket_type),
        spin: options.recv_spin,
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
            options.recv_queue_capacity().max(16),
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
