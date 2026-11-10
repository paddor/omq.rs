//! Backend socket creation on first bind or connect, and the receive pump.

use std::ffi::c_int;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use omq_tokio::engine::StateSignal;

use super::{DEFAULT_HWM, OmqSocket};
use crate::error::ETERM;
use crate::notify::{NotifyHandle, RecvNotify};
use crate::opts::{MechanismOverlay, SocketOverlay, ZapOverlay};

/// Dual yring consumers for the recv path.
#[derive(Debug)]
pub(crate) struct RecvConsumers {
    /// Filled directly by the first peer's `ConnectionDriver`.
    pub fast: yring::Consumer<omq_tokio::Message>,
    /// Filled by the recv pump task (fallback for second+ peers).
    pub pump: yring::Consumer<omq_tokio::Message>,
    prefer_pump: bool,
}

impl RecvConsumers {
    #[expect(
        clippy::len_zero,
        reason = "is_empty registers waiters; this snapshot only selects the next source"
    )]
    pub(crate) fn try_pop(&mut self) -> Option<(omq_tokio::Message, bool)> {
        // This length snapshot selects the next source only. Failed pops
        // still register both rings' empty waiters before the caller parks.
        if self.prefer_pump && self.pump.len() > 0 {
            self.prefer_pump = false;
            return self.pump.prefetch_and_pop_with_full();
        }

        if let Some(message) = self.fast.prefetch_and_pop_with_full() {
            self.prefer_pump = true;
            return Some(message);
        }

        self.prefer_pump = false;
        self.pump.prefetch_and_pop_with_full()
    }

    pub(crate) fn refresh(&mut self, config: Option<&Arc<omq_tokio::engine::RecvSinkConfig>>) {
        if self.fast.is_disconnected()
            && let Some(config) = config
            && let Some(consumer) = config.try_take_pending_consumer()
        {
            self.fast = consumer;
        }
    }
}

/// ZAP-related overlay values, snapshotted under the overlay lock.
struct ZapSettings {
    mechanism: MechanismOverlay,
    zap: ZapOverlay,
    identity: Bytes,
}

/// Overlay values read under one lock when the backend socket is created.
struct MaterializeSettings {
    opts: omq_tokio::Options,
    recv_hwm: usize,
    queue_without_ready_peer: bool,
    zap: ZapSettings,
}

impl MaterializeSettings {
    fn from_overlay(overlay: &SocketOverlay) -> Self {
        Self {
            opts: overlay.to_options(),
            recv_hwm: omq_tokio::options::queue_capacity(
                overlay.recv_hwm.unwrap_or(DEFAULT_HWM as u32),
            ),
            queue_without_ready_peer: !overlay.immediate,
            zap: ZapSettings {
                mechanism: overlay.mechanism.clone(),
                zap: overlay.zap.clone(),
                identity: overlay.identity.clone(),
            },
        }
    }
}

/// Route server-side authentication through the context's ZAP handler when
/// the mechanism and domain call for it. Returns whether ZAP is in use.
fn configure_zap_authentication(
    sock: &Arc<OmqSocket>,
    opts: &mut omq_tokio::Options,
    zap_settings: ZapSettings,
) -> Result<bool, c_int> {
    let ZapSettings {
        mechanism,
        zap:
            ZapOverlay {
                domain: zap_domain,
                enforce: zap_enforce,
            },
        identity,
    } = zap_settings;

    let server_mechanism = matches!(
        mechanism,
        MechanismOverlay::Null
            | MechanismOverlay::PlainServer
            | MechanismOverlay::CurveServer { .. }
    );

    if zap_enforce && zap_domain.is_empty() && server_mechanism {
        return Err(libc::EINVAL);
    }

    // PLAIN servers always use ZAP; NULL and CURVE servers only with a domain.
    let needs_zap = matches!(mechanism, MechanismOverlay::PlainServer)
        || (!zap_domain.is_empty()
            && matches!(
                mechanism,
                MechanismOverlay::Null | MechanismOverlay::CurveServer { .. }
            ));

    if !needs_zap {
        return Ok(false);
    }

    if zap_domain.is_empty() {
        return Err(libc::EINVAL);
    }

    let zap = Arc::clone(&sock.ctx.zap);
    let timeout = opts
        .handshake_timeout
        .unwrap_or(std::time::Duration::from_secs(30));

    let authenticator = omq_tokio::Authenticator::new_with_result(move |peer| {
        let query = crate::zap::ZapQuery {
            domain: &zap_domain,
            address: peer.peer_address.as_deref().unwrap_or(""),
            identity: &identity,
            peer,
        };

        zap.authorize(&query, timeout)
    });

    let setup = std::mem::replace(&mut opts.mechanism, omq_tokio::MechanismSetup::Null);
    opts.mechanism = with_authenticator(setup, authenticator);
    Ok(true)
}

/// Hand a server mechanism's authentication to `authenticator`.
fn with_authenticator(
    setup: omq_tokio::MechanismSetup,
    authenticator: omq_tokio::Authenticator,
) -> omq_tokio::MechanismSetup {
    match setup {
        omq_tokio::MechanismSetup::Null => omq_tokio::MechanismSetup::NullServer { authenticator },
        omq_tokio::MechanismSetup::PlainServer { .. } => {
            omq_tokio::MechanismSetup::PlainServer { authenticator }
        }
        omq_tokio::MechanismSetup::CurveServer {
            our_keypair,
            mut options,
        } => {
            options.authenticator = Some(authenticator);
            omq_tokio::MechanismSetup::CurveServer {
                our_keypair,
                options,
            }
        }
        other => other,
    }
}

/// Materialize the omq-tokio socket on the io thread with current overlay
/// options, then start the recv pump. Called once on first bind/connect
/// so that options set between `zmq_socket` and first bind/connect take
/// effect.
pub(crate) fn ensure_materialized(sock: &Arc<OmqSocket>) -> Result<(), c_int> {
    if sock.is_zap_handler() {
        return Err(libc::EINVAL);
    }

    let Some(ctx) = sock.ctx.io_context().cloned() else {
        return Err(ETERM);
    };

    if sock.inner.get().is_some() {
        return Ok(());
    }

    let MaterializeSettings {
        mut opts,
        recv_hwm,
        queue_without_ready_peer,
        zap,
    } = {
        let overlay = sock.overlay.lock().map_err(|_| ETERM)?;
        MaterializeSettings::from_overlay(&overlay)
    };

    let needs_zap = configure_zap_authentication(sock, &mut opts, zap)?;
    opts.validate().map_err(|_| libc::EINVAL)?;

    // CAS guarantees exactly one thread wins the materialization race.
    if sock
        .endpoints
        .bound_or_connected
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        // Another thread already materialized. Spin until inner is set.
        while sock.inner.get().is_none() {
            std::hint::spin_loop();
        }

        return Ok(());
    }

    // SAFETY: materialization runs before hot-path send ownership and freezes
    // bind/connect-scoped options for this socket.
    *unsafe { sock.send.queue_without_ready_peer.get_unchecked() } = queue_without_ready_peer;

    let socket_type = sock.socket_type;

    let cap = recv_hwm.max(16);
    let (fast_prod, fast_cons) = yring::spsc(cap);
    let (pump_prod, pump_cons) = yring::spsc(cap);
    // SAFETY: materialization happens before user recv access and must not
    // bind the later app-facing owner thread.
    *unsafe { sock.recv.consumers.get_unchecked() } = Some(RecvConsumers {
        fast: fast_cons,
        pump: pump_cons,
        prefer_pump: false,
    });

    let recv_space = Arc::new(StateSignal::new());
    let _ = sock.recv.space.set(recv_space.clone());

    let recv_sink_cfg = build_recv_sink(sock, cap, needs_zap, fast_prod, recv_space.clone());
    let _ = sock.recv.sink_config.set(recv_sink_cfg.clone());

    // Enter the tokio runtime context so that Socket::new (which calls
    // tokio::spawn internally for its driver task) and the recv pump
    // spawn below are scheduled on the context's runtime.
    let handle = ctx.handle().clone();
    let _guard = handle.enter();

    let inner = Arc::new(ctx.socket_for_libzmq(socket_type, opts, Some(recv_sink_cfg)));
    let recv_pump = spawn_recv_pump(sock, &inner, pump_prod, recv_space);

    let _ = sock.inner.set(inner);
    let _ = sock.recv.pump.set(recv_pump);
    Ok(())
}

/// Build the direct receive sink. ZAP-authenticated sockets use a shared
/// MPSC queue so every peer's authentication properties stay attached to
/// its messages; the normal hot path retains the SPSC yring.
fn build_recv_sink(
    sock: &Arc<OmqSocket>,
    cap: usize,
    needs_zap: bool,
    fast_prod: yring::Producer<omq_tokio::Message>,
    recv_space: Arc<StateSignal>,
) -> Arc<omq_tokio::engine::RecvSinkConfig> {
    let notify = sock.notify.clone();
    let signal_cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || notify.signal_recv());

    let recv_sink = if needs_zap {
        let (sink, receiver) = omq_tokio::engine::RecvSink::authenticated(cap, signal_cb.clone());
        // SAFETY: like recv_cons, this is set before any user recv access.
        *unsafe { sock.recv.authenticated.get_unchecked() } = Some(receiver);

        sink
    } else {
        omq_tokio::engine::RecvSink::Yring(omq_tokio::engine::YringSink {
            producer: fast_prod,
            signal: Box::new({
                let f = signal_cb.clone();
                move || f()
            }),
            space: recv_space.clone(),
        })
    };

    Arc::new(omq_tokio::engine::RecvSinkConfig::new(
        recv_sink, signal_cb, recv_space, cap,
    ))
}

/// Relay complete transport items into the pump yring. Request/reply
/// validation and admission belong to the application receive path.
/// Handles peers whose drivers push to the internal receive queues.
/// For the single-peer case, the `async_channel` stays empty
/// and this task idles. Must run inside the context runtime.
fn spawn_recv_pump(
    sock: &Arc<OmqSocket>,
    inner: &Arc<omq_tokio::Socket>,
    mut pump_prod: yring::Producer<omq_tokio::Message>,
    recv_space: Arc<StateSignal>,
) -> tokio::task::JoinHandle<()> {
    let s_recv = inner.clone();
    let pump_notify = sock.notify.clone();

    tokio::spawn(async move {
        let recv_notify = pump_notify.recv_notifier();
        while let Ok(msg) = s_recv.recv_for_external_recv().await {
            push_to_pump(&mut pump_prod, msg, recv_notify, &recv_space).await;
        }
    })
}

async fn push_to_pump(
    prod: &mut yring::Producer<omq_tokio::Message>,
    msg: omq_tokio::Message,
    recv_notify: RecvNotify,
    space: &StateSignal,
) {
    let mut m = msg;
    loop {
        match prod.push(m) {
            Ok(()) => break,
            Err(returned) => m = returned,
        }

        // Full: watch for space, then retry once so a pop between the two
        // pushes cannot be missed.
        let seen = space.generation();
        let changed = space.changed_after(seen);
        tokio::pin!(changed);

        match prod.push(m) {
            Ok(()) => break,
            Err(returned) => {
                m = returned;
                changed.await;
            }
        }
    }

    if let yring::FlushResult::Flushed {
        was_empty: true, ..
    } = prod.flush_and_check()
    {
        recv_notify.signal();
    }
}
