//! Fan-out send: per-peer `PeerWireSlot`, filtered by subscription.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. On every `send`, the message is encoded once
//! (via `pre_encode`), then the pre-encoded chunks are pushed into
//! each matching peer's `EncodedQueue`. The driver flushes to the wire.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use smallvec::SmallVec;

use bytes::Bytes;

use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::transform::MessageEncoder;

use super::peer_send::PeerSend;
use super::subscription::SubscriptionSet;
use crate::engine::wire_slot::TryEncodeResult;

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

fn yield_interval(peer_count: usize) -> u32 {
    let n = (peer_count as u32).max(1);
    (256 / n).max(4)
}

enum CachedResult {
    SoleWire(TryEncodeResult),
    Cached {
        targets: Arc<Vec<PeerSend>>,
        encoder: Option<Arc<Mutex<MessageEncoder>>>,
        all_wire: bool,
    },
    Miss,
}

#[derive(Debug)]
pub(crate) struct FanOutArena {
    eq: Mutex<EncodedQueue>,
    data_ready: Notify,
    pending: AtomicBool,
}

impl FanOutArena {
    fn new() -> Self {
        Self {
            eq: Mutex::new(EncodedQueue::one_shot()),
            data_ready: Notify::new(),
            pending: AtomicBool::new(false),
        }
    }

    fn encode_and_signal(&self, msg: &Message, encoder: Option<&Mutex<MessageEncoder>>) {
        let mut eq = self.eq.lock().unwrap();
        if let Some(enc_mtx) = encoder {
            let transformed = {
                let mut enc = enc_mtx.lock().expect("fan_out_encoder poisoned");
                match enc.encode(msg) {
                    Ok(t) => t,
                    Err(_) => return,
                }
            };
            for wire_msg in &transformed {
                eq.encode_auto(wire_msg);
            }
        } else {
            eq.encode_auto(msg);
        }
        drop(eq);
        if !self.pending.swap(true, Ordering::Release) {
            self.data_ready.notify_one();
        }
    }

    fn drain_to(&self, targets: &[PeerSend], chunks: &mut Vec<Bytes>) {
        chunks.clear();
        {
            let mut eq = self.eq.lock().unwrap();
            eq.drain_into_vec(chunks, 1024);
            self.pending.store(false, Ordering::Relaxed);
        }
        if chunks.is_empty() {
            return;
        }
        for t in targets {
            if let PeerSend::Wire { slot, .. } = t {
                let _ = slot.try_push_encoded(chunks);
            }
        }
    }

    fn drain_to_targets(&self, targets: &[PeerSend]) {
        let mut chunks = Vec::new();
        self.drain_to(targets, &mut chunks);
    }
}

#[derive(Debug)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    cached: Mutex<CachedFanOut>,
    arena: Arc<FanOutArena>,
}

#[derive(Debug, Default)]
struct CachedFanOut {
    generation: u64,
    sole_wire: Option<PeerSend>,
    all_targets: Option<Arc<Vec<PeerSend>>>,
    encoder: Option<Arc<Mutex<MessageEncoder>>>,
    all_wire: bool,
}

impl Clone for Submitter {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            cached: Mutex::new(CachedFanOut::default()),
            arena: self.arena.clone(),
        }
    }
}

impl Submitter {
    fn validate_group(msg: Message) -> core::result::Result<(Message, Option<String>), Error> {
        if msg.len() != 2 {
            return Err(Error::Protocol(
                "RADIO send requires [group, body] (2 parts)".into(),
            ));
        }
        let group_bytes = msg.part_bytes(0).unwrap_or_default();
        if group_bytes.len() > u8::MAX as usize {
            return Err(Error::Protocol(
                "RADIO group name too long (max 255 bytes)".into(),
            ));
        }
        let group = String::from_utf8_lossy(&group_bytes).into_owned();
        Ok((msg, Some(group)))
    }

    fn prepare(&self, msg: Message) -> core::result::Result<(Message, Option<String>), Error> {
        match self.mode {
            FanOutMode::SubscriptionPrefix => Ok((msg, None)),
            FanOutMode::Group => Self::validate_group(msg),
        }
    }

    fn try_cached(&self, msg: &Message, group: Option<&str>) -> CachedResult {
        if group.is_some() {
            return CachedResult::Miss;
        }
        let current = self.generation.load(Ordering::Acquire);
        let mut cached = self.cached.lock().unwrap();
        if cached.generation != current {
            if cached.all_wire
                && let Some(ref targets) = cached.all_targets
            {
                self.arena.drain_to_targets(targets);
            }
            let g = self.inner.lock().expect("fanout inner poisoned");
            if g.all_subscribe_all && g.all_targets.len() == 1 && g.fan_out_encoder.is_none() {
                cached.sole_wire = Some(g.all_targets[0].clone());
                cached.all_targets = None;
                cached.encoder = None;
                cached.all_wire = false;
            } else if g.all_subscribe_all && !g.all_targets.is_empty() {
                cached.sole_wire = None;
                let targets: Vec<PeerSend> = g.all_targets.to_vec();
                cached.all_wire = targets.iter().all(|t| matches!(t, PeerSend::Wire { .. }));
                cached.all_targets = Some(Arc::new(targets));
                cached.encoder.clone_from(&g.fan_out_encoder);
            } else {
                cached.sole_wire = None;
                cached.all_targets = None;
                cached.encoder = None;
                cached.all_wire = false;
            }
            cached.generation = current;
        }
        if let Some(ref target) = cached.sole_wire {
            return CachedResult::SoleWire(target.try_encode(msg));
        }
        if let Some(ref targets) = cached.all_targets {
            return CachedResult::Cached {
                targets: targets.clone(),
                encoder: cached.encoder.clone(),
                all_wire: cached.all_wire,
            };
        }
        CachedResult::Miss
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let (forwarded, group) = self
            .prepare(msg)
            .map_err(omq_proto::error::TrySendError::Error)?;

        match self.try_cached(&forwarded, group.as_deref()) {
            CachedResult::SoleWire(r) => {
                return match r {
                    TryEncodeResult::Ok | TryEncodeResult::Dead | TryEncodeResult::Ineligible => {
                        Ok(())
                    }
                    TryEncodeResult::Full => Err(omq_proto::error::TrySendError::Full(forwarded)),
                };
            }
            CachedResult::Cached {
                targets,
                encoder,
                all_wire,
            } => {
                if all_wire {
                    self.arena.drain_to_targets(&targets);
                }
                if self.xpub_nodrop && !targets_have_space(&targets) {
                    return Err(omq_proto::error::TrySendError::Full(forwarded));
                }
                dispatch_to_targets(&targets, &forwarded, encoder.as_deref());
                return Ok(());
            }
            CachedResult::Miss => {}
        }

        let (targets, encoder) = self.collect_targets(&forwarded, group.as_deref());
        if self.xpub_nodrop && !targets_have_space(&targets) {
            return Err(omq_proto::error::TrySendError::Full(forwarded));
        }
        dispatch_to_targets(&targets, &forwarded, encoder.as_deref());
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = self.prepare(msg)?;

        match self.try_cached(&forwarded, group.as_deref()) {
            CachedResult::SoleWire(TryEncodeResult::Ok) => return Ok(()),
            CachedResult::Cached {
                targets,
                encoder,
                all_wire,
            } => {
                if all_wire {
                    self.arena.encode_and_signal(&forwarded, encoder.as_deref());
                    let interval = yield_interval(targets.len());
                    if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
                        tokio::task::yield_now().await;
                    }
                } else {
                    if self.xpub_nodrop {
                        while !targets_have_space(&targets) {
                            tokio::task::yield_now().await;
                        }
                    }
                    dispatch_to_targets(&targets, &forwarded, encoder.as_deref());
                    let interval = yield_interval(targets.len());
                    if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
                        tokio::task::yield_now().await;
                    }
                }
                return Ok(());
            }
            _ => {}
        }

        let (targets, encoder) = self.collect_targets(&forwarded, group.as_deref());
        if self.xpub_nodrop {
            while !targets_have_space(&targets) {
                tokio::task::yield_now().await;
            }
        }
        dispatch_to_targets(&targets, &forwarded, encoder.as_deref());
        let interval = yield_interval(targets.len());
        if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    fn collect_targets(
        &self,
        msg: &Message,
        group: Option<&str>,
    ) -> (SmallVec<[PeerSend; 8]>, Option<Arc<Mutex<MessageEncoder>>>) {
        let g = self.inner.lock().expect("fanout inner poisoned");
        let targets = if g.all_subscribe_all && matches!(self.mode, FanOutMode::SubscriptionPrefix)
        {
            g.all_targets.clone()
        } else {
            g.peers
                .values()
                .filter(|p| match (self.mode, group) {
                    (FanOutMode::Group, Some(grp)) => p.any_groups || p.groups.contains(grp),
                    (FanOutMode::SubscriptionPrefix, _) => {
                        p.subscriptions.matches(&first_frame_bytes(msg))
                    }
                    (FanOutMode::Group, None) => false,
                })
                .map(|p| p.target.clone())
                .collect()
        };
        let encoder = g.fan_out_encoder.clone();
        (targets, encoder)
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
    arena: Arc<FanOutArena>,
    pump_cancel: CancellationToken,
    pump_spawned: bool,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    all_subscribe_all: bool,
    all_targets: SmallVec<[PeerSend; 8]>,
    fan_out_encoder: Option<Arc<Mutex<MessageEncoder>>>,
    #[allow(dead_code)]
    options: Options,
}

impl std::fmt::Debug for FanOutInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOutInner")
            .field("peers", &self.peers.len())
            .field("all_subscribe_all", &self.all_subscribe_all)
            .field("has_fan_out_encoder", &self.fan_out_encoder.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    target: PeerSend,
}

impl FanOutInner {
    #[allow(clippy::unused_self)]
    fn init_fan_out_encoder(&mut self) {
        #[cfg(feature = "lz4")]
        {
            use omq_proto::endpoint::{Endpoint, Host};
            let dummy = Endpoint::Lz4Tcp {
                host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
                port: 0,
            };
            // Strip the dict: per-connection dict shipping requires the
            // driver's per-peer encoder, which the fan-out path bypasses.
            // Dictless compression still applies the lz4 block transform.
            let mut opts = self.options.clone();
            opts.compression_dict = None;
            if let Some((enc, _dec)) = MessageEncoder::for_endpoint(&dummy, &opts) {
                self.fan_out_encoder = Some(Arc::new(Mutex::new(enc.new_offload())));
            }
        }
    }

    fn recompute_subscribe_all(&mut self) {
        self.all_subscribe_all = !self.peers.is_empty()
            && self
                .peers
                .values()
                .all(|p| p.subscriptions.is_subscribe_all());
        if self.all_subscribe_all {
            self.all_targets = self.peers.values().map(|p| p.target.clone()).collect();
        } else {
            self.all_targets.clear();
        }
    }
}

impl FanOutSend {
    pub(crate) fn new(options: &Options, mode: FanOutMode) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FanOutInner {
                peers: FxHashMap::default(),
                all_subscribe_all: false,
                all_targets: SmallVec::new(),
                fan_out_encoder: None,
                options: options.clone(),
            })),
            generation: Arc::new(AtomicU64::new(0)),
            mode,
            xpub_nodrop: options.xpub_nodrop,
            arena: Arc::new(FanOutArena::new()),
            pump_cancel: CancellationToken::new(),
            pump_spawned: false,
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            cached: Mutex::new(CachedFanOut::default()),
            arena: self.arena.clone(),
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: DriverHandle) {
        self.add_peer(peer_id, handle, false);
    }

    pub(crate) fn connection_added_any_groups(&mut self, peer_id: u64, handle: DriverHandle) {
        self.add_peer(peer_id, handle, true);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(&mut self, peer_id: u64, handle: DriverHandle, any_groups: bool) {
        if !self.pump_spawned {
            self.pump_spawned = true;
            tokio::spawn(fan_out_pump(
                self.arena.clone(),
                self.inner.clone(),
                self.generation.clone(),
                self.pump_cancel.child_token(),
            ));
        }
        let has_transform = handle.wire_slot.as_ref().is_some_and(|s| s.has_transform);
        let target = PeerSend::from_handle(&handle);
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups,
                target,
            },
        );
        if has_transform && g.fan_out_encoder.is_none() {
            g.init_fan_out_encoder();
        }
        g.recompute_subscribe_all();
        self.bump_generation();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if g.peers.remove(&peer_id).is_some() {
            g.recompute_subscribe_all();
            drop(g);
            self.bump_generation();
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.add(&prefix);
            g.recompute_subscribe_all();
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.remove(prefix);
            g.recompute_subscribe_all();
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.insert(s.to_string());
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.remove(s);
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.pump_cancel.cancel();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let eq = self.arena.eq.lock().unwrap();
        if !eq.is_empty() {
            return false;
        }
        drop(eq);
        let g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.values().all(|p| p.target.is_empty())
    }
}

fn targets_have_space(targets: &[PeerSend]) -> bool {
    targets.iter().all(|t| match t {
        PeerSend::Wire { slot, .. } => slot.has_space(),
        PeerSend::Inbox(_) => true,
    })
}

fn dispatch_to_targets(
    targets: &[PeerSend],
    msg: &Message,
    encoder: Option<&Mutex<MessageEncoder>>,
) {
    match targets.len() {
        0 => {}
        1 => {
            let _ = targets[0].try_encode(msg);
        }
        _ => {
            use std::cell::RefCell;
            thread_local! {
                static ARENA: RefCell<EncodedQueue> = RefCell::new(
                    EncodedQueue::one_shot(),
                );
                static CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
            }
            ARENA.with(|cell| {
                let eq = &mut *cell.borrow_mut();
                if let Some(enc_mtx) = encoder {
                    let transformed = {
                        let mut enc = enc_mtx.lock().expect("fan_out_encoder poisoned");
                        match enc.encode(msg) {
                            Ok(t) => t,
                            Err(_) => return,
                        }
                    };
                    for wire_msg in &transformed {
                        eq.encode_auto(wire_msg);
                    }
                } else {
                    eq.encode_auto(msg);
                }
                CHUNKS.with(|drain| {
                    let chunks = &mut *drain.borrow_mut();
                    chunks.clear();
                    eq.drain_into_vec(chunks, 1024);
                    for t in targets {
                        match t {
                            PeerSend::Wire { slot, .. } => {
                                let _ = slot.try_push_encoded(chunks);
                            }
                            PeerSend::Inbox(tx) => {
                                let _ = tx.try_send(DriverCommand::SendMessage(msg.clone()));
                            }
                        }
                    }
                    chunks.clear();
                });
            });
        }
    }
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

async fn fan_out_pump(
    arena: Arc<FanOutArena>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    let mut chunks = Vec::new();
    let mut cached_gen = 0u64;
    let mut targets: Vec<PeerSend> = Vec::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                refresh_pump_targets(&inner, &generation, &mut cached_gen, &mut targets);
                arena.drain_to(&targets, &mut chunks);
                return;
            }
            () = async { arena.data_ready.notified().await } => {
                let current = generation.load(Ordering::Acquire);
                if cached_gen != current {
                    refresh_pump_targets(&inner, &generation, &mut cached_gen, &mut targets);
                }
                arena.drain_to(&targets, &mut chunks);
            }
        }
    }
}

fn refresh_pump_targets(
    inner: &Mutex<FanOutInner>,
    generation: &AtomicU64,
    cached_gen: &mut u64,
    targets: &mut Vec<PeerSend>,
) {
    let g = inner.lock().expect("fanout inner poisoned");
    targets.clear();
    targets.extend_from_slice(&g.all_targets);
    *cached_gen = generation.load(Ordering::Acquire);
}
