//! Implementation registry: peers, transports, and capabilities.

use TransportKind::{Inproc, Ipc, Quic, Tcp, Ws};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImplClass {
    Classic,
    IoUring,
    Curve,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportKind {
    Tcp,
    Ipc,
    Inproc,
    Ws,
    Quic,
}

impl TransportKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Ipc => "ipc",
            Self::Inproc => "inproc",
            Self::Ws => "ws",
            Self::Quic => "quic",
        }
    }

    /// URI scheme for transports whose binder reports an ephemeral port.
    pub(super) fn port_scheme(self) -> Option<&'static str> {
        match self {
            Self::Tcp => Some("tcp"),
            Self::Quic => Some("quic"),
            Self::Ipc | Self::Inproc | Self::Ws => None,
        }
    }
}

#[expect(clippy::struct_excessive_bools)]
pub(crate) struct ImplDef {
    pub name: &'static str,
    pub binary_from: Option<&'static str>,
    pub prefix: &'static str,
    pub class: Option<ImplClass>,
    pub main: bool,
    pub transports: &'static [TransportKind],
    pub inproc_tput_subcmd: &'static str,
    pub inproc_lat_subcmd: &'static str,
    pub inproc_pubsub_subcmd: &'static str,
    pub pub_needs_peer_count: bool,
    pub fanout_subcmd: &'static str,
    pub fanio_needs_peer_count: bool,
    pub supports_pubsub: bool,
    pub env: &'static [(&'static str, &'static str)],
}

pub(super) static IMPLS: &[ImplDef] = &[
    ImplDef {
        name: "remoc",
        binary_from: None,
        prefix: "R",
        class: None,
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[],
    },
    ImplDef {
        name: "omq-tokio-ct",
        binary_from: None,
        prefix: "t",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc, Ws, Quic],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "pub-fanout",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "omq-tokio-1t",
        binary_from: None,
        prefix: "b",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Ipc, Inproc, Quic],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "omq-tokio-1t-spin50",
        binary_from: Some("omq-tokio-1t"),
        prefix: "s",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Ipc, Inproc, Quic],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_BENCH_RECV_SPIN_US", "50")],
    },
    ImplDef {
        name: "omq-tokio-mt",
        binary_from: Some("omq-tokio-ct"),
        prefix: "m",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_BENCH_MT_RUNTIME", "1")],
    },
    ImplDef {
        name: "omq-tokio-2t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "u",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc, Ws, Quic],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "2")],
    },
    ImplDef {
        name: "omq-tokio-3t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "w",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "3")],
    },
    ImplDef {
        name: "omq-tokio-4t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "x",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "4")],
    },
    ImplDef {
        name: "omq-tokio-2ut",
        binary_from: Some("omq-tokio-1t"),
        prefix: "v",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Inproc],
        inproc_tput_subcmd: "inproc-2ut",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_IO_THREADS", "1")],
    },
    ImplDef {
        name: "omq-tokio-2ut-blocking",
        binary_from: Some("omq-tokio-1t"),
        prefix: "q",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Inproc],
        inproc_tput_subcmd: "inproc-2ut-blocking",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_IO_THREADS", "1")],
    },
    ImplDef {
        name: "omq-tokio-exclusive",
        binary_from: Some("omq-tokio-ct"),
        prefix: "E",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[],
    },
    ImplDef {
        name: "libzmq",
        binary_from: None,
        prefix: "z",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc, Ws],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "libzmq-2t",
        binary_from: Some("libzmq"),
        prefix: "Y",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Ipc, Ws],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("ZMQ_IO_THREADS", "2")],
    },
    ImplDef {
        name: "tmq",
        binary_from: None,
        prefix: "m",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "r0z-async",
        binary_from: None,
        prefix: "n",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "monocoque-tokio-ct",
        binary_from: None,
        prefix: "o",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "zmq.rs",
        binary_from: None,
        prefix: "q",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "rzmq",
        binary_from: None,
        prefix: "r",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "rzmq-iouring",
        binary_from: Some("rzmq"),
        prefix: "R",
        class: Some(ImplClass::IoUring),
        main: true,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("RZMQ_IO_URING", "1")],
    },
    ImplDef {
        name: "libzmq-curve-1t",
        binary_from: Some("libzmq"),
        prefix: "lc1",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("ZMQ_IO_THREADS", "1"), ("ZMQ_BENCH_CURVE", "1")],
    },
    ImplDef {
        name: "libzmq-curve-2t",
        binary_from: Some("libzmq"),
        prefix: "lc2",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("ZMQ_IO_THREADS", "2"), ("ZMQ_BENCH_CURVE", "1")],
    },
    ImplDef {
        name: "omq-curve-1t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "oc1",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_BENCH_MECHANISM", "curve")],
    },
    ImplDef {
        name: "omq-curve-2t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "oc2",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "2"), ("OMQ_BENCH_MECHANISM", "curve")],
    },
];

pub(super) fn find_impl(name: &str) -> Option<&'static ImplDef> {
    IMPLS.iter().find(|i| i.name == name)
}

impl ImplDef {
    pub(super) fn uses_mt_runtime(&self) -> bool {
        matches!(
            self.name,
            "omq-tokio-mt" | "zmq.rs" | "rzmq" | "rzmq-iouring"
        )
    }

    pub(super) fn affinity(&self, cpus: &'static str) -> Option<&'static str> {
        (!self.uses_mt_runtime()).then_some(cpus)
    }
}

pub(super) fn supports_pushpull(def: &ImplDef) -> bool {
    !matches!(def.name, "omq-tokio-exclusive" | "omq-tokio-1t-spin50")
}

pub(super) fn supports_latency_pair(def: &ImplDef, pair: &str, profile: &str) -> bool {
    (pair == "req-rep" && profile == "default")
        || (def.binary_from.unwrap_or(def.name) == "libzmq"
            && profile == "default"
            && matches!(pair, "router-dealer" | "router-router" | "pair"))
        || (def.name != "omq-tokio-exclusive"
            && matches!(
                def.binary_from.unwrap_or(def.name),
                "omq-tokio-1t" | "omq-tokio-ct"
            ))
}

pub(super) fn latency_env<'a>(
    def: &ImplDef,
    pair: &'a str,
    profile: &'a str,
) -> Vec<(&'a str, &'a str)> {
    let mut env = def.env.to_vec();
    env.push(("OMQ_BENCH_LATENCY_PAIR", pair));
    env.push(("OMQ_BENCH_WORKLOAD_PROFILE", profile));
    // Keep inherited experiment settings from silently changing the baseline.
    if !env.iter().any(|&(key, _)| key == "OMQ_BENCH_RECV_SPIN_US") {
        env.push(("OMQ_BENCH_RECV_SPIN_US", "0"));
    }
    if !env.iter().any(|&(key, _)| key == "OMQ_IO_THREADS") {
        match def.binary_from.unwrap_or(def.name) {
            "omq-tokio-1t" => env.push(("OMQ_IO_THREADS", "1")),
            "omq-tokio-ct" => env.push(("OMQ_IO_THREADS", "0")),
            _ => {}
        }
    }
    env
}

pub(super) fn supports_fanio(def: &ImplDef) -> bool {
    !def.fanout_subcmd.is_empty()
}

pub(super) fn latency_req_subcmd(def: &ImplDef) -> &'static str {
    if def.name == "omq-tokio-exclusive" {
        "req-exclusive"
    } else {
        "req"
    }
}

pub(super) fn latency_rep_subcmd(def: &ImplDef) -> &'static str {
    if def.name == "omq-tokio-exclusive" {
        "rep-exclusive"
    } else {
        "rep"
    }
}

#[cfg(test)]
pub(super) fn impl_io_threads(def: &ImplDef) -> &'static str {
    def.env
        .iter()
        .find_map(|&(k, v)| matches!(k, "OMQ_IO_THREADS" | "ZMQ_IO_THREADS").then_some(v))
        .unwrap_or("1")
}
