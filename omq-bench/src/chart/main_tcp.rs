use super::common::{
    self, C_AERON, C_GRPC, C_IROH, C_LIBZMQ, C_LIBZMQ_2T, C_NATS, C_OMQ_1T, C_OMQ_2T, C_OMQ_3T,
    C_OMQ_4T, C_OMQ_CT, C_OMQ_EXCLUSIVE, C_OMQ_MT, C_OMQ_SPIN, C_RABBITMQ, C_REDIS, C_RZMQ,
    C_RZMQ_IOURING, C_TMQ, C_ZENOH, C_ZMQRS, CpuData, Impl, LatencyMap, ValMap,
    draw_latency_brokered_with_versions, draw_latency_single_panel_with_versions,
    draw_throughput_dual_panel_brokered_with_versions,
    draw_throughput_dual_panel_fixed_2m_msgs_with_versions,
    draw_throughput_dual_panel_with_versions, load_latency, load_tput, out_dir,
};

const TPUT_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];
const PUBSUB_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
const LAT_SIZES: &[u64] = &[16, 32, 64, 256, 1024, 4096];

const PUSHPULL_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "1 IO",
        color: C_LIBZMQ,
    },
    Impl {
        key: "omq-tokio-1t",
        label: "omq",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-ct",
        label: "omq",
        threads: "CT",
        color: C_OMQ_CT,
    },
    Impl {
        key: "omq-tokio-mt",
        label: "omq",
        threads: "12 MT",
        color: C_OMQ_MT,
    },
    Impl {
        key: "tmq",
        label: "tmq",
        threads: "1 IO",
        color: C_TMQ,
    },
    Impl {
        key: "zmq.rs",
        label: "zmq.rs",
        threads: "",
        color: C_ZMQRS,
    },
    Impl {
        key: "rzmq",
        label: "rzmq",
        threads: "",
        color: C_RZMQ,
    },
    Impl {
        key: "rzmq-iouring",
        label: "rzmq-iouring",
        threads: "",
        color: C_RZMQ_IOURING,
    },
];

const REQREP_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "1 IO",
        color: C_LIBZMQ,
    },
    Impl {
        key: "omq-tokio-1t",
        label: "omq",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-1t-spin50",
        label: "omq (50 us spin)",
        threads: "1 IO",
        color: C_OMQ_SPIN,
    },
    Impl {
        key: "omq-tokio-ct",
        label: "omq",
        threads: "CT",
        color: C_OMQ_CT,
    },
    Impl {
        key: "omq-tokio-exclusive",
        label: "omq",
        threads: "EXCL",
        color: C_OMQ_EXCLUSIVE,
    },
    Impl {
        key: "tmq",
        label: "tmq",
        threads: "1 IO",
        color: C_TMQ,
    },
    Impl {
        key: "zmq.rs",
        label: "zmq.rs",
        threads: "",
        color: C_ZMQRS,
    },
    Impl {
        key: "rzmq",
        label: "rzmq",
        threads: "",
        color: C_RZMQ,
    },
    Impl {
        key: "rzmq-iouring",
        label: "rzmq-iouring",
        threads: "",
        color: C_RZMQ_IOURING,
    },
];

const MOM_IMPLS: &[Impl] = &[
    Impl {
        key: "omq-tokio-1t",
        label: "OMQ / TCP",
        threads: "",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-1t-spin50",
        label: "OMQ / TCP (50 μs spin)",
        threads: "",
        color: C_OMQ_SPIN,
    },
    Impl {
        key: "grpc-rust",
        label: "gRPC over HTTP/2",
        threads: "",
        color: C_GRPC,
    },
    Impl {
        key: "rabbitmq",
        label: "AMQP 0-9-1",
        threads: "RabbitMQ",
        color: C_RABBITMQ,
    },
    Impl {
        key: "aeron-udp-2proc",
        label: "Aeron / UDP",
        threads: "",
        color: C_AERON,
    },
    Impl {
        key: "nats",
        label: "NATS",
        threads: "nats-server",
        color: C_NATS,
    },
    Impl {
        key: "redis-streams",
        label: "Redis Streams",
        threads: "Redis",
        color: C_REDIS,
    },
    Impl {
        key: "zenoh-tcp-2proc",
        label: "zenoh / TCP",
        threads: "",
        color: C_ZENOH,
    },
    Impl {
        key: "iroh-quic-2proc",
        label: "iroh / QUIC",
        threads: "",
        color: C_IROH,
    },
];

const MOM_UDP_IMPLS: &[Impl] = &[Impl {
    key: "aeron-udp-2proc",
    label: "Aeron / UDP",
    threads: "",
    color: C_AERON,
}];

const MOM_QUIC_IMPLS: &[Impl] = &[Impl {
    key: "iroh-quic-2proc",
    label: "iroh / QUIC",
    threads: "",
    color: C_IROH,
}];

fn merge_values(dst: &mut ValMap, src: ValMap) {
    for (size, values) in src {
        dst.entry(size).or_default().extend(values);
    }
}

fn mom_tcp_impls() -> Vec<Impl> {
    MOM_IMPLS
        .iter()
        .copied()
        .filter(|imp| imp.key != "aeron-udp-2proc" && imp.key != "iroh-quic-2proc")
        .collect()
}

fn mom_throughput() -> (ValMap, ValMap, std::collections::BTreeMap<String, CpuData>) {
    let (mut tput, mut msgs, mut cpu) = load_tput("throughput", "tcp", None, &mom_tcp_impls());
    for (transport, impls) in [("udp", MOM_UDP_IMPLS), ("quic", MOM_QUIC_IMPLS)] {
        let (other_tput, other_msgs, other_cpu) = load_tput("throughput", transport, None, impls);
        merge_values(&mut tput, other_tput);
        merge_values(&mut msgs, other_msgs);
        cpu.extend(other_cpu);
    }
    (tput, msgs, cpu)
}

fn mom_latency() -> (LatencyMap, std::collections::BTreeMap<String, CpuData>) {
    let (mut lat, mut cpu) = load_latency("tcp", LAT_SIZES, &mom_tcp_impls());
    for (transport, impls) in [("udp", MOM_UDP_IMPLS), ("quic", MOM_QUIC_IMPLS)] {
        let (other_lat, other_cpu) = load_latency(transport, LAT_SIZES, impls);
        for (size, values) in other_lat {
            lat.entry(size).or_default().extend(values);
        }
        cpu.extend(other_cpu);
    }
    (lat, cpu)
}

const PUBSUB_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "1 IO",
        color: C_LIBZMQ,
    },
    Impl {
        key: "libzmq-2t",
        label: "libzmq",
        threads: "2 IO",
        color: C_LIBZMQ_2T,
    },
    Impl {
        key: "omq-tokio-1t",
        label: "omq",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-2t",
        label: "omq",
        threads: "2 IO",
        color: C_OMQ_2T,
    },
    Impl {
        key: "omq-tokio-3t",
        label: "omq",
        threads: "3 IO",
        color: C_OMQ_3T,
    },
    Impl {
        key: "omq-tokio-4t",
        label: "omq",
        threads: "4 IO",
        color: C_OMQ_4T,
    },
    Impl {
        key: "tmq",
        label: "tmq",
        threads: "1 IO",
        color: C_TMQ,
    },
    Impl {
        key: "zmq.rs",
        label: "zmq.rs",
        threads: "",
        color: C_ZMQRS,
    },
    Impl {
        key: "rzmq",
        label: "rzmq",
        threads: "",
        color: C_RZMQ,
    },
    Impl {
        key: "rzmq-iouring",
        label: "rzmq-iouring",
        threads: "",
        color: C_RZMQ_IOURING,
    },
];

pub(crate) fn generate() {
    let dir = out_dir();

    // PUSH/PULL
    let (tput, msgs, cpu) = load_tput("throughput", "tcp", None, PUSHPULL_IMPLS);
    if !tput.is_empty() {
        let out = dir.join("main_pushpull_tcp.svg");
        draw_throughput_dual_panel_fixed_2m_msgs_with_versions(
            &out,
            "PUSH/PULL throughput, ZMQ-family TCP loopback, 2-process",
            TPUT_SIZES,
            PUSHPULL_IMPLS,
            &tput,
            &msgs,
            &cpu,
            "snd CPU%",
            "rcv CPU%",
        )
        .expect("draw pushpull chart");
        eprintln!("Written: {}", out.display());
    }

    // Producer/consumer throughput across direct, RPC, and brokered transports.
    let (tput, msgs, cpu) = mom_throughput();
    if !tput.is_empty() {
        let out = dir.join("main_mom_tcp.svg");
        draw_throughput_dual_panel_brokered_with_versions(
            &out,
            "Producer/consumer throughput, loopback, one flow",
            TPUT_SIZES,
            MOM_IMPLS,
            &tput,
            &msgs,
            &cpu,
            "snd CPU%",
            "broker CPU%",
            "rcv CPU%",
        )
        .expect("draw RPC/MOM chart");
        eprintln!("Written: {}", out.display());
    }

    // Sequential request/reply-like latency across direct, RPC, and brokered transports.
    let (lat, cpu) = mom_latency();
    if !lat.is_empty() {
        let out = dir.join("main_mom_latency_tcp.svg");
        draw_latency_brokered_with_versions(
            &out,
            "Sequential request/reply-like latency, loopback, one flow",
            LAT_SIZES,
            MOM_IMPLS,
            &lat,
            &cpu,
        )
        .expect("draw RPC/MOM latency chart");
        eprintln!("Written: {}", out.display());
    }

    // PUB/SUB (32 peers)
    let (tput, msgs, cpu) = load_tput("pub_sub", "tcp", Some(32), PUBSUB_IMPLS);
    if !tput.is_empty() {
        let out = dir.join("main_pubsub_tcp.svg");
        draw_throughput_dual_panel_with_versions(
            &out,
            "PUB/SUB throughput (32 peers), TCP loopback, 2-process",
            PUBSUB_SIZES,
            PUBSUB_IMPLS,
            &tput,
            &msgs,
            &cpu,
            "snd CPU%",
            "",
        )
        .expect("draw pubsub chart");
        eprintln!("Written: {}", out.display());
    }

    // REQ/REP latency
    let (lat, cpu) = load_latency("tcp", LAT_SIZES, REQREP_IMPLS);
    if !lat.is_empty() {
        let out = dir.join("main_reqrep_tcp.svg");
        let range = common::auto_lat_range(&lat);
        draw_latency_single_panel_with_versions(
            &out,
            "REQ/REP latency, TCP loopback, 2-process",
            LAT_SIZES,
            REQREP_IMPLS,
            &lat,
            &cpu,
            range,
        )
        .expect("draw reqrep chart");
        eprintln!("Written: {}", out.display());
    }
}
