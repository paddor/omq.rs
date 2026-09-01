use super::common::{
    self, C_GRPC, C_KAFKA, C_LIBZMQ, C_LIBZMQ_2T, C_NATS, C_OMQ_1T, C_OMQ_2T, C_OMQ_3T, C_OMQ_4T,
    C_OMQ_CT, C_OMQ_EXCLUSIVE, C_OMQ_MT, C_RABBITMQ, C_REDIS, C_RZMQ, C_RZMQ_IOURING, C_TMQ,
    C_ZMQRS, Impl, draw_latency_single_panel_with_versions,
    draw_throughput_dual_panel_brokered_with_versions,
    draw_throughput_dual_panel_fixed_2m_msgs_with_versions,
    draw_throughput_dual_panel_with_versions, load_latency, load_tput, out_dir,
};

const TPUT_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];
const PUBSUB_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
const LAT_SIZES: &[u64] = &[16, 64, 256, 1024, 4096];

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
        label: "ZMTP",
        threads: "",
        color: C_OMQ_1T,
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
        key: "kafka",
        label: "Kafka",
        threads: "Redpanda",
        color: C_KAFKA,
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
];

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
    let (tput, msgs, cpu) = load_tput("throughput", "tcp", None, MOM_IMPLS);
    if !tput.is_empty() {
        let out = dir.join("main_mom_tcp.svg");
        draw_throughput_dual_panel_brokered_with_versions(
            &out,
            "Producer/consumer throughput, direct/RPC/brokered TCP loopback",
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
