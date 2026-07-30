use super::common::{
    self, C_LIBZMQ, C_LIBZMQ_2T, C_OMQ_1T, C_OMQ_2T, C_OMQ_CT, C_RZMQ, C_RZMQ_IOURING, C_ZMQRS,
    Impl, draw_latency_single_panel, draw_throughput_dual_panel,
    draw_throughput_dual_panel_fixed_2m_msgs, load_latency, load_tput, out_dir,
};

const TPUT_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304,
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
        draw_throughput_dual_panel_fixed_2m_msgs(
            &out,
            "PUSH/PULL throughput, TCP loopback, 2-process",
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

    // PUB/SUB (32 peers)
    let (tput, msgs, cpu) = load_tput("pub_sub", "tcp", Some(32), PUBSUB_IMPLS);
    if !tput.is_empty() {
        let out = dir.join("main_pubsub_tcp.svg");
        draw_throughput_dual_panel(
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
        draw_latency_single_panel(
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
