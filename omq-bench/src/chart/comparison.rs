use super::common::{
    self, C_LIBZMQ, C_OMQ_1T, C_OMQ_CT, C_OMQ_EXCLUSIVE, C_OMQ_SPIN, COMPARISON_LATENCY_SIZES,
    COMPARISON_SIZES, Impl, draw_latency_single_panel, draw_throughput_dual_panel, load_latency,
    load_tput, out_dir,
};

const TCP_TPUT_IMPLS: &[Impl] = &[
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
];

const IPC_TPUT_IMPLS: &[Impl] = &[
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
];

const INPROC_TPUT_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "2 UT",
        color: C_LIBZMQ,
    },
    Impl {
        key: "omq-tokio-ct",
        label: "omq",
        threads: "CT",
        color: C_OMQ_CT,
    },
    Impl {
        key: "omq-tokio-2ut",
        label: "omq",
        threads: "2 UT",
        color: C_OMQ_1T,
    },
];

const TCP_LAT_IMPLS: &[Impl] = &[
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
        label: "omq (50 μs spin)",
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
];

const IPC_LAT_IMPLS: &[Impl] = &[
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
];

const INPROC_LAT_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "2 UT",
        color: C_LIBZMQ,
    },
    Impl {
        key: "omq-tokio-1t",
        label: "omq",
        threads: "2 UT",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-ct",
        label: "omq",
        threads: "CT",
        color: C_OMQ_CT,
    },
];

struct TransportConfig {
    transport: &'static str,
    label: &'static str,
    tput_impls: &'static [Impl],
    lat_impls: &'static [Impl],
    log_gbs: bool,
}

const TRANSPORTS: &[TransportConfig] = &[
    TransportConfig {
        transport: "tcp",
        label: "TCP loopback, 2-process",
        tput_impls: TCP_TPUT_IMPLS,
        lat_impls: TCP_LAT_IMPLS,
        log_gbs: false,
    },
    TransportConfig {
        transport: "ipc",
        label: "IPC, 2-process",
        tput_impls: IPC_TPUT_IMPLS,
        lat_impls: IPC_LAT_IMPLS,
        log_gbs: false,
    },
    TransportConfig {
        transport: "inproc",
        label: "inproc",
        tput_impls: INPROC_TPUT_IMPLS,
        lat_impls: INPROC_LAT_IMPLS,
        log_gbs: true,
    },
];

pub(crate) fn generate() {
    let dir = out_dir();

    for tc in TRANSPORTS {
        // PUSH/PULL throughput
        let (tput, msgs, cpu) = load_tput("throughput", tc.transport, None, tc.tput_impls);
        if !tput.is_empty() {
            let sub = dir.join("pushpull");
            std::fs::create_dir_all(&sub).ok();
            let out = sub.join(format!("{}.svg", tc.transport));
            if tc.log_gbs {
                common::draw_throughput_dual_panel_log_gbs(
                    &out,
                    &format!("PUSH/PULL throughput: {}", tc.label),
                    COMPARISON_SIZES,
                    tc.tput_impls,
                    &tput,
                    &msgs,
                    &cpu,
                    "snd CPU%",
                    "rcv CPU%",
                )
                .expect("draw pushpull chart");
            } else {
                draw_throughput_dual_panel(
                    &out,
                    &format!("PUSH/PULL throughput: {}", tc.label),
                    COMPARISON_SIZES,
                    tc.tput_impls,
                    &tput,
                    &msgs,
                    &cpu,
                    "snd CPU%",
                    "rcv CPU%",
                )
                .expect("draw pushpull chart");
            }
            eprintln!("Written: {}", out.display());
        }

        // REQ/REP latency
        let (lat, cpu) = load_latency(tc.transport, COMPARISON_LATENCY_SIZES, tc.lat_impls);
        if !lat.is_empty() {
            let sub = dir.join("reqrep");
            std::fs::create_dir_all(&sub).ok();
            let out = sub.join(format!("{}.svg", tc.transport));
            let range = common::auto_lat_range(&lat);
            draw_latency_single_panel(
                &out,
                &format!("REQ/REP latency: {}", tc.label),
                COMPARISON_LATENCY_SIZES,
                tc.lat_impls,
                &lat,
                &cpu,
                range,
            )
            .expect("draw reqrep chart");
            eprintln!("Written: {}", out.display());
        }
    }
}
