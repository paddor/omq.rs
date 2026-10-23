use super::impls::TransportKind::Tcp;
use super::impls::{impl_io_threads, latency_rep_subcmd, latency_req_subcmd};
use super::*;

fn io_threads_from_env<'a>(env: &'a [(&'a str, &'a str)]) -> &'a str {
    env.iter()
        .find_map(|&(k, v)| matches!(k, "OMQ_IO_THREADS" | "ZMQ_IO_THREADS").then_some(v))
        .unwrap_or("1")
}

#[test]
fn two_io_omq_impls_use_blocking_binary_source() {
    let ct = find_impl("omq-tokio-ct").unwrap();
    let two_thread = find_impl("omq-tokio-2t").unwrap();
    let curve_two_thread = find_impl("omq-curve-2t").unwrap();

    assert_eq!(ct.binary_from, None);
    assert_eq!(two_thread.binary_from, Some("omq-tokio-1t"));
    assert_eq!(curve_two_thread.binary_from, Some("omq-tokio-1t"));
}

#[test]
fn mt_runtime_omq_uses_tokio_peer_and_runtime_env() {
    let def = find_impl("omq-tokio-mt").unwrap();

    assert_eq!(def.binary_from, Some("omq-tokio-ct"));
    assert_eq!(def.env, &[("OMQ_BENCH_MT_RUNTIME", "1")]);
    assert!(matches!(def.transports, [Tcp]));
}

#[test]
fn paired_benchmark_envs_match_exactly() {
    for def in IMPLS {
        let sender_env = def.env.to_vec();
        let receiver_env = def.env.to_vec();
        let sender_io = io_threads_from_env(&sender_env);
        let receiver_io = io_threads_from_env(&receiver_env);

        assert_eq!(sender_env, receiver_env, "{}", def.name);
        assert_eq!(sender_io, receiver_io, "{}", def.name);
    }
}

#[test]
fn latency_variants_preserve_runtime_and_spin_configuration() {
    for (name, io, spin) in [
        ("omq-tokio-ct", "0", "0"),
        ("omq-tokio-1t", "1", "0"),
        ("omq-tokio-1t-spin50", "1", "50"),
        ("omq-tokio-2t", "2", "0"),
    ] {
        let def = find_impl(name).unwrap();
        let env = latency_env(def, "router-router", "latency");
        assert!(env.contains(&("OMQ_BENCH_LATENCY_PAIR", "router-router")));
        assert!(env.contains(&("OMQ_BENCH_WORKLOAD_PROFILE", "latency")));
        assert!(env.contains(&("OMQ_BENCH_RECV_SPIN_US", spin)));
        assert!(env.contains(&("OMQ_IO_THREADS", io)));
        assert!(supports_latency_pair(def, "router-router", "latency"));
    }
    for name in ["libzmq", "omq-tokio-exclusive", "zmq.rs"] {
        let def = find_impl(name).unwrap();
        assert!(supports_latency_pair(def, "req-rep", "default"));
        assert!(!supports_latency_pair(def, "router-router", "latency"));
    }
    for name in ["libzmq", "libzmq-2t"] {
        let def = find_impl(name).unwrap();
        for pair in ["router-dealer", "router-router", "pair"] {
            assert!(supports_latency_pair(def, pair, "default"));
        }
        for pair in ["client-server", "peer", "channel"] {
            assert!(!supports_latency_pair(def, pair, "default"));
        }
    }
    assert!(!supports_pushpull(
        find_impl("omq-tokio-1t-spin50").unwrap()
    ));
}

#[test]
fn two_io_impls_configure_two_io_threads() {
    for name in [
        "omq-tokio-2t",
        "libzmq-2t",
        "omq-curve-2t",
        "libzmq-curve-2t",
    ] {
        let def = find_impl(name).unwrap();
        assert_eq!(impl_io_threads(def), "2", "{name}");
    }
}

#[test]
fn latency_runner_caps_sizes_at_4kib() {
    assert_eq!(
        latency_sizes_from(&[16, 1024, 4096, 8192, 16384]),
        vec![16, 1024, 4096]
    );
}

#[test]
fn exclusive_latency_uses_exclusive_req_and_rep() {
    let def = find_impl("omq-tokio-exclusive").unwrap();

    assert_eq!(latency_req_subcmd(def), "req-exclusive");
    assert_eq!(latency_rep_subcmd(def), "rep-exclusive");
}
