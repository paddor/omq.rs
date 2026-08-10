//! Enforced ABI inventory for `include/zmq.h`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const HEADER: &str = include_str!("../include/zmq.h");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FunctionStatus {
    Implemented,
    Alias,
    Extension,
    Partial,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FunctionEntry {
    name: &'static str,
    status: FunctionStatus,
}

const FUNCTION_MATRIX: &[FunctionEntry] = &[
    FunctionEntry {
        name: "omq_ctx_from_share_key",
        status: FunctionStatus::Extension,
    },
    FunctionEntry {
        name: "omq_ctx_share_key",
        status: FunctionStatus::Extension,
    },
    FunctionEntry {
        name: "zmq_atomic_counter_dec",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_atomic_counter_destroy",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_atomic_counter_inc",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_atomic_counter_new",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_atomic_counter_set",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_atomic_counter_value",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_bind",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_close",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_connect",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_connect_peer",
        status: FunctionStatus::Unsupported,
    },
    FunctionEntry {
        name: "zmq_ctx_destroy",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_ctx_get",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ctx_get_ext",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ctx_new",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ctx_set",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ctx_set_ext",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ctx_shutdown",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ctx_term",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_curve_keypair",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_curve_public",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_device",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_disconnect",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_disconnect_peer",
        status: FunctionStatus::Unsupported,
    },
    FunctionEntry {
        name: "zmq_errno",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_getsockopt",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_has",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_init",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_join",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_leave",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_close",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_copy",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_data",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_get",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_gets",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_group",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_init",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_init_buffer",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_init_data",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_init_size",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_more",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_move",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_recv",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_routing_id",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_send",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_set",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_set_group",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_set_routing_id",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_msg_size",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poll",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_add",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_add_fd",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_destroy",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_fd",
        status: FunctionStatus::Partial,
    },
    FunctionEntry {
        name: "zmq_poller_modify",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_modify_fd",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_new",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_remove",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_remove_fd",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_size",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_wait",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_poller_wait_all",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_ppoll",
        status: FunctionStatus::Partial,
    },
    FunctionEntry {
        name: "zmq_proxy",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_proxy_steerable",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_recv",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_recviov",
        status: FunctionStatus::Unsupported,
    },
    FunctionEntry {
        name: "zmq_recvmsg",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_send",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_send_const",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_sendiov",
        status: FunctionStatus::Unsupported,
    },
    FunctionEntry {
        name: "zmq_sendmsg",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_setsockopt",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_sleep",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_socket",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_socket_get_peer_state",
        status: FunctionStatus::Unsupported,
    },
    FunctionEntry {
        name: "zmq_socket_monitor",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_socket_monitor_pipes_stats",
        status: FunctionStatus::Unsupported,
    },
    FunctionEntry {
        name: "zmq_socket_monitor_versioned",
        status: FunctionStatus::Partial,
    },
    FunctionEntry {
        name: "zmq_stopwatch_intermediate",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_stopwatch_start",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_stopwatch_stop",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_strerror",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_term",
        status: FunctionStatus::Alias,
    },
    FunctionEntry {
        name: "zmq_threadclose",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_threadstart",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_add",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_cancel",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_destroy",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_execute",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_new",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_reset",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_set_interval",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_timers_timeout",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_unbind",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_version",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_z85_decode",
        status: FunctionStatus::Implemented,
    },
    FunctionEntry {
        name: "zmq_z85_encode",
        status: FunctionStatus::Implemented,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetStatus {
    RoundTrip,
    Command,
    AcceptedNoop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetStatus {
    RoundTrip,
    State,
    CompatDefault,
    WriteOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketOptionEntry {
    name: &'static str,
    set: SetStatus,
    get: GetStatus,
}

const SOCKET_OPTION_MATRIX: &[SocketOptionEntry] = &[
    opt(
        "ZMQ_AFFINITY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_BACKLOG", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_BINDTODEVICE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_BLOCKY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_BUSY_POLL",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_CONFLATE", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_CONNECT_ROUTING_ID",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_CONNECT_TIMEOUT",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_CURVE_PUBLICKEY",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_CURVE_SECRETKEY",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_CURVE_SERVER",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_CURVE_SERVERKEY",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_DISCONNECT_MSG",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_EVENTS", SetStatus::AcceptedNoop, GetStatus::State),
    opt("ZMQ_FD", SetStatus::AcceptedNoop, GetStatus::State),
    opt(
        "ZMQ_GSSAPI_PLAINTEXT",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_GSSAPI_PRINCIPAL",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_GSSAPI_PRINCIPAL_NAMETYPE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_GSSAPI_SERVER",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_GSSAPI_SERVICE_PRINCIPAL",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_HANDSHAKE_IVL",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_HEARTBEAT_IVL",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_HEARTBEAT_TIMEOUT",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_HEARTBEAT_TTL",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_HELLO_MSG",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_HICCUP_MSG",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_IDENTITY", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_IMMEDIATE", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_IN_BATCH_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_INVERT_MATCHING",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_IPV6", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_LAST_ENDPOINT",
        SetStatus::AcceptedNoop,
        GetStatus::State,
    ),
    opt("ZMQ_LINGER", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_LOOPBACK_FASTPATH",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_MAXMSGSIZE", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_MECHANISM", SetStatus::AcceptedNoop, GetStatus::State),
    opt(
        "ZMQ_METADATA",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_MULTICAST_HOPS",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_MULTICAST_LOOP",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_MULTICAST_MAXTPDU",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_BLOCK_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_BUFFER_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_MODE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_NUM_AUTOPARITY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_NUM_PARITY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_PUSH",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_SEGMENT_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_NORM_UNICAST_NACK",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_ONLY_FIRST_SUBSCRIBE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_OUT_BATCH_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_PLAIN_PASSWORD",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_PLAIN_SERVER",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_PLAIN_USERNAME",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_PRIORITY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_PROBE_ROUTER",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_RATE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_RCVBUF", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_RCVHWM", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_RCVMORE", SetStatus::AcceptedNoop, GetStatus::State),
    opt("ZMQ_RCVTIMEO", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_RECONNECT_IVL",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_RECONNECT_IVL_MAX",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_RECONNECT_STOP",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_RECOVERY_IVL",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_REQ_CORRELATE",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_REQ_RELAXED",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_ROUTER_HANDOVER",
        SetStatus::AcceptedNoop,
        GetStatus::State,
    ),
    opt(
        "ZMQ_ROUTER_MANDATORY",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_ROUTER_NOTIFY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_ROUTER_RAW",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_ROUTING_ID", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_SNDHWM", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_SNDBUF", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt("ZMQ_SNDTIMEO", SetStatus::RoundTrip, GetStatus::RoundTrip),
    opt(
        "ZMQ_SOCKS_PASSWORD",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_SOCKS_PROXY",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_SOCKS_USERNAME",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_STREAM_NOTIFY",
        SetStatus::AcceptedNoop,
        GetStatus::State,
    ),
    opt("ZMQ_SUBSCRIBE", SetStatus::Command, GetStatus::WriteOnly),
    opt(
        "ZMQ_TCP_KEEPALIVE",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_TCP_KEEPALIVE_CNT",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_TCP_KEEPALIVE_IDLE",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_TCP_KEEPALIVE_INTVL",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_TCP_MAXRT",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_THREAD_SAFE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_TOPICS_COUNT",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt("ZMQ_TOS", SetStatus::AcceptedNoop, GetStatus::CompatDefault),
    opt("ZMQ_TYPE", SetStatus::AcceptedNoop, GetStatus::State),
    opt("ZMQ_UNSUBSCRIBE", SetStatus::Command, GetStatus::WriteOnly),
    opt(
        "ZMQ_USE_FD",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_VMCI_BUFFER_MAX_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_VMCI_BUFFER_MIN_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_VMCI_BUFFER_SIZE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_VMCI_CONNECT_TIMEOUT",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_WSS_CERT_PEM",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_WSS_HOSTNAME",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_WSS_KEY_PEM",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_WSS_TRUST_PEM",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_WSS_TRUST_SYSTEM",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_XPUB_MANUAL",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_XPUB_MANUAL_LAST_VALUE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_XPUB_NODROP",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_XPUB_VERBOSE",
        SetStatus::RoundTrip,
        GetStatus::RoundTrip,
    ),
    opt(
        "ZMQ_XPUB_VERBOSER",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_XPUB_WELCOME_MSG",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_XSUB_VERBOSE_UNSUBSCRIBE",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_ZAP_DOMAIN",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
    opt(
        "ZMQ_ZAP_ENFORCE_DOMAIN",
        SetStatus::AcceptedNoop,
        GetStatus::CompatDefault,
    ),
];

const fn opt(name: &'static str, set: SetStatus, get: GetStatus) -> SocketOptionEntry {
    SocketOptionEntry { name, set, get }
}

#[test]
fn function_matrix_matches_header_and_source() {
    let header = exported_functions_from_header();
    let source = no_mangle_functions_from_source();
    let matrix = function_matrix_names();

    assert_unique("function matrix", &matrix);
    assert_eq!(header, matrix, "function matrix must match include/zmq.h");
    assert_eq!(source, header, "source exports must match include/zmq.h");

    assert_status_present(FunctionStatus::Extension);
    assert_status_present(FunctionStatus::Partial);
    assert_status_present(FunctionStatus::Unsupported);
}

#[test]
fn socket_option_matrix_matches_header() {
    let header = socket_options_from_header();
    let matrix = socket_option_matrix_names();

    assert_unique("socket option matrix", &matrix);
    assert_eq!(
        header, matrix,
        "socket option matrix must match include/zmq.h"
    );

    assert_set_status_present(SetStatus::Command);
    assert_set_status_present(SetStatus::AcceptedNoop);
    assert_get_status_present(GetStatus::CompatDefault);
    assert_get_status_present(GetStatus::WriteOnly);
}

fn exported_functions_from_header() -> Vec<String> {
    let mut names = Vec::new();
    let mut current = String::new();
    let mut collecting = false;

    for line in HEADER.lines().map(str::trim) {
        if line.starts_with("ZMQ_EXPORT") {
            collecting = true;
            current.clear();
        }
        if collecting {
            current.push(' ');
            current.push_str(line);
            if line.contains(';') {
                names.push(function_name_from_decl(&current));
                collecting = false;
            }
        }
    }

    names.sort();
    names
}

fn no_mangle_functions_from_source() -> Vec<String> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut names = Vec::new();

    for entry in std::fs::read_dir(manifest.join("src")).expect("src dir") {
        let entry = entry.expect("src dir entry");
        if entry.path().extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }

        let source = std::fs::read_to_string(entry.path()).expect("source file");
        let mut armed = false;
        for line in source.lines().map(str::trim) {
            if line == "#[unsafe(no_mangle)]" {
                armed = true;
                continue;
            }
            if armed
                && line.starts_with("pub extern \"C\" fn ")
                && let Some(name) = line.split_whitespace().nth(4)
            {
                names.push(name.split('(').next().unwrap_or(name).to_owned());
                armed = false;
            }
        }
    }

    names.sort();
    names
}

fn function_name_from_decl(decl: &str) -> String {
    let before_paren = decl.split('(').next().expect("function decl");
    before_paren
        .replace('*', " * ")
        .split_whitespace()
        .rev()
        .find(|token| token.starts_with("zmq_") || token.starts_with("omq_"))
        .expect("function name")
        .to_owned()
}

fn socket_options_from_header() -> Vec<String> {
    let block = HEADER
        .split("/*  Socket options")
        .nth(1)
        .expect("socket option block")
        .split("/*  Deprecated options")
        .next()
        .expect("socket option block end");

    let mut names = Vec::new();
    for line in block.lines().map(str::trim) {
        if !line.starts_with("#define ZMQ_") {
            continue;
        }
        let Some(name) = line.split_whitespace().nth(1) else {
            continue;
        };
        names.push(name.to_owned());
    }

    names.sort();
    names
}

fn function_matrix_names() -> Vec<String> {
    let mut names = FUNCTION_MATRIX
        .iter()
        .map(|entry| entry.name.to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn socket_option_matrix_names() -> Vec<String> {
    let mut names = SOCKET_OPTION_MATRIX
        .iter()
        .map(|entry| entry.name.to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn assert_unique(label: &str, names: &[String]) {
    let mut seen = BTreeSet::new();
    let mut duplicates = Vec::new();
    for name in names {
        if !seen.insert(name) {
            duplicates.push(name.clone());
        }
    }
    assert!(duplicates.is_empty(), "{label} duplicates: {duplicates:?}");
}

fn assert_status_present(status: FunctionStatus) {
    assert!(
        FUNCTION_MATRIX.iter().any(|entry| entry.status == status),
        "missing function status {status:?}"
    );
}

fn assert_set_status_present(status: SetStatus) {
    assert!(
        SOCKET_OPTION_MATRIX.iter().any(|entry| entry.set == status),
        "missing set status {status:?}"
    );
}

fn assert_get_status_present(status: GetStatus) {
    assert!(
        SOCKET_OPTION_MATRIX.iter().any(|entry| entry.get == status),
        "missing get status {status:?}"
    );
}

#[test]
fn option_status_counts_are_stable() {
    let mut counts = BTreeMap::new();
    for entry in SOCKET_OPTION_MATRIX {
        *counts
            .entry(format!("{:?}/{:?}", entry.set, entry.get))
            .or_insert(0) += 1;
    }

    assert_eq!(counts.get("RoundTrip/RoundTrip"), Some(&44));
    assert_eq!(counts.get("AcceptedNoop/CompatDefault"), Some(&55));
    assert_eq!(counts.get("AcceptedNoop/State"), Some(&8));
    assert_eq!(counts.get("Command/WriteOnly"), Some(&2));
}
