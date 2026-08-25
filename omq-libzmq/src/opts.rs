//! Socket options overlay and `zmq_setsockopt` / `zmq_getsockopt`.
//!
//! The match arms in `zmq_setsockopt` and `zmq_getsockopt` are
//! intentionally repetitive: each option is self-contained and easy
//! to audit. Extracting duration-conversion helpers was considered
//! and rejected as not worth the indirection.
#![expect(clippy::cast_possible_wrap)]

use std::ffi::c_int;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::MechanismSetup;
use omq_tokio::options::{KeepAlive, OnMute, ReconnectPolicy};

use crate::error::fail;
#[cfg(unix)]
use crate::notify::NotifyHandle;
use crate::socket::DEFAULT_HWM;

macro_rules! lock_overlay {
    ($sock:expr) => {
        match $sock.overlay.lock() {
            Ok(g) => g,
            Err(_) => return crate::error::fail(crate::error::ETERM),
        }
    };
}

#[derive(Clone, Debug)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct SocketOverlay {
    pub send_hwm: Option<u32>,
    pub recv_hwm: Option<u32>,
    pub linger: LingerSetting,
    pub identity: Bytes,
    pub router_mandatory: bool,
    pub reconnect_ivl: Option<Duration>,
    pub reconnect_ivl_max: Option<Duration>,
    pub heartbeat_ivl: Option<Duration>,
    pub heartbeat_ttl: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub handshake_ivl: Option<Duration>,
    pub max_message_size: Option<usize>,
    pub arena_threshold: Option<usize>,
    pub conflate: bool,
    pub tcp_keepalive: i32,
    pub tcp_keepalive_cnt: Option<u32>,
    pub tcp_keepalive_idle: Option<Duration>,
    pub tcp_keepalive_intvl: Option<Duration>,
    pub mechanism: MechanismOverlay,
    pub sndbuf: Option<usize>,
    pub rcvbuf: Option<usize>,
    pub xpub_verbose: bool,
    pub on_mute: OnMute,
    pub compression_level: Option<i32>,
    pub compression_dict: Option<Bytes>,
    pub compression_auto_train: bool,
    pub ipv6: bool,
    pub backlog: i32,
    pub immediate: bool,
    pub connect_timeout: i32,
    pub probe_router: bool,
    pub req_correlate: bool,
    pub req_relaxed: bool,
    pub xpub_nodrop: bool,
    pub reconnect_stop: i32,
    pub wss_key_pem: Option<Vec<u8>>,
    pub wss_cert_pem: Option<Vec<u8>>,
    pub wss_trust_pem: Option<Vec<u8>>,
    pub wss_hostname: Option<String>,
    pub wss_trust_system: bool,
}

impl Default for SocketOverlay {
    fn default() -> Self {
        Self {
            send_hwm: None,
            recv_hwm: None,
            linger: LingerSetting::Unset,
            identity: Bytes::new(),
            router_mandatory: false,
            reconnect_ivl: Some(Duration::from_millis(100)),
            reconnect_ivl_max: None,
            heartbeat_ivl: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_ivl: None,
            max_message_size: None,
            arena_threshold: None,
            conflate: false,
            tcp_keepalive: 0,
            tcp_keepalive_cnt: None,
            tcp_keepalive_idle: None,
            tcp_keepalive_intvl: None,
            mechanism: MechanismOverlay::Null,
            sndbuf: None,
            rcvbuf: None,
            xpub_verbose: false,
            on_mute: OnMute::Block,
            compression_level: None,
            compression_dict: None,
            compression_auto_train: false,
            ipv6: false,
            backlog: 0,
            immediate: false,
            connect_timeout: 0,
            probe_router: false,
            req_correlate: false,
            req_relaxed: false,
            xpub_nodrop: false,
            reconnect_stop: 0,
            wss_key_pem: None,
            wss_cert_pem: None,
            wss_trust_pem: None,
            wss_hostname: None,
            wss_trust_system: true,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) enum MechanismOverlay {
    #[default]
    Null,
    PlainServer,
    PlainClient {
        username: String,
        password: String,
    },
    CurveServer {
        secret_key: [u8; 32],
    },
    CurveClient {
        public_key: [u8; 32],
        secret_key: [u8; 32],
        server_key: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum LingerSetting {
    #[default]
    Unset,
    Forever,
    Finite(Duration),
}

impl SocketOverlay {
    pub(crate) fn effective_linger(&self) -> Option<Duration> {
        match self.linger {
            LingerSetting::Unset | LingerSetting::Forever => None,
            LingerSetting::Finite(d) => Some(d),
        }
    }

    fn linger_ms(&self) -> i32 {
        match self.linger {
            LingerSetting::Unset | LingerSetting::Forever => -1,
            LingerSetting::Finite(d) => d.as_millis() as i32,
        }
    }

    pub(crate) fn to_options(&self) -> omq_tokio::Options {
        let keepalive = match self.tcp_keepalive {
            1 => KeepAlive::Enabled {
                idle: self.tcp_keepalive_idle.unwrap_or(Duration::from_mins(1)),
                intvl: self.tcp_keepalive_intvl.unwrap_or(Duration::from_secs(10)),
                cnt: self.tcp_keepalive_cnt.unwrap_or(6),
            },
            0 => KeepAlive::Disabled,
            _ => KeepAlive::Default,
        };
        let reconnect = match (self.reconnect_ivl, self.reconnect_ivl_max) {
            (None, _) => ReconnectPolicy::Disabled,
            (Some(min), None) => ReconnectPolicy::Fixed(min),
            (Some(min), Some(max)) => ReconnectPolicy::Exponential { min, max },
        };
        let mechanism = match &self.mechanism {
            MechanismOverlay::Null => MechanismSetup::Null,
            MechanismOverlay::PlainServer => MechanismSetup::PlainServer {
                authenticator: omq_tokio::Authenticator::new(|_| true),
            },
            MechanismOverlay::PlainClient { username, password } => MechanismSetup::PlainClient {
                username: username.clone(),
                password: password.clone(),
            },
            MechanismOverlay::CurveServer { secret_key } => {
                let sec = omq_tokio::CurveSecretKey::from_bytes(*secret_key);
                let crypto_sec = crypto_box::SecretKey::from(*secret_key);
                let crypto_pub = crypto_sec.public_key();
                let pubk = omq_tokio::CurvePublicKey::from_bytes(*crypto_pub.as_bytes());
                MechanismSetup::CurveServer {
                    our_keypair: omq_tokio::CurveKeypair {
                        secret: sec,
                        public: pubk,
                    },
                    options: omq_tokio::CurveServerOptions::default(),
                }
            }
            MechanismOverlay::CurveClient {
                public_key,
                secret_key,
                server_key,
            } => MechanismSetup::CurveClient {
                our_keypair: omq_tokio::CurveKeypair {
                    secret: omq_tokio::CurveSecretKey::from_bytes(*secret_key),
                    public: omq_tokio::CurvePublicKey::from_bytes(*public_key),
                },
                server_public: omq_tokio::CurvePublicKey::from_bytes(*server_key),
            },
        };
        omq_tokio::Options {
            send_hwm: self.send_hwm.unwrap_or(DEFAULT_HWM as u32),
            recv_hwm: self.recv_hwm.unwrap_or(DEFAULT_HWM as u32),
            linger: self.effective_linger(),
            identity: self.identity.clone(),
            max_message_size: self.max_message_size,
            arena_threshold: self.arena_threshold,
            router_mandatory: self.router_mandatory,
            heartbeat_interval: self.heartbeat_ivl,
            heartbeat_ttl: self.heartbeat_ttl,
            heartbeat_timeout: self.heartbeat_timeout,
            handshake_timeout: match (self.handshake_ivl, self.connect_timeout) {
                (Some(h), t) if t > 0 => Some(h.min(Duration::from_millis(t as u64))),
                (None, t) if t > 0 => Some(Duration::from_millis(t as u64)),
                (None, _) => Some(Duration::from_millis(DEFAULT_HANDSHAKE_IVL_MS as u64)),
                (h, _) => h,
            },
            conflate: self.conflate,
            tcp_keepalive: keepalive,
            reconnect,
            send_buffer_size: self.sndbuf,
            recv_buffer_size: self.rcvbuf,
            mechanism,
            on_mute: self.on_mute,
            compression_level: self.compression_level,
            compression_dict: self.compression_dict.clone(),
            compression_auto_train: self.compression_auto_train,
            xpub_nodrop: self.xpub_nodrop,
            reconnect_stop_conn_refused: (self.reconnect_stop & 1) != 0,
            wss_tls: omq_tokio::options::WssTls {
                server_cert_pem: self.wss_cert_pem.clone(),
                server_key_pem: self.wss_key_pem.clone(),
                trust_pem: self.wss_trust_pem.clone(),
                hostname: self.wss_hostname.clone(),
                trust_system: self.wss_trust_system,
                accept_invalid_certs: false,
            },
            ..Default::default()
        }
    }
}

// ZMQ socket option constants
const ZMQ_SNDHWM: c_int = 23;
const ZMQ_RCVHWM: c_int = 24;
const ZMQ_SNDTIMEO: c_int = 28;
const ZMQ_RCVTIMEO: c_int = 27;
const ZMQ_SUBSCRIBE: c_int = 6;
const ZMQ_UNSUBSCRIBE: c_int = 7;
const ZMQ_LINGER: c_int = 17;
const ZMQ_IDENTITY: c_int = 5;
const ZMQ_RCVMORE: c_int = 13;
const ZMQ_TYPE: c_int = 16;
const ZMQ_FD: c_int = 14;
const ZMQ_EVENTS: c_int = 15;
const ZMQ_RECONNECT_IVL: c_int = 18;
const ZMQ_RECONNECT_IVL_MAX: c_int = 21;
const ZMQ_HEARTBEAT_IVL: c_int = 75;
const ZMQ_HEARTBEAT_TTL: c_int = 76;
const ZMQ_HEARTBEAT_TIMEOUT: c_int = 77;
const ZMQ_HANDSHAKE_IVL: c_int = 66;
const ZMQ_MAXMSGSIZE: c_int = 22;
const ZMQ_ROUTER_MANDATORY: c_int = 33;
const ZMQ_CONFLATE: c_int = 54;
const ZMQ_TCP_KEEPALIVE: c_int = 34;
const ZMQ_TCP_KEEPALIVE_CNT: c_int = 35;
const ZMQ_TCP_KEEPALIVE_IDLE: c_int = 36;
const ZMQ_TCP_KEEPALIVE_INTVL: c_int = 37;
const ZMQ_SNDBUF: c_int = 11;
const ZMQ_RCVBUF: c_int = 12;
const ZMQ_XPUB_VERBOSE: c_int = 40;
const ZMQ_LAST_ENDPOINT: c_int = 32;
const ZMQ_MECHANISM: c_int = 43;
const ZMQ_PLAIN_SERVER: c_int = 44;
const ZMQ_PLAIN_USERNAME: c_int = 45;
const ZMQ_PLAIN_PASSWORD: c_int = 46;
const ZMQ_CURVE_SERVER: c_int = 47;
const ZMQ_CURVE_PUBLICKEY: c_int = 48;
const ZMQ_CURVE_SECRETKEY: c_int = 49;
const ZMQ_CURVE_SERVERKEY: c_int = 50;

const ZMQ_BACKLOG: c_int = 19;
const ZMQ_IMMEDIATE: c_int = 39;
const ZMQ_IPV6: c_int = 42;
const ZMQ_PROBE_ROUTER: c_int = 51;
const ZMQ_REQ_CORRELATE: c_int = 52;
const ZMQ_REQ_RELAXED: c_int = 53;
const ZMQ_ROUTER_HANDOVER: c_int = 56;
const ZMQ_XPUB_NODROP: c_int = 69;
const ZMQ_CONNECT_TIMEOUT: c_int = 79;

const ZMQ_AFFINITY: c_int = 4;
const ZMQ_RATE: c_int = 8;
const ZMQ_RECOVERY_IVL: c_int = 9;
const ZMQ_MULTICAST_HOPS: c_int = 25;
const ZMQ_IPV4ONLY: c_int = 31;
const ZMQ_TCP_ACCEPT_FILTER: c_int = 38;
const ZMQ_ROUTER_RAW: c_int = 41;
const ZMQ_ZAP_DOMAIN: c_int = 55;
const ZMQ_TOS: c_int = 57;
const ZMQ_IPC_FILTER_PID: c_int = 58;
const ZMQ_IPC_FILTER_UID: c_int = 59;
const ZMQ_IPC_FILTER_GID: c_int = 60;
const ZMQ_CONNECT_ROUTING_ID: c_int = 61;
const ZMQ_GSSAPI_SERVER: c_int = 62;
const ZMQ_GSSAPI_PRINCIPAL: c_int = 63;
const ZMQ_GSSAPI_SERVICE_PRINCIPAL: c_int = 64;
const ZMQ_GSSAPI_PLAINTEXT: c_int = 65;
const ZMQ_SOCKS_PROXY: c_int = 68;
const ZMQ_BLOCKY: c_int = 70;
const ZMQ_XPUB_MANUAL: c_int = 71;
const ZMQ_XPUB_WELCOME_MSG: c_int = 72;
const ZMQ_STREAM_NOTIFY: c_int = 73;
const ZMQ_INVERT_MATCHING: c_int = 74;
const ZMQ_XPUB_VERBOSER: c_int = 78;
const ZMQ_TCP_MAXRT: c_int = 80;
const ZMQ_THREAD_SAFE: c_int = 81;
const ZMQ_MULTICAST_MAXTPDU: c_int = 84;
const ZMQ_VMCI_BUFFER_SIZE: c_int = 85;
const ZMQ_VMCI_BUFFER_MIN_SIZE: c_int = 86;
const ZMQ_VMCI_BUFFER_MAX_SIZE: c_int = 87;
const ZMQ_VMCI_CONNECT_TIMEOUT: c_int = 88;
const ZMQ_USE_FD: c_int = 89;
const ZMQ_GSSAPI_PRINCIPAL_NAMETYPE: c_int = 90;
const ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE: c_int = 91;
const ZMQ_BINDTODEVICE: c_int = 92;
const ZMQ_ZAP_ENFORCE_DOMAIN: c_int = 93;
const ZMQ_LOOPBACK_FASTPATH: c_int = 94;
const ZMQ_METADATA: c_int = 95;
const ZMQ_MULTICAST_LOOP: c_int = 96;
const ZMQ_ROUTER_NOTIFY: c_int = 97;
const ZMQ_XPUB_MANUAL_LAST_VALUE: c_int = 98;
const ZMQ_SOCKS_USERNAME: c_int = 99;
const ZMQ_SOCKS_PASSWORD: c_int = 100;
const ZMQ_IN_BATCH_SIZE: c_int = 101;
const ZMQ_OUT_BATCH_SIZE: c_int = 102;
const ZMQ_WSS_KEY_PEM: c_int = 103;
const ZMQ_WSS_CERT_PEM: c_int = 104;
const ZMQ_WSS_TRUST_PEM: c_int = 105;
const ZMQ_WSS_HOSTNAME: c_int = 106;
const ZMQ_WSS_TRUST_SYSTEM: c_int = 107;
const ZMQ_ONLY_FIRST_SUBSCRIBE: c_int = 108;
const ZMQ_RECONNECT_STOP: c_int = 109;
const ZMQ_HELLO_MSG: c_int = 110;
const ZMQ_DISCONNECT_MSG: c_int = 111;
const ZMQ_PRIORITY: c_int = 112;
const ZMQ_BUSY_POLL: c_int = 113;
const ZMQ_HICCUP_MSG: c_int = 114;
const ZMQ_XSUB_VERBOSE_UNSUBSCRIBE: c_int = 115;
const ZMQ_TOPICS_COUNT: c_int = 116;
const ZMQ_NORM_MODE: c_int = 117;
const ZMQ_NORM_UNICAST_NACK: c_int = 118;
const ZMQ_NORM_BUFFER_SIZE: c_int = 119;
const ZMQ_NORM_SEGMENT_SIZE: c_int = 120;
const ZMQ_NORM_BLOCK_SIZE: c_int = 121;
const ZMQ_NORM_NUM_PARITY: c_int = 122;
const ZMQ_NORM_NUM_AUTOPARITY: c_int = 123;
const ZMQ_NORM_PUSH: c_int = 124;

const ZMQ_POLLIN: c_int = crate::consts::ZMQ_POLLIN;
const ZMQ_POLLOUT: c_int = crate::consts::ZMQ_POLLOUT;

const ZMQ_NULL: c_int = 0;
const ZMQ_PLAIN: c_int = 1;
const ZMQ_CURVE: c_int = 2;

const DEFAULT_HANDSHAKE_IVL_MS: i32 = 30_000;
const OMQ_ON_MUTE: c_int = 1004;
const OMQ_COMPRESSION_LEVEL: c_int = 1005;
const OMQ_COMPRESSION_DICT: c_int = 1006;
const OMQ_COMPRESSION_AUTO_TRAIN: c_int = 1007;
const OMQ_ARENA_THRESHOLD: c_int = 10_001;
const OMQ_ON_MUTE_BLOCK: c_int = 0;
const OMQ_ON_MUTE_DROP_NEWEST: c_int = 1;
const OMQ_ON_MUTE_DROP_OLDEST: c_int = 2;
const ZSTD_LEVEL_MIN: i32 = -8;
const ZSTD_LEVEL_MAX: i32 = 4;
const COMPRESSION_DICT_MAX: usize = 8 * 1024;

#[expect(clippy::too_many_lines)]
#[unsafe(no_mangle)]
pub extern "C" fn zmq_setsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> c_int {
    if sock.is_null() {
        return crate::error::fail(crate::error::ENOTSOCK);
    }
    // SAFETY: caller guarantees sock is a valid socket pointer from zmq_socket.
    let sock_arc = unsafe { &*(sock.cast::<std::sync::Arc<crate::socket::OmqSocket>>()) };

    match option {
        ZMQ_SNDTIMEO => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            sock_arc
                .sndtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_RCVTIMEO => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            sock_arc
                .rcvtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_SNDHWM => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            if v <= 0 {
                return fail(libc::EINVAL);
            }
            lock_overlay!(sock_arc).send_hwm = Some(v as u32);
        }
        ZMQ_RCVHWM => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            if v <= 0 {
                return fail(libc::EINVAL);
            }
            lock_overlay!(sock_arc).recv_hwm = Some(v as u32);
        }
        ZMQ_LINGER => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).linger = if v < 0 {
                LingerSetting::Forever
            } else {
                LingerSetting::Finite(Duration::from_millis(v as u64))
            };
        }
        ZMQ_IDENTITY => {
            if optval.is_null() {
                return crate::error::fail(libc::EFAULT);
            }
            // SAFETY: optval is non-null (checked above); optvallen bytes are readable.
            let bytes = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
            lock_overlay!(sock_arc).identity = Bytes::copy_from_slice(bytes);
        }
        ZMQ_RECONNECT_IVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).reconnect_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).reconnect_ivl_max = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_IVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).heartbeat_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TTL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).heartbeat_ttl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).heartbeat_timeout = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HANDSHAKE_IVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).handshake_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_MAXMSGSIZE => {
            let Some(v) = read_i64(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).max_message_size = if v < 0 { None } else { Some(v as usize) };
        }
        OMQ_ARENA_THRESHOLD => {
            let Some(v) = read_i64(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            if v < -1 {
                return fail(libc::EINVAL);
            }
            lock_overlay!(sock_arc).arena_threshold = if v < 0 {
                None
            } else {
                let Ok(v) = usize::try_from(v) else {
                    return fail(libc::EINVAL);
                };
                Some(v)
            };
        }
        ZMQ_ROUTER_MANDATORY => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).router_mandatory = v != 0;
        }
        ZMQ_CONFLATE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).conflate = v != 0;
        }
        ZMQ_TCP_KEEPALIVE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).tcp_keepalive = v;
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).tcp_keepalive_cnt = if v <= 0 { None } else { Some(v as u32) };
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).tcp_keepalive_idle = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).tcp_keepalive_intvl = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_SNDBUF => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).sndbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_RCVBUF => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).rcvbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_XPUB_VERBOSE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).xpub_verbose = v != 0;
        }
        OMQ_ON_MUTE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let on_mute = match v {
                OMQ_ON_MUTE_BLOCK => OnMute::Block,
                OMQ_ON_MUTE_DROP_NEWEST => OnMute::DropNewest,
                OMQ_ON_MUTE_DROP_OLDEST => OnMute::DropOldest,
                _ => return fail(libc::EINVAL),
            };
            lock_overlay!(sock_arc).on_mute = on_mute;
        }
        OMQ_COMPRESSION_LEVEL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            if !(ZSTD_LEVEL_MIN..=ZSTD_LEVEL_MAX).contains(&v) {
                return fail(libc::EINVAL);
            }
            lock_overlay!(sock_arc).compression_level = Some(v);
        }
        OMQ_COMPRESSION_DICT => {
            let Some(v) = read_bytes(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            if v.len() > COMPRESSION_DICT_MAX {
                return fail(libc::EINVAL);
            }
            lock_overlay!(sock_arc).compression_dict = if v.is_empty() {
                None
            } else {
                Some(Bytes::from(v))
            };
        }
        OMQ_COMPRESSION_AUTO_TRAIN => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).compression_auto_train = v != 0;
        }
        ZMQ_SUBSCRIBE => {
            return do_subscribe(sock_arc, optval, optvallen, true);
        }
        ZMQ_UNSUBSCRIBE => {
            return do_subscribe(sock_arc, optval, optvallen, false);
        }
        ZMQ_PLAIN_SERVER => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            if v != 0 {
                ov.mechanism = MechanismOverlay::PlainServer;
            } else if matches!(ov.mechanism, MechanismOverlay::PlainServer) {
                ov.mechanism = MechanismOverlay::Null;
            }
        }
        ZMQ_PLAIN_USERNAME => {
            let Some(s) = read_string(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::PlainClient { username, .. } => *username = s,
                _ => {
                    ov.mechanism = MechanismOverlay::PlainClient {
                        username: s,
                        password: String::new(),
                    };
                }
            }
        }
        ZMQ_PLAIN_PASSWORD => {
            let Some(s) = read_string(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::PlainClient { password, .. } => *password = s,
                _ => {
                    ov.mechanism = MechanismOverlay::PlainClient {
                        username: String::new(),
                        password: s,
                    };
                }
            }
        }
        ZMQ_CURVE_SERVER => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            if v != 0 {
                if !matches!(ov.mechanism, MechanismOverlay::CurveServer { .. }) {
                    ov.mechanism = MechanismOverlay::CurveServer {
                        secret_key: [0; 32],
                    };
                }
            } else if matches!(ov.mechanism, MechanismOverlay::CurveServer { .. }) {
                ov.mechanism = MechanismOverlay::Null;
            }
        }
        ZMQ_CURVE_PUBLICKEY => {
            let Some(key) = read_key(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::CurveClient { public_key, .. } => *public_key = key,
                _ => {
                    ov.mechanism = MechanismOverlay::CurveClient {
                        public_key: key,
                        secret_key: [0; 32],
                        server_key: [0; 32],
                    };
                }
            }
        }
        ZMQ_CURVE_SECRETKEY => {
            let Some(key) = read_key(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::CurveServer { secret_key, .. }
                | MechanismOverlay::CurveClient { secret_key, .. } => *secret_key = key,
                _ => {
                    ov.mechanism = MechanismOverlay::CurveClient {
                        public_key: [0; 32],
                        secret_key: key,
                        server_key: [0; 32],
                    };
                }
            }
        }
        ZMQ_CURVE_SERVERKEY => {
            let Some(key) = read_key(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::CurveClient { server_key, .. } => *server_key = key,
                _ => {
                    ov.mechanism = MechanismOverlay::CurveClient {
                        public_key: [0; 32],
                        secret_key: [0; 32],
                        server_key: key,
                    };
                }
            }
        }
        ZMQ_IPV6 => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).ipv6 = v != 0;
        }
        ZMQ_IPV4ONLY => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).ipv6 = v == 0;
        }
        // Always-on in omq; accept silently.
        #[expect(clippy::match_same_arms)]
        ZMQ_ROUTER_HANDOVER => {}
        ZMQ_BACKLOG => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).backlog = v;
        }
        ZMQ_IMMEDIATE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).immediate = v != 0;
        }
        ZMQ_CONNECT_TIMEOUT => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).connect_timeout = v;
        }
        ZMQ_PROBE_ROUTER => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).probe_router = v != 0;
        }
        ZMQ_REQ_CORRELATE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).req_correlate = v != 0;
        }
        ZMQ_REQ_RELAXED => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).req_relaxed = v != 0;
        }
        ZMQ_XPUB_NODROP => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).xpub_nodrop = v != 0;
        }
        ZMQ_RECONNECT_STOP => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).reconnect_stop = v;
        }
        ZMQ_WSS_KEY_PEM => {
            let Some(v) = read_bytes(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).wss_key_pem = none_if_empty(v);
        }
        ZMQ_WSS_CERT_PEM => {
            let Some(v) = read_bytes(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).wss_cert_pem = none_if_empty(v);
        }
        ZMQ_WSS_TRUST_PEM => {
            let Some(v) = read_bytes(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).wss_trust_pem = none_if_empty(v);
        }
        ZMQ_WSS_HOSTNAME => {
            let Some(v) = read_string(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).wss_hostname = if v.is_empty() { None } else { Some(v) };
        }
        ZMQ_WSS_TRUST_SYSTEM => {
            let Some(v) = read_i32(optval, optvallen) else {
                return fail(libc::EINVAL);
            };
            lock_overlay!(sock_arc).wss_trust_system = v != 0;
        }
        #[expect(clippy::match_same_arms)]
        ZMQ_AFFINITY
        | ZMQ_RATE
        | ZMQ_RECOVERY_IVL
        | ZMQ_MULTICAST_HOPS
        | ZMQ_RCVMORE
        | ZMQ_FD
        | ZMQ_EVENTS
        | ZMQ_TYPE
        | ZMQ_LAST_ENDPOINT
        | ZMQ_TCP_ACCEPT_FILTER
        | ZMQ_ROUTER_RAW
        | ZMQ_MECHANISM
        | ZMQ_ZAP_DOMAIN
        | ZMQ_TOS
        | ZMQ_IPC_FILTER_PID
        | ZMQ_IPC_FILTER_UID
        | ZMQ_IPC_FILTER_GID
        | ZMQ_CONNECT_ROUTING_ID
        | ZMQ_GSSAPI_SERVER
        | ZMQ_GSSAPI_PRINCIPAL
        | ZMQ_GSSAPI_SERVICE_PRINCIPAL
        | ZMQ_GSSAPI_PLAINTEXT
        | ZMQ_SOCKS_PROXY
        | ZMQ_BLOCKY
        | ZMQ_XPUB_MANUAL
        | ZMQ_XPUB_WELCOME_MSG
        | ZMQ_STREAM_NOTIFY
        | ZMQ_INVERT_MATCHING
        | ZMQ_XPUB_VERBOSER
        | ZMQ_TCP_MAXRT
        | ZMQ_THREAD_SAFE
        | ZMQ_MULTICAST_MAXTPDU
        | ZMQ_VMCI_BUFFER_SIZE
        | ZMQ_VMCI_BUFFER_MIN_SIZE
        | ZMQ_VMCI_BUFFER_MAX_SIZE
        | ZMQ_VMCI_CONNECT_TIMEOUT
        | ZMQ_USE_FD
        | ZMQ_GSSAPI_PRINCIPAL_NAMETYPE
        | ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE
        | ZMQ_BINDTODEVICE
        | ZMQ_ZAP_ENFORCE_DOMAIN
        | ZMQ_LOOPBACK_FASTPATH
        | ZMQ_METADATA
        | ZMQ_MULTICAST_LOOP
        | ZMQ_ROUTER_NOTIFY
        | ZMQ_XPUB_MANUAL_LAST_VALUE
        | ZMQ_SOCKS_USERNAME
        | ZMQ_SOCKS_PASSWORD
        | ZMQ_IN_BATCH_SIZE
        | ZMQ_OUT_BATCH_SIZE
        | ZMQ_ONLY_FIRST_SUBSCRIBE
        | ZMQ_HELLO_MSG
        | ZMQ_DISCONNECT_MSG
        | ZMQ_PRIORITY
        | ZMQ_BUSY_POLL
        | ZMQ_HICCUP_MSG
        | ZMQ_XSUB_VERBOSE_UNSUBSCRIBE
        | ZMQ_TOPICS_COUNT
        | ZMQ_NORM_MODE
        | ZMQ_NORM_UNICAST_NACK
        | ZMQ_NORM_BUFFER_SIZE
        | ZMQ_NORM_SEGMENT_SIZE
        | ZMQ_NORM_BLOCK_SIZE
        | ZMQ_NORM_NUM_PARITY
        | ZMQ_NORM_NUM_AUTOPARITY
        | ZMQ_NORM_PUSH => {}
        _ => return crate::error::fail(libc::EINVAL),
    }
    0
}

fn do_subscribe(
    sock_arc: &std::sync::Arc<crate::socket::OmqSocket>,
    optval: *const libc::c_void,
    optvallen: usize,
    subscribe: bool,
) -> c_int {
    if optval.is_null() && optvallen > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    let prefix = if optval.is_null() {
        Bytes::new()
    } else {
        // SAFETY: optval is non-null (checked above) with optvallen readable bytes.
        Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(optval.cast::<u8>(), optvallen)
        })
    };
    crate::socket::ensure_materialized(sock_arc);
    let Some(inner) = sock_arc.inner.get() else {
        return crate::error::fail(crate::error::ETERM);
    };
    let result = if subscribe {
        crate::socket::with_socket(&sock_arc.ctx, inner, move |s| async move {
            s.subscribe(prefix).await
        })
    } else {
        crate::socket::with_socket(&sock_arc.ctx, inner, move |s| async move {
            s.unsubscribe(prefix).await
        })
    };
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => crate::error::fail(crate::error::map_omq_err(e)),
        Err(()) => crate::error::fail(crate::error::ETERM),
    }
}

#[expect(clippy::too_many_lines)]
#[unsafe(no_mangle)]
pub extern "C" fn zmq_getsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> c_int {
    if sock.is_null() {
        return crate::error::fail(crate::error::ENOTSOCK);
    }
    // SAFETY: caller guarantees sock is a valid socket pointer from zmq_socket.
    let sock_arc = unsafe { &*(sock.cast::<std::sync::Arc<crate::socket::OmqSocket>>()) };

    match option {
        ZMQ_SNDTIMEO => write_i32(
            optval,
            optvallen,
            sock_arc
                .sndtimeo_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_RCVTIMEO => write_i32(
            optval,
            optvallen,
            sock_arc
                .rcvtimeo_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_SNDHWM => {
            let v = lock_overlay!(sock_arc).send_hwm.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RCVHWM => {
            let v = lock_overlay!(sock_arc).recv_hwm.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_LINGER => {
            let v = lock_overlay!(sock_arc).linger_ms();
            write_i32(optval, optvallen, v)
        }
        ZMQ_IDENTITY => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(optval, optvallen, &ov.identity)
        }
        ZMQ_RCVMORE => {
            let more = sock_arc
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed);
            write_i32(optval, optvallen, i32::from(more))
        }
        ZMQ_TYPE => {
            use omq_tokio::SocketType;
            let v: i32 = match sock_arc.socket_type {
                SocketType::Pair => 0,
                SocketType::Pub => 1,
                SocketType::Sub => 2,
                SocketType::Req => 3,
                SocketType::Rep => 4,
                SocketType::Dealer => 5,
                SocketType::Router => 6,
                SocketType::Pull => 7,
                SocketType::Push => 8,
                SocketType::XPub => 9,
                SocketType::XSub => 10,
                SocketType::Server => 12,
                SocketType::Client => 13,
                SocketType::Radio => 14,
                SocketType::Dish => 15,
                SocketType::Gather => 16,
                SocketType::Scatter => 17,
                SocketType::Peer => 19,
                SocketType::Channel => 20,
                SocketType::Stream => 11,
                _ => return crate::error::fail(libc::EINVAL),
            };
            write_i32(optval, optvallen, v)
        }
        ZMQ_FD => {
            #[cfg(windows)]
            return crate::error::fail(libc::ENOPROTOOPT);
            #[cfg(unix)]
            {
                let fd = sock_arc.notify.recv_fd();
                write_i32(optval, optvallen, fd)
            }
        }
        ZMQ_EVENTS => {
            let mut events = ZMQ_POLLOUT; // optimistic: always writable
            crate::socket::adopt_pending_bypass_recv(sock_arc);
            let drain_nonempty = sock_arc
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed);
            // SAFETY: libzmq sockets are accessed by at most one application thread.
            let recv_cons_has_data = unsafe { sock_arc.recv_cons.get() }
                .as_ref()
                .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty());
            // SAFETY: same socket-thread invariant as above.
            let bypass_recv_has_data = unsafe { sock_arc.bypass_recv.get() }
                .as_ref()
                .is_some_and(|br| !br.is_empty());
            let has_data = drain_nonempty || recv_cons_has_data || bypass_recv_has_data;
            if has_data {
                events |= ZMQ_POLLIN;
            }
            write_i32(optval, optvallen, events)
        }
        ZMQ_RECONNECT_IVL => {
            let v = lock_overlay!(sock_arc)
                .reconnect_ivl
                .map_or(-1, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let v = lock_overlay!(sock_arc)
                .reconnect_ivl_max
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_IVL => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_ivl
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_TTL => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_ttl
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_timeout
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HANDSHAKE_IVL => {
            let v = lock_overlay!(sock_arc)
                .handshake_ivl
                .map_or(DEFAULT_HANDSHAKE_IVL_MS, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_MAXMSGSIZE => {
            let v = lock_overlay!(sock_arc)
                .max_message_size
                .map_or(-1i64, |n| n as i64);
            write_i64(optval, optvallen, v)
        }
        OMQ_ARENA_THRESHOLD => {
            let v = lock_overlay!(sock_arc)
                .arena_threshold
                .map_or(omq_tokio::options::DEFAULT_ARENA_THRESHOLD as i64, |n| {
                    n as i64
                });
            write_i64(optval, optvallen, v)
        }
        ZMQ_ROUTER_MANDATORY => {
            let v = lock_overlay!(sock_arc).router_mandatory;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_CONFLATE => {
            let v = lock_overlay!(sock_arc).conflate;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_TCP_KEEPALIVE => {
            let v = lock_overlay!(sock_arc).tcp_keepalive;
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_cnt
                .map_or(-1, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_idle
                .map_or(-1, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_intvl
                .map_or(-1, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_SNDBUF => {
            let v = lock_overlay!(sock_arc).sndbuf.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RCVBUF => {
            let v = lock_overlay!(sock_arc).rcvbuf.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_XPUB_VERBOSE => {
            let v = lock_overlay!(sock_arc).xpub_verbose;
            write_i32(optval, optvallen, i32::from(v))
        }
        #[expect(clippy::match_same_arms)]
        OMQ_ON_MUTE => {
            let v = match lock_overlay!(sock_arc).on_mute {
                OnMute::Block => OMQ_ON_MUTE_BLOCK,
                OnMute::DropNewest => OMQ_ON_MUTE_DROP_NEWEST,
                OnMute::DropOldest => OMQ_ON_MUTE_DROP_OLDEST,
                _ => OMQ_ON_MUTE_BLOCK,
            };
            write_i32(optval, optvallen, v)
        }
        OMQ_COMPRESSION_LEVEL => {
            let v = lock_overlay!(sock_arc).compression_level.unwrap_or(0);
            write_i32(optval, optvallen, v)
        }
        OMQ_COMPRESSION_DICT => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(
                optval,
                optvallen,
                ov.compression_dict.as_deref().unwrap_or(b""),
            )
        }
        OMQ_COMPRESSION_AUTO_TRAIN => {
            let v = lock_overlay!(sock_arc).compression_auto_train;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_LAST_ENDPOINT => {
            let Ok(ep) = sock_arc.last_endpoint.lock() else {
                return crate::error::fail(crate::error::ETERM);
            };
            let s = ep.as_deref().unwrap_or("");
            write_string(optval, optvallen, s.as_bytes())
        }
        ZMQ_MECHANISM => {
            let v = match lock_overlay!(sock_arc).mechanism {
                MechanismOverlay::Null => ZMQ_NULL,
                MechanismOverlay::PlainServer | MechanismOverlay::PlainClient { .. } => ZMQ_PLAIN,
                MechanismOverlay::CurveServer { .. } | MechanismOverlay::CurveClient { .. } => {
                    ZMQ_CURVE
                }
            };
            write_i32(optval, optvallen, v)
        }
        ZMQ_PLAIN_SERVER => {
            let v = matches!(
                lock_overlay!(sock_arc).mechanism,
                MechanismOverlay::PlainServer
            );
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_PLAIN_USERNAME => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::PlainClient { ref username, .. } = ov.mechanism {
                write_string(optval, optvallen, username.as_bytes())
            } else {
                write_string(optval, optvallen, b"")
            }
        }
        ZMQ_PLAIN_PASSWORD => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::PlainClient { ref password, .. } = ov.mechanism {
                write_string(optval, optvallen, password.as_bytes())
            } else {
                write_string(optval, optvallen, b"")
            }
        }
        ZMQ_CURVE_SERVER => {
            let v = matches!(
                lock_overlay!(sock_arc).mechanism,
                MechanismOverlay::CurveServer { .. }
            );
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_CURVE_PUBLICKEY => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::CurveClient { ref public_key, .. } = ov.mechanism {
                write_key(optval, optvallen, public_key)
            } else {
                write_key(optval, optvallen, &[0; 32])
            }
        }
        ZMQ_CURVE_SECRETKEY => {
            let ov = lock_overlay!(sock_arc);
            let key = match ov.mechanism {
                MechanismOverlay::CurveServer { ref secret_key, .. }
                | MechanismOverlay::CurveClient { ref secret_key, .. } => secret_key,
                _ => &[0; 32],
            };
            write_key(optval, optvallen, key)
        }
        ZMQ_CURVE_SERVERKEY => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::CurveClient { ref server_key, .. } = ov.mechanism {
                write_key(optval, optvallen, server_key)
            } else {
                write_key(optval, optvallen, &[0; 32])
            }
        }
        ZMQ_IPV6 => {
            let v = lock_overlay!(sock_arc).ipv6;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_ROUTER_HANDOVER | ZMQ_BLOCKY | ZMQ_STREAM_NOTIFY => write_i32(optval, optvallen, 1),
        ZMQ_BACKLOG => write_i32(optval, optvallen, lock_overlay!(sock_arc).backlog),
        ZMQ_IMMEDIATE => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).immediate),
        ),
        ZMQ_CONNECT_TIMEOUT => {
            write_i32(optval, optvallen, lock_overlay!(sock_arc).connect_timeout)
        }
        ZMQ_PROBE_ROUTER => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).probe_router),
        ),
        ZMQ_REQ_CORRELATE => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).req_correlate),
        ),
        ZMQ_REQ_RELAXED => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).req_relaxed),
        ),
        ZMQ_XPUB_NODROP => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).xpub_nodrop),
        ),
        ZMQ_RECONNECT_STOP => write_i32(optval, optvallen, lock_overlay!(sock_arc).reconnect_stop),
        ZMQ_WSS_KEY_PEM => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(optval, optvallen, ov.wss_key_pem.as_deref().unwrap_or(b""))
        }
        ZMQ_WSS_CERT_PEM => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(optval, optvallen, ov.wss_cert_pem.as_deref().unwrap_or(b""))
        }
        ZMQ_WSS_TRUST_PEM => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(
                optval,
                optvallen,
                ov.wss_trust_pem.as_deref().unwrap_or(b""),
            )
        }
        ZMQ_WSS_HOSTNAME => {
            let ov = lock_overlay!(sock_arc);
            write_string(
                optval,
                optvallen,
                ov.wss_hostname.as_deref().unwrap_or("").as_bytes(),
            )
        }
        ZMQ_WSS_TRUST_SYSTEM => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).wss_trust_system),
        ),
        ZMQ_IPV4ONLY => write_i32(optval, optvallen, i32::from(!lock_overlay!(sock_arc).ipv6)),
        ZMQ_MULTICAST_MAXTPDU => write_i32(optval, optvallen, 1500),
        ZMQ_USE_FD => write_i32(optval, optvallen, -1),
        ZMQ_AFFINITY
        | ZMQ_VMCI_BUFFER_SIZE
        | ZMQ_VMCI_BUFFER_MIN_SIZE
        | ZMQ_VMCI_BUFFER_MAX_SIZE => write_i64(optval, optvallen, 0),
        ZMQ_RATE
        | ZMQ_RECOVERY_IVL
        | ZMQ_MULTICAST_HOPS
        | ZMQ_TOS
        | ZMQ_IPC_FILTER_PID
        | ZMQ_IPC_FILTER_UID
        | ZMQ_IPC_FILTER_GID
        | ZMQ_ROUTER_RAW
        | ZMQ_GSSAPI_SERVER
        | ZMQ_GSSAPI_PLAINTEXT
        | ZMQ_XPUB_MANUAL
        | ZMQ_INVERT_MATCHING
        | ZMQ_XPUB_VERBOSER
        | ZMQ_TCP_MAXRT
        | ZMQ_THREAD_SAFE
        | ZMQ_VMCI_CONNECT_TIMEOUT
        | ZMQ_GSSAPI_PRINCIPAL_NAMETYPE
        | ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE
        | ZMQ_ZAP_ENFORCE_DOMAIN
        | ZMQ_LOOPBACK_FASTPATH
        | ZMQ_MULTICAST_LOOP
        | ZMQ_ROUTER_NOTIFY
        | ZMQ_XPUB_MANUAL_LAST_VALUE
        | ZMQ_IN_BATCH_SIZE
        | ZMQ_OUT_BATCH_SIZE
        | ZMQ_ONLY_FIRST_SUBSCRIBE
        | ZMQ_PRIORITY
        | ZMQ_BUSY_POLL
        | ZMQ_XSUB_VERBOSE_UNSUBSCRIBE
        | ZMQ_TOPICS_COUNT
        | ZMQ_NORM_MODE
        | ZMQ_NORM_UNICAST_NACK
        | ZMQ_NORM_BUFFER_SIZE
        | ZMQ_NORM_SEGMENT_SIZE
        | ZMQ_NORM_BLOCK_SIZE
        | ZMQ_NORM_NUM_PARITY
        | ZMQ_NORM_NUM_AUTOPARITY
        | ZMQ_NORM_PUSH => write_i32(optval, optvallen, 0),
        ZMQ_TCP_ACCEPT_FILTER
        | ZMQ_ZAP_DOMAIN
        | ZMQ_SOCKS_PROXY
        | ZMQ_CONNECT_ROUTING_ID
        | ZMQ_GSSAPI_PRINCIPAL
        | ZMQ_GSSAPI_SERVICE_PRINCIPAL
        | ZMQ_BINDTODEVICE
        | ZMQ_METADATA
        | ZMQ_SOCKS_USERNAME
        | ZMQ_SOCKS_PASSWORD => write_string(optval, optvallen, b""),
        ZMQ_XPUB_WELCOME_MSG | ZMQ_HELLO_MSG | ZMQ_DISCONNECT_MSG | ZMQ_HICCUP_MSG => {
            write_bytes(optval, optvallen, b"")
        }
        _ => crate::error::fail(libc::EINVAL),
    }
}

fn read_i32(optval: *const libc::c_void, optvallen: usize) -> Option<i32> {
    if optval.is_null() || optvallen < 4 {
        return None;
    }
    // SAFETY: optval is non-null (checked above) and points to at least 4 readable bytes.
    Some(unsafe { std::ptr::read_unaligned(optval.cast::<i32>()) })
}

fn read_i64(optval: *const libc::c_void, optvallen: usize) -> Option<i64> {
    if optval.is_null() || optvallen < 8 {
        return None;
    }
    // SAFETY: optval is non-null (checked above) and points to at least 8 readable bytes.
    Some(unsafe { std::ptr::read_unaligned(optval.cast::<i64>()) })
}

fn read_string(optval: *const libc::c_void, optvallen: usize) -> Option<String> {
    if optval.is_null() {
        return None;
    }
    if optvallen == 0 {
        return Some(String::new());
    }
    // SAFETY: optval is non-null with optvallen > 0 (checked above).
    let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
    Some(String::from_utf8_lossy(slice).into_owned())
}

fn read_bytes(optval: *const libc::c_void, optvallen: usize) -> Option<Vec<u8>> {
    if optvallen == 0 {
        return Some(Vec::new());
    }
    if optval.is_null() {
        return None;
    }
    // SAFETY: optval is non-null with optvallen > 0 (checked above).
    Some(unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) }.to_vec())
}

fn none_if_empty(bytes: Vec<u8>) -> Option<Vec<u8>> {
    if bytes.is_empty() { None } else { Some(bytes) }
}

fn read_key(optval: *const libc::c_void, optvallen: usize) -> Option<[u8; 32]> {
    if optval.is_null() {
        return None;
    }
    let mut key = [0u8; 32];
    if optvallen == 32 {
        // SAFETY: optval is non-null (checked above) and optvallen == 32.
        let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), 32) };
        key.copy_from_slice(slice);
        return Some(key);
    }
    if optvallen >= 40 {
        // SAFETY: optval is non-null (checked above) and optvallen >= 40.
        let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), 40) };
        let Ok(s) = std::str::from_utf8(slice) else {
            return None;
        };
        if let Ok(decoded) = omq_tokio::proto::z85::decode(s)
            && decoded.len() == 32
        {
            key.copy_from_slice(&decoded);
            return Some(key);
        }
    }
    None
}

fn write_i32(optval: *mut libc::c_void, optvallen: *mut usize, val: i32) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above); reading the available size.
    let avail = unsafe { *optvallen };
    if avail < 4 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least 4 bytes available (checked above).
    unsafe {
        std::ptr::write_bytes(optval.cast::<u8>(), 0, avail);
        std::ptr::write_unaligned(optval.cast::<i32>(), val);
        *optvallen = 4;
    }
    0
}

fn write_i64(optval: *mut libc::c_void, optvallen: *mut usize, val: i64) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail < 8 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least 8 bytes available (checked above).
    unsafe {
        std::ptr::write_bytes(optval.cast::<u8>(), 0, avail);
        std::ptr::write_unaligned(optval.cast::<i64>(), val);
        *optvallen = 8;
    }
    0
}

fn write_bytes(optval: *mut libc::c_void, optvallen: *mut usize, data: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail < data.len() {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least data.len() bytes available.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), optval.cast::<u8>(), data.len());
        *optvallen = data.len();
    }
    0
}

fn write_string(optval: *mut libc::c_void, optvallen: *mut usize, data: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    let needed = data.len() + 1;
    if avail < needed {
        return fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least `needed` bytes available.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), optval.cast::<u8>(), data.len());
        *optval.cast::<u8>().add(data.len()) = 0;
        *optvallen = needed;
    }
    0
}

fn write_key(optval: *mut libc::c_void, optvallen: *mut usize, key: &[u8; 32]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail >= 41
        && let Ok(z85) = omq_tokio::proto::z85::encode(key)
    {
        // SAFETY: optval has at least 41 bytes available (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(z85.as_ptr(), optval.cast::<u8>(), 40);
            *(optval.cast::<u8>()).add(40) = 0;
            *optvallen = 41;
        }
        return 0;
    }
    if avail >= 32 {
        // SAFETY: optval has at least 32 bytes available (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), optval.cast::<u8>(), 32);
            *optvallen = 32;
        }
        return 0;
    }
    crate::error::fail(libc::EINVAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_ivl_is_milliseconds_in_options() {
        let overlay = SocketOverlay {
            handshake_ivl: Some(Duration::from_millis(10)),
            ..Default::default()
        };

        assert_eq!(
            overlay.to_options().handshake_timeout,
            Some(Duration::from_millis(10))
        );
    }

    #[test]
    fn default_handshake_ivl_is_materialized() {
        let overlay = SocketOverlay::default();

        assert_eq!(
            overlay.to_options().handshake_timeout,
            Some(Duration::from_millis(DEFAULT_HANDSHAKE_IVL_MS as u64))
        );
    }

    #[test]
    fn default_linger_matches_libzmq_forever() {
        let overlay = SocketOverlay::default();

        assert_eq!(overlay.linger_ms(), -1);
        assert_eq!(overlay.effective_linger(), None);
        assert_eq!(overlay.to_options().linger, None);
    }

    #[test]
    fn explicit_linger_zero_maps_to_native_zero() {
        let overlay = SocketOverlay {
            linger: LingerSetting::Finite(Duration::ZERO),
            ..Default::default()
        };

        assert_eq!(overlay.linger_ms(), 0);
        assert_eq!(overlay.effective_linger(), Some(Duration::ZERO));
        assert_eq!(overlay.to_options().linger, Some(Duration::ZERO));
    }

    #[test]
    fn arena_threshold_maps_to_backend_options() {
        let default_overlay = SocketOverlay::default();
        assert_eq!(default_overlay.to_options().arena_threshold, None);

        let overlay = SocketOverlay {
            arena_threshold: Some(2048),
            ..Default::default()
        };
        assert_eq!(overlay.to_options().arena_threshold, Some(2048));
    }

    #[test]
    fn compression_fields_map_to_backend_options() {
        let overlay = SocketOverlay {
            compression_level: Some(1),
            compression_dict: Some(Bytes::from_static(b"dict")),
            compression_auto_train: true,
            ..Default::default()
        };
        let opts = overlay.to_options();

        assert_eq!(opts.compression_level, Some(1));
        assert_eq!(opts.compression_dict.as_deref(), Some(&b"dict"[..]));
        assert!(opts.compression_auto_train);
    }

    #[test]
    fn on_mute_maps_to_backend_options() {
        let overlay = SocketOverlay {
            on_mute: OnMute::DropOldest,
            ..Default::default()
        };

        assert_eq!(overlay.to_options().on_mute, OnMute::DropOldest);
    }

    #[test]
    fn connect_timeout_caps_handshake_ivl_in_milliseconds() {
        let overlay = SocketOverlay {
            handshake_ivl: Some(Duration::from_secs(30)),
            connect_timeout: 10,
            ..Default::default()
        };

        assert_eq!(
            overlay.to_options().handshake_timeout,
            Some(Duration::from_millis(10))
        );
    }
}
