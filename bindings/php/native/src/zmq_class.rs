//! The `ZMQ` class: constants and static helpers.

use std::ffi::{CStr, c_char};
use std::sync::OnceLock;
use std::time::Instant;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, ZendStr, Zval};

use crate::errors::socket_error;
use crate::util::with_bytes;

/// Holder of ext-zmq's constants; not instantiable.
#[php_class]
#[php(name = "ZMQ")]
#[derive(Debug)]
pub struct Zmq;

#[php_impl]
impl Zmq {
    const SOCKET_PAIR: i64 = 0;
    const SOCKET_PUB: i64 = 1;
    const SOCKET_SUB: i64 = 2;
    const SOCKET_REQ: i64 = 3;
    const SOCKET_REP: i64 = 4;
    const SOCKET_XREQ: i64 = 5;
    const SOCKET_XREP: i64 = 6;
    const SOCKET_PUSH: i64 = 8;
    const SOCKET_PULL: i64 = 7;
    const SOCKET_DEALER: i64 = 5;
    const SOCKET_ROUTER: i64 = 6;
    const SOCKET_XSUB: i64 = 10;
    const SOCKET_XPUB: i64 = 9;
    const SOCKET_STREAM: i64 = 11;
    const SOCKET_UPSTREAM: i64 = 7;
    const SOCKET_DOWNSTREAM: i64 = 8;
    const POLL_IN: i64 = 1;
    const POLL_OUT: i64 = 2;
    const MODE_SNDMORE: i64 = 2;
    const MODE_NOBLOCK: i64 = 1;
    const MODE_DONTWAIT: i64 = 1;
    const ERR_INTERNAL: i64 = -99;
    const ERR_EAGAIN: i64 = 11;
    const ERR_ENOTSUP: i64 = 95;
    const ERR_EFSM: i64 = 156_384_763;
    const ERR_ETERM: i64 = 156_384_765;
    const SOCKOPT_GSSAPI_PRINCIPAL_NAMETYPE: i64 = 90;
    const SOCKOPT_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE: i64 = 91;
    const SOCKOPT_BINDTODEVICE: i64 = 92;
    const SOCKOPT_HEARTBEAT_IVL: i64 = 75;
    const SOCKOPT_HEARTBEAT_TTL: i64 = 76;
    const SOCKOPT_HEARTBEAT_TIMEOUT: i64 = 77;
    const SOCKOPT_USE_FD: i64 = 89;
    const SOCKOPT_XPUB_MANUAL: i64 = 71;
    const SOCKOPT_XPUB_WELCOME_MSG: i64 = 72;
    const SOCKOPT_STREAM_NOTIFY: i64 = 73;
    const SOCKOPT_INVERT_MATCHING: i64 = 74;
    const SOCKOPT_XPUB_VERBOSER: i64 = 78;
    const SOCKOPT_CONNECT_TIMEOUT: i64 = 79;
    const SOCKOPT_TCP_MAXRT: i64 = 80;
    const SOCKOPT_THREAD_SAFE: i64 = 81;
    const SOCKOPT_MULTICAST_MAXTPDU: i64 = 84;
    const SOCKOPT_VMCI_BUFFER_SIZE: i64 = 85;
    const SOCKOPT_VMCI_BUFFER_MIN_SIZE: i64 = 86;
    const SOCKOPT_VMCI_BUFFER_MAX_SIZE: i64 = 87;
    const SOCKOPT_VMCI_CONNECT_TIMEOUT: i64 = 88;
    const SOCKOPT_TOS: i64 = 57;
    const SOCKOPT_ROUTER_HANDOVER: i64 = 56;
    const SOCKOPT_CONNECT_RID: i64 = 61;
    const SOCKOPT_HANDSHAKE_IVL: i64 = 66;
    const SOCKOPT_SOCKS_PROXY: i64 = 68;
    const SOCKOPT_XPUB_NODROP: i64 = 69;
    const SOCKOPT_ROUTER_MANDATORY: i64 = 33;
    const SOCKOPT_PROBE_ROUTER: i64 = 51;
    const SOCKOPT_REQ_RELAXED: i64 = 53;
    const SOCKOPT_REQ_CORRELATE: i64 = 52;
    const SOCKOPT_CONFLATE: i64 = 54;
    const SOCKOPT_ZAP_DOMAIN: i64 = 55;
    const SOCKOPT_MECHANISM: i64 = 43;
    const SOCKOPT_PLAIN_SERVER: i64 = 44;
    const SOCKOPT_PLAIN_USERNAME: i64 = 45;
    const SOCKOPT_PLAIN_PASSWORD: i64 = 46;
    const SOCKOPT_CURVE_SERVER: i64 = 47;
    const SOCKOPT_CURVE_PUBLICKEY: i64 = 48;
    const SOCKOPT_CURVE_SECRETKEY: i64 = 49;
    const SOCKOPT_CURVE_SERVERKEY: i64 = 50;
    const SOCKOPT_GSSAPI_SERVER: i64 = 62;
    const SOCKOPT_GSSAPI_PLAINTEXT: i64 = 65;
    const SOCKOPT_GSSAPI_PRINCIPAL: i64 = 63;
    const SOCKOPT_GSSAPI_SERVICE_PRINCIPAL: i64 = 64;
    const SOCKOPT_IPV6: i64 = 42;
    const SOCKOPT_IMMEDIATE: i64 = 39;
    const SOCKOPT_SNDHWM: i64 = 23;
    const SOCKOPT_RCVHWM: i64 = 24;
    const SOCKOPT_MAXMSGSIZE: i64 = 22;
    const SOCKOPT_MULTICAST_HOPS: i64 = 25;
    const SOCKOPT_XPUB_VERBOSE: i64 = 40;
    const SOCKOPT_TCP_KEEPALIVE: i64 = 34;
    const SOCKOPT_TCP_KEEPALIVE_IDLE: i64 = 36;
    const SOCKOPT_TCP_KEEPALIVE_CNT: i64 = 35;
    const SOCKOPT_TCP_KEEPALIVE_INTVL: i64 = 37;
    const SOCKOPT_TCP_ACCEPT_FILTER: i64 = 38;
    const SOCKOPT_LAST_ENDPOINT: i64 = 32;
    const SOCKOPT_ROUTER_RAW: i64 = 41;
    const SOCKOPT_IPV4ONLY: i64 = 31;
    const SOCKOPT_DELAY_ATTACH_ON_CONNECT: i64 = 39;
    const SOCKOPT_HWM: i64 = 2001;
    const SOCKOPT_AFFINITY: i64 = 4;
    const SOCKOPT_IDENTITY: i64 = 5;
    const SOCKOPT_RATE: i64 = 8;
    const SOCKOPT_RECOVERY_IVL: i64 = 9;
    const SOCKOPT_RCVTIMEO: i64 = 27;
    const SOCKOPT_SNDTIMEO: i64 = 28;
    const SOCKOPT_SNDBUF: i64 = 11;
    const SOCKOPT_RCVBUF: i64 = 12;
    const SOCKOPT_LINGER: i64 = 17;
    const SOCKOPT_RECONNECT_IVL: i64 = 18;
    const SOCKOPT_RECONNECT_IVL_MAX: i64 = 21;
    const SOCKOPT_BACKLOG: i64 = 19;
    const SOCKOPT_SUBSCRIBE: i64 = 6;
    const SOCKOPT_UNSUBSCRIBE: i64 = 7;
    const SOCKOPT_TYPE: i64 = 16;
    const SOCKOPT_RCVMORE: i64 = 13;
    const SOCKOPT_FD: i64 = 14;
    const SOCKOPT_EVENTS: i64 = 15;
    const CTXOPT_MAX_SOCKETS: i64 = 2;
    const CTXOPT_MAX_SOCKETS_DEFAULT: i64 = 1023;
    const EVENT_CONNECTED: i64 = 1;
    const EVENT_CONNECT_DELAYED: i64 = 2;
    const EVENT_CONNECT_RETRIED: i64 = 4;
    const EVENT_LISTENING: i64 = 8;
    const EVENT_BIND_FAILED: i64 = 16;
    const EVENT_ACCEPTED: i64 = 32;
    const EVENT_ACCEPT_FAILED: i64 = 64;
    const EVENT_CLOSED: i64 = 128;
    const EVENT_CLOSE_FAILED: i64 = 256;
    const EVENT_DISCONNECTED: i64 = 512;
    const EVENT_MONITOR_STOPPED: i64 = 1024;
    const EVENT_ALL: i64 = 65535;
    const LIBZMQ_VER: &'static str = "4.3.6";
    const LIBZMQ_VERSION: &'static str = "4.3.6";
    const LIBZMQ_VERSION_ID: i64 = 40_306;
    const LIBZMQ_VERSION_MAJOR: i64 = 4;
    const LIBZMQ_VERSION_MINOR: i64 = 3;
    const LIBZMQ_VERSION_PATCH: i64 = 6;

    #[php(vis = "private")]
    pub fn __construct() -> Self {
        Self
    }

    /// Monotonic milliseconds since the extension first asked.
    pub fn clock() -> i64 {
        static START: OnceLock<Instant> = OnceLock::new();
        let elapsed = START.get_or_init(Instant::now).elapsed().as_millis();
        i64::try_from(elapsed).unwrap_or(i64::MAX)
    }

    /// Z85-encode `data`; its length must be a multiple of 4.
    #[php(name = "z85encode")]
    pub fn z85_encode(data: &Zval) -> PhpResult<Zval> {
        with_bytes(data, |bytes| {
            let mut out = Zval::new();
            if bytes.is_empty() || bytes.len() % 4 != 0 {
                out.set_null();
                return out;
            }

            let mut encoded = vec![0_u8; bytes.len() / 4 * 5 + 1];
            let rc =
                omq_zmq::zmq_z85_encode(encoded.as_mut_ptr().cast(), bytes.as_ptr(), bytes.len());
            if rc.is_null() {
                out.set_null();
            } else {
                encoded.pop();
                out.set_zend_string(ZendStr::new(&encoded, false));
            }

            out
        })
    }

    /// Decode a Z85 string; its length must be a multiple of 5.
    #[php(name = "z85decode")]
    pub fn z85_decode(data: &Zval) -> PhpResult<Zval> {
        with_bytes(data, |text| {
            let mut out = Zval::new();
            if text.is_empty() || text.len() % 5 != 0 {
                out.set_null();
                return out;
            }

            let mut source = text.to_vec();
            source.push(0);
            let mut decoded = vec![0_u8; text.len() / 5 * 4];
            let rc =
                omq_zmq::zmq_z85_decode(decoded.as_mut_ptr(), source.as_ptr().cast::<c_char>());
            if rc.is_null() {
                out.set_null();
            } else {
                out.set_zend_string(ZendStr::new(&decoded, false));
            }

            out
        })
    }

    /// A new CURVE key pair as Z85 strings: `public_key`, `secret_key`.
    #[php(name = "curveKeyPair")]
    pub fn curve_key_pair() -> PhpResult<ZBox<ZendHashTable>> {
        let mut public = [0 as c_char; 41];
        let mut secret = [0 as c_char; 41];
        if omq_zmq::zmq_curve_keypair(public.as_mut_ptr(), secret.as_mut_ptr()) != 0 {
            return Err(socket_error("Failed to generate key pair"));
        }

        let mut pair = ZendHashTable::new();
        for (name, key) in [("public_key", &public), ("secret_key", &secret)] {
            // SAFETY: zmq_curve_keypair writes NUL-terminated Z85 keys.
            let key = unsafe { CStr::from_ptr(key.as_ptr()) };
            let mut value = Zval::new();
            value.set_zend_string(ZendStr::new(key.to_bytes(), false));
            pair.insert(name, value)?;
        }

        Ok(pair)
    }
}
