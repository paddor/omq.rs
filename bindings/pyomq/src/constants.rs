//! libzmq integer constants. Values match libzmq exactly so existing
//! pyzmq code that uses literal numbers (or imports `zmq.PUSH` etc.)
//! works untouched.

use pyo3::prelude::*;

// Socket types (libzmq `zmq.h` + `zmq_draft.h`):
pub const PAIR: i32 = 0;
pub const PUB: i32 = 1;
pub const SUB: i32 = 2;
pub const REQ: i32 = 3;
pub const REP: i32 = 4;
pub const DEALER: i32 = 5;
pub const ROUTER: i32 = 6;
pub const PULL: i32 = 7;
pub const PUSH: i32 = 8;
pub const XPUB: i32 = 9;
pub const XSUB: i32 = 10;
// Draft socket types (libzmq `zmq_draft.h`):
pub const SERVER: i32 = 12;
pub const CLIENT: i32 = 13;
pub const RADIO: i32 = 14;
pub const DISH: i32 = 15;
pub const GATHER: i32 = 16;
pub const SCATTER: i32 = 17;
pub const PEER: i32 = 19;
pub const CHANNEL: i32 = 20;

// Socket options:
pub const AFFINITY: i32 = 4;
pub const IDENTITY: i32 = 5;
pub const SUBSCRIBE: i32 = 6;
pub const UNSUBSCRIBE: i32 = 7;
pub const RCVMORE: i32 = 13;
pub const TYPE: i32 = 16;
pub const LINGER: i32 = 17;
pub const RECONNECT_IVL: i32 = 18;
pub const BACKLOG: i32 = 19;
pub const RECONNECT_IVL_MAX: i32 = 21;
pub const MAXMSGSIZE: i32 = 22;
pub const SNDHWM: i32 = 23;
pub const RCVHWM: i32 = 24;
pub const RCVTIMEO: i32 = 27;
pub const SNDTIMEO: i32 = 28;
pub const ROUTER_MANDATORY: i32 = 33;
pub const TCP_KEEPALIVE: i32 = 34;
pub const TCP_KEEPALIVE_CNT: i32 = 35;
pub const TCP_KEEPALIVE_IDLE: i32 = 36;
pub const TCP_KEEPALIVE_INTVL: i32 = 37;
pub const IMMEDIATE: i32 = 39;
pub const IPV6: i32 = 42;
pub const HEARTBEAT_IVL: i32 = 75;
pub const HEARTBEAT_TTL: i32 = 76;
pub const HEARTBEAT_TIMEOUT: i32 = 77;
pub const HANDSHAKE_IVL: i32 = 66;
pub const CONFLATE: i32 = 54;

// CURVE options:
pub const CURVE_SERVER: i32 = 47;
pub const CURVE_PUBLICKEY: i32 = 48;
pub const CURVE_SECRETKEY: i32 = 49;
pub const CURVE_SERVERKEY: i32 = 50;

// send / recv flags:
pub const SNDMORE: i32 = 2;
pub const NOBLOCK: i32 = 1;
pub const DONTWAIT: i32 = NOBLOCK;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    macro_rules! cs {
        ($($name:ident),* $(,)?) => {
            $( m.add(stringify!($name), $name)?; )*
        };
    }
    cs!(
        PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB,
        SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, PEER, CHANNEL,
        AFFINITY, IDENTITY, SUBSCRIBE, UNSUBSCRIBE, RCVMORE, TYPE, LINGER,
        RECONNECT_IVL, BACKLOG, RECONNECT_IVL_MAX, MAXMSGSIZE, SNDHWM, RCVHWM,
        RCVTIMEO, SNDTIMEO, ROUTER_MANDATORY,
        TCP_KEEPALIVE, TCP_KEEPALIVE_CNT, TCP_KEEPALIVE_IDLE, TCP_KEEPALIVE_INTVL,
        IMMEDIATE, IPV6, HEARTBEAT_IVL, HEARTBEAT_TTL, HEARTBEAT_TIMEOUT,
        HANDSHAKE_IVL, CONFLATE,
        CURVE_SERVER, CURVE_PUBLICKEY, CURVE_SECRETKEY, CURVE_SERVERKEY,
        SNDMORE, NOBLOCK, DONTWAIT
    );
    Ok(())
}
