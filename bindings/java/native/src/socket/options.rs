//! Socket option entry points.

#[cfg(feature = "quic")]
use crate::convert::java_string;
use crate::convert::{
    byte_array, duration_from_millis, optional_duration_from_millis, optional_usize_from_long,
};
use crate::error::{guard, throw_java, throw_omq};
use crate::socket::socket_from_handle;
use bytes::Bytes;
use jni::JNIEnv;
#[cfg(feature = "quic")]
use jni::objects::JString;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jint, jlong};
use omq_tokio::Error;
use omq_tokio::options::{KeepAlive, OnMute, ReconnectPolicy, WorkloadProfile};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetLinger(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let linger = optional_duration_from_millis(millis)?;
            socket.set_option(|options| options.linger = linger)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetIdentity(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    identity: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let identity = Bytes::from(byte_array(env, identity)?);
            socket.set_option(|options| options.identity = identity)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetSendHighWaterMark(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    hwm: jint,
) {
    guard(&mut env, (), |env| {
        if hwm < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "HWM must be non-negative",
            );
            return;
        }
        if let Err(error) = socket_from_handle(handle)
            .and_then(|socket| socket.set_option(|options| options.send_hwm = hwm as u32))
        {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReceiveHighWaterMark(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    hwm: jint,
) {
    guard(&mut env, (), |env| {
        if hwm < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "HWM must be non-negative",
            );
            return;
        }
        if let Err(error) = socket_from_handle(handle)
            .and_then(|socket| socket.set_option(|options| options.recv_hwm = hwm as u32))
        {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHeartbeatInterval(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let interval = optional_duration_from_millis(millis)?;
            socket.set_option(|options| options.heartbeat_interval = interval)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHandshakeTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(millis)?;
            socket.set_option(|options| options.handshake_timeout = timeout)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxMessageSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    size: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let max = if size < 0 { None } else { Some(size as usize) };
            socket.set_option(|options| options.max_message_size = max)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxMessageParts(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = optional_usize_from_long("max message parts", parts)?;
            socket.set_option(move |options| options.max_message_parts = parts)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxPeerSubscriptions(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    max: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let max = optional_usize_from_long("max peer subscriptions", max)?;
            socket.set_option(move |options| options.max_peer_subscriptions = max)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionAutoTrain(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let enabled = enabled != 0;
        if let Err(error) = socket_from_handle(handle).and_then(|socket| {
            socket.set_option(|options| options.compression_auto_train = enabled)
        }) {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    threshold: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let threshold = if threshold < 0 {
                None
            } else {
                Some(threshold as usize)
            };
            socket.set_option(|options| options.compression_threshold = threshold)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionLevel(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    level: jint,
) {
    guard(&mut env, (), |env| {
        let level = if level == i32::MIN { None } else { Some(level) };
        if let Err(error) = socket_from_handle(handle)
            .and_then(|socket| socket.set_option(|options| options.compression_level = level))
        {
            throw_omq(env, error);
        }
    });
}

#[cfg(feature = "quic")]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetQuicServerCertificate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    cert_pem: JByteArray<'_>,
    key_pem: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let cert_pem = byte_array(env, cert_pem)?;
            let key_pem = byte_array(env, key_pem)?;
            socket_from_handle(handle)?.set_option(|options| {
                options.quic.server_cert_pem = Some(cert_pem);
                options.quic.server_key_pem = Some(key_pem);
            })
        })();
        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[cfg(feature = "quic")]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetQuicTrustPem(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    trust_pem: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let trust_pem = byte_array(env, trust_pem)?;
            socket_from_handle(handle)?.set_option(|options| {
                options.quic.trust_pem = Some(trust_pem);
            })
        })();
        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[cfg(feature = "quic")]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetQuicTrustSystem(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        if let Err(error) = socket_from_handle(handle).and_then(|socket| {
            socket.set_option(|options| options.quic.trust_system = enabled != 0)
        }) {
            throw_omq(env, error);
        }
    });
}

#[cfg(feature = "quic")]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetQuicServerName(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    server_name: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let server_name = java_string(env, server_name)?;
            socket_from_handle(handle)?.set_option(|options| {
                options.quic.server_name = Some(server_name);
            })
        })();
        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetWorkloadProfile(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    profile: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let profile = match profile {
                -1 => None,
                0 => Some(WorkloadProfile::Throughput),
                1 => Some(WorkloadProfile::Latency),
                other => return Err(Error::Config(format!("unknown workload profile {other}"))),
            };
            socket.set_option(move |options| options.workload_profile = profile)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReconnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    mode: jint,
    min_millis: jlong,
    max_millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let reconnect = match mode {
                0 => ReconnectPolicy::Disabled,
                1 => ReconnectPolicy::Fixed(duration_from_millis(min_millis)?),
                2 => {
                    let min = duration_from_millis(min_millis)?;
                    let max = duration_from_millis(max_millis)?;
                    if max < min {
                        return Err(Error::Config(
                            "reconnect max must be greater than or equal to min".to_string(),
                        ));
                    }
                    ReconnectPolicy::Exponential { min, max }
                }
                other => return Err(Error::Config(format!("unknown reconnect mode {other}"))),
            };
            socket.set_option(move |options| options.reconnect = reconnect)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReconnectStopConnRefused(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.reconnect_stop_conn_refused = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHeartbeatTtl(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let ttl = optional_duration_from_millis(millis)?;
            socket.set_option(move |options| options.heartbeat_ttl = ttl)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHeartbeatTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(millis)?;
            socket.set_option(move |options| options.heartbeat_timeout = timeout)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxPendingHandshakes(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    max: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            if max <= 0 {
                return Err(Error::Config(
                    "max pending handshakes must be greater than zero".to_string(),
                ));
            }
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.max_pending_handshakes = max as usize)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetConflate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.conflate = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetRouterMandatory(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.router_mandatory = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetOnMute(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    mode: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let on_mute = match mode {
                0 => OnMute::Block,
                1 => OnMute::DropNewest,
                2 => OnMute::DropOldest,
                other => return Err(Error::Config(format!("unknown on-mute mode {other}"))),
            };
            socket.set_option(move |options| options.on_mute = on_mute)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetTcpKeepalive(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    mode: jint,
    idle_millis: jlong,
    interval_millis: jlong,
    count: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keepalive = match mode {
                0 => KeepAlive::Default,
                1 => KeepAlive::Disabled,
                2 => {
                    if count <= 0 {
                        return Err(Error::Config(
                            "TCP keepalive count must be greater than zero".to_string(),
                        ));
                    }
                    KeepAlive::Enabled {
                        idle: duration_from_millis(idle_millis)?,
                        intvl: duration_from_millis(interval_millis)?,
                        cnt: count as u32,
                    }
                }
                other => return Err(Error::Config(format!("unknown TCP keepalive mode {other}"))),
            };
            socket.set_option(move |options| options.tcp_keepalive = keepalive)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetSendBufferSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("send buffer size", bytes)?;
            socket.set_option(move |options| options.send_buffer_size = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReceiveBufferSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("receive buffer size", bytes)?;
            socket.set_option(move |options| options.recv_buffer_size = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionDict(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    dict: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let dict = byte_array(env, dict)?;
            let dict = if dict.is_empty() {
                None
            } else {
                Some(Bytes::from(dict))
            };
            socket.set_option(move |options| options.compression_dict = dict)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionDictCapacity(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("compression dictionary capacity", bytes)?;
            socket.set_option(move |options| options.compression_dict_capacity = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxReceiveDictSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("max receive dictionary size", bytes)?;
            socket.set_option(move |options| options.max_recv_dict_size = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionOffloadThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("compression offload threshold", bytes)?;
            socket.set_option(move |options| options.compression_offload_threshold = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetLargeMessageThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("large message threshold", bytes)?
                .filter(|bytes| *bytes != 0);
            socket.set_option(move |options| options.large_message_threshold = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetArenaThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("arena threshold", bytes)?;
            socket.set_option(move |options| options.arena_threshold = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetTransmitSlotCap(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("transmit slot capacity", bytes)?;
            socket.set_option(move |options| options.transmit_slot_cap = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetXpubNoDrop(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.xpub_nodrop = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}
