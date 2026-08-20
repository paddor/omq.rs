use std::time::Duration;

use bytes::Bytes;

use rb_sys::VALUE;

use crate::rb::{self, RbResult, RubyErr};

#[expect(
    clippy::too_many_lines,
    reason = "flat option mapping mirrors omq-proto Options fields"
)]
pub fn build_options(hash: VALUE) -> RbResult<omq_tokio::Options> {
    rb::check_hash(hash)?;

    let mut opts = omq_tokio::Options::default();

    if let Some(v) = get_opt_u32(hash, "send_hwm")? {
        opts.send_hwm = v;
    }
    if let Some(v) = get_opt_u32(hash, "recv_hwm")? {
        opts.recv_hwm = v;
    }
    if let Some(v) = get_rate_limit(hash, "recv_rate_limit")? {
        opts.recv_rate_limit = Some(v);
    }
    if let Some(v) = get_rate_limit(hash, "recv_ip_rate_limit")? {
        opts.recv_ip_rate_limit = Some(v);
    }
    if let Some(v) = get_opt_string(hash, "workload_profile")? {
        opts.workload_profile = Some(match v.as_str() {
            "throughput" => omq_proto::options::WorkloadProfile::Throughput,
            "latency" => omq_proto::options::WorkloadProfile::Latency,
            _ => {
                return Err(RubyErr::arg(
                    "workload_profile must be :throughput or :latency",
                ));
            }
        });
    }
    if let Some(v) = get_opt_f64(hash, "linger")? {
        opts.linger = if v.is_infinite() && v.is_sign_positive() {
            None
        } else {
            Some(duration_from_seconds("linger", v)?)
        };
    }
    if let Some(v) = get_opt_bytes(hash, "identity")?
        && !v.is_empty()
    {
        opts.identity = Bytes::from(v);
    }
    if let Some(v) = get_opt_bool(hash, "router_mandatory")? {
        opts.router_mandatory = v;
    }
    if let Some(v) = get_opt_bool(hash, "conflate")? {
        opts.conflate = v;
    }
    if let Some(v) = get_opt_duration(hash, "heartbeat_interval")? {
        opts.heartbeat_interval = Some(v);
    }
    if let Some(v) = get_opt_duration(hash, "heartbeat_ttl")? {
        opts.heartbeat_ttl = Some(v);
    }
    if let Some(v) = get_opt_duration(hash, "heartbeat_timeout")? {
        opts.heartbeat_timeout = Some(v);
    }
    if let Some(v) = get_opt_duration(hash, "handshake_timeout")? {
        opts.handshake_timeout = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "max_pending_handshakes")? {
        opts.max_pending_handshakes = v;
    }
    if let Some(v) = get_opt_usize(hash, "max_message_size")? {
        opts.max_message_size = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "sndbuf")? {
        opts.send_buffer_size = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "rcvbuf")? {
        opts.recv_buffer_size = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "large_message_threshold")? {
        opts.large_message_threshold = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "arena_threshold")? {
        opts.arena_threshold = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "transmit_slot_cap")? {
        opts.transmit_slot_cap = Some(v);
    }
    if let Some(v) = get_opt_bool(hash, "xpub_nodrop")? {
        opts.xpub_nodrop = v;
    }
    if let Some(v) = get_opt_bool(hash, "reconnect_stop_conn_refused")? {
        opts.reconnect_stop_conn_refused = v;
    }
    if let Some(v) = get_opt_bytes(hash, "compression_dict")? {
        opts.compression_dict = (!v.is_empty()).then(|| Bytes::from(v));
    }
    if let Some(v) = get_opt_bool(hash, "compression_auto_train")? {
        opts.compression_auto_train = v;
    }
    if let Some(v) = get_opt_usize(hash, "compression_threshold")? {
        opts.compression_threshold = Some(v);
    }
    if let Some(v) = get_opt_i64(hash, "compression_level")? {
        opts.compression_level =
            Some(i32::try_from(v).map_err(|_| RubyErr::arg("compression_level must fit in i32"))?);
    }
    if let Some(v) = get_opt_usize(hash, "compression_dict_capacity")? {
        opts.compression_dict_capacity = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "max_recv_dict_size")? {
        opts.max_recv_dict_size = Some(v);
    }
    if let Some(v) = get_opt_i64(hash, "compression_offload_threshold")? {
        opts.compression_offload_threshold = if v < 0 { None } else { Some(v as usize) };
    }
    if let Some(v) = get_opt_string(hash, "on_mute")? {
        opts.on_mute = match v.as_str() {
            "drop_newest" | "drop" => omq_tokio::OnMute::DropNewest,
            "drop_oldest" => omq_tokio::OnMute::DropOldest,
            "block" => omq_tokio::OnMute::Block,
            _ => {
                return Err(RubyErr::arg(
                    "on_mute must be :block, :drop_newest, or :drop_oldest",
                ));
            }
        };
    }

    if let Some(v) = get_opt_f64(hash, "reconnect_interval")? {
        opts.reconnect = omq_proto::options::ReconnectPolicy::Fixed(duration_from_seconds(
            "reconnect_interval",
            v,
        )?);
    }
    if let Some(min) = get_opt_f64(hash, "reconnect_interval_min")? {
        let max = get_opt_f64(hash, "reconnect_interval_max")?.unwrap_or(min * 16.0);
        opts.reconnect = omq_proto::options::ReconnectPolicy::Exponential {
            min: duration_from_seconds("reconnect_interval min", min)?,
            max: duration_from_seconds("reconnect_interval max", max)?,
        };
    }

    let mut mechanism_type = get_opt_string(hash, "mechanism_type")?;
    if mechanism_type.is_none() {
        let curve = option_present(
            hash,
            &[
                "curve_server",
                "curve_publickey",
                "curve_public_key",
                "curve_secretkey",
                "curve_secret_key",
                "curve_serverkey",
                "curve_server_key",
            ],
        )?;
        let plain = option_present(hash, &["plain_server", "plain_username", "plain_password"])?;
        mechanism_type = if curve {
            Some("curve".to_owned())
        } else if plain {
            Some("plain".to_owned())
        } else {
            None
        };
    }
    if let Some(mechanism_type) = mechanism_type {
        apply_mechanism(hash, &mechanism_type, &mut opts)?;
    }

    opts.validate()
        .map_err(|error| RubyErr::arg(error.to_string()))?;
    Ok(opts)
}

fn apply_mechanism(hash: VALUE, mech_type: &str, opts: &mut omq_tokio::Options) -> RbResult<()> {
    match mech_type {
        "null" => {}

        #[cfg(feature = "curve")]
        "curve" => {
            let is_server =
                get_opt_bool_alias(hash, &["curve_server", "mechanism_server"])?.unwrap_or(false);
            let pub_key = get_opt_bytes_alias(
                hash,
                &[
                    "curve_publickey",
                    "curve_public_key",
                    "mechanism_public_key",
                ],
            )?;
            let sec_key = get_opt_bytes_alias(
                hash,
                &[
                    "curve_secretkey",
                    "curve_secret_key",
                    "mechanism_secret_key",
                ],
            )?;

            if is_server {
                let public = parse_curve_public_key(
                    &pub_key
                        .ok_or_else(|| RubyErr::arg("CURVE server requires curve_publickey"))?,
                    "curve_publickey",
                )?;
                let secret = curve_secret_key(
                    &sec_key
                        .ok_or_else(|| RubyErr::arg("CURVE server requires curve_secretkey"))?,
                    "curve_secretkey",
                )?;
                validate_curve_keypair(&public, &secret)?;
                opts.mechanism = omq_proto::MechanismSetup::CurveServer {
                    our_keypair: omq_proto::CurveKeypair { public, secret },
                    options: omq_proto::CurveServerOptions::default(),
                };
            } else {
                let server_key = get_opt_bytes_alias(
                    hash,
                    &[
                        "curve_serverkey",
                        "curve_server_key",
                        "mechanism_server_key",
                    ],
                )?;
                let public = parse_curve_public_key(
                    &pub_key
                        .ok_or_else(|| RubyErr::arg("CURVE client requires curve_publickey"))?,
                    "curve_publickey",
                )?;
                let secret = curve_secret_key(
                    &sec_key
                        .ok_or_else(|| RubyErr::arg("CURVE client requires curve_secretkey"))?,
                    "curve_secretkey",
                )?;
                validate_curve_keypair(&public, &secret)?;
                let server_public = parse_curve_public_key(
                    &server_key
                        .ok_or_else(|| RubyErr::arg("CURVE client requires curve_serverkey"))?,
                    "curve_serverkey",
                )?;
                opts.mechanism = omq_proto::MechanismSetup::CurveClient {
                    our_keypair: omq_proto::CurveKeypair { public, secret },
                    server_public,
                };
            }
        }

        #[cfg(feature = "plain")]
        "plain" => {
            if get_opt_bool_alias(hash, &["plain_server", "mechanism_server"])?.unwrap_or(false) {
                opts.mechanism = omq_proto::MechanismSetup::PlainServer {
                    authenticator: omq_proto::Authenticator::new(|_| true),
                };
            } else {
                let username =
                    get_opt_string_alias(hash, &["plain_username", "mechanism_username"])?
                        .ok_or_else(|| RubyErr::arg("PLAIN client requires plain_username"))?;
                let password =
                    get_opt_string_alias(hash, &["plain_password", "mechanism_password"])?
                        .ok_or_else(|| RubyErr::arg("PLAIN client requires plain_password"))?;
                opts.mechanism = omq_proto::MechanismSetup::PlainClient { username, password };
            }
        }

        _ => return Err(RubyErr::arg(format!("unknown mechanism_type: {mech_type}"))),
    }
    Ok(())
}

#[cfg(feature = "curve")]
pub(crate) fn parse_curve_public_key(
    bytes: &[u8],
    label: &str,
) -> RbResult<omq_proto::CurvePublicKey> {
    if let Ok(raw) = <[u8; 32]>::try_from(bytes) {
        return Ok(omq_proto::CurvePublicKey::from_bytes(raw));
    }
    let z85 = std::str::from_utf8(bytes)
        .map_err(|_| RubyErr::arg(format!("{label} must be raw bytes or Z85 ASCII")))?;
    omq_proto::CurvePublicKey::from_z85(z85)
        .map_err(|error| RubyErr::arg(format!("invalid {label}: {error}")))
}

#[cfg(feature = "curve")]
fn curve_secret_key(bytes: &[u8], label: &str) -> RbResult<omq_proto::CurveSecretKey> {
    if let Ok(raw) = <[u8; 32]>::try_from(bytes) {
        return Ok(omq_proto::CurveSecretKey::from_bytes(raw));
    }
    let z85 = std::str::from_utf8(bytes)
        .map_err(|_| RubyErr::arg(format!("{label} must be raw bytes or Z85 ASCII")))?;
    omq_proto::CurveSecretKey::from_z85(z85)
        .map_err(|error| RubyErr::arg(format!("invalid {label}: {error}")))
}

#[cfg(feature = "curve")]
fn validate_curve_keypair(
    public: &omq_proto::CurvePublicKey,
    secret: &omq_proto::CurveSecretKey,
) -> RbResult<()> {
    if secret.derive_public().as_bytes() == public.as_bytes() {
        Ok(())
    } else {
        Err(RubyErr::arg("CURVE public and secret keys do not match"))
    }
}

fn option_present(hash: VALUE, keys: &[&str]) -> RbResult<bool> {
    for key in keys {
        if rb::hash_get(hash, key)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn get_opt_bytes_alias(hash: VALUE, keys: &[&str]) -> RbResult<Option<Vec<u8>>> {
    for key in keys {
        if rb::hash_get(hash, key)?.is_some() {
            return get_opt_bytes(hash, key);
        }
    }
    Ok(None)
}

fn get_opt_string_alias(hash: VALUE, keys: &[&str]) -> RbResult<Option<String>> {
    for key in keys {
        if rb::hash_get(hash, key)?.is_some() {
            return get_opt_string(hash, key);
        }
    }
    Ok(None)
}

fn get_opt_bool_alias(hash: VALUE, keys: &[&str]) -> RbResult<Option<bool>> {
    for key in keys {
        if rb::hash_get(hash, key)?.is_some() {
            return get_opt_bool(hash, key);
        }
    }
    Ok(None)
}

fn get_opt_bytes(hash: VALUE, key: &str) -> RbResult<Option<Vec<u8>>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_bytes(v)?)),
        None => Ok(None),
    }
}

fn get_opt_string(hash: VALUE, key: &str) -> RbResult<Option<String>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_string(v)?)),
        None => Ok(None),
    }
}

fn get_opt_i64(hash: VALUE, key: &str) -> RbResult<Option<i64>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_i64(v)?)),
        None => Ok(None),
    }
}

fn get_opt_f64(hash: VALUE, key: &str) -> RbResult<Option<f64>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_f64(v)?)),
        None => Ok(None),
    }
}

fn get_opt_usize(hash: VALUE, key: &str) -> RbResult<Option<usize>> {
    let Some(v) = get_opt_i64(hash, key)? else {
        return Ok(None);
    };

    usize::try_from(v)
        .map(Some)
        .map_err(|_| RubyErr::arg(format!("{key} must be non-negative")))
}

fn get_opt_u32(hash: VALUE, key: &str) -> RbResult<Option<u32>> {
    let Some(v) = get_opt_i64(hash, key)? else {
        return Ok(None);
    };

    u32::try_from(v)
        .map(Some)
        .map_err(|_| RubyErr::arg(format!("{key} must fit in a 32-bit unsigned integer")))
}

fn get_rate_limit(hash: VALUE, key: &str) -> RbResult<Option<omq_proto::MessageRateLimit>> {
    let Some(value) = rb::hash_get(hash, key)? else {
        return Ok(None);
    };
    if value == rb::qnil() {
        return Ok(None);
    }
    rb::check_hash(value)?;

    let rate = get_opt_u32(value, "messages_per_second")?
        .or(get_opt_u32(value, "rate")?)
        .ok_or_else(|| RubyErr::arg(format!("{key} requires :messages_per_second")))?;
    let burst = get_opt_u32(value, "burst")?
        .ok_or_else(|| RubyErr::arg(format!("{key} requires :burst")))?;
    Ok(Some(omq_proto::MessageRateLimit::new(rate, burst)))
}

fn get_opt_bool(hash: VALUE, key: &str) -> RbResult<Option<bool>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_bool(v)?)),
        None => Ok(None),
    }
}

fn get_opt_duration(hash: VALUE, key: &str) -> RbResult<Option<Duration>> {
    match get_opt_f64(hash, key)? {
        Some(v) => Ok(Some(duration_from_seconds(key, v)?)),
        None => Ok(None),
    }
}

fn duration_from_seconds(label: &str, value: f64) -> RbResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|_| RubyErr::arg(format!("{label} must be finite and non-negative")))
}
