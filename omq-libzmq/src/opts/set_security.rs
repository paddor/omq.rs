//! PLAIN, CURVE, WebSocket, and WSS options for `zmq_setsockopt`.

#[allow(
    clippy::wildcard_imports,
    reason = "option code table; the lint skips test builds"
)]
use super::codes::*;
use super::raw::{OptIn, none_if_empty, parse_origins};
use super::{MechanismOverlay, SecretBytes, overlay};
use crate::error::{c_try, fail};
use crate::socket::OmqSocket;
use std::ffi::c_int;
use std::sync::Arc;

/// PLAIN server role and ZAP domain.
pub(super) fn set_plain_server_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_PLAIN_SERVER => {
            let v = optval.i32()?;
            let mut ov = overlay(sock_arc)?;
            if v != 0 {
                ov.mechanism = MechanismOverlay::PlainServer;
            } else if matches!(
                ov.mechanism,
                MechanismOverlay::PlainServer | MechanismOverlay::PlainServerCredentials { .. }
            ) {
                ov.mechanism = MechanismOverlay::Null;
            }
        }
        ZMQ_ZAP_DOMAIN => {
            let domain = optval.string()?;
            if domain.len() > 255 || !domain.is_ascii() {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.zap.domain = domain;
        }
        ZMQ_ZAP_ENFORCE_DOMAIN => {
            let value = optval.i32()?;
            overlay(sock_arc)?.zap.enforce = value != 0;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// PLAIN client credentials.
pub(super) fn set_plain_client_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_PLAIN_USERNAME => {
            let s = optval.string()?;
            if s.len() > 255 || !s.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(libc::EINVAL);
            }
            let mut ov = overlay(sock_arc)?;
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
            let s = optval.string()?;
            if s.len() > 255 || !s.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(libc::EINVAL);
            }
            let mut ov = overlay(sock_arc)?;
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
        _ => return Ok(false),
    }

    Ok(true)
}

/// CURVE server role and secret key.
pub(super) fn set_curve_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_CURVE_SERVER => {
            let v = optval.i32()?;
            let mut ov = overlay(sock_arc)?;
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
        ZMQ_CURVE_SECRETKEY => {
            let key = optval.key()?;
            let mut ov = overlay(sock_arc)?;
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
        _ => return Ok(false),
    }

    Ok(true)
}

/// CURVE client public and server keys.
pub(super) fn set_curve_client_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_CURVE_PUBLICKEY => {
            let key = optval.key()?;
            let mut ov = overlay(sock_arc)?;
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
        ZMQ_CURVE_SERVERKEY => {
            let key = optval.key()?;
            let mut ov = overlay(sock_arc)?;
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
        _ => return Ok(false),
    }

    Ok(true)
}

/// WebSocket peer limit and origin policy.
pub(super) fn set_ws_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        OMQ_WS_MAX_READY_PEERS => {
            if sock_arc.inner.get().is_some() {
                return Err(libc::EBUSY);
            }
            let value = optval.i32()?;
            if value <= 0 {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.ws.max_ready_peers = value;
        }
        OMQ_WS_ALLOWED_ORIGINS => {
            // Security policy must take effect, never silently mutate an
            // overlay after its immutable backend options were consumed.
            if sock_arc.inner.get().is_some() {
                return Err(libc::EBUSY);
            }
            let origins = parse_origins(&optval.string()?)?;
            overlay(sock_arc)?.ws.allowed_origins = origins;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// WebSocket TLS material.
pub(super) fn set_wss_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_WSS_KEY_PEM => {
            let v = optval.bytes()?;
            overlay(sock_arc)?.wss.key_pem = none_if_empty(v).map(SecretBytes);
        }
        ZMQ_WSS_CERT_PEM => {
            let v = optval.bytes()?;
            overlay(sock_arc)?.wss.cert_pem = none_if_empty(v);
        }
        ZMQ_WSS_TRUST_PEM => {
            let v = optval.bytes()?;
            overlay(sock_arc)?.wss.trust_pem = none_if_empty(v);
        }
        ZMQ_WSS_HOSTNAME => {
            let v = optval.string()?;
            overlay(sock_arc)?.wss.hostname = if v.is_empty() { None } else { Some(v) };
        }
        ZMQ_WSS_TRUST_SYSTEM => {
            let v = optval.i32()?;
            overlay(sock_arc)?.wss.trust_system = v != 0;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// One exact username/password pair in an OMQ fixed PLAIN server allowlist.
///
/// Each field is borrowed for the duration of
/// [`omq_socket_set_plain_server_credentials`] and may be null only when its
/// corresponding length is zero.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct OmqPlainCredential {
    /// Username bytes.
    pub username: *const u8,
    /// Username byte length.
    pub username_size: usize,
    /// Password bytes.
    pub password: *const u8,
    /// Password byte length.
    pub password_size: usize,
}

fn copy_plain_credential_field(ptr: *const u8, len: usize) -> Result<String, c_int> {
    if ptr.is_null() && len != 0 {
        return Err(libc::EFAULT);
    }
    if len > 255 {
        return Err(libc::EINVAL);
    }
    if len == 0 {
        return Ok(String::new());
    }
    // SAFETY: the non-null pointer is a caller-provided readable buffer of
    // `len` bytes. The C ABI requires it to remain valid for this call.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Err(libc::EINVAL);
    };
    if !text.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(libc::EINVAL);
    }
    Ok(text.to_owned())
}

/// Configure an OMQ-specific fixed PLAIN server credential allowlist.
///
/// Call this before the socket is bound or connected. Each username and
/// password must contain at most 255 ASCII VCHAR bytes. The function copies
/// every credential, so the input array and field buffers are borrowed only
/// for the call. A null array is accepted only when `credential_count` is
/// zero; an empty list denies every client.
///
/// Returns `0` on success, or `-1` with the C API error number set. Standard
/// `ZMQ_PLAIN_USERNAME` and `ZMQ_PLAIN_PASSWORD` remain client-only options,
/// while `ZMQ_PLAIN_SERVER` uses ZAP.
#[unsafe(no_mangle)]
pub extern "C" fn omq_socket_set_plain_server_credentials(
    sock: *mut libc::c_void,
    credentials: *const OmqPlainCredential,
    credential_count: usize,
) -> c_int {
    if sock.is_null() || (credentials.is_null() && credential_count != 0) {
        return fail(libc::EFAULT);
    }
    if credential_count > isize::MAX as usize / std::mem::size_of::<OmqPlainCredential>() {
        return fail(libc::EINVAL);
    }
    // SAFETY: C callers pass null or a pointer from this API.
    let socket = c_try!(unsafe { crate::handle::socket(sock) });
    if socket
        .endpoints
        .bound_or_connected
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return fail(libc::EINVAL);
    }
    let entries = if credential_count == 0 {
        &[]
    } else {
        // SAFETY: the non-null pointer is a caller-provided readable array of
        // `credential_count` entries. The size check prevents slice overflow.
        unsafe { std::slice::from_raw_parts(credentials, credential_count) }
    };
    let mut copied = Vec::new();
    if copied.try_reserve_exact(entries.len()).is_err() {
        return fail(libc::ENOMEM);
    }
    for entry in entries {
        let username = c_try!(copy_plain_credential_field(
            entry.username,
            entry.username_size
        ));
        let password = c_try!(copy_plain_credential_field(
            entry.password,
            entry.password_size
        ));
        copied.push((username, password));
    }
    c_try!(overlay(socket)).mechanism = MechanismOverlay::PlainServerCredentials {
        credentials: copied,
    };
    0
}
