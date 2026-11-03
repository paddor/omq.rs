//! Endpoint calls: bind, connect, unbind, disconnect, peer disconnect, and
//! DISH group membership.

use std::ffi::{CStr, c_int, c_void};
use std::net::{IpAddr, Ipv6Addr};
use std::os::raw::c_char;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use omq_tokio::SocketType;
use omq_tokio::endpoint::{Endpoint, Host};

use super::{OmqSocket, materialized, run_on_socket};
use crate::error::{ETERM, c_try, fail, set_errno};
use crate::opts::SocketOverlay;

/// Rewrite `Host::Wildcard` to IPv6 unspecified (::) for dual-stack bind.
fn ipv6_rewrite_wildcard(ep: Endpoint) -> Endpoint {
    match ep {
        Endpoint::Tcp {
            host: Host::Wildcard,
            port,
        } => Endpoint::Tcp {
            host: Host::Ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
            port,
        },
        other => other,
    }
}

/// Validate `sock_ptr` and `addr`, parse `addr` as a `CStr` and then as an
/// `Endpoint`. Returns the socket reference, owned address string, and
/// parsed endpoint on success. Also checks the context terminated flag.
///
/// # Safety
///
/// `sock_ptr` must be null or readable for a handle tag (null returns
/// `ENOTSOCK`). `addr` must be a valid C string pointer or null (returns
/// `EFAULT`).
unsafe fn parse_endpoint_args<'a>(
    sock_ptr: *mut c_void,
    addr: *const c_char,
) -> Result<(&'a Arc<OmqSocket>, String, Endpoint), c_int> {
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = unsafe { crate::handle::socket(sock_ptr) }?;
    if addr.is_null() {
        return Err(libc::EFAULT);
    }
    if sock.ctx.is_effectively_terminated() {
        return Err(ETERM);
    }
    // SAFETY: addr is non-null (checked above); caller guarantees a valid C string.
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return Err(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();

    // DART's buffer ownership API is currently native Rust only. Cargo
    // feature unification must not silently expose it through the C facade.
    if addr_str.starts_with("dart://") {
        return Err(libc::EINVAL);
    }

    let Ok(endpoint) = omq_tokio::Endpoint::from_str(&addr_str) else {
        return Err(libc::EINVAL);
    };
    Ok((sock, addr_str, endpoint))
}

/// Validate `sock_ptr` and `cstr`, parse `cstr` as a UTF-8 `CStr`.
/// Does **not** parse an endpoint or check the terminated flag (join/leave
/// operate on group names, not transport addresses).
///
/// # Safety
///
/// `sock_ptr` must be null or readable for a handle tag.
/// `cstr` must be a valid C string pointer or null.
unsafe fn parse_group_args<'a>(
    sock_ptr: *mut c_void,
    cstr: *const c_char,
) -> Result<(&'a Arc<OmqSocket>, Bytes), c_int> {
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = unsafe { crate::handle::socket(sock_ptr) }?;
    if cstr.is_null() {
        return Err(libc::EFAULT);
    }
    // SAFETY: cstr is non-null (checked above); caller guarantees a valid C string.
    let Ok(g) = unsafe { CStr::from_ptr(cstr) }.to_str() else {
        return Err(libc::EINVAL);
    };
    Ok((sock, Bytes::copy_from_slice(g.as_bytes())))
}

/// Read codec parameters for this operation even after backend materialization.
/// Validate both the requested overlay mechanism and the backend's actual
/// mechanism; the latter is checked by the native endpoint setup.
fn compression_options_for_endpoint(
    overlay: &SocketOverlay,
    endpoint: &Endpoint,
) -> Result<Option<omq_tokio::CompressionOptions>, c_int> {
    if omq_tokio::CompressionKind::for_endpoint(endpoint).is_none() {
        return Ok(None);
    }
    let options = overlay.to_options();
    omq_tokio::CompressionKind::for_endpoint_with_mechanism(endpoint, &options.mechanism)
        .map_err(|_| libc::EINVAL)?;
    options.validate().map_err(|_| libc::EINVAL)?;
    Ok(Some(omq_tokio::CompressionOptions::from(&options)))
}

/// Bind a null-terminated endpoint URI; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_bind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, mut endpoint) = c_try!(unsafe { parse_endpoint_args(sock_ptr, addr) });

    if addr_str == crate::zap::ENDPOINT {
        c_try!(sock.ctx.zap.bind(sock));
        sock.zap_handler.store(true, Ordering::Release);
        sock.bound_or_connected.store(true, Ordering::Release);
        if let Ok(mut ep) = sock.last_endpoint.lock() {
            *ep = Some(addr_str);
        }
        return 0;
    }
    if sock.zap_handler.load(Ordering::Acquire) {
        return fail(libc::EINVAL);
    }

    // ZMQ_IPV6: rewrite wildcard bind to IPv6 unspecified (dual-stack).
    let Ok(ov) = sock.overlay.lock() else {
        return fail(ETERM);
    };
    if ov.ipv6 {
        endpoint = ipv6_rewrite_wildcard(endpoint);
    }
    let compression = c_try!(compression_options_for_endpoint(&ov, &endpoint));
    drop(ov);

    let inner = c_try!(materialized(sock));
    let resolved = c_try!(run_on_socket(sock, inner, move |s| async move {
        if let Some(compression) = compression {
            s.bind_with_compression_options(endpoint.clone(), compression)
                .await?;
        } else {
            s.bind(endpoint.clone()).await?;
        }
        Ok(s.last_bound_endpoint().map(|ep| ep.to_string()))
    }));
    if let Ok(mut ep) = sock.last_endpoint.lock() {
        *ep = resolved.or(Some(addr_str));
    }
    0
}

/// Register a null-terminated endpoint URI for connection and automatic reconnection.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, endpoint) = c_try!(unsafe { parse_endpoint_args(sock_ptr, addr) });

    if addr_str == crate::zap::ENDPOINT {
        return fail(libc::EINVAL);
    }
    if sock.zap_handler.load(Ordering::Acquire) {
        return fail(libc::EINVAL);
    }

    let compression = {
        let Ok(overlay) = sock.overlay.lock() else {
            return fail(ETERM);
        };
        c_try!(compression_options_for_endpoint(&overlay, &endpoint))
    };

    let inner = c_try!(materialized(sock));
    c_try!(run_on_socket(sock, inner, move |s| async move {
        if let Some(compression) = compression {
            s.connect_with_compression_options(endpoint, compression)
                .await
        } else {
            s.connect(endpoint).await
        }
    }));
    sock.connect_count.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut ep) = sock.last_endpoint.lock() {
        *ep = Some(addr_str);
    }
    0
}

/// Remove a bound endpoint by its null-terminated URI; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_unbind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, endpoint) = c_try!(unsafe { parse_endpoint_args(sock_ptr, addr) });

    if addr_str == crate::zap::ENDPOINT && sock.zap_handler.swap(false, Ordering::AcqRel) {
        sock.ctx.zap.unbind(sock.id);
        sock.bound_or_connected.store(false, Ordering::Release);
        if let Ok(mut endpoint) = sock.last_endpoint.lock() {
            *endpoint = None;
        }
        return 0;
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    c_try!(run_on_socket(sock, inner, move |s| async move {
        s.unbind(endpoint).await
    }));
    0
}

/// Remove a connected endpoint and stop retries; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_disconnect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, _addr_str, endpoint) = c_try!(unsafe { parse_endpoint_args(sock_ptr, addr) });

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    c_try!(run_on_socket(sock, inner, move |s| async move {
        s.disconnect(endpoint).await
    }));
    // Atomic::try_update is unstable on MSRV 1.93.
    #[allow(deprecated)]
    let _ = sock
        .connect_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            Some(n.saturating_sub(1))
        });
    0
}

/// Unsupported peer-connect entry point; return zero with ENOTSUP for a valid socket.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect_peer(sock_ptr: *mut c_void, _addr: *const libc::c_char) -> u32 {
    if sock_ptr.is_null() {
        set_errno(crate::error::ENOTSOCK);
        return 0;
    }
    set_errno(crate::error::ENOTSUP);
    0
}

/// Close one SERVER peer by routing id; return zero or -1 with errno.
/// Unknown ids fail with EHOSTUNREACH. Other socket types, including PEER,
/// fail with ENOTSUP.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_disconnect_peer(sock_ptr: *mut c_void, routing_id: u32) -> c_int {
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = c_try!(unsafe { crate::handle::socket(sock_ptr) });
    if sock.ctx.is_effectively_terminated() {
        return fail(ETERM);
    }
    if sock.socket_type != SocketType::Server {
        return fail(crate::error::ENOTSUP);
    }
    // An unmaterialized socket has no peers.
    let Some(inner) = sock.inner.get() else {
        return fail(libc::EHOSTUNREACH);
    };

    c_try!(run_on_socket(sock, inner, move |s| async move {
        let Some(peer) = s.peer_info(routing_id).await? else {
            return Err(omq_tokio::error::Error::Unroutable);
        };
        s.disconnect_peer(peer.connection_id).await
    }));
    0
}

/// Join a null-terminated DISH group; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_join(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    let (sock, g) = c_try!(unsafe { parse_group_args(sock_ptr, group) });
    let inner = c_try!(materialized(sock));
    c_try!(run_on_socket(sock, inner, move |s| async move {
        s.join(g).await
    }));
    0
}

/// Leave a null-terminated DISH group; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_leave(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    let (sock, g) = c_try!(unsafe { parse_group_args(sock_ptr, group) });
    let inner = c_try!(materialized(sock));
    c_try!(run_on_socket(sock, inner, move |s| async move {
        s.leave(g).await
    }));
    0
}
