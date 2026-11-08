//! Authentication callbacks, credentials, and key conversion.

#[cfg(any(feature = "plain", feature = "curve"))]
use crate::convert::str_from_c;
#[cfg(feature = "curve")]
use crate::convert::string_to_raw;
use crate::error::CONFIG;
#[cfg(feature = "curve")]
use crate::error::status_from_result;
#[cfg(any(feature = "plain", feature = "curve"))]
use crate::ffi::OmqGoAuthPeer;
use crate::ffi::{OmqGoPlainCredential, OmqGoStatus};
use crate::socket::OmqGoSocket;
#[cfg(any(feature = "plain", feature = "curve"))]
use crate::socket::options::set_socket_option;
#[cfg(any(feature = "plain", feature = "curve"))]
use omq_tokio::Authenticator;
#[cfg(feature = "curve")]
use omq_tokio::CurveKeypair;
#[cfg(feature = "curve")]
use omq_tokio::CurvePublicKey;
#[cfg(feature = "curve")]
use omq_tokio::CurveSecretKey;
#[cfg(feature = "curve")]
use omq_tokio::CurveServerOptions;
#[cfg(any(feature = "plain", feature = "curve"))]
use omq_tokio::{Error, MechanismPeerInfo, MechanismSetup};
use std::ffi::c_char;
#[cfg(any(feature = "plain", feature = "curve"))]
use std::ffi::c_int;
#[cfg(any(feature = "plain", feature = "curve"))]
use std::ptr;
#[cfg(feature = "plain")]
use std::slice;

#[cfg(any(feature = "plain", feature = "curve"))]
unsafe extern "C" {
    fn omqGoAuthCallback(callback_id: u64, peer: *const OmqGoAuthPeer) -> c_int;
}

#[cfg(feature = "curve")]
fn curve_keypair_from_z85(public_key: &str, secret_key: &str) -> Result<CurveKeypair, Error> {
    let public = CurvePublicKey::from_z85(public_key)?;
    let secret = CurveSecretKey::from_z85(secret_key)?;
    Ok(CurveKeypair { public, secret })
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn go_authenticator(callback_id: u64) -> Authenticator {
    Authenticator::new(move |peer| go_authenticate(callback_id, peer))
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn go_authenticate(callback_id: u64, peer: &MechanismPeerInfo) -> bool {
    let mechanism = peer.mechanism.as_str().unwrap_or("");
    let public_key = peer_public_key_z85(peer);

    let (mechanism_data, mechanism_len) = str_view(mechanism);
    let (public_key_data, public_key_len) = str_view(&public_key);
    let (identity_data, identity_len) = optional_bytes_view(peer.identity.as_deref());
    let (peer_address_data, peer_address_len) = optional_str_view(peer.peer_address.as_deref());
    let (username_data, username_len) = optional_str_view(peer.username.as_deref());
    let (password_data, password_len) = optional_str_view(peer.password.as_deref());

    let c_peer = OmqGoAuthPeer {
        mechanism_data,
        mechanism_len,
        public_key_data,
        public_key_len,
        identity_data,
        identity_len,
        peer_address_data,
        peer_address_len,
        username_data,
        username_len,
        password_data,
        password_len,
    };
    unsafe { omqGoAuthCallback(callback_id, &c_peer) != 0 }
}

#[cfg(feature = "curve")]
fn peer_public_key_z85(peer: &MechanismPeerInfo) -> String {
    if peer.mechanism == omq_proto::proto::MechanismName::CURVE {
        return CurvePublicKey::from_bytes(peer.public_key).to_z85();
    }
    String::new()
}

#[cfg(all(feature = "plain", not(feature = "curve")))]
fn peer_public_key_z85(_peer: &MechanismPeerInfo) -> String {
    String::new()
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn str_view(value: &str) -> (*const u8, usize) {
    if value.is_empty() {
        return (ptr::null(), 0);
    }
    (value.as_ptr(), value.len())
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn optional_str_view(value: Option<&str>) -> (*const u8, usize) {
    value.map_or((ptr::null(), 0), str_view)
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn optional_bytes_view(value: Option<&[u8]>) -> (*const u8, usize) {
    match value {
        Some(bytes) if !bytes.is_empty() => (bytes.as_ptr(), bytes.len()),
        _ => (ptr::null(), 0),
    }
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_curve_keypair(
    public_key: *mut *mut c_char,
    secret_key: *mut *mut c_char,
) -> OmqGoStatus {
    if public_key.is_null() || secret_key.is_null() {
        return OmqGoStatus::err(CONFIG, "null CURVE key output pointer");
    }
    let keypair = CurveKeypair::generate();
    unsafe {
        *public_key = string_to_raw(keypair.public.to_z85());
        *secret_key = string_to_raw(keypair.secret.to_z85());
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_curve_keypair(
    _public_key: *mut *mut c_char,
    _secret_key: *mut *mut c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_curve_public(
    secret_key: *const c_char,
    public_key: *mut *mut c_char,
) -> OmqGoStatus {
    if public_key.is_null() {
        return OmqGoStatus::err(CONFIG, "null CURVE public key output pointer");
    }
    let result = (|| {
        let secret = CurveSecretKey::from_z85(str_from_c(secret_key)?)?;
        unsafe {
            *public_key = string_to_raw(secret.derive_public().to_z85());
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_curve_public(
    _secret_key: *const c_char,
    _public_key: *mut *mut c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "C" fn omq_go_socket_set_plain_server(
    socket: *mut OmqGoSocket,
    credentials: *const OmqGoPlainCredential,
    credential_count: usize,
) -> OmqGoStatus {
    let result = (|| {
        if credentials.is_null() && credential_count != 0 {
            return Err(Error::Config("null PLAIN credential array".into()));
        }
        if credential_count > isize::MAX as usize / std::mem::size_of::<OmqGoPlainCredential>() {
            return Err(Error::Config("PLAIN credential count is too large".into()));
        }
        let credentials = if credential_count == 0 {
            &[]
        } else {
            // SAFETY: the C caller provides `credential_count` readable entries.
            unsafe { slice::from_raw_parts(credentials, credential_count) }
        };
        let expected = credentials
            .iter()
            .map(|credential| {
                Ok((
                    str_from_c(credential.username)?.to_string(),
                    str_from_c(credential.password)?.to_string(),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::PlainServer {
                authenticator: Authenticator::plain_credentials(expected),
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "C" fn omq_go_socket_set_plain_server(
    _socket: *mut OmqGoSocket,
    _credentials: *const OmqGoPlainCredential,
    _credential_count: usize,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "PLAIN support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "C" fn omq_go_socket_set_plain_server_callback(
    socket: *mut OmqGoSocket,
    callback_id: u64,
) -> OmqGoStatus {
    if callback_id == 0 {
        return OmqGoStatus::err(CONFIG, "invalid auth callback id");
    }
    set_socket_option(socket, move |options| {
        options.mechanism = MechanismSetup::PlainServer {
            authenticator: go_authenticator(callback_id),
        };
    })
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "C" fn omq_go_socket_set_plain_server_callback(
    _socket: *mut OmqGoSocket,
    _callback_id: u64,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "PLAIN support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "C" fn omq_go_socket_set_plain_client(
    socket: *mut OmqGoSocket,
    username: *const c_char,
    password: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let username = str_from_c(username)?.to_string();
        let password = str_from_c(password)?.to_string();
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::PlainClient { username, password };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "C" fn omq_go_socket_set_plain_client(
    _socket: *mut OmqGoSocket,
    _username: *const c_char,
    _password: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "PLAIN support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_socket_set_curve_server(
    socket: *mut OmqGoSocket,
    public_key: *const c_char,
    secret_key: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let keypair = curve_keypair_from_z85(str_from_c(public_key)?, str_from_c(secret_key)?)?;
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::CurveServer {
                our_keypair: keypair,
                options: CurveServerOptions::default(),
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_socket_set_curve_server(
    _socket: *mut OmqGoSocket,
    _public_key: *const c_char,
    _secret_key: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_socket_set_curve_server_callback(
    socket: *mut OmqGoSocket,
    public_key: *const c_char,
    secret_key: *const c_char,
    callback_id: u64,
) -> OmqGoStatus {
    if callback_id == 0 {
        return OmqGoStatus::err(CONFIG, "invalid auth callback id");
    }
    let result = (|| {
        let keypair = curve_keypair_from_z85(str_from_c(public_key)?, str_from_c(secret_key)?)?;
        Ok(set_socket_option(socket, move |options| {
            let mut curve_options = CurveServerOptions::default();
            curve_options.authenticator = Some(go_authenticator(callback_id));
            options.mechanism = MechanismSetup::CurveServer {
                our_keypair: keypair,
                options: curve_options,
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_socket_set_curve_server_callback(
    _socket: *mut OmqGoSocket,
    _public_key: *const c_char,
    _secret_key: *const c_char,
    _callback_id: u64,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_socket_set_curve_client(
    socket: *mut OmqGoSocket,
    public_key: *const c_char,
    secret_key: *const c_char,
    server_public_key: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let keypair = curve_keypair_from_z85(str_from_c(public_key)?, str_from_c(secret_key)?)?;
        let server_public = CurvePublicKey::from_z85(str_from_c(server_public_key)?)?;
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::CurveClient {
                our_keypair: keypair,
                server_public,
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_socket_set_curve_client(
    _socket: *mut OmqGoSocket,
    _public_key: *const c_char,
    _secret_key: *const c_char,
    _server_public_key: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}
