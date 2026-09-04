//! ZeroMQ Authentication Protocol (RFC 27) inproc bridge.
//!
//! Native OMQ authenticators are synchronous. This context-local bridge gives
//! libzmq applications the standard REP/ROUTER ZAP socket interface without
//! scheduling the handler on the Tokio runtime that is waiting for it.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;

use crate::error::{EFSM, ETERM};
use crate::notify::NotifyHandle;
use crate::socket::OmqSocket;

pub(crate) const ENDPOINT: &str = "inproc://zeromq.zap.01";
const VERSION: &[u8] = b"1.0";
const REQUEST_ID: &[u8] = b"1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HandlerKind {
    Rep,
    Router,
}

#[derive(Debug)]
struct Exchange {
    route_id: u64,
    request_id: Bytes,
    request: Vec<Bytes>,
    response: Mutex<Option<omq_tokio::AuthenticationResult>>,
    response_ready: Condvar,
}

impl Exchange {
    fn complete(&self, response: omq_tokio::AuthenticationResult) {
        if let Ok(mut slot) = self.response.lock() {
            *slot = Some(response);
            Condvar::notify_one(&self.response_ready);
        }
    }
}

#[derive(Debug)]
struct Delivery {
    exchange: Arc<Exchange>,
    frames: VecDeque<Bytes>,
}

#[derive(Debug)]
struct Handler {
    id: u64,
    socket: Weak<OmqSocket>,
    kind: HandlerKind,
    inbox: VecDeque<Arc<Exchange>>,
    receiving: Option<Delivery>,
    rep_reply_route: Option<u64>,
}

#[derive(Debug, Default)]
struct State {
    handler: Option<Handler>,
    exchanges: HashMap<u64, Arc<Exchange>>,
}

#[derive(Debug, Default)]
pub(crate) struct ZapService {
    next_route_id: AtomicU64,
    state: Mutex<State>,
}

impl ZapService {
    pub(crate) fn bind(&self, handler: &Arc<OmqSocket>) -> Result<(), i32> {
        let kind = match handler.socket_type {
            omq_tokio::SocketType::Rep => HandlerKind::Rep,
            omq_tokio::SocketType::Router => HandlerKind::Router,
            _ => return Err(libc::EINVAL),
        };
        if handler.bound_or_connected.load(Ordering::Acquire) {
            return Err(libc::EINVAL);
        }
        let mut state = self.state.lock().map_err(|_| ETERM)?;
        if state
            .handler
            .as_ref()
            .is_some_and(|bound| bound.socket.upgrade().is_some())
        {
            return Err(libc::EADDRINUSE);
        }
        state.handler = Some(Handler {
            id: handler.id,
            socket: Arc::downgrade(handler),
            kind,
            inbox: VecDeque::new(),
            receiving: None,
            rep_reply_route: None,
        });
        Ok(())
    }

    pub(crate) fn unbind(&self, handler_id: u64) {
        let exchanges = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if state.handler.as_ref().map(|handler| handler.id) != Some(handler_id) {
                return;
            }
            state.handler = None;
            state
                .exchanges
                .drain()
                .map(|(_, exchange)| exchange)
                .collect::<Vec<_>>()
        };
        for exchange in exchanges {
            exchange.complete(internal_error());
        }
    }

    pub(crate) fn shutdown(&self) {
        let handler_id = self
            .state
            .lock()
            .ok()
            .and_then(|state| state.handler.as_ref().map(|handler| handler.id));
        if let Some(handler_id) = handler_id {
            self.unbind(handler_id);
        }
    }

    pub(crate) fn authorize(
        &self,
        domain: &str,
        address: &str,
        identity: &[u8],
        peer: &omq_tokio::MechanismPeerInfo,
        timeout: Duration,
    ) -> omq_tokio::AuthenticationResult {
        let Ok(request) = build_request(domain, address, identity, peer) else {
            return internal_error();
        };
        let route_id = self.next_route_id.fetch_add(1, Ordering::Relaxed);
        let exchange = Arc::new(Exchange {
            route_id,
            request_id: Bytes::from_static(REQUEST_ID),
            request,
            response: Mutex::new(None),
            response_ready: Condvar::new(),
        });

        let handler = {
            let Ok(mut state) = self.state.lock() else {
                return internal_error();
            };
            let Some(handler) = state.handler.as_mut() else {
                return internal_error();
            };
            let Some(socket) = handler.socket.upgrade() else {
                return internal_error();
            };
            handler.inbox.push_back(Arc::clone(&exchange));
            socket.drain_nonempty.store(true, Ordering::Release);
            state.exchanges.insert(route_id, Arc::clone(&exchange));
            socket
        };
        handler.notify.signal_recv();

        let Ok(response) = exchange.response.lock() else {
            return internal_error();
        };
        let Ok((response, wait)) =
            exchange
                .response_ready
                .wait_timeout_while(response, timeout, |response| response.is_none())
        else {
            return internal_error();
        };
        if wait.timed_out() && response.is_none() {
            drop(response);
            self.cancel(route_id);
            return internal_error();
        }
        response.clone().unwrap_or_else(internal_error)
    }

    pub(crate) fn try_recv_frame(&self, handler_id: u64) -> Result<Option<(Bytes, bool)>, i32> {
        let mut state = self.state.lock().map_err(|_| ETERM)?;
        let handler = state
            .handler
            .as_mut()
            .filter(|handler| handler.id == handler_id)
            .ok_or(EFSM)?;
        if handler.kind == HandlerKind::Rep && handler.rep_reply_route.is_some() {
            return Err(EFSM);
        }
        ensure_delivery(handler);
        let Some(delivery) = handler.receiving.as_mut() else {
            update_readable(handler);
            return Ok(None);
        };
        let frame = delivery
            .frames
            .pop_front()
            .expect("ZAP delivery is non-empty");
        let more = !delivery.frames.is_empty();
        if !more {
            let route_id = delivery.exchange.route_id;
            handler.receiving = None;
            if handler.kind == HandlerKind::Rep {
                handler.rep_reply_route = Some(route_id);
            }
        }
        update_readable(handler);
        Ok(Some((frame, more)))
    }

    pub(crate) fn try_recv_message(&self, handler_id: u64) -> Result<Option<Vec<Bytes>>, i32> {
        let mut state = self.state.lock().map_err(|_| ETERM)?;
        let handler = state
            .handler
            .as_mut()
            .filter(|handler| handler.id == handler_id)
            .ok_or(EFSM)?;
        if handler.kind == HandlerKind::Rep && handler.rep_reply_route.is_some() {
            return Err(EFSM);
        }
        ensure_delivery(handler);
        let Some(delivery) = handler.receiving.take() else {
            update_readable(handler);
            return Ok(None);
        };
        let route_id = delivery.exchange.route_id;
        let frames = delivery.frames.into_iter().collect();
        if handler.kind == HandlerKind::Rep {
            handler.rep_reply_route = Some(route_id);
        }
        update_readable(handler);
        Ok(Some(frames))
    }

    pub(crate) fn respond(&self, handler_id: u64, response: &[Bytes]) -> Result<(), i32> {
        let (exchange, result, handler_socket, wake_handler) = {
            let mut state = self.state.lock().map_err(|_| ETERM)?;
            let handler = state
                .handler
                .as_mut()
                .filter(|handler| handler.id == handler_id)
                .ok_or(EFSM)?;
            let (route_id, frames) = match handler.kind {
                HandlerKind::Rep => {
                    let route_id = handler.rep_reply_route.take().ok_or(EFSM)?;
                    (route_id, Some(response))
                }
                HandlerKind::Router => {
                    let route_id = parse_router_response_route(response)?;
                    let body = response
                        .get(1)
                        .is_some_and(Bytes::is_empty)
                        .then(|| &response[2..]);
                    (route_id, body)
                }
            };
            let Some(exchange) = state.exchanges.remove(&route_id) else {
                return Err(libc::EHOSTUNREACH);
            };
            let result = frames
                .and_then(|frames| parse_response(frames, &exchange.request_id).ok())
                .unwrap_or_else(internal_error);
            let handler = state.handler.as_ref().expect("handler remains bound");
            let socket = handler.socket.upgrade();
            let wake = handler.kind == HandlerKind::Rep && !handler.inbox.is_empty();
            update_readable(handler);
            (exchange, result, socket, wake)
        };
        exchange.complete(result);
        if wake_handler && let Some(handler) = handler_socket {
            handler.notify.signal_recv();
        }
        Ok(())
    }

    pub(crate) fn can_send(&self, handler_id: u64) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.handler.as_ref().is_some_and(|handler| {
                handler.id == handler_id
                    && match handler.kind {
                        HandlerKind::Rep => handler.rep_reply_route.is_some(),
                        HandlerKind::Router => !state.exchanges.is_empty(),
                    }
            })
        })
    }

    pub(crate) fn has_more(&self, handler_id: u64) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.handler.as_ref().is_some_and(|handler| {
                handler.id == handler_id
                    && handler
                        .receiving
                        .as_ref()
                        .is_some_and(|delivery| !delivery.frames.is_empty())
            })
        })
    }

    pub(crate) fn has_input(&self, handler_id: u64) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.handler.as_ref().is_some_and(|handler| {
                handler.id == handler_id
                    && (handler.receiving.is_some()
                        || ((handler.kind == HandlerKind::Router
                            || handler.rep_reply_route.is_none())
                            && !handler.inbox.is_empty()))
            })
        })
    }

    fn cancel(&self, route_id: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.exchanges.remove(&route_id);
        let Some(handler) = state.handler.as_mut() else {
            return;
        };
        handler
            .inbox
            .retain(|exchange| exchange.route_id != route_id);
        if handler
            .receiving
            .as_ref()
            .is_some_and(|delivery| delivery.exchange.route_id == route_id)
        {
            handler.receiving = None;
        }
        if handler.rep_reply_route == Some(route_id) {
            handler.rep_reply_route = None;
        }
        update_readable(handler);
    }
}

fn ensure_delivery(handler: &mut Handler) {
    if handler.receiving.is_some() {
        return;
    }
    let Some(exchange) = handler.inbox.pop_front() else {
        return;
    };
    let mut frames = VecDeque::new();
    if handler.kind == HandlerKind::Router {
        frames.push_back(Bytes::copy_from_slice(&exchange.route_id.to_be_bytes()));
        frames.push_back(Bytes::new());
    }
    frames.extend(exchange.request.iter().cloned());
    handler.receiving = Some(Delivery { exchange, frames });
}

fn update_readable(handler: &Handler) {
    let readable = handler.receiving.is_some()
        || ((handler.kind == HandlerKind::Router || handler.rep_reply_route.is_none())
            && !handler.inbox.is_empty());
    if let Some(socket) = handler.socket.upgrade() {
        socket.drain_nonempty.store(readable, Ordering::Release);
    }
}

fn parse_router_response_route(response: &[Bytes]) -> Result<u64, i32> {
    if response
        .first()
        .is_none_or(|route| route.len() != size_of::<u64>())
    {
        return Err(libc::EINVAL);
    }
    let route_id = u64::from_be_bytes(
        response[0]
            .as_ref()
            .try_into()
            .expect("route length checked"),
    );
    Ok(route_id)
}

fn build_request(
    domain: &str,
    address: &str,
    identity: &[u8],
    peer: &omq_tokio::MechanismPeerInfo,
) -> Result<Vec<Bytes>, ()> {
    validate_ascii_string(domain.as_bytes(), false)?;
    let address: IpAddr = address.parse().map_err(|_| ())?;
    let address = address.to_string();
    validate_ascii_string(address.as_bytes(), false)?;
    if identity.len() > 255 {
        return Err(());
    }

    let (mechanism, credentials) = match peer.mechanism {
        omq_tokio::proto::MechanismName::NULL => (Bytes::from_static(b"NULL"), Vec::new()),
        omq_tokio::proto::MechanismName::PLAIN => {
            let username = peer.username.as_deref().ok_or(())?;
            let password = peer.password.as_deref().ok_or(())?;
            validate_vchar(username.as_bytes())?;
            validate_vchar(password.as_bytes())?;
            (
                Bytes::from_static(b"PLAIN"),
                vec![
                    Bytes::copy_from_slice(username.as_bytes()),
                    Bytes::copy_from_slice(password.as_bytes()),
                ],
            )
        }
        omq_tokio::proto::MechanismName::CURVE => (
            Bytes::from_static(b"CURVE"),
            vec![Bytes::copy_from_slice(&peer.public_key)],
        ),
        _ => return Err(()),
    };

    let mut request = vec![
        Bytes::from_static(VERSION),
        Bytes::from_static(REQUEST_ID),
        Bytes::copy_from_slice(domain.as_bytes()),
        Bytes::from(address),
        Bytes::copy_from_slice(identity),
        mechanism,
    ];
    request.extend(credentials);
    Ok(request)
}

fn parse_response(
    response: &[Bytes],
    request_id: &[u8],
) -> Result<omq_tokio::AuthenticationResult, ()> {
    if response.len() != 6 || response[0].as_ref() != VERSION || response[1].as_ref() != request_id
    {
        return Err(());
    }
    let status = match response[2].as_ref() {
        b"200" => omq_tokio::AuthenticationStatus::Success,
        b"300" => omq_tokio::AuthenticationStatus::TemporaryFailure,
        b"400" => omq_tokio::AuthenticationStatus::Denied,
        b"500" => omq_tokio::AuthenticationStatus::InternalError,
        _ => return Err(()),
    };
    validate_ascii_string(&response[3], true)?;
    validate_ascii_string(&response[4], true)?;
    if status != omq_tokio::AuthenticationStatus::Success
        && (!response[4].is_empty() || !response[5].is_empty())
    {
        return Err(());
    }
    let metadata = parse_metadata(&response[5])?;
    Ok(omq_tokio::AuthenticationResult {
        status,
        user_id: Some(response[4].clone()),
        metadata,
    })
}

fn parse_metadata(mut metadata: &[u8]) -> Result<Vec<(String, Bytes)>, ()> {
    let mut properties = Vec::new();
    while !metadata.is_empty() {
        let name_len = usize::from(metadata[0]);
        if name_len == 0 || metadata.len() < 1 + name_len + 4 {
            return Err(());
        }
        let name = &metadata[1..=name_len];
        if !name
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'+'))
        {
            return Err(());
        }
        let value_len = u32::from_be_bytes(
            metadata[1 + name_len..1 + name_len + 4]
                .try_into()
                .expect("four-byte length"),
        ) as usize;
        let value_start = 1 + name_len + 4;
        let value_end = value_start.checked_add(value_len).ok_or(())?;
        if metadata.len() < value_end {
            return Err(());
        }
        let name = String::from_utf8(name.to_vec()).expect("validated ASCII property name");
        properties.push((
            name,
            Bytes::copy_from_slice(&metadata[value_start..value_end]),
        ));
        metadata = &metadata[value_end..];
    }
    Ok(properties)
}

fn validate_ascii_string(value: &[u8], allow_empty: bool) -> Result<(), ()> {
    if value.len() > 255 || (!allow_empty && value.is_empty()) || !value.is_ascii() {
        return Err(());
    }
    Ok(())
}

fn validate_vchar(value: &[u8]) -> Result<(), ()> {
    if value.len() > 255 || !value.iter().all(u8::is_ascii_graphic) {
        return Err(());
    }
    Ok(())
}

fn internal_error() -> omq_tokio::AuthenticationResult {
    omq_tokio::AuthenticationResult {
        status: omq_tokio::AuthenticationStatus::InternalError,
        user_id: None,
        metadata: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(mechanism: omq_tokio::proto::MechanismName) -> omq_tokio::MechanismPeerInfo {
        omq_tokio::MechanismPeerInfo {
            mechanism,
            public_key: [7; 32],
            identity: None,
            peer_address: Some("127.0.0.1".into()),
            username: None,
            password: None,
        }
    }

    fn metadata(name: &[u8], value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(name.len() as u8);
        out.extend_from_slice(name);
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
        out
    }

    #[test]
    fn response_accepts_all_standard_statuses() {
        for (wire, expected) in [
            (b"200".as_slice(), omq_tokio::AuthenticationStatus::Success),
            (
                b"300".as_slice(),
                omq_tokio::AuthenticationStatus::TemporaryFailure,
            ),
            (b"400".as_slice(), omq_tokio::AuthenticationStatus::Denied),
            (
                b"500".as_slice(),
                omq_tokio::AuthenticationStatus::InternalError,
            ),
        ] {
            let frames = [b"1.0".as_slice(), b"1", wire, b"", b"", b""].map(Bytes::copy_from_slice);
            assert_eq!(parse_response(&frames, b"1").unwrap().status, expected);
        }
    }

    #[test]
    fn response_rejects_bad_envelope_and_strings() {
        for frames in [
            vec![b"1.0".as_slice(), b"1", b"200", b"", b""],
            vec![b"1.0".as_slice(), b"1", b"200", b"", b"", b"", b"extra"],
            vec![b"1.1".as_slice(), b"1", b"200", b"", b"", b""],
            vec![b"1.0".as_slice(), b"2", b"200", b"", b"", b""],
            vec![b"1.0".as_slice(), b"1", b"201", b"", b"", b""],
            vec![b"1.0".as_slice(), b"1", b"200", b"\x80", b"", b""],
            vec![b"1.0".as_slice(), b"1", b"200", b"", b"\x80", b""],
            vec![b"1.0".as_slice(), b"1", b"400", b"", b"user", b""],
            vec![b"1.0".as_slice(), b"1", b"500", b"", b"", b"metadata"],
        ] {
            let frames: Vec<Bytes> = frames.into_iter().map(Bytes::copy_from_slice).collect();
            assert!(parse_response(&frames, b"1").is_err());
        }
        let long = Bytes::from(vec![b'x'; 256]);
        let frames = [
            Bytes::from_static(b"1.0"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"200"),
            long,
            Bytes::new(),
            Bytes::new(),
        ];
        assert!(parse_response(&frames, b"1").is_err());
        let frames = [
            Bytes::from_static(b"1.0"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"200"),
            Bytes::new(),
            Bytes::from(vec![b'x'; 256]),
            Bytes::new(),
        ];
        assert!(parse_response(&frames, b"1").is_err());
    }

    #[test]
    fn response_parses_user_id_and_metadata() {
        let encoded = metadata(b"Role", b"admin");
        let frames = [
            Bytes::from_static(b"1.0"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"200"),
            Bytes::from_static(b"OK"),
            Bytes::from_static(b"alice"),
            Bytes::from(encoded),
        ];
        let result = parse_response(&frames, b"1").unwrap();
        assert_eq!(result.user_id.as_deref(), Some(b"alice".as_slice()));
        assert_eq!(
            result.metadata,
            vec![("Role".into(), Bytes::from_static(b"admin"))]
        );
    }

    #[test]
    fn metadata_rejects_empty_invalid_and_truncated_properties() {
        assert!(parse_metadata(&[0]).is_err());
        assert!(parse_metadata(&metadata(b"bad name", b"x")).is_err());
        assert!(parse_metadata(&[1, b'X', 0, 0, 0, 2, b'x']).is_err());
    }

    #[test]
    fn request_shape_matches_each_standard_mechanism() {
        let null = build_request(
            "global",
            "127.0.0.1",
            b"server",
            &peer(omq_tokio::proto::MechanismName::NULL),
        )
        .unwrap();
        assert_eq!(null.len(), 6);
        assert_eq!(null[5], b"NULL".as_slice());

        let mut plain_peer = peer(omq_tokio::proto::MechanismName::PLAIN);
        plain_peer.username = Some("alice".into());
        plain_peer.password = Some("secret".into());
        let plain = build_request("global", "::1", b"", &plain_peer).unwrap();
        assert_eq!(plain.len(), 8);
        assert_eq!(plain[3], b"::1".as_slice());
        assert_eq!(plain[6], b"alice".as_slice());
        assert_eq!(plain[7], b"secret".as_slice());

        let curve = build_request(
            "global",
            "127.0.0.1",
            b"",
            &peer(omq_tokio::proto::MechanismName::CURVE),
        )
        .unwrap();
        assert_eq!(curve.len(), 7);
        assert_eq!(curve[6].as_ref(), &[7; 32]);
    }

    #[test]
    fn request_rejects_invalid_rfc_string_fields() {
        let null = peer(omq_tokio::proto::MechanismName::NULL);
        assert!(build_request("", "127.0.0.1", b"", &null).is_err());
        let non_ascii_domain =
            String::from_utf8(vec![b'g', b'l', 0xc3, 0xb6, b'b', b'a', b'l']).expect("valid UTF-8");
        assert!(build_request(&non_ascii_domain, "127.0.0.1", b"", &null).is_err());
        assert!(build_request("global", "localhost", b"", &null).is_err());
        assert!(build_request("global", "127.0.0.1", &[0; 256], &null).is_err());

        let mut plain = peer(omq_tokio::proto::MechanismName::PLAIN);
        plain.username = Some("has space".into());
        plain.password = Some("secret".into());
        assert!(build_request("global", "127.0.0.1", b"", &plain).is_err());
    }

    #[test]
    fn router_response_route_is_independent_of_the_req_delimiter() {
        let route = 42u64.to_be_bytes();
        let valid = [Bytes::copy_from_slice(&route), Bytes::new()];
        assert_eq!(parse_router_response_route(&valid).unwrap(), 42);
        assert_eq!(parse_router_response_route(&valid[..1]).unwrap(), 42);
        assert!(parse_router_response_route(&[Bytes::new()]).is_err());
    }
}
