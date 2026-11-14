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

/// Handshake facts sent to the ZAP handler in one request.
#[derive(Debug)]
pub(crate) struct ZapQuery<'a> {
    pub domain: &'a str,
    pub address: &'a str,
    pub identity: &'a [u8],
    pub peer: &'a omq_tokio::MechanismPeerInfo,
}

impl ZapService {
    pub(crate) fn bind(&self, handler: &Arc<OmqSocket>) -> Result<(), i32> {
        let kind = match handler.socket_type {
            omq_tokio::SocketType::Rep => HandlerKind::Rep,
            omq_tokio::SocketType::Router => HandlerKind::Router,
            _ => return Err(libc::EINVAL),
        };

        if handler.endpoints.bound_or_connected.load(Ordering::Acquire) {
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

    /// Ask the bound ZAP handler to authorize one handshake, blocking the
    /// handshake task until it replies or `timeout` passes.
    pub(crate) fn authorize(
        &self,
        query: &ZapQuery<'_>,
        timeout: Duration,
    ) -> omq_tokio::AuthenticationResult {
        let Ok(request) = build_request(query.domain, query.address, query.identity, query.peer)
        else {
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
            socket.recv.drain_nonempty.store(true, Ordering::Release);
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
        socket
            .recv
            .drain_nonempty
            .store(readable, Ordering::Release);
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
    // The address frame carries an IP address. A peer without one, such as
    // an IPC peer, gets an empty frame, as in libzmq.
    let address = address
        .parse::<IpAddr>()
        .map(|address| address.to_string())
        .unwrap_or_default();

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
mod tests;
