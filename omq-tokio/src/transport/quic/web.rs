//! WebTransport session setup on a `quic://` listener.
//!
//! HTTP/3 SETTINGS, extended CONNECT, and stream association belong to
//! `omq-webtrans-quinn`. OMQ checks the CONNECT resource and browser
//! Origin, then expects exactly two client bidirectional streams. Each starts
//! with an 8-byte role preface because WebTransport stream IDs are not
//! fixed:
//!
//! ```text
//! data:     'O' 'M' 'Q' 'D' 0x01 0x00 0x00 0x00, then unchanged ZMTP
//! liveness: 'O' 'M' 'Q' 'L' 0x01 0x00 0x00 0x00, then liveness records
//! ```
//!
//! Either stream may arrive first. The listener writes only the liveness
//! preface; its data stream starts directly with ZMTP.

use omq_proto::options::QuicOptions;
use omq_proto::proto::web_address::normalize_ws_origin;
use omq_proto::{Error, Result};
use omq_webtrans_quinn::http::StatusCode;

use super::carrier::{Carrier, RecvHalf, SendHalf};
use super::{Roles, liveness};

pub(super) const DATA_PREFACE: [u8; 8] = [b'O', b'M', b'Q', b'D', 1, 0, 0, 0];

fn failed(reason: impl Into<String>) -> Error {
    Error::HandshakeFailed(reason.into())
}

/// Resource and Origin policy of one `quic://` listener.
#[derive(Debug)]
pub(super) struct Policy {
    resource: String,
    allowed_origins: Vec<String>,
}

impl Policy {
    pub(super) fn new(resource: &str, options: &QuicOptions) -> Result<Self> {
        Ok(Self {
            resource: resource.to_owned(),
            allowed_origins: options
                .allowed_origins
                .iter()
                .map(|origin| normalize_ws_origin(origin))
                .collect::<Result<_>>()?,
        })
    }

    fn check(&self, request: &omq_webtrans_quinn::Request) -> Result<(), StatusCode> {
        let url = request.url();
        let resource = match url.query() {
            Some(query) => format!("{}?{query}", url.path()),
            None => url.path().to_owned(),
        };
        if resource != self.resource {
            return Err(StatusCode::NOT_FOUND);
        }
        // Native clients may omit Origin; a present Origin must be allowed.
        if let Some(origin) = request.origin() {
            let allowed = normalize_ws_origin(origin)
                .is_ok_and(|origin| self.allowed_origins.contains(&origin));
            if !allowed {
                return Err(StatusCode::FORBIDDEN);
            }
        }
        Ok(())
    }
}

/// Complete HTTP/3 CONNECT and classify the two OMQ streams by preface.
pub(super) async fn accept_roles(connection: quinn::Connection, policy: &Policy) -> Result<Roles> {
    let request = omq_webtrans_quinn::Request::accept(connection)
        .await
        .map_err(|e| failed(format!("WebTransport CONNECT: {e:?}")))?;
    if let Err(status) = policy.check(&request) {
        let _ = request.close(status).await;
        return Err(failed(format!("WebTransport request rejected: {status}")));
    }
    let session = request
        .ok()
        .await
        .map_err(|e| failed(format!("WebTransport response: {e}")))?;
    let roles = match classify_streams(&session).await {
        Ok((data, mut liveness)) => liveness::write_preface(&mut liveness.0)
            .await
            .map(|()| (data, liveness)),
        Err(e) => Err(e),
    };
    if roles.is_err() {
        session.close(super::code::SETUP_ERROR, b"");
    }
    let (data, liveness) = roles?;
    Ok(Roles {
        carrier: Carrier::Web(Box::new(session)),
        data,
        liveness,
    })
}

type Pair = (SendHalf, RecvHalf);

async fn classify_streams(session: &omq_webtrans_quinn::Session) -> Result<(Pair, Pair)> {
    let mut data = None;
    let mut control = None;
    while data.is_none() || control.is_none() {
        let (send, recv) = session
            .accept_bi()
            .await
            .map_err(|e| failed(format!("WebTransport stream: {e}")))?;
        let mut recv = RecvHalf::Web(recv);
        let mut preface = [0; 8];
        recv.read_exact(&mut preface).await?;
        let slot = match preface {
            DATA_PREFACE => &mut data,
            liveness::PREFACE => &mut control,
            _ => return Err(failed("invalid WebTransport role preface")),
        };
        if slot.replace((SendHalf::Web(send), recv)).is_some() {
            return Err(failed("duplicate WebTransport stream role"));
        }
    }
    Ok((data.expect("data role"), control.expect("liveness role")))
}
