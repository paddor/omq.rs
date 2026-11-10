//! Concrete wire framing, independent of carrier IO and message transforms.

#[cfg(feature = "ws")]
use omq_proto::proto::connection::WsRole;

/// Resolve once during connection materialization. A ZWS role determines
/// both codec framing and transmit-slot masking; these cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireFraming {
    Zmtp,
    #[cfg(feature = "ws")]
    Zws(WsRole),
}

impl WireFraming {
    pub(crate) fn is_ws(self) -> bool {
        self != Self::Zmtp
    }

    #[cfg(feature = "ws")]
    pub(crate) fn ws_role(self) -> Option<WsRole> {
        match self {
            Self::Zmtp => None,
            Self::Zws(role) => Some(role),
        }
    }

    /// Role for outgoing ZWS frames; ZMTP framing never frames WS.
    #[cfg(feature = "ws")]
    #[inline]
    pub(crate) fn ws_send_role(self) -> WsRole {
        match self {
            Self::Zws(role) => role,
            Self::Zmtp => WsRole::Server,
        }
    }
}

#[cfg(test)]
mod tests;
