//! Backend-neutral handle-frame eligibility policy.

use bytes::Bytes;

use crate::message::Message;

/// Admission limits for direct framing into a peer's transmit queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleFrameCaps {
    /// Maximum queued encoded bytes before admission is suspended.
    pub byte_cap: usize,
    /// Maximum queued messages before admission is suspended.
    pub message_cap: usize,
}

/// Connection state used to select an eligible framing path.
#[derive(Debug, Clone, Copy)]
#[expect(clippy::struct_excessive_bools)]
pub struct HandleFrameState<'a> {
    /// Whether frame encryption is active.
    pub uses_crypto: bool,
    /// Whether the security and protocol handshakes are complete.
    pub handshake_done: bool,
    /// Whether a compression transform is installed.
    pub has_transform: bool,
    /// Plaintext sentinel and exclusive per-part passthrough threshold.
    pub transform_passthrough: Option<&'a (Bytes, usize)>,
    /// Whether the connection uses WebSocket framing.
    pub is_ws: bool,
    /// Encoded bytes already queued for transmission.
    pub queued_bytes: usize,
    /// Messages already queued for transmission.
    pub queued_messages: usize,
}

/// Selected framing path or reason direct framing cannot proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleFrameDecision<'a> {
    /// Encode ordinary ZMTP frames.
    Plain,
    /// Encode ZWS frames without a compression transform.
    WebSocket,
    /// Frame plaintext parts with a transform passthrough sentinel.
    TransformPassthrough {
        /// Prefix selecting plaintext decoding at the receiver.
        sentinel: &'a Bytes,
    },
    /// The transmit queue has reached an admission limit.
    Full,
    /// Handshake, encryption, or transformation requires the codec path.
    Ineligible,
}

/// Select a framing path from connection state, admission limits, and message size.
pub fn decide_handle_frame<'a>(
    state: HandleFrameState<'a>,
    caps: HandleFrameCaps,
    msg: &Message,
) -> HandleFrameDecision<'a> {
    if state.uses_crypto || !state.handshake_done {
        return HandleFrameDecision::Ineligible;
    }
    if state.queued_bytes >= caps.byte_cap || state.queued_messages >= caps.message_cap {
        return HandleFrameDecision::Full;
    }
    if state.is_ws {
        if state.has_transform {
            return HandleFrameDecision::Ineligible;
        }
        return HandleFrameDecision::WebSocket;
    }
    if !state.has_transform {
        return HandleFrameDecision::Plain;
    }
    if let Some((sentinel, threshold)) = state.transform_passthrough
        && msg.iter().all(|part| part.len() < *threshold)
    {
        return HandleFrameDecision::TransformPassthrough { sentinel };
    }
    HandleFrameDecision::Ineligible
}

/// Whether plain pre-encoded bytes may enter the transmit queue.
pub fn can_push_pre_framed(state: HandleFrameState<'_>, caps: HandleFrameCaps) -> bool {
    !state.uses_crypto
        && state.handshake_done
        && !state.has_transform
        && !state.is_ws
        && state.queued_bytes < caps.byte_cap
        && state.queued_messages < caps.message_cap
}
