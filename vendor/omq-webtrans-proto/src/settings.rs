//! HTTP/3 SETTINGS frame helpers for WebTransport.

use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use bytes::{Buf, BufMut, BytesMut};

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Frame, UniStream, VarInt, VarIntUnexpectedEnd};
use crate::grease::is_grease_value;
const MAX_SETTINGS_FRAME_SIZE: usize = 16 * 1024;

/// WebTransport over HTTP/3 wire version selected for one connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebTransportVersion {
    /// Draft-02, used by current Chromium and Firefox releases.
    Draft02,
    /// Draft-07, used by Safari 26.
    Draft07,
    /// Draft-16, requiring reliable QUIC stream resets.
    Draft16,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
/// HTTP/3 SETTINGS identifier.
pub struct Setting(pub VarInt);

impl Setting {
    /// Decode a settings identifier.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, VarIntUnexpectedEnd> {
        Ok(Setting(VarInt::decode(buf)?))
    }

    /// Encode a settings identifier.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.0.encode(buf)
    }

    /// Return the encoded size of this identifier.
    pub fn size(&self) -> usize {
        self.0.size()
    }

    // Reference: https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.4.1
    /// Return `true` when the setting uses RFC 9114 GREASE spacing.
    pub fn is_grease(&self) -> bool {
        is_grease_value(self.0.into_inner())
    }
}

impl Debug for Setting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Setting::QPACK_MAX_TABLE_CAPACITY => write!(f, "QPACK_MAX_TABLE_CAPACITY"),
            Setting::MAX_FIELD_SECTION_SIZE => write!(f, "MAX_FIELD_SECTION_SIZE"),
            Setting::QPACK_BLOCKED_STREAMS => write!(f, "QPACK_BLOCKED_STREAMS"),
            Setting::ENABLE_CONNECT_PROTOCOL => write!(f, "ENABLE_CONNECT_PROTOCOL"),
            Setting::ENABLE_DATAGRAM => write!(f, "ENABLE_DATAGRAM"),
            Setting::WT_ENABLED => write!(f, "WT_ENABLED"),
            Setting::LEGACY_WT_ENABLED => write!(f, "LEGACY_WT_ENABLED"),
            Setting::WEBTRANSPORT_MAX_SESSIONS => write!(f, "WEBTRANSPORT_MAX_SESSIONS"),
            x if x.is_grease() => write!(f, "GREASE SETTING [{:x?}]", x.0.into_inner()),
            x => write!(f, "UNKNOWN_SETTING [{:x?}]", x.0.into_inner()),
        }
    }
}

impl Setting {
    /// Build a settings identifier from a known `u32` value.
    pub const fn from_u32(value: u32) -> Self {
        Self(VarInt::from_u32(value))
    }

    // HTTP/3 settings that WebTransport ignores.
    /// HTTP/3 QPACK dynamic table capacity setting.
    pub const QPACK_MAX_TABLE_CAPACITY: Setting = Setting::from_u32(0x1); // Default is 0, which disables the dynamic table.
    /// HTTP/3 maximum header field section size setting.
    pub const MAX_FIELD_SECTION_SIZE: Setting = Setting::from_u32(0x6);
    /// HTTP/3 QPACK blocked streams setting.
    pub const QPACK_BLOCKED_STREAMS: Setting = Setting::from_u32(0x7);

    // Both values are required for WebTransport.
    /// HTTP/3 extended CONNECT enable flag.
    pub const ENABLE_CONNECT_PROTOCOL: Setting = Setting::from_u32(0x8);
    /// HTTP/3 datagram support flag (current).
    pub const ENABLE_DATAGRAM: Setting = Setting::from_u32(0x33);
    /// WebTransport over HTTP/3 draft-16 enable flag.
    pub const WT_ENABLED: Setting = Setting::from_u32(0x2c7cf000);
    /// WebTransport over HTTP/3 draft-02 enable flag.
    pub const LEGACY_WT_ENABLED: Setting = Setting::from_u32(0x2b603742);
    /// WebTransport over HTTP/3 draft-07 session limit; nonzero enables it.
    pub const WEBTRANSPORT_MAX_SESSIONS: Setting = Setting::from_u32(0xc671706a);
}

#[derive(Error, Debug, Clone)]
/// Errors returned while encoding or decoding SETTINGS exchanges.
pub enum SettingsError {
    /// Input ended before a full SETTINGS payload was available.
    #[error("unexpected end of input")]
    UnexpectedEnd,

    /// First unidirectional stream type was not the expected control stream.
    #[error("unexpected stream type {0:?}")]
    UnexpectedStreamType(UniStream),

    /// Frame type was not SETTINGS.
    #[error("unexpected frame {0:?}")]
    UnexpectedFrame(Frame),

    /// Invalid varint or truncated settings pair inside a frame payload.
    #[error("invalid size")]
    InvalidSize,

    /// A SETTINGS identifier occurred more than once in the same frame.
    #[error("duplicate setting {0:?}")]
    DuplicateSetting(Setting),

    /// A boolean SETTINGS value was neither zero nor one.
    #[error("invalid value {1} for setting {0:?}")]
    InvalidValue(Setting, VarInt),

    /// SETTINGS frame exceeded the implementation's bounded decode limit.
    #[error("SETTINGS frame exceeds the 16 KiB implementation limit")]
    MessageTooLong,

    /// I/O error while reading or writing the SETTINGS exchange.
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for SettingsError {
    fn from(err: std::io::Error) -> Self {
        SettingsError::Io(Arc::new(err))
    }
}

// A map of SETTINGS identifiers to values.
#[derive(Default, Debug)]
/// Parsed HTTP/3 settings map keyed by [`Setting`].
pub struct Settings(HashMap<Setting, VarInt>);

impl Settings {
    /// Decode a control stream prefix and SETTINGS frame from an in-memory buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, SettingsError> {
        let typ = UniStream::decode(buf).map_err(|_| SettingsError::UnexpectedEnd)?;
        if typ != UniStream::CONTROL {
            return Err(SettingsError::UnexpectedStreamType(typ));
        }

        let (typ, mut data) = Frame::read(buf).map_err(|_| SettingsError::UnexpectedEnd)?;
        if typ != Frame::SETTINGS {
            return Err(SettingsError::UnexpectedFrame(typ));
        }

        let mut settings = Settings::default();
        while data.has_remaining() {
            // Use InvalidSize because retrying will not help.
            let id = Setting::decode(&mut data).map_err(|_| SettingsError::InvalidSize)?;
            let value = VarInt::decode(&mut data).map_err(|_| SettingsError::InvalidSize)?;
            // Only retain non-GREASE entries.
            if !id.is_grease() {
                if settings.0.contains_key(&id) {
                    return Err(SettingsError::DuplicateSetting(id));
                }
                if matches!(
                    id,
                    Setting::ENABLE_CONNECT_PROTOCOL
                        | Setting::ENABLE_DATAGRAM
                        | Setting::WT_ENABLED
                        | Setting::LEGACY_WT_ENABLED
                ) && value.into_inner() > 1
                {
                    return Err(SettingsError::InvalidValue(id, value));
                }
                settings.0.insert(id, value);
            }
        }

        Ok(settings)
    }

    /// Read and decode one SETTINGS exchange from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, SettingsError> {
        let stream_type = VarInt::read(stream)
            .await
            .map_err(|_| SettingsError::UnexpectedEnd)?;
        let stream_type = UniStream(stream_type);
        if stream_type != UniStream::CONTROL {
            return Err(SettingsError::UnexpectedStreamType(stream_type));
        }

        loop {
            let typ = VarInt::read(stream)
                .await
                .map_err(|_| SettingsError::UnexpectedEnd)?;
            let length = VarInt::read(stream)
                .await
                .map_err(|_| SettingsError::UnexpectedEnd)?;
            let length =
                usize::try_from(length.into_inner()).map_err(|_| SettingsError::MessageTooLong)?;
            if length > MAX_SETTINGS_FRAME_SIZE {
                return Err(SettingsError::MessageTooLong);
            }

            let mut payload = vec![0; length];
            stream.read_exact(&mut payload).await?;
            let typ = Frame(typ);
            if typ.is_grease() {
                continue;
            }

            let mut frame =
                Vec::with_capacity(stream_type.0.size() + typ.0.size() + VarInt::MAX_SIZE + length);
            stream_type.encode(&mut frame);
            typ.encode(&mut frame);
            VarInt::try_from(length)
                .map_err(|_| SettingsError::MessageTooLong)?
                .encode(&mut frame);
            frame.extend_from_slice(&payload);
            return Self::decode(&mut frame.as_slice());
        }
    }

    /// Encode this settings map as a control stream prefix followed by a SETTINGS frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        UniStream::CONTROL.encode(buf);
        Frame::SETTINGS.encode(buf);

        let payload_len = self.payload_len();
        VarInt::try_from(payload_len as u64)
            .expect("settings payload length exceeds VarInt bounds")
            .encode(buf);

        for (id, value) in &self.0 {
            id.encode(buf);
            value.encode(buf);
        }
    }

    /// Encode and write this settings map to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), SettingsError> {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut buf);
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }

    /// Advertise every supported WebTransport over HTTP/3 draft version.
    /// Draft-16 needs reliable stream resets, so `draft16` says whether the
    /// QUIC stack provides them.
    pub fn enable_webtransport(&mut self, draft16: bool) {
        self.insert(Setting::ENABLE_CONNECT_PROTOCOL, VarInt::from_u32(1));
        self.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));
        if draft16 {
            self.insert(Setting::WT_ENABLED, VarInt::from_u32(1));
        }
        self.insert(Setting::LEGACY_WT_ENABLED, VarInt::from_u32(1));
        self.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, VarInt::from_u32(1));
    }

    /// Return whether server SETTINGS advertise a supported WebTransport draft.
    pub fn supports_webtransport_server(&self) -> bool {
        self.negotiated_version(true, true).is_some()
    }

    /// Return whether client SETTINGS advertise a supported WebTransport draft.
    pub fn supports_webtransport_client(&self) -> bool {
        self.negotiated_version(false, true).is_some()
    }

    /// Select the draft after the QUIC handshake.
    /// Draft-16 requires reliable stream resets; draft-02 and draft-07 do not.
    /// Draft-02 wins over draft-07, keeping the handshake Chromium and Firefox use.
    pub fn negotiated_version(
        &self,
        peer_is_server: bool,
        peer_reset_stream_at: bool,
    ) -> Option<WebTransportVersion> {
        let enabled = |id| self.get(&id).map(|v| v.into_inner()) == Some(1);
        if !enabled(Setting::ENABLE_DATAGRAM) {
            return None;
        }
        if peer_reset_stream_at
            && enabled(Setting::WT_ENABLED)
            && (!peer_is_server || enabled(Setting::ENABLE_CONNECT_PROTOCOL))
        {
            return Some(WebTransportVersion::Draft16);
        }
        if enabled(Setting::LEGACY_WT_ENABLED) {
            return Some(WebTransportVersion::Draft02);
        }
        let max_sessions = self
            .get(&Setting::WEBTRANSPORT_MAX_SESSIONS)
            .map_or(0, |v| v.into_inner());
        if max_sessions > 0 && (!peer_is_server || enabled(Setting::ENABLE_CONNECT_PROTOCOL)) {
            return Some(WebTransportVersion::Draft07);
        }
        None
    }

    fn payload_len(&self) -> usize {
        self.0
            .iter()
            .map(|(id, value)| id.size() + value.size())
            .sum()
    }

    fn encoded_len(&self) -> usize {
        let payload_len = self.payload_len();
        UniStream::CONTROL.0.size()
            + Frame::SETTINGS.0.size()
            + VarInt::try_from(payload_len as u64)
                .expect("settings payload length exceeds VarInt bounds")
                .size()
            + payload_len
    }
}

impl Deref for Settings {
    type Target = HashMap<Setting, VarInt>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Settings {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_settings_enable_webtransport() {
        let mut settings = Settings::default();
        settings.enable_webtransport(true);

        assert!(settings.supports_webtransport_server());
        assert_eq!(
            settings.get(&Setting::WT_ENABLED),
            Some(&VarInt::from_u32(1))
        );
        assert_eq!(
            settings.get(&Setting::LEGACY_WT_ENABLED),
            Some(&VarInt::from_u32(1))
        );
        assert_eq!(
            settings.get(&Setting::WEBTRANSPORT_MAX_SESSIONS),
            Some(&VarInt::from_u32(1))
        );
    }

    #[test]
    fn settings_without_reliable_reset_omit_draft16() {
        let mut settings = Settings::default();
        settings.enable_webtransport(false);

        assert_eq!(settings.get(&Setting::WT_ENABLED), None);
        assert_eq!(
            settings.negotiated_version(true, true),
            Some(WebTransportVersion::Draft02)
        );
    }

    #[test]
    fn negotiates_legacy_without_reliable_reset() {
        let mut settings = Settings::default();
        settings.insert(Setting::LEGACY_WT_ENABLED, VarInt::from_u32(1));
        settings.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));
        assert_eq!(
            settings.negotiated_version(false, false),
            Some(WebTransportVersion::Draft02)
        );
        settings.insert(Setting::WT_ENABLED, VarInt::from_u32(1));
        assert_eq!(
            settings.negotiated_version(false, false),
            Some(WebTransportVersion::Draft02)
        );
        assert_eq!(
            settings.negotiated_version(false, true),
            Some(WebTransportVersion::Draft16)
        );
        settings.remove(&Setting::LEGACY_WT_ENABLED);
        assert_eq!(settings.negotiated_version(false, false), None);
    }

    #[test]
    fn negotiates_draft07_for_safari() {
        // Safari 26 offers draft-07 and draft-13 without reset_stream_at
        let mut settings = Settings::default();
        settings.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));
        settings.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, VarInt::from_u32(100));
        settings.insert(Setting::from_u32(0x14e9cd29), VarInt::from_u32(100));
        assert_eq!(
            settings.negotiated_version(false, false),
            Some(WebTransportVersion::Draft07)
        );

        settings.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, VarInt::from_u32(0));
        assert_eq!(settings.negotiated_version(false, false), None);
    }

    #[test]
    fn prefers_draft02_over_draft07() {
        let mut settings = Settings::default();
        settings.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));
        settings.insert(Setting::LEGACY_WT_ENABLED, VarInt::from_u32(1));
        settings.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, VarInt::from_u32(1));
        assert_eq!(
            settings.negotiated_version(false, false),
            Some(WebTransportVersion::Draft02)
        );
    }

    #[test]
    fn rejects_duplicate_settings() {
        let mut data = Vec::new();
        UniStream::CONTROL.encode(&mut data);
        Frame::SETTINGS.encode(&mut data);
        VarInt::from_u32(4).encode(&mut data);
        Setting::ENABLE_DATAGRAM.encode(&mut data);
        VarInt::from_u32(1).encode(&mut data);
        Setting::ENABLE_DATAGRAM.encode(&mut data);
        VarInt::from_u32(1).encode(&mut data);

        let error = Settings::decode(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            error,
            SettingsError::DuplicateSetting(Setting::ENABLE_DATAGRAM)
        ));
    }

    #[test]
    fn rejects_non_boolean_enabled_value() {
        let mut data = Vec::new();
        UniStream::CONTROL.encode(&mut data);
        Frame::SETTINGS.encode(&mut data);
        VarInt::from_u32(5).encode(&mut data);
        Setting::WT_ENABLED.encode(&mut data);
        VarInt::from_u32(2).encode(&mut data);

        let error = Settings::decode(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            error,
            SettingsError::InvalidValue(Setting::WT_ENABLED, _)
        ));
    }

    #[test]
    fn server_settings_require_extended_connect() {
        let mut settings = Settings::default();
        settings.insert(Setting::WT_ENABLED, VarInt::from_u32(1));
        settings.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));

        assert!(settings.supports_webtransport_client());
        assert!(!settings.supports_webtransport_server());

        settings.insert(Setting::ENABLE_CONNECT_PROTOCOL, VarInt::from_u32(1));
        assert!(settings.supports_webtransport_server());

        let mut settings = Settings::default();
        settings.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, VarInt::from_u32(1));
        settings.insert(Setting::ENABLE_DATAGRAM, VarInt::from_u32(1));
        assert!(settings.supports_webtransport_client());
        assert!(!settings.supports_webtransport_server());

        settings.insert(Setting::ENABLE_CONNECT_PROTOCOL, VarInt::from_u32(1));
        assert!(settings.supports_webtransport_server());
    }
}
