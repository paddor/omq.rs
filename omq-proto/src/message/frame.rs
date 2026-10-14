use super::Payload;

/// ZMTP frame flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrameFlags {
    /// MORE: more frames follow in this message.
    pub more: bool,
    /// COMMAND: frame is a ZMTP command, not application data.
    pub command: bool,
}

impl FrameFlags {
    /// Last data frame in a message.
    pub const LAST: Self = Self {
        more: false,
        command: false,
    };
    /// Intermediate data frame (more frames follow).
    pub const MORE: Self = Self {
        more: true,
        command: false,
    };
    /// A ZMTP command frame (terminal by definition; MORE is not allowed with COMMAND).
    pub const COMMAND: Self = Self {
        more: false,
        command: true,
    };
}

/// A single ZMTP frame on the wire.
#[derive(Clone, Debug, Default)]
pub struct Frame {
    /// Wire flags (MORE, COMMAND).
    pub flags: FrameFlags,
    /// Frame body.
    pub payload: Payload,
}

impl Frame {
    /// Create a frame with explicit flags.
    pub fn new(payload: impl Into<Payload>, flags: FrameFlags) -> Self {
        Self {
            flags,
            payload: payload.into(),
        }
    }

    /// Create a data frame. `more = true` sets the MORE flag.
    pub fn data(payload: impl Into<Payload>, more: bool) -> Self {
        let flags = if more {
            FrameFlags::MORE
        } else {
            FrameFlags::LAST
        };
        Self::new(payload, flags)
    }

    /// Create a COMMAND frame.
    pub fn command(payload: impl Into<Payload>) -> Self {
        Self::new(payload, FrameFlags::COMMAND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn frame_flags_consts() {
        assert_eq!(
            FrameFlags::LAST,
            FrameFlags {
                more: false,
                command: false
            }
        );
        assert_eq!(
            FrameFlags::MORE,
            FrameFlags {
                more: true,
                command: false
            }
        );
        assert_eq!(
            FrameFlags::COMMAND,
            FrameFlags {
                more: false,
                command: true
            }
        );
    }

    #[test]
    fn frame_constructors() {
        let f = Frame::data(Bytes::from_static(b"x"), false);
        assert_eq!(f.flags, FrameFlags::LAST);
        assert_eq!(f.payload.len(), 1);

        let f = Frame::data(Bytes::from_static(b"x"), true);
        assert_eq!(f.flags, FrameFlags::MORE);

        let f = Frame::command(Bytes::from_static(b"READY"));
        assert_eq!(f.flags, FrameFlags::COMMAND);
    }
}
