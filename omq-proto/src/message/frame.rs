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
mod tests;
