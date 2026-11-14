//! READY handshake datagram codec.

use super::{Datagram, MAX_DATAGRAM, Phase, VERSION, decode, supports};
use crate::proto::SocketType;

/// Most properties one READY datagram may carry.
const MAX_READY_PROPERTIES: usize = 16;

/// READY metadata, borrowed from one datagram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ready<'a> {
    /// Socket type advertised for compatibility checking.
    pub socket_type: SocketType,
    /// Optional peer routing identity.
    pub identity: Option<&'a [u8]>,
    /// Whether the peer requests a READY response.
    pub reply_requested: bool,
    /// Fresh nonzero local receiver session ID.
    pub session: u64,
    /// Echoed remote session ID; zero in HELLO.
    pub echo: u64,
    /// Challenge-confirmation phase.
    pub phase: Phase,
}

const fn valid_echo(phase: Phase, echo: u64) -> bool {
    match phase {
        Phase::Hello => echo == 0,
        Phase::Welcome | Phase::Confirm => echo != 0,
    }
}

/// Encode READY into preallocated storage.
pub fn encode_ready(ready: Ready<'_>, output: &mut [u8]) -> Option<usize> {
    if ready.session == 0
        || !valid_echo(ready.phase, ready.echo)
        || !supports(ready.socket_type)
        || ready
            .identity
            .is_some_and(|id| id.is_empty() || id.len() > 255)
        || (ready.socket_type == SocketType::Peer && ready.identity.is_none())
    {
        return None;
    }

    let mut writer = Writer { output, cursor: 0 };
    writer.write(b"\xc0\x05READY")?;
    writer.property(b"Socket-Type", ready.socket_type.as_str().as_bytes())?;

    if let Some(identity) = ready.identity {
        writer.property(b"Identity", identity)?;
    }

    writer.property(b"DART-Version", &[VERSION])?;
    writer.property(b"Reply-Requested", &[u8::from(ready.reply_requested)])?;
    writer.property(b"Session", &ready.session.to_le_bytes())?;
    writer.property(b"Echo", &ready.echo.to_le_bytes())?;
    writer.property(b"Phase", &[ready.phase as u8])?;

    Some(writer.cursor)
}

struct Writer<'a> {
    output: &'a mut [u8],
    cursor: usize,
}

impl Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> Option<()> {
        let end = self.cursor.checked_add(bytes.len())?;
        if end > MAX_DATAGRAM {
            return None;
        }

        self.output
            .get_mut(self.cursor..end)?
            .copy_from_slice(bytes);
        self.cursor = end;

        Some(())
    }

    fn property(&mut self, name: &[u8], value: &[u8]) -> Option<()> {
        self.write(&[name.len() as u8])?;
        self.write(name)?;
        self.write(&(value.len() as u32).to_be_bytes())?;

        self.write(value)
    }
}

fn property(bytes: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let (&name_len, rest) = bytes.split_first()?;
    let (name, rest) = rest.split_at_checked(usize::from(name_len))?;

    if name.is_empty() || !name.is_ascii() {
        return None;
    }

    let (length, rest) = rest.split_at_checked(4)?;
    let length = u32::from_be_bytes(length.try_into().ok()?) as usize;
    let (value, rest) = rest.split_at_checked(length)?;

    Some((name, value, rest))
}

/// Validate required metadata and all duplicate properties, without allocation.
pub fn decode_ready(bytes: &[u8]) -> Option<Ready<'_>> {
    let Datagram::Command {
        name: b"READY",
        body,
    } = decode(bytes)?
    else {
        return None;
    };

    let mut fields = ReadyFields::default();
    let mut rest = body;
    let mut count = 0;

    while !rest.is_empty() {
        // A READY needs seven properties. Bound the duplicate scan below.
        count += 1;
        if count > MAX_READY_PROPERTIES {
            return None;
        }

        let (name, value, remaining) = property(rest)?;
        if repeats_earlier_property(body, rest, name)? {
            return None;
        }

        fields.set(name, value)?;
        rest = remaining;
    }

    fields.finish()
}

/// Whether `name` already appears in `body` before the property at `current`.
fn repeats_earlier_property(body: &[u8], current: &[u8], name: &[u8]) -> Option<bool> {
    // At most 1200 bytes of control input. Compare earlier names directly
    // rather than constructing a per-datagram set of allocated strings.
    let mut earlier = body;
    while earlier.len() > current.len() {
        let (previous, _, next) = property(earlier)?;
        if previous == name {
            return Some(true);
        }

        earlier = next;
    }

    Some(false)
}

/// READY properties seen so far. Unknown names are ignored.
#[derive(Default)]
struct ReadyFields<'a> {
    socket_type: Option<SocketType>,
    identity: Option<&'a [u8]>,
    version: Option<&'a [u8]>,
    reply_requested: Option<bool>,
    session: Option<u64>,
    echo: Option<u64>,
    phase: Option<Phase>,
}

impl<'a> ReadyFields<'a> {
    /// Record one property. `None` rejects the whole datagram.
    fn set(&mut self, name: &[u8], value: &'a [u8]) -> Option<()> {
        match name {
            b"Socket-Type" => self.socket_type = SocketType::from_wire(value),
            b"Identity" => {
                if value.is_empty() || !crate::message::is_valid_peer_identity(value) {
                    return None;
                }

                self.identity = Some(value);
            }
            b"DART-Version" => self.version = Some(value),
            b"Reply-Requested" => {
                self.reply_requested = match value {
                    [0] => Some(false),
                    [1] => Some(true),
                    _ => return None,
                };
            }
            b"Session" => self.session = value.try_into().ok().map(u64::from_le_bytes),
            b"Echo" => self.echo = value.try_into().ok().map(u64::from_le_bytes),
            b"Phase" => {
                self.phase = match value {
                    [0] => Some(Phase::Hello),
                    [1] => Some(Phase::Welcome),
                    [2] => Some(Phase::Confirm),
                    _ => return None,
                }
            }
            _ => {}
        }

        Some(())
    }

    /// Require every mandatory property and check cross-field rules.
    fn finish(self) -> Option<Ready<'a>> {
        let socket_type = self.socket_type?;
        let phase = self.phase?;
        let echo = self.echo?;
        let session = self.session?;

        if session == 0
            || !valid_echo(phase, echo)
            || !supports(socket_type)
            || self.version? != [VERSION]
            || (socket_type == SocketType::Peer && self.identity.is_none())
        {
            return None;
        }

        Some(Ready {
            socket_type,
            identity: self.identity,
            reply_requested: self.reply_requested?,
            session,
            echo,
            phase,
        })
    }
}
