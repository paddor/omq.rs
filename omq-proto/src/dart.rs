//! Bounded DART datagrams. Decoding borrows the input and never allocates.

use crate::proto::SocketType;

/// Largest ordinary DATA body. Larger messages are fragmented.
pub const MAX_BODY: usize = 1024;
/// Largest DATA, fragment, or control UDP payload.
pub const MAX_DATAGRAM: usize = 1200;
/// Largest PACKED payload on a 1500-byte IPv6 path, without IP fragmentation.
pub const MAX_PACKED_DATAGRAM: usize = 1452;
/// READY protocol version.
pub const VERSION: u8 = 1;

mod credit;
mod handshake;
mod session;
pub use credit::{AdmissionCounter, CreditCounter};
pub use handshake::Handshake;
pub use session::{Admission, Ecn, FragmentBuffer, Session, SessionConfig, SessionStats, Transmit};

/// Bytes in the tag, session ID, and sequence header.
pub const DATA_HEADER: usize = 17;
/// Bytes in a first-fragment header, including the full message length.
pub const FIRST_HEADER: usize = DATA_HEADER + 8;
/// Largest CONT body; FIRST's length and group occupy part of this space.
pub const MAX_FRAGMENT_BODY: usize = MAX_DATAGRAM - DATA_HEADER;
/// Maximum messages coalesced into one UDP datagram.
pub const MAX_PACKED_MESSAGES: usize = 128;

/// Payloads with a byte-length table validated against their UDP boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackedMessages<'a> {
    lengths: &'a [u8],
    bytes: &'a [u8],
}

impl<'a> PackedMessages<'a> {
    /// Return the number of packed messages.
    pub const fn message_count(self) -> usize {
        self.lengths.len()
    }

    /// Iterate over validated message bodies in wire order.
    pub fn iter(self) -> impl ExactSizeIterator<Item = &'a [u8]> + Clone {
        Payloads {
            lengths: self.lengths,
            bytes: self.bytes,
        }
    }
}

#[derive(Clone, Debug)]
struct Payloads<'a> {
    lengths: &'a [u8],
    bytes: &'a [u8],
}

impl<'a> Iterator for Payloads<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let (&length, lengths) = self.lengths.split_first()?;
        let (payload, rest) = self.bytes.split_at(usize::from(length));
        self.lengths = lengths;
        self.bytes = rest;
        Some(payload)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.lengths.len(), Some(self.lengths.len()))
    }
}

impl ExactSizeIterator for Payloads<'_> {}

fn decode_packed<'a>(lengths: &'a [u8], bytes: &'a [u8], first: u64) -> Option<PackedMessages<'a>> {
    let count = lengths.len();
    if !(2..=MAX_PACKED_MESSAGES).contains(&count)
        || lengths
            .iter()
            .map(|length| usize::from(*length))
            .sum::<usize>()
            != bytes.len()
        || first.checked_add(count as u64).is_none()
    {
        return None;
    }
    Some(PackedMessages { lengths, bytes })
}

/// Three-message challenge confirmation. Each participant supplies a fresh
/// nonzero receiver session; repeated welcome packets repeat confirmation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    /// Connector announces its receiver session.
    Hello,
    /// Listener announces its receiver session and echoes the connector's.
    Welcome,
    /// Connector confirms the listener's receiver session.
    Confirm,
}

/// Cumulative feedback. Counts describe unique messages successfully retained,
/// not UDP arrivals that could be duplicated or rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status {
    /// Receiver session ID whose sequence is being acknowledged.
    pub session: u64,
    /// Monotonically increasing feedback serial number.
    pub serial: u64,
    /// First sequence position not cumulatively retained.
    pub ack: u64,
    /// Exclusive receive-credit right edge.
    pub credit: u64,
    /// Unique retained units by ECT(0), ECT(1), CE, Not-ECT, and unavailable ECN.
    pub counts: [u64; 5],
}

/// Borrowed reliable packet, validated against its complete UDP boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Packet<'a> {
    /// First fragment announcing the complete message length.
    First {
        /// Receiver session ID identifying this sequence.
        session: u64,
        /// Sequence position of the message or fragment.
        sequence: u64,
        /// Complete message body length in bytes.
        length: u64,
        /// Borrowed body bytes, including socket-specific metadata where applicable.
        payload: &'a [u8],
    },
    /// Subsequent fragment; sequence order determines its position.
    Continuation {
        /// Receiver session ID identifying this sequence.
        session: u64,
        /// Sequence position of the message or fragment.
        sequence: u64,
        /// Borrowed body bytes, including socket-specific metadata where applicable.
        payload: &'a [u8],
    },
    /// Complete unfragmented application message.
    Data {
        /// Receiver session ID identifying this sequence.
        session: u64,
        /// Sequence position of the message or fragment.
        sequence: u64,
        /// Borrowed body bytes, including socket-specific metadata where applicable.
        payload: &'a [u8],
    },
    /// Consecutive small messages sharing one UDP datagram.
    Packed {
        /// Receiver session ID identifying this sequence.
        session: u64,
        /// First sequence position in this range.
        first: u64,
        /// Validated consecutive message bodies.
        messages: PackedMessages<'a>,
    },
    /// Cumulative receipt, credit, and ECN feedback.
    Status(Status),
    /// Request retransmission of a missing sequence range.
    Nak {
        /// Receiver session ID identifying this sequence.
        session: u64,
        /// First sequence position in this range.
        first: u64,
        /// Number of consecutive missing sequence units.
        count: u32,
    },
    /// Announce the sender's next sequence position and request feedback.
    Probe {
        /// Receiver session ID identifying this sequence.
        session: u64,
        /// Next sequence position awaiting initial transmission.
        next: u64,
    },
}

fn word(bytes: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        bytes.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

/// Decode a reliable packet; return `None` for an invalid UDP payload.
pub fn decode_packet(bytes: &[u8]) -> Option<Packet<'_>> {
    let tag = *bytes.first()?;
    if (2..=MAX_PACKED_MESSAGES as u8).contains(&tag) {
        if bytes.len() > MAX_PACKED_DATAGRAM {
            return None;
        }
        let count = usize::from(tag);
        let lengths = bytes.get(1..1 + count)?;
        let session = word(bytes, 1 + count)?;
        let first = word(bytes, 9 + count)?;
        if session == 0 {
            return None;
        }
        return Some(Packet::Packed {
            session,
            first,
            messages: decode_packed(lengths, &bytes[DATA_HEADER + count..], first)?,
        });
    }
    if bytes.len() > MAX_DATAGRAM {
        return None;
    }
    let session = word(bytes, 1)?;
    if session == 0 {
        return None;
    }
    match tag {
        0x81 => Some(Packet::First {
            session,
            sequence: word(bytes, 9)?,
            length: word(bytes, 17)?,
            payload: &bytes[FIRST_HEADER..],
        }),
        0x82 => Some(Packet::Continuation {
            session,
            sequence: word(bytes, 9)?,
            payload: &bytes[DATA_HEADER..],
        }),
        1 => Some(Packet::Data {
            session,
            sequence: word(bytes, 9)?,
            payload: &bytes[17..],
        }),
        0xC1 if bytes.len() == 73 => Some(Packet::Status(Status {
            session,
            serial: word(bytes, 9)?,
            ack: word(bytes, 17)?,
            credit: word(bytes, 25)?,
            counts: [
                word(bytes, 33)?,
                word(bytes, 41)?,
                word(bytes, 49)?,
                word(bytes, 57)?,
                word(bytes, 65)?,
            ],
        })),
        0xC2 if bytes.len() == 21 => Some(Packet::Nak {
            session,
            first: word(bytes, 9)?,
            count: u32::from_le_bytes(bytes[17..21].try_into().ok()?),
        }),
        0xC3 if bytes.len() == 17 => Some(Packet::Probe {
            session,
            next: word(bytes, 9)?,
        }),
        _ => None,
    }
}

/// Encode a control packet without allocation or padding.
pub fn encode_packet(packet: Packet<'_>, output: &mut [u8]) -> Option<usize> {
    let (tag, session, length) = match packet {
        Packet::Status(status) => (0xC1, status.session, 73),
        Packet::Nak { session, .. } => (0xC2, session, 21),
        Packet::Probe { session, .. } => (0xC3, session, 17),
        Packet::Data { .. }
        | Packet::Packed { .. }
        | Packet::First { .. }
        | Packet::Continuation { .. } => return None,
    };
    let output = output.get_mut(..length)?;
    if session == 0 {
        return None;
    }
    output[0] = tag;
    output[1..9].copy_from_slice(&session.to_le_bytes());
    match packet {
        Packet::Status(status) => {
            for (chunk, value) in output[9..].as_chunks_mut::<8>().0.iter_mut().zip(
                [status.serial, status.ack, status.credit]
                    .into_iter()
                    .chain(status.counts),
            ) {
                chunk.copy_from_slice(&value.to_le_bytes());
            }
        }
        Packet::Nak { first, count, .. } => {
            output[9..17].copy_from_slice(&first.to_le_bytes());
            output[17..21].copy_from_slice(&count.to_le_bytes());
        }
        Packet::Probe { next, .. } => output[9..17].copy_from_slice(&next.to_le_bytes()),
        Packet::Data { .. }
        | Packet::Packed { .. }
        | Packet::First { .. }
        | Packet::Continuation { .. } => unreachable!(),
    }
    Some(length)
}

/// Validate the body and routing metadata before local queue admission.
/// `identity_prefix` is true for PEER's compatibility multipart send form.
#[inline]
pub fn validate_message(
    socket_type: SocketType,
    message: &crate::Message,
    identity_prefix: bool,
) -> crate::Result<()> {
    let start = usize::from(identity_prefix);
    let grouped = socket_type == SocketType::Radio;
    let expected = start + if grouped { 2 } else { 1 };
    if !supports(socket_type) || message.len() != expected {
        return Err(crate::Error::Protocol(
            "DART requires exactly one body plus routing metadata".into(),
        ));
    }
    if grouped {
        let group = message.part_slice(start).ok_or_else(|| {
            crate::Error::Protocol("DART requires contiguous group metadata".into())
        })?;
        if group.is_empty() || group.len() > 255 {
            return Err(crate::Error::Protocol(
                "DART group length must be 1..=255 bytes".into(),
            ));
        }
    }
    // Every Message part has contiguous Payload storage. Checking its count
    // already establishes that the body exists; no body borrow is needed.
    Ok(())
}

/// Encode one fragment. Only FIRST carries the full u64 body length and group.
pub fn encode_fragment(
    session: u64,
    sequence: u64,
    length: Option<u64>,
    body: &[u8],
    group: Option<&[u8]>,
    output: &mut [u8],
) -> Option<usize> {
    let prefix = if length.is_some() {
        FIRST_HEADER
    } else {
        DATA_HEADER
    };
    if session == 0
        || body.is_empty()
        || body.len() > MAX_FRAGMENT_BODY
        || (length.is_none() && group.is_some())
        || group.is_some_and(|g| g.is_empty() || g.len() > 255)
    {
        return None;
    }
    let header = prefix + group.map_or(0, |g| g.len() + 1);
    let size = header + body.len();
    let output = output.get_mut(..size.min(MAX_DATAGRAM))?;
    if size > MAX_DATAGRAM {
        return None;
    }
    output[0] = if length.is_some() { 0x81 } else { 0x82 };
    output[1..9].copy_from_slice(&session.to_le_bytes());
    output[9..17].copy_from_slice(&sequence.to_le_bytes());
    if let Some(length) = length {
        output[17..25].copy_from_slice(&length.to_le_bytes());
    }
    if let Some(group) = group {
        output[prefix] = group.len() as u8;
        output[prefix + 1..header].copy_from_slice(group);
    }
    output[header..].copy_from_slice(body);
    Some(size)
}

/// A validated outer datagram. Socket-pair validation remains with the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Datagram<'a> {
    /// Body plus any socket-specific group metadata.
    Data(&'a [u8]),
    /// A control command with borrowed name and body.
    Command {
        /// ASCII command name without its length prefix.
        name: &'a [u8],
        /// Command-specific bytes following the name.
        body: &'a [u8],
    },
}

/// Supported socket types. DART accepts only single-part application messages.
pub const fn supports(socket_type: SocketType) -> bool {
    matches!(
        socket_type,
        SocketType::Client
            | SocketType::Server
            | SocketType::Scatter
            | SocketType::Gather
            | SocketType::Peer
            | SocketType::Radio
            | SocketType::Dish
            | SocketType::Channel
    )
}

/// Validate the datagram boundary and tag, including an empty data body.
pub fn decode(bytes: &[u8]) -> Option<Datagram<'_>> {
    if bytes.len() > MAX_DATAGRAM {
        return None;
    }
    let (&tag, payload) = bytes.split_first()?;
    match tag {
        1 if payload.len() >= 16 => Some(Datagram::Data(&payload[16..])),
        0xC0 => {
            let (&length, payload) = payload.split_first()?;
            let (name, body) = payload.split_at_checked(usize::from(length))?;
            if name.is_empty() || !name.is_ascii() {
                return None;
            }
            Some(Datagram::Command { name, body })
        }
        _ => None,
    }
}

/// Borrow an ordinary body, or a RADIO/DISH group and body.
pub fn data_body(payload: &[u8], grouped: bool) -> Option<(Option<&[u8]>, &[u8])> {
    let (group, body) = fragment_body(payload, grouped)?;
    (body.len() <= MAX_BODY).then_some((group, body))
}

/// Borrow a fragment body and optional FIRST group within the datagram bound.
pub fn fragment_body(payload: &[u8], grouped: bool) -> Option<(Option<&[u8]>, &[u8])> {
    let (group, body) = if grouped {
        let (&length, rest) = payload.split_first()?;
        if length == 0 {
            return None;
        }
        let (group, body) = rest.split_at_checked(usize::from(length))?;
        (Some(group), body)
    } else {
        (None, payload)
    };
    (payload.len() <= MAX_FRAGMENT_BODY).then_some((group, body))
}

/// Encode exact lengths into caller-owned storage. No padding is added.
/// Isolated codec convenience using destination 1 and sequence 0. Runtime
/// carriers must use `Session::prepare_data` with their actual incarnation.
pub fn encode_data(body: &[u8], group: Option<&[u8]>, output: &mut [u8]) -> Option<usize> {
    encode_sequenced_data(1, 0, body, group, output)
}

/// Encode one reliable body with its destination session and logical sequence.
pub fn encode_sequenced_data(
    session: u64,
    sequence: u64,
    body: &[u8],
    group: Option<&[u8]>,
    output: &mut [u8],
) -> Option<usize> {
    if body.len() > MAX_BODY {
        return None;
    }
    let header = match group {
        Some(group) if group.is_empty() || group.len() > 255 => return None,
        Some(group) => DATA_HEADER + 1 + group.len(),
        None => DATA_HEADER,
    };
    let length = header + body.len();
    if length > MAX_DATAGRAM || output.len() < length {
        return None;
    }
    output[0] = 1;
    output[1..9].copy_from_slice(&session.to_le_bytes());
    output[9..17].copy_from_slice(&sequence.to_le_bytes());
    if let Some(group) = group {
        output[DATA_HEADER] = group.len() as u8;
        output[DATA_HEADER + 1..header].copy_from_slice(group);
    }
    output[header..length].copy_from_slice(body);
    Some(length)
}

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
    let mut rest = body;
    let mut socket_type = None;
    let mut identity = None;
    let mut version = None;
    let mut reply_requested = None;
    let mut session = None;
    let mut echo = None;
    let mut phase = None;
    while !rest.is_empty() {
        let current = rest;
        let (name, value, remaining) = property(rest)?;
        // At most 1200 bytes of control input. Compare earlier names directly
        // rather than constructing a per-datagram set of allocated strings.
        let mut earlier = body;
        while earlier.len() > current.len() {
            let (previous, _, next) = property(earlier)?;
            if previous == name {
                return None;
            }
            earlier = next;
        }
        match name {
            b"Socket-Type" => socket_type = SocketType::from_wire(value),
            b"Identity" => {
                if value.is_empty() || value.len() > 255 {
                    return None;
                }
                identity = Some(value);
            }
            b"DART-Version" => version = Some(value),
            b"Reply-Requested" => {
                reply_requested = match value {
                    [0] => Some(false),
                    [1] => Some(true),
                    _ => return None,
                };
            }
            b"Session" => session = value.try_into().ok().map(u64::from_le_bytes),
            b"Echo" => echo = value.try_into().ok().map(u64::from_le_bytes),
            b"Phase" => {
                phase = match value {
                    [0] => Some(Phase::Hello),
                    [1] => Some(Phase::Welcome),
                    [2] => Some(Phase::Confirm),
                    _ => return None,
                }
            }
            _ => {}
        }
        rest = remaining;
    }
    let socket_type = socket_type?;
    let phase = phase?;
    let echo = echo?;
    if session? == 0
        || !valid_echo(phase, echo)
        || !supports(socket_type)
        || version? != [VERSION]
        || (socket_type == SocketType::Peer && identity.is_none())
    {
        return None;
    }
    Some(Ready {
        socket_type,
        identity,
        reply_requested: reply_requested?,
        session: session?,
        echo,
        phase,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_roundtrips_exact_boundaries() {
        let mut output = [0; MAX_DATAGRAM];
        for length in [0, 1, 16, 64, MAX_BODY] {
            let body = vec![7; length];
            for group in [None, Some(b"news".as_slice())] {
                let n = encode_data(&body, group, &mut output).unwrap();
                let Some(Datagram::Data(payload)) = decode(&output[..n]) else {
                    panic!()
                };
                assert_eq!(
                    data_body(payload, group.is_some()),
                    Some((group, body.as_slice()))
                );
            }
        }
    }

    #[test]
    fn rejects_invalid_tags_and_truncated_commands() {
        assert_eq!(decode(&[]), None);
        for tag in 0..=255 {
            assert_eq!(decode(&[tag]), None);
        }
        assert_eq!(decode(b"\x01\x02\0\x03abc\0\x03def"), None);
        assert_eq!(decode(b"\xc0\x00"), None);
        assert_eq!(decode(b"\xc0\x05READ"), None);
        assert_eq!(decode(b"\xc0\x01\xff"), None);
        assert_eq!(decode(&[0; MAX_DATAGRAM + 1]), None);
        assert!(data_body(&[0; MAX_BODY + 1], false).is_none());
    }

    #[test]
    fn group_budget_and_empty_body_are_enforced() {
        let mut output = [0; MAX_DATAGRAM];
        let group = [b'g'; 255];
        assert_eq!(
            encode_data(&[0; 927], Some(&group), &mut output),
            Some(MAX_DATAGRAM)
        );
        assert_eq!(encode_data(&[0; 928], Some(&group), &mut output), None);
        assert_eq!(encode_data(&[0; MAX_BODY + 1], None, &mut output), None);
        assert_eq!(encode_data(b"", Some(b""), &mut output), None);
        assert_eq!(encode_data(b"", Some(&[0; 256]), &mut output), None);
        assert_eq!(encode_data(b"abc", None, &mut [0; 3]), None);
        assert!(data_body(&[0], true).is_none());
        assert!(data_body(&[4, 1, 2], true).is_none());
    }

    #[test]
    fn ready_roundtrips_all_supported_types_and_request_values() {
        for socket_type in [
            SocketType::Client,
            SocketType::Server,
            SocketType::Scatter,
            SocketType::Gather,
            SocketType::Peer,
            SocketType::Radio,
            SocketType::Dish,
            SocketType::Channel,
        ] {
            for reply_requested in [false, true] {
                let ready = Ready {
                    socket_type,
                    identity: Some(b"identity"),
                    reply_requested,
                    session: 1,
                    echo: 0,
                    phase: Phase::Hello,
                };
                let mut output = [0; MAX_DATAGRAM];
                let n = encode_ready(ready, &mut output).unwrap();
                assert_eq!(decode_ready(&output[..n]), Some(ready));
                for prefix in 0..n {
                    assert!(decode_ready(&output[..prefix]).is_none(), "prefix {prefix}");
                }
            }
        }
    }

    fn append_property(bytes: &mut Vec<u8>, name: &[u8], value: &[u8]) {
        bytes.push(name.len() as u8);
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
        bytes.extend_from_slice(value);
    }

    #[test]
    fn required_metadata_and_duplicates_are_checked() {
        let mut output = [0; MAX_DATAGRAM];
        let ready = Ready {
            socket_type: SocketType::Peer,
            identity: Some(b"peer"),
            reply_requested: true,
            session: 1,
            echo: 0,
            phase: Phase::Hello,
        };
        let n = encode_ready(ready, &mut output).unwrap();
        for name in [
            b"Socket-Type".as_slice(),
            b"Identity",
            b"DART-Version",
            b"Reply-Requested",
        ] {
            let mut duplicate = output[..n].to_vec();
            append_property(&mut duplicate, name, b"ignored");
            assert_eq!(decode_ready(&duplicate), None);
        }
        let mut extra = output[..n].to_vec();
        append_property(&mut extra, b"Extension", b"value");
        assert_eq!(decode_ready(&extra), Some(ready));
        append_property(&mut extra, b"Extension", b"again");
        assert_eq!(decode_ready(&extra), None);
        assert_eq!(decode_ready(b"\xc0\x05READY"), None);
        for (name, value) in [
            (b"DART-Version".as_slice(), b"\x00".as_slice()),
            (b"DART-Version", b"\x02"),
            (b"DART-Version", b"\x04"),
            (b"DART-Version", b"\x05"),
            (b"Reply-Requested", b"\x02"),
            (b"Reply-Requested", b""),
            (b"Socket-Type", b"PUSH"),
            (b"Socket-Type", b"unknown"),
        ] {
            let mut bytes = b"\xc0\x05READY".to_vec();
            for (key, valid) in [
                (b"Socket-Type".as_slice(), b"CHANNEL".as_slice()),
                (b"DART-Version", &[VERSION]),
                (b"Reply-Requested", b"\x01"),
            ] {
                append_property(&mut bytes, key, if key == name { value } else { valid });
            }
            assert!(decode_ready(&bytes).is_none());
        }
    }

    #[test]
    fn identity_and_control_buffer_limits_are_checked() {
        let mut output = [0; MAX_DATAGRAM];
        for identity in [None, Some(b"".as_slice()), Some([0; 256].as_slice())] {
            assert_eq!(
                encode_ready(
                    Ready {
                        socket_type: SocketType::Peer,
                        identity,
                        reply_requested: false,
                        session: 1,
                        echo: 0,
                        phase: Phase::Hello,
                    },
                    &mut output
                ),
                None
            );
        }
        let ready = Ready {
            socket_type: SocketType::Peer,
            identity: Some(&[0; 255]),
            reply_requested: false,
            session: 1,
            echo: 0,
            phase: Phase::Hello,
        };
        assert!(encode_ready(ready, &mut output).is_some());
        assert_eq!(encode_ready(ready, &mut [0; 7]), None);
        assert_eq!(
            encode_ready(
                Ready {
                    socket_type: SocketType::Push,
                    ..ready
                },
                &mut output
            ),
            None
        );
        let mut bytes = b"\xc0\x05READY".to_vec();
        bytes.extend_from_slice(b"\x01X\xff\xff\xff\xff");
        assert_eq!(decode_ready(&bytes), None);
        assert!(decode_ready(b"\xc0\x05READY\x00\x00\x00\x00\x00").is_none());
    }
}
