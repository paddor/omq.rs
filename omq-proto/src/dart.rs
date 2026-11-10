//! Bounded DART datagrams. Decoding borrows the input and never allocates.

use crate::proto::SocketType;

mod credit;
mod handshake;
mod ready;
mod session;

pub use credit::{AdmissionCounter, CreditCounter};
pub use handshake::Handshake;
pub use ready::{Ready, decode_ready, encode_ready};
pub use session::{
    Admission, DataBatch, Ecn, FragmentBuffer, Receipt, Session, SessionConfig, SessionStats,
    Transmit,
};

/// Largest ordinary DATA body. Larger messages are fragmented.
pub const MAX_BODY: usize = 1024;

/// Largest DATA, fragment, or control UDP payload.
pub const MAX_DATAGRAM: usize = 1200;

/// Largest PACKED payload on a 1500-byte IPv6 path, without IP fragmentation.
pub const MAX_PACKED_DATAGRAM: usize = 1452;

/// READY protocol version.
pub const VERSION: u8 = 1;

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

    // PACKED reuses its tag as the message count, so it owns tags 2..=128.
    if (2..=MAX_PACKED_MESSAGES as u8).contains(&tag) {
        return decode_packed_packet(bytes, usize::from(tag));
    }

    if bytes.len() > MAX_DATAGRAM {
        return None;
    }

    let session = word(bytes, 1)?;
    // Receiver session IDs are always nonzero.
    if session == 0 {
        return None;
    }

    // Control packets have exact sizes: tag and session, then STATUS carries
    // eight u64 words, NAK a u64 and a u32, and PROBE one u64.
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
        0xC1 if bytes.len() == 73 => decode_status(bytes, session).map(Packet::Status),
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

/// Decode PACKED: tag/count, length table, session, first sequence, bodies.
#[inline]
fn decode_packed_packet(bytes: &[u8], count: usize) -> Option<Packet<'_>> {
    if bytes.len() > MAX_PACKED_DATAGRAM {
        return None;
    }

    let lengths = bytes.get(1..1 + count)?;
    let session = word(bytes, 1 + count)?;
    let first = word(bytes, 9 + count)?;

    if session == 0 {
        return None;
    }

    Some(Packet::Packed {
        session,
        first,
        messages: decode_packed(lengths, &bytes[DATA_HEADER + count..], first)?,
    })
}

fn decode_status(bytes: &[u8], session: u64) -> Option<Status> {
    Some(Status {
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
    })
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

/// Header fields of one fragment datagram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FragmentHeader<'a> {
    /// Receiver session ID identifying this sequence.
    pub session: u64,
    /// Sequence position of this fragment.
    pub sequence: u64,
    /// Complete body length; present only on FIRST.
    pub length: Option<u64>,
    /// RADIO group; present only on FIRST.
    pub group: Option<&'a [u8]>,
}

/// Encode one fragment. Only FIRST carries the full u64 body length and group.
pub fn encode_fragment(
    header: FragmentHeader<'_>,
    body: &[u8],
    output: &mut [u8],
) -> Option<usize> {
    let FragmentHeader {
        session,
        sequence,
        length,
        group,
    } = header;

    let prefix = if length.is_some() {
        FIRST_HEADER
    } else {
        DATA_HEADER
    };

    // Only FIRST may carry a group, and a group must fit its u8 length.
    if session == 0
        || body.is_empty()
        || body.len() > MAX_FRAGMENT_BODY
        || (length.is_none() && group.is_some())
        || group.is_some_and(|g| g.is_empty() || g.len() > 255)
    {
        return None;
    }

    let header_len = prefix + group.map_or(0, |g| g.len() + 1);
    let size = header_len + body.len();
    let output = output.get_mut(..size.min(MAX_DATAGRAM))?;

    if size > MAX_DATAGRAM {
        return None;
    }

    let tag = if length.is_some() { 0x81 } else { 0x82 };
    write_sequenced_header(output, tag, session, sequence);

    if let Some(length) = length {
        output[DATA_HEADER..FIRST_HEADER].copy_from_slice(&length.to_le_bytes());
    }

    if let Some(group) = group {
        write_group(&mut output[prefix..header_len], group);
    }

    output[header_len..].copy_from_slice(body);
    Some(size)
}

/// Write the tag, destination session, and sequence that open DATA and
/// fragment datagrams.
#[inline]
fn write_sequenced_header(output: &mut [u8], tag: u8, session: u64, sequence: u64) {
    output[0] = tag;
    output[1..9].copy_from_slice(&session.to_le_bytes());
    output[9..DATA_HEADER].copy_from_slice(&sequence.to_le_bytes());
}

/// Write a RADIO group as its length byte and bytes into exactly
/// `1 + group.len()` bytes of `output`.
#[inline]
fn write_group(output: &mut [u8], group: &[u8]) {
    output[0] = group.len() as u8;
    output[1..].copy_from_slice(group);
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

    write_sequenced_header(output, 1, session, sequence);
    if let Some(group) = group {
        write_group(&mut output[DATA_HEADER..header], group);
    }

    output[header..length].copy_from_slice(body);
    Some(length)
}

#[cfg(test)]
mod tests;
