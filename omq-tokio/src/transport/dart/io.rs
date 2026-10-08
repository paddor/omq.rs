use std::io::{self, IoSliceMut};
use std::net::{IpAddr, SocketAddr};

use omq_proto::dart::MAX_DATAGRAM;
use quinn_udp::{BATCH_SIZE, EcnCodepoint, RecvMeta, Transmit, UdpSocketState};
use tokio::io::Interest;
use tokio::net::UdpSocket;

const MAX_SEGMENTS: usize = 64;
const MAX_BATCH_BYTES: usize = 64_000;

/// A nonblocking UDP carrier with Quinn's offloads and packet metadata.
///
/// Construct it on the IO runtime that will drive it. The caller handles
/// protocol validation, readiness waits, work budgets, and peer-failure drops.
#[derive(Debug)]
pub struct DartIo {
    socket: UdpSocket,
    state: UdpSocketState,
    ecn_v4: Option<bool>,
    ecn_v6: Option<bool>,
}

impl DartIo {
    pub fn capabilities(&self) -> super::DartCapabilities {
        super::DartCapabilities {
            max_gso_segments: self.max_gso_segments(),
            max_gro_segments: self.gro_segments(),
            ecn_ipv4: self.ecn_v4,
            ecn_ipv6: self.ecn_v6,
            may_fragment: self.may_fragment(),
        }
    }

    pub fn new(socket: std::net::UdpSocket) -> io::Result<Self> {
        let state = UdpSocketState::new((&socket).into())?;
        let (ecn_v4, ecn_v6) = ecn_support(&socket);
        Ok(Self {
            socket: UdpSocket::from_std(socket)?,
            state,
            ecn_v4,
            ecn_v6,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Current capability, including a runtime downshift after offload failure.
    pub fn max_gso_segments(&self) -> usize {
        self.state.max_gso_segments().clamp(1, MAX_SEGMENTS)
    }

    pub fn gro_segments(&self) -> usize {
        self.state.gro_segments().clamp(1, MAX_SEGMENTS)
    }

    pub fn may_fragment(&self) -> bool {
        self.state.may_fragment()
    }

    /// `None` means the platform cannot confirm ECN metadata availability.
    pub fn ecn_receive_supported(&self, source: IpAddr) -> Option<bool> {
        match source {
            IpAddr::V4(_) => self.ecn_v4,
            IpAddr::V6(address) if address.to_ipv4_mapped().is_some() => self.ecn_v4,
            IpAddr::V6(_) => self.ecn_v6,
        }
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.socket.readable().await
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.socket.writable().await
    }

    /// Send a prefix of concatenated equal-length datagrams. A final datagram
    /// may be shorter. Returns the number accepted; the remainder stays with
    /// the caller. A fallback never sends several bodies as one plain packet.
    ///
    /// `ecn` is a low-level facility. Socket policy must separately authorize
    /// ECT marking with application feedback and rate control.
    pub fn try_send_segments(
        &self,
        destination: SocketAddr,
        source: Option<IpAddr>,
        ecn: Option<EcnCodepoint>,
        contents: &[u8],
        segment_size: usize,
    ) -> io::Result<usize> {
        if contents.is_empty()
            || contents.len() > MAX_BATCH_BYTES
            || !(1..=MAX_DATAGRAM).contains(&segment_size)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DART segment batch",
            ));
        }
        let initial_cap = self.max_gso_segments();
        match self.send_window(
            destination,
            source,
            ecn,
            contents,
            segment_size,
            initial_cap,
        ) {
            Err(_) if initial_cap > 1 && self.max_gso_segments() == 1 => {
                self.send_window(destination, source, ecn, contents, segment_size, 1)
            }
            result => result,
        }
    }

    fn send_window(
        &self,
        destination: SocketAddr,
        source: Option<IpAddr>,
        ecn: Option<EcnCodepoint>,
        contents: &[u8],
        segment_size: usize,
        max_segments: usize,
    ) -> io::Result<usize> {
        let (length, count) = send_window(contents.len(), segment_size, max_segments);
        let transmit = Transmit {
            destination,
            ecn,
            contents: &contents[..length],
            segment_size: (count > 1).then_some(segment_size),
            src_ip: source,
        };
        self.socket.try_io(Interest::WRITABLE, || {
            self.state.try_send((&self.socket).into(), &transmit)
        })?;
        Ok(count)
    }
}

fn send_window(length: usize, segment_size: usize, max_segments: usize) -> (usize, usize) {
    let length = length.min(segment_size * max_segments);
    (length, length.div_ceil(segment_size))
}

fn ecn_support(socket: &std::net::UdpSocket) -> (Option<bool>, Option<bool>) {
    #[cfg(windows)]
    {
        // Quinn's Windows setup requires IP_RECVECN/IPV6_RECVECN to succeed.
        let ipv6 = socket.local_addr().is_ok_and(|address| address.is_ipv6());
        (Some(true), ipv6.then_some(true))
    }
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd"
    ))]
    {
        let socket = socket2::SockRef::from(socket);
        (socket.recv_tos_v4().ok(), socket.recv_tclass_v6().ok())
    }
    #[cfg(not(any(
        windows,
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd"
    )))]
    {
        let _ = socket;
        (None, None)
    }
}

/// Reusable receive arenas and metadata. Allocation occurs only at setup.
#[derive(Debug)]
pub struct ReceiveBatch {
    storage: Box<[u8]>,
    slot_size: usize,
    metadata: [RecvMeta; BATCH_SIZE],
    count: usize,
    cursor: usize,
    offset: usize,
}

/// One complete datagram borrowed from the most recent receive batch.
#[derive(Clone, Copy, Debug)]
pub struct ReceivedDatagram<'a> {
    pub source: SocketAddr,
    pub destination: Option<IpAddr>,
    pub ecn: Option<EcnCodepoint>,
    pub bytes: &'a [u8],
}

impl ReceiveBatch {
    pub fn new(gro_segments: usize) -> Self {
        // Linux's batched Quinn receive does not expose MSG_TRUNC. Keep one
        // guard byte beyond every admissible aggregate and drop full arenas.
        // Also fit a whole ordinary UDP packet on platforms without GRO.
        let slot_size = (MAX_DATAGRAM * gro_segments.clamp(1, MAX_SEGMENTS)).max(65_536) + 1;
        Self {
            storage: vec![0; slot_size * BATCH_SIZE].into_boxed_slice(),
            slot_size,
            metadata: [RecvMeta::default(); BATCH_SIZE],
            count: 0,
            cursor: 0,
            offset: 0,
        }
    }

    /// Reset old metadata before receiving, including after `WouldBlock`.
    pub fn try_receive(&mut self, io: &DartIo) -> io::Result<usize> {
        self.receive_inner::<BATCH_SIZE>(io, false)
    }

    pub(crate) fn try_receive_one(&mut self, io: &DartIo) -> io::Result<usize> {
        self.receive_inner::<1>(io, false)
    }

    /// Poll the nonblocking socket during an explicitly bounded spin window.
    /// Reactor readiness can be stale until the runtime gets another turn.
    pub(crate) fn try_receive_spinning(&mut self, io: &DartIo) -> io::Result<usize> {
        self.receive_inner::<BATCH_SIZE>(io, true)
    }

    /// Poll one buffer for the latency profile, without preparing an idle
    /// batch. The buffer still accepts GRO and has the same truncation guard.
    pub(crate) fn try_receive_one_spinning(&mut self, io: &DartIo) -> io::Result<usize> {
        self.receive_inner::<1>(io, true)
    }

    fn receive_inner<const N: usize>(&mut self, io: &DartIo, spinning: bool) -> io::Result<usize> {
        self.count = 0;
        self.cursor = 0;
        self.offset = 0;
        let mut chunks = self.storage.chunks_mut(self.slot_size);
        let mut slices: [IoSliceMut<'_>; N] = std::array::from_fn(|_| {
            IoSliceMut::new(chunks.next().expect("preallocated receive arena"))
        });
        let mut receive = || {
            io.state
                .recv((&io.socket).into(), &mut slices, &mut self.metadata[..N])
        };
        self.count = if spinning {
            receive()?
        } else {
            io.socket.try_io(Interest::READABLE, receive)?
        };
        Ok(self.count)
    }

    pub fn received_buffers(&self) -> usize {
        self.count
    }

    /// Consume one complete datagram. The cursor survives a budget yield;
    /// receive again only after this returns `None`.
    pub fn pop_datagram(&mut self) -> Option<ReceivedDatagram<'_>> {
        while self.cursor < self.count {
            let meta = self.metadata[self.cursor];
            if !self.accepts(&meta) {
                self.cursor += 1;
                self.offset = 0;
                continue;
            }
            let start = self.cursor * self.slot_size + self.offset;
            let end_offset = (self.offset + meta.stride).min(meta.len);
            let end = self.cursor * self.slot_size + end_offset;
            self.offset = end_offset;
            if end_offset == meta.len {
                self.cursor += 1;
                self.offset = 0;
            }
            return Some(ReceivedDatagram {
                source: meta.addr,
                destination: meta.dst_ip,
                ecn: meta.ecn,
                bytes: &self.storage[start..end],
            });
        }
        None
    }

    pub(crate) fn peek_datagram(&mut self) -> Option<ReceivedDatagram<'_>> {
        while self.cursor < self.count {
            let meta = self.metadata[self.cursor];
            if !self.accepts(&meta) {
                self.cursor += 1;
                self.offset = 0;
                continue;
            }
            let start = self.cursor * self.slot_size + self.offset;
            let end = self.cursor * self.slot_size + (self.offset + meta.stride).min(meta.len);
            return Some(ReceivedDatagram {
                source: meta.addr,
                destination: meta.dst_ip,
                ecn: meta.ecn,
                bytes: &self.storage[start..end],
            });
        }
        None
    }

    pub(crate) fn advance_datagram(&mut self) {
        let meta = self.metadata[self.cursor];
        self.offset = (self.offset + meta.stride).min(meta.len);
        if self.offset == meta.len {
            self.cursor += 1;
            self.offset = 0;
        }
    }

    pub fn has_pending_datagrams(&self) -> bool {
        self.cursor < self.count
    }

    /// Invalid or possibly truncated aggregates, counted as buffers. Their
    /// exact original datagram count may be unavailable after truncation.
    pub fn rejected_buffers(&self) -> usize {
        self.metadata[..self.count]
            .iter()
            .filter(|meta| !self.accepts(meta))
            .count()
    }

    fn accepts(&self, meta: &RecvMeta) -> bool {
        meta.len > 0 && meta.len < self.slot_size && (1..=MAX_DATAGRAM).contains(&meta.stride)
    }

    /// Split GRO at its exact stride. A shorter final segment is preserved.
    /// Protocol validation still belongs to the socket worker.
    pub fn datagrams(&self) -> impl Iterator<Item = ReceivedDatagram<'_>> {
        self.metadata[..self.count]
            .iter()
            .enumerate()
            .filter_map(|(index, meta)| {
                if !self.accepts(meta) {
                    return None;
                }
                let start = index * self.slot_size;
                Some((&self.storage[start..start + meta.len], meta))
            })
            .flat_map(|(bytes, meta)| {
                bytes
                    .chunks(meta.stride)
                    .map(move |bytes| ReceivedDatagram {
                        source: meta.addr,
                        destination: meta.dst_ip,
                        ecn: meta.ecn,
                        bytes,
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::dart::{Datagram, decode, encode_sequenced_data};

    #[test]
    fn send_windows_preserve_exact_lengths_and_fallback_boundaries() {
        assert_eq!(send_window(17 * 64, 17, 64), (17 * 64, 64));
        assert_eq!(send_window(17 * 63 + 3, 17, 64), (17 * 63 + 3, 64));
        assert_eq!(send_window(17 * 64, 17, 1), (17, 1));
        assert_eq!(send_window(17 * 63 + 3, 17, 1), (17, 1));
        assert_eq!(send_window(3, 17, 1), (3, 1));
    }

    #[test]
    fn gro_boundaries_short_final_segment_and_metadata_are_preserved() {
        let mut batch = ReceiveBatch::new(64);
        batch.count = 1;
        let mut expected = Vec::new();
        let mut length = 0;
        for (sequence, body) in [&[1, 2][..], &[3, 4][..], &[][..]].into_iter().enumerate() {
            let size =
                encode_sequenced_data(1, sequence as u64, body, None, &mut batch.storage[length..])
                    .unwrap();
            expected.push(batch.storage[length..length + size].to_vec());
            length += size;
        }
        batch.metadata[0] = RecvMeta {
            len: length,
            stride: expected[0].len(),
            addr: "127.0.0.1:1".parse().unwrap(),
            ecn: Some(EcnCodepoint::Ce),
            dst_ip: Some("127.0.0.1".parse().unwrap()),
        };
        let datagrams: Vec<_> = batch.datagrams().collect();
        assert_eq!(datagrams.len(), 3);
        assert!(expected[2].len() < expected[0].len());
        for (datagram, packet) in datagrams.into_iter().zip(&expected) {
            assert_eq!(datagram.bytes, packet);
            assert_eq!(datagram.ecn, Some(EcnCodepoint::Ce));
            assert_eq!(datagram.destination, Some("127.0.0.1".parse().unwrap()));
            assert_eq!(datagram.source.port(), 1);
            assert!(matches!(decode(datagram.bytes), Some(Datagram::Data(_))));
        }
        assert_eq!(batch.rejected_buffers(), 0);
        // The worker stops after a budget and resumes the same batch.
        for expected in &expected {
            assert!(batch.has_pending_datagrams());
            assert_eq!(batch.peek_datagram().unwrap().bytes, expected);
            // Peeking after a runtime yield preserves the same segment.
            assert_eq!(batch.peek_datagram().unwrap().bytes, expected);
            assert_eq!(batch.peek_datagram().unwrap().ecn, Some(EcnCodepoint::Ce));
            batch.advance_datagram();
        }
        assert!(!batch.has_pending_datagrams());
        assert!(batch.pop_datagram().is_none());
    }

    #[test]
    fn full_arenas_invalid_strides_and_oversized_segments_are_rejected() {
        let mut batch = ReceiveBatch::new(64);
        batch.count = 1;
        for (length, stride) in [
            (0, 0),
            (1, 0),
            (1201, 1201),
            (batch.slot_size, 1),
            (batch.slot_size + 1, 17),
        ] {
            batch.metadata[0].len = length;
            batch.metadata[0].stride = stride;
            assert_eq!(batch.datagrams().count(), 0);
            assert_eq!(batch.rejected_buffers(), 1);
        }
        batch.metadata[0].len = MAX_DATAGRAM * 64;
        batch.metadata[0].stride = MAX_DATAGRAM;
        assert_eq!(batch.datagrams().count(), 64);
        assert_eq!(batch.rejected_buffers(), 0);
    }

    #[tokio::test]
    async fn loopback_offload_datagrams_keep_individual_lengths() {
        let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
        let sender = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
        let target = receiver.local_addr().unwrap();
        let wire = [0, 1, 2, 0, 3, 4, 0];
        let mut offset = 0;
        while offset < wire.len() {
            sender.writable().await.unwrap();
            match sender.try_send_segments(target, None, None, &wire[offset..], 3) {
                Ok(count) => offset = (offset + count * 3).min(wire.len()),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("send: {error}"),
            }
        }
        let mut batch = ReceiveBatch::new(receiver.gro_segments());
        let mut actual = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while actual.len() < 3 {
                receiver.readable().await.unwrap();
                match batch.try_receive(&receiver) {
                    Ok(_) => {
                        actual.extend(batch.datagrams().map(|datagram| datagram.bytes.to_vec()));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => panic!("receive: {error}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(actual, [vec![0, 1, 2], vec![0, 3, 4], vec![0]]);
        assert_eq!(
            batch.try_receive(&receiver).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(batch.received_buffers(), 0);
        assert_eq!(batch.datagrams().count(), 0);
    }

    #[tokio::test]
    async fn ipv6_destination_and_ecn_metadata_are_preserved() {
        let receiver = DartIo::new(std::net::UdpSocket::bind("[::1]:0").unwrap()).unwrap();
        let sender = DartIo::new(std::net::UdpSocket::bind("[::1]:0").unwrap()).unwrap();
        let target = receiver.local_addr().unwrap();
        sender.writable().await.unwrap();
        assert_eq!(
            sender
                .try_send_segments(target, None, Some(EcnCodepoint::Ect0), &[0, 7], 2)
                .unwrap(),
            1
        );
        let mut batch = ReceiveBatch::new(receiver.gro_segments());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                receiver.readable().await.unwrap();
                match batch.try_receive(&receiver) {
                    Ok(_) => break,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => panic!("receive: {error}"),
                }
            }
        })
        .await
        .unwrap();
        let datagram = batch.datagrams().next().unwrap();
        assert_eq!(datagram.bytes, &[0, 7]);
        assert!(datagram.source.is_ipv6());
        assert_eq!(datagram.source.port(), sender.local_addr().unwrap().port());
        if receiver.ecn_receive_supported(datagram.source.ip()) == Some(true) {
            assert_eq!(datagram.ecn, Some(EcnCodepoint::Ect0));
        }
        if let Some(destination) = datagram.destination {
            assert_eq!(destination, target.ip());
        }
    }

    #[tokio::test]
    async fn oversized_wire_datagrams_are_discarded() {
        let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .send_to(&[0; 2048], receiver.local_addr().unwrap())
            .unwrap();
        let mut batch = ReceiveBatch::new(receiver.gro_segments());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                receiver.readable().await.unwrap();
                match batch.try_receive(&receiver) {
                    Ok(_) => break,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => panic!("receive: {error}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(batch.received_buffers(), 1);
        assert_eq!(batch.rejected_buffers(), 1);
        assert_eq!(batch.datagrams().count(), 0);
    }

    #[tokio::test]
    async fn explicit_spin_can_receive_before_reactor_readiness_refreshes() {
        let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut batch = ReceiveBatch::new(receiver.gro_segments());
        assert_eq!(
            batch.try_receive(&receiver).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        sender
            .send_to(&[0, 7], receiver.local_addr().unwrap())
            .unwrap();
        // No runtime await has occurred since clearing cached readiness.
        batch.try_receive_spinning(&receiver).unwrap();
        assert_eq!(batch.pop_datagram().unwrap().bytes, &[0, 7]);
    }
}
