//! Datagram preparation and transmit bookkeeping.

use super::{DataBatch, FragmentBuffer, MIN_RTO, PROBE_INTERVAL, Prepared, Session, Transmit};
use crate::DartCongestion;
use crate::dart::{DATA_HEADER, MAX_BODY, MAX_DATAGRAM, Packet, Status, encode_packet};
use std::time::Duration;

impl<B: FragmentBuffer> Session<B> {
    /// Encode pending feedback without committing it; return its token and length.
    pub fn prepare_control(&self, now: Duration, output: &mut [u8]) -> Option<(Transmit, usize)> {
        let (transmit, packet) = if self.status_pending || now >= self.status_at {
            (
                Transmit::Status,
                Packet::Status(Status {
                    session: self.local,
                    serial: self.status_serial.checked_add(1)?,
                    ack: self.receive_next,
                    credit: self.receive_credit,
                    counts: self.ecn_counts,
                }),
            )
        } else if self.receive_next < self.gap_high
            && self.rx[self.index(self.receive_next)].is_none()
            && now >= self.nak_at
        {
            let mut end = self.receive_next + 1;
            while end < self.gap_high
                && end - self.receive_next < 64
                && self.rx[self.index(end)].is_none()
            {
                end += 1;
            }
            (
                Transmit::Nak {
                    first: self.receive_next,
                },
                Packet::Nak {
                    session: self.local,
                    first: self.receive_next,
                    count: (end - self.receive_next) as u32,
                },
            )
        } else if now >= self.probe_at {
            (
                Transmit::Probe,
                Packet::Probe {
                    session: self.remote,
                    next: self.send_cursor,
                },
            )
        } else {
            return None;
        };
        Some((transmit, encode_packet(packet, output)?))
    }

    /// Prepare the next ordered initial transmission or an explicitly indexed
    /// retained repair. No body is removed before acknowledgment.
    pub fn prepare_data(
        &mut self,
        sequence: u64,
        now: Duration,
        grouped: bool,
        output: &mut [u8],
    ) -> Option<(Transmit, usize)> {
        self.prepare_batch_data(sequence, DataBatch::new(now, grouped), output)
    }

    /// Prepare one packet in a batch. `batch.reserved_bytes` must include
    /// every preceding, uncommitted initial packet in this batch so the
    /// complete submission fits the congestion window. Repairs are submitted
    /// alone.
    pub fn prepare_batch_data(
        &mut self,
        sequence: u64,
        batch: DataBatch,
        output: &mut [u8],
    ) -> Option<(Transmit, usize)> {
        let remote = self.remote;
        let prepared =
            self.prepare_parts(sequence, batch.now, batch.grouped, batch.reserved_bytes)?;
        let length = prepared.encode(remote, sequence, output)?;
        Some((prepared.token, length))
    }

    /// Coalesce up to 128 already queued initial messages. An isolated
    /// message, payloads over 255 bytes, and every repair retain ordinary DATA
    /// framing. The caller's preallocated token capacity bounds the turn;
    /// this never grows it.
    /// `batch.reserved_bytes` includes conservative per-message wire costs
    /// across every prepared datagram and changes only for appended tokens.
    pub fn prepare_packed_data(
        &mut self,
        first: u64,
        batch: &mut DataBatch,
        output: &mut [u8],
        tokens: &mut Vec<Transmit>,
    ) -> Option<usize> {
        let max_tokens = tokens.capacity() - tokens.len();
        let (first_token, length, count) =
            self.prepare_packed_prefix(first, batch, output, max_tokens)?;

        for offset in 0..count {
            tokens.push(if offset == 0 {
                first_token
            } else {
                Transmit::Data {
                    sequence: first + offset as u64,
                    repair: false,
                }
            });
        }

        Some(length)
    }

    /// Prepare one datagram as a contiguous sequence prefix. Returns its first
    /// token, encoded length, and number of wire positions. Repairs occupy one
    /// position; initial prefixes can be committed without storing each token.
    pub fn prepare_packed_prefix(
        &mut self,
        first: u64,
        batch: &mut DataBatch,
        output: &mut [u8],
        max_tokens: usize,
    ) -> Option<(Transmit, usize, usize)> {
        let limit = crate::dart::MAX_PACKED_MESSAGES.min(max_tokens);
        if limit == 0 {
            return None;
        }

        let remote = self.remote;
        let count = self.plan_packed(first, limit, output)?;
        // PACKED needs two messages; a single one keeps DATA framing.
        if count < 2 {
            let prepared =
                self.prepare_parts(first, batch.now, batch.grouped, batch.reserved_bytes)?;
            let length = prepared.encode(remote, first, output)?;
            batch.reserved_bytes += prepared.bytes;
            return Some((prepared.token, length, 1));
        }

        // The length table sits between the tag and the session ID.
        output[1 + count..9 + count].copy_from_slice(&remote.to_le_bytes());
        output[9 + count..DATA_HEADER + count].copy_from_slice(&first.to_le_bytes());
        // Only initial transmissions at the send cursor may be packed.
        if first < self.peer_ack || first < self.send_cursor {
            return None;
        }
        if batch.now < self.pace_at {
            self.stats.congestion_stalls += 1;
            return None;
        }

        let (actual, mut length) = self.fill_packed(first, count, batch, output);
        if actual == 0 {
            return None;
        }
        if actual < count {
            // Congestion can shorten the planned prefix. Collapse the table,
            // preserving ordinary DATA framing when only one message fits.
            let table = if actual == 1 { 0 } else { actual };
            output.copy_within(1 + count..DATA_HEADER + count, 1 + table);
            output.copy_within(DATA_HEADER + count..length, DATA_HEADER + table);
            length -= count - table;
        }

        output[0] = actual as u8;
        Some((
            Transmit::Data {
                sequence: first,
                repair: false,
            },
            length,
            actual,
        ))
    }

    /// Plan the PACKED length table using cached wire lengths, so the common
    /// case writes every payload directly to its final position. Returns how
    /// many leading messages fit.
    #[inline]
    fn plan_packed(&self, first: u64, limit: usize, output: &mut [u8]) -> Option<usize> {
        let capacity = output.len().min(self.packed_mtu);
        let mut count = 0;
        let mut payload_bytes = 0;

        for offset in 0..limit {
            let sequence = first.checked_add(offset as u64)?;
            if sequence >= self.send_next || sequence >= self.peer_credit {
                break;
            }
            let Some(sent) = self.tx[self.index(sequence)].as_ref() else {
                break;
            };

            // Each table entry is one byte, so a packed payload is at most 255.
            let payload = sent.bytes - DATA_HEADER;
            if sent.repair
                || sent.fragment.is_some()
                || payload > usize::from(u8::MAX)
                || DATA_HEADER + count + 1 + payload_bytes + payload > capacity
            {
                break;
            }

            output[1 + count] = payload as u8;
            payload_bytes += payload;
            count += 1;
        }

        Some(count)
    }

    /// Copy planned payloads behind a `count`-entry table. Congestion or an
    /// unexpected shape can stop early. Returns packed messages and length.
    #[inline]
    fn fill_packed(
        &mut self,
        first: u64,
        count: usize,
        batch: &mut DataBatch,
        output: &mut [u8],
    ) -> (usize, usize) {
        let grouped = batch.grouped;
        let mut length = DATA_HEADER + count;
        let mut actual = 0;

        for offset in 0..count {
            let sequence = first + offset as u64;
            let sent = self.tx[self.index(sequence)]
                .as_ref()
                .expect("planned retained message");
            if self.config.congestion == DartCongestion::Adaptive
                && !sent.in_flight
                && self
                    .flight
                    .saturating_add(batch.reserved_bytes)
                    .saturating_add(sent.bytes)
                    > self.cwnd
            {
                self.stats.congestion_stalls += 1;
                break;
            }
            if sent.message.len() != 1 + usize::from(grouped) {
                break;
            }

            let payload = usize::from(output[1 + offset]);
            // The plan excludes fragments and repairs. Shape validation at
            // submission and the cached wire length bound this payload.
            let body = sent
                .message
                .part_slice(usize::from(grouped))
                .expect("retained body");
            let destination = &mut output[length..length + payload];
            if grouped {
                let group = sent.message.part_slice(0).expect("retained group");
                destination[0] = group.len() as u8;
                destination[1..=group.len()].copy_from_slice(group);
                destination[1 + group.len()..].copy_from_slice(body);
            } else {
                destination.copy_from_slice(body);
            }

            length += payload;
            batch.reserved_bytes += sent.bytes;
            actual += 1;
        }

        (actual, length)
    }

    #[inline]
    fn prepare_parts(
        &mut self,
        sequence: u64,
        now: Duration,
        grouped: bool,
        reserved_bytes: usize,
    ) -> Option<Prepared<'_>> {
        if sequence < self.peer_ack || sequence >= self.send_next {
            return None;
        }
        let index = self.index(sequence);
        let sent = self.tx[index].as_ref()?;
        let repair = sent.repair;
        if !repair && (sequence < self.send_cursor || sequence >= self.peer_credit) {
            if sequence >= self.peer_credit {
                self.stats.credit_stalls += 1;
            }
            return None;
        }
        // A paced scalar repair must pass even when later retained packets
        // exceed the reduced congestion window; ACKs cannot cross its gap.
        if now < self.pace_at
            || (self.config.congestion == DartCongestion::Adaptive
                && !repair
                && !sent.in_flight
                && self
                    .flight
                    .saturating_add(reserved_bytes)
                    .saturating_add(sent.bytes)
                    > self.cwnd)
        {
            self.stats.congestion_stalls += 1;
            return None;
        }
        if sent.message.len() != 1 + usize::from(grouped) {
            return None;
        }
        let whole_body = sent.message.part_slice(usize::from(grouped))?;
        let body = sent
            .fragment
            .map_or(whole_body, |part| &whole_body[part.start..part.end]);
        if body.len()
            > sent
                .fragment
                .map_or(MAX_BODY, |_| crate::dart::MAX_FRAGMENT_BODY)
        {
            return None;
        }
        let group = if grouped && sent.fragment.is_none_or(|part| part.length.is_some()) {
            Some(sent.message.part_slice(0)?)
        } else {
            None
        };
        if group.is_some_and(|group| group.is_empty() || group.len() > 255) {
            return None;
        }
        let prepared = Prepared {
            token: Transmit::Data { sequence, repair },
            body,
            group,
            bytes: sent.bytes,
            fragment: sent.fragment,
        };
        (sent.bytes <= MAX_DATAGRAM).then_some(prepared)
    }

    /// Search only the bounded retained window. The runtime also caps the
    /// number and bytes of packets prepared per turn.
    pub fn next_repair(&self) -> Option<u64> {
        if self.repairs == 0 {
            return None;
        }
        (self.peer_ack..self.send_cursor).find(|sequence| {
            self.tx[self.index(*sequence)]
                .as_ref()
                .is_some_and(|sent| sent.repair)
        })
    }

    /// Commit a prepared token only after the carrier accepts its datagram.
    #[inline]
    pub fn commit_transmit(&mut self, transmit: Transmit, now: Duration) {
        match transmit {
            Transmit::Data { sequence, repair } => {
                let index = self.index(sequence);
                let sent = self.tx[index].as_mut().expect("prepared retained message");
                if repair {
                    self.stats.retransmitted += 1;
                } else {
                    assert_eq!(sequence, self.send_cursor);
                    self.send_cursor += 1;
                }
                sent.first.get_or_insert(now);
                sent.last = now;
                sent.attempts = sent.attempts.saturating_add(1);
                if sent.repair {
                    self.repairs -= 1;
                }
                sent.repair = false;
                if !sent.in_flight {
                    self.flight += sent.bytes;
                    sent.in_flight = true;
                }
                if self.ecn_enabled && !repair {
                    self.marked_sent += 1;
                }
                if let Some(rate) = self.pacing_rate {
                    if self.pacing_bytes != sent.bytes {
                        self.pacing_bytes = sent.bytes;
                        self.pacing_delay = Duration::from_nanos(
                            (sent.bytes as u64 * 1_000_000_000 / rate.max(1)).max(1),
                        );
                    }
                    self.pace_at = self.pace_at.max(now) + self.pacing_delay;
                }
            }
            Transmit::Status => {
                self.advertised_credit = self.receive_credit;
                self.status_serial += 1;
                self.status_pending = false;
                self.status_messages = 0;
                // Idle sessions need only periodic liveness; pending receipt
                // enables the short feedback interval below.
                self.status_at = now + PROBE_INTERVAL;
            }
            Transmit::Nak { .. } => {
                self.nak_at = now + self.rtt.max(MIN_RTO);
            }
            Transmit::Probe => {
                self.probe_at = now + PROBE_INTERVAL;
            }
        }
    }

    /// True for an ordinary message or the final fragment of a large message.
    pub fn transmit_completes_message(&self, transmit: Transmit) -> bool {
        match transmit {
            Transmit::Data { sequence, .. } => self.tx[self.index(sequence)]
                .as_ref()
                .is_some_and(|sent| sent.fragment.is_none_or(|part| part.last)),
            _ => false,
        }
    }
}
