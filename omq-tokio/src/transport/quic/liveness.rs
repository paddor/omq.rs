//! Carrier liveness on the second bidirectional stream.
//!
//! Wire format, identical in both directions:
//!
//! ```text
//! preface: 'O' 'M' 'Q' 'L' version=0x01 0x00 0x00 0x00   (8 bytes, once)
//! record:  type (1 byte) || value (8 bytes, big-endian)  (9 bytes)
//!   0x01 PING  value = sender-chosen nonce
//!   0x02 PONG  value = nonce of the PING being answered
//! ```
//!
//! Anything else closes the connection with `CONTROL_ERROR`. Records carry
//! liveness only; they have no ordering relation to the data stream. Each
//! side answers only the newest unanswered PING, so a PING flood cannot
//! queue replies. At most one record is buffered for writing.

use std::time::Duration;

use omq_proto::Options;
use tokio::time::Instant;

use super::carrier::{Carrier, CtlRead, RecvHalf, SendHalf};
use super::code;

pub(super) const PREFACE: [u8; 8] = [b'O', b'M', b'Q', b'L', 1, 0, 0, 0];
const RECORD_LEN: usize = 9;
const PING: u8 = 0x01;
const PONG: u8 = 0x02;
/// Records handled before yielding to other tasks on this runtime.
const RECORD_BUDGET: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Record {
    Ping(u64),
    Pong(u64),
}

impl Record {
    fn encode(self) -> [u8; RECORD_LEN] {
        let (kind, value) = match self {
            Self::Ping(value) => (PING, value),
            Self::Pong(value) => (PONG, value),
        };
        let mut out = [0; RECORD_LEN];
        out[0] = kind;
        out[1..].copy_from_slice(&value.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8; RECORD_LEN]) -> Option<Self> {
        let value = u64::from_be_bytes(bytes[1..].try_into().expect("record value"));
        match bytes[0] {
            PING => Some(Self::Ping(value)),
            PONG => Some(Self::Pong(value)),
            _ => None,
        }
    }
}

/// Heartbeat options mapped onto carrier liveness. ZMTP PING/PONG on the
/// data stream is disabled for QUIC peers; this replaces it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LivenessConfig {
    /// PING cadence; `None` answers PINGs but never sends or times out.
    pub(crate) interval: Option<Duration>,
    /// Peer is dead when no record arrives for this long.
    pub(crate) timeout: Duration,
}

impl LivenessConfig {
    pub(crate) fn from_options(options: &Options) -> Self {
        let interval = options.heartbeat_interval.filter(|ivl| !ivl.is_zero());
        Self {
            interval,
            timeout: options
                .heartbeat_timeout
                .or(interval)
                .unwrap_or(Duration::MAX),
        }
    }
}

/// Cancellation-safe fixed-size reader. Stream reads are cancel-safe;
/// partial records stay in `buf` across select turns.
pub(super) struct RecordReader {
    stream: RecvHalf,
    buf: [u8; RECORD_LEN],
    filled: usize,
}

pub(super) enum ReadOutcome {
    Record(Record),
    /// Close with this application code.
    Violation(u32),
    /// The connection is already gone.
    Lost,
}

impl RecordReader {
    pub(super) fn new(stream: RecvHalf) -> Self {
        Self {
            stream,
            buf: [0; RECORD_LEN],
            filled: 0,
        }
    }

    pub(super) async fn next(&mut self) -> ReadOutcome {
        let need = RECORD_LEN;
        while self.filled < need {
            match self.stream.read(&mut self.buf[self.filled..need]).await {
                CtlRead::Data(n) => self.filled += n,
                CtlRead::Lost => return ReadOutcome::Lost,
                // The liveness stream must live as long as the peer.
                CtlRead::Finished | CtlRead::Failed => {
                    return ReadOutcome::Violation(code::CONTROL_ERROR);
                }
            }
        }
        self.filled = 0;
        match Record::decode(&self.buf) {
            Some(record) => ReadOutcome::Record(record),
            None => ReadOutcome::Violation(code::CONTROL_ERROR),
        }
    }
}

/// Read and validate the peer preface during transport setup.
pub(super) async fn read_preface(stream: &mut RecvHalf) -> omq_proto::Result<()> {
    let mut buf = [0; PREFACE.len()];
    stream.read_exact(&mut buf).await?;
    if buf == PREFACE {
        Ok(())
    } else {
        Err(omq_proto::Error::HandshakeFailed(
            "invalid QUIC liveness preface".into(),
        ))
    }
}

/// Write the local preface before exposing the data stream to ZMTP.
pub(super) async fn write_preface(stream: &mut SendHalf) -> omq_proto::Result<()> {
    stream.raise_priority();
    let mut written = 0;
    while written < PREFACE.len() {
        match stream.write(&PREFACE[written..]).await {
            Some(n) if n > 0 => written += n,
            _ => {
                return Err(omq_proto::Error::HandshakeFailed(
                    "QUIC liveness stream ended during preface".into(),
                ));
            }
        }
    }
    Ok(())
}

/// One buffered output unit. A newer PONG replaces an unsent older one;
/// output is refilled only after the previous unit is fully written.
struct RecordWriter {
    stream: SendHalf,
    out: [u8; RECORD_LEN],
    len: usize,
    written: usize,
    pong: Option<u64>,
    ping: Option<u64>,
}

impl RecordWriter {
    fn new(stream: SendHalf) -> Self {
        Self {
            stream,
            out: [0; RECORD_LEN],
            len: 0,
            written: 0,
            pong: None,
            ping: None,
        }
    }

    fn refill(&mut self) {
        if self.written < self.len {
            return;
        }
        let record = if let Some(nonce) = self.pong.take() {
            Record::Pong(nonce)
        } else if let Some(nonce) = self.ping.take() {
            Record::Ping(nonce)
        } else {
            return;
        };
        self.out = record.encode();
        self.len = RECORD_LEN;
        self.written = 0;
    }

    fn has_output(&self) -> bool {
        self.written < self.len
    }

    /// Cancel-safe: stream writes admit bytes atomically.
    async fn write(&mut self) -> bool {
        match self.stream.write(&self.out[self.written..self.len]).await {
            Some(n) => {
                self.written += n;
                true
            }
            None => false,
        }
    }
}

/// Serve liveness until the connection ends. Closes the connection with a
/// specific code on liveness or protocol failure.
pub(super) async fn run(carrier: Carrier, send: SendHalf, recv: RecvHalf, config: LivenessConfig) {
    if let Some(code) = serve(&carrier, send, recv, config).await {
        carrier.close(code);
    }
}

async fn serve(
    carrier: &Carrier,
    send: SendHalf,
    recv: RecvHalf,
    config: LivenessConfig,
) -> Option<u32> {
    let mut reader = RecordReader::new(recv);
    let mut writer = RecordWriter::new(send);
    writer.stream.raise_priority();
    let start = Instant::now();
    let mut unanswered_since = None;
    let mut next_ping = config.interval.and_then(|ivl| start.checked_add(ivl));
    let mut nonce = 0u64;
    let mut budget = RECORD_BUDGET;
    loop {
        writer.refill();
        let death =
            unanswered_since.and_then(|sent_at: Instant| sent_at.checked_add(config.timeout));
        tokio::select! {
            outcome = reader.next() => {
                match outcome {
                    ReadOutcome::Record(Record::Ping(value)) => writer.pong = Some(value),
                    ReadOutcome::Record(Record::Pong(_)) => {}
                    ReadOutcome::Violation(code) => return Some(code),
                    ReadOutcome::Lost => return None,
                }
                unanswered_since = None;
                budget -= 1;
                if budget == 0 {
                    budget = RECORD_BUDGET;
                    tokio::task::yield_now().await;
                }
            }
            ok = writer.write(), if writer.has_output() => {
                if !ok {
                    return None;
                }
                if writer.out[0] == 1 && writer.written == writer.len {
                    unanswered_since.get_or_insert_with(Instant::now);
                }
            }
            () = sleep_until_opt(next_ping), if next_ping.is_some() => {
                nonce = nonce.wrapping_add(1);
                writer.ping = Some(nonce);
                // A delayed runtime sends one probe, not a backlog of missed
                // ticks. Unrepresentable deadlines behave as infinite waits.
                next_ping = config.interval.and_then(|ivl| Instant::now().checked_add(ivl));
            }
            () = sleep_until_opt(death), if death.is_some() => {
                return Some(code::LIVENESS_TIMEOUT);
            }
            arrived = carrier.unexpected_stream() => {
                return arrived.then_some(code::UNEXPECTED_STREAM);
            }
        }
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_reject_unknown_types() {
        for record in [Record::Ping(0), Record::Ping(u64::MAX), Record::Pong(7)] {
            assert_eq!(Record::decode(&record.encode()), Some(record));
        }
        assert_eq!(
            Record::Ping(0x0102_0304_0506_0708).encode(),
            [1, 1, 2, 3, 4, 5, 6, 7, 8]
        );
        let mut unknown = Record::Pong(1).encode();
        unknown[0] = 3;
        assert_eq!(Record::decode(&unknown), None);
        unknown[0] = 0;
        assert_eq!(Record::decode(&unknown), None);
    }

    #[test]
    fn heartbeat_options_map_to_liveness() {
        let mut options = Options::default();
        let off = LivenessConfig::from_options(&options);
        assert!(off.interval.is_none());
        options.heartbeat_interval = Some(Duration::from_millis(100));
        let on = LivenessConfig::from_options(&options);
        assert_eq!(on.interval, Some(Duration::from_millis(100)));
        assert_eq!(on.timeout, Duration::from_millis(100));
        options.heartbeat_timeout = Some(Duration::from_millis(300));
        assert_eq!(
            LivenessConfig::from_options(&options).timeout,
            Duration::from_millis(300)
        );
    }
}
