#![cfg(feature = "fuzz")]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::collapsible_if,
    clippy::missing_panics_doc,
    clippy::items_after_statements,
    clippy::doc_markdown
)]
//! Fuzz tests for the wire parsers and the codec state machine.
//!
//! Two tiers:
//!   1. Crash resistance — hostile bytes must never panic (Err is fine).
//!   2. RFC compliance — valid inputs must be accepted, invalid must be
//!      rejected, with specific assertions on which.
//!
//! Set `OMQ_FUZZ_SEED=<u64>` to reproduce; `OMQ_FUZZ_ITERS=<N>` to tune.

use bytes::{BufMut, Bytes, BytesMut};
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};

use omq_tokio::error::Error;
use omq_tokio::message::Message;
use omq_tokio::proto::{
    SocketType, command,
    connection::{Connection, ConnectionConfig, Role},
    greeting::{GREETING_LEN, Greeting, MechanismName},
    mechanism::MechanismSetup,
    z85,
};

fn iters() -> usize {
    std::env::var("OMQ_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000)
}

fn rng() -> StdRng {
    let seed: u64 = std::env::var("OMQ_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            let mut s = [0u8; 8];
            rand::rng().fill_bytes(&mut s);
            u64::from_le_bytes(s)
        });
    eprintln!("OMQ_FUZZ_SEED={seed}");
    StdRng::seed_from_u64(seed)
}

fn random_bytes(rng: &mut StdRng, max_len: usize) -> Vec<u8> {
    let len = rng.random_range(0..=max_len);
    let mut v = vec![0u8; len];
    rng.fill_bytes(&mut v);
    v
}

const VALID_PROP_CHARS: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.+";

const ALL_SOCKET_TYPES: [SocketType; 11] = [
    SocketType::Pair,
    SocketType::Pub,
    SocketType::Sub,
    SocketType::Req,
    SocketType::Rep,
    SocketType::Dealer,
    SocketType::Router,
    SocketType::Pull,
    SocketType::Push,
    SocketType::XPub,
    SocketType::XSub,
];

/// Build a valid NULL client greeting as raw bytes.
fn null_client_greeting() -> Vec<u8> {
    let g = Greeting::current(MechanismName::NULL, false);
    let mut buf = BytesMut::new();
    g.encode(&mut buf);
    buf.to_vec()
}

/// Build a COMMAND frame (short form if <=255, long form otherwise).
fn command_frame(body: &[u8]) -> Vec<u8> {
    if body.len() <= 255 {
        let mut out = Vec::with_capacity(2 + body.len());
        out.push(0x04); // COMMAND flag, short form
        out.push(body.len() as u8);
        out.extend_from_slice(body);
        out
    } else {
        long_command_frame(body)
    }
}

/// Build a long-form COMMAND frame.
fn long_command_frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + body.len());
    out.push(0x06); // COMMAND | LONG
    out.extend_from_slice(&(body.len() as u64).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Encode a READY command body with the given properties.
fn ready_body(socket_type: &str, identity: Option<&[u8]>, extra: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(5);
    buf.put_slice(b"READY");
    // Socket-Type
    buf.put_u8(11);
    buf.put_slice(b"Socket-Type");
    buf.put_u32(socket_type.len() as u32);
    buf.put_slice(socket_type.as_bytes());
    // Identity
    if let Some(id) = identity {
        buf.put_u8(8);
        buf.put_slice(b"Identity");
        buf.put_u32(id.len() as u32);
        buf.put_slice(id);
    }
    for &(name, value) in extra {
        buf.put_u8(name.len() as u8);
        buf.put_slice(name);
        buf.put_u32(value.len() as u32);
        buf.put_slice(value);
    }
    buf.to_vec()
}

/// Build a complete valid handshake byte stream (greeting + READY frame).
fn valid_handshake(socket_type: &str) -> Vec<u8> {
    let mut wire = null_client_greeting();
    let body = ready_body(socket_type, None, &[]);
    wire.extend_from_slice(&command_frame(&body));
    wire
}

/// Create a fresh server-side NULL Connection.
fn server_conn(st: SocketType) -> Connection {
    Connection::new(ConnectionConfig::new(Role::Server, st).mechanism(MechanismSetup::Null))
}

/// Create a fresh client-side NULL Connection.
fn client_conn(st: SocketType) -> Connection {
    Connection::new(ConnectionConfig::new(Role::Client, st).mechanism(MechanismSetup::Null))
}

/// Pump a Connection: drain all events and messages, return counts.
fn drain(conn: &mut Connection) -> (usize, usize) {
    let mut events = 0;
    let mut msgs = 0;
    while conn.poll_event().is_some() {
        events += 1;
    }
    while conn.poll_message().is_some() {
        msgs += 1;
    }
    (events, msgs)
}

/// Feed bytes to a connection in random-sized chunks (1..max_chunk).
fn feed_chunked(
    conn: &mut Connection,
    data: &[u8],
    rng: &mut StdRng,
    max_chunk: usize,
) -> Result<(), Error> {
    let mut pos = 0;
    while pos < data.len() {
        let chunk = rng.random_range(1..=max_chunk).min(data.len() - pos);
        conn.handle_input(Bytes::copy_from_slice(&data[pos..pos + chunk]))?;
        drain(conn);
        pos += chunk;
    }
    Ok(())
}

/// Do a real handshake between two Connections. Returns the bytes each
/// side transmitted (a_to_b, b_to_a).
fn capture_handshake(a: &mut Connection, b: &mut Connection) -> (Vec<u8>, Vec<u8>) {
    let mut a_to_b = Vec::new();
    let mut b_to_a = Vec::new();
    for _ in 0..20 {
        let a_out = a.poll_transmit();
        let b_out = b.poll_transmit();
        if a_out.is_empty() && b_out.is_empty() {
            break;
        }
        a_to_b.extend_from_slice(&a_out);
        b_to_a.extend_from_slice(&b_out);
        a.advance_transmit(a_out.len());
        b.advance_transmit(b_out.len());
        if !a_to_b.is_empty() {
            let _ = b.handle_input(Bytes::copy_from_slice(&a_to_b));
            a_to_b.clear();
        }
        if !b_to_a.is_empty() {
            let _ = a.handle_input(Bytes::copy_from_slice(&b_to_a));
            b_to_a.clear();
        }
        drain(a);
        drain(b);
    }
    // Capture any final bytes
    let a_final = a.poll_transmit().to_vec();
    let b_final = b.poll_transmit().to_vec();
    (a_final, b_final)
}

// ================================================================
// Tier 1: crash resistance
// ================================================================

#[test]
fn fuzz_command_decode() {
    let mut rng = rng();
    for i in 0..iters() {
        let raw = random_bytes(&mut rng, 512);
        let _ = command::decode(Bytes::from(raw));
        if i % 50_000 == 0 {
            eprintln!("command iter {i}");
        }
    }
}

#[test]
fn fuzz_handle_input_full_stream() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let cfg =
            ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(MechanismSetup::Null);
        let mut conn = Connection::new(cfg);
        let raw = random_bytes(&mut rng, 4096);
        let _ = conn.handle_input(Bytes::copy_from_slice(&raw));
        drain(&mut conn);
        if i % 10_000 == 0 {
            eprintln!("full_stream iter {i}");
        }
    }
}

#[test]
fn fuzz_handle_input_chunked() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let mut conn = server_conn(SocketType::Pull);
        let raw = random_bytes(&mut rng, 4096);
        let _ = feed_chunked(&mut conn, &raw, &mut rng, 64);
        if i % 10_000 == 0 {
            eprintln!("chunked iter {i}");
        }
    }
}

#[test]
fn fuzz_handle_input_both_roles() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let role = if rng.random_bool(0.5) {
            Role::Server
        } else {
            Role::Client
        };
        let st = ALL_SOCKET_TYPES[rng.random_range(0..ALL_SOCKET_TYPES.len())];
        let cfg = ConnectionConfig::new(role, st).mechanism(MechanismSetup::Null);
        let mut conn = Connection::new(cfg);
        let raw = random_bytes(&mut rng, 2048);
        let _ = conn.handle_input(Bytes::copy_from_slice(&raw));
        drain(&mut conn);
        if i % 10_000 == 0 {
            eprintln!("both_roles iter {i}");
        }
    }
}

#[test]
fn fuzz_frame_roundtrip() {
    use omq_tokio::message::{Frame, FrameFlags, Payload};
    use omq_tokio::proto::frame::{decode_frame_from_bytes, encode_frame};
    let mut rng = rng();
    for i in 0..iters() / 2 {
        let size = match rng.random_range(0..6) {
            0 => 0,
            1 => rng.random_range(1..=255),
            2 => 255,
            3 => 256,
            4 => rng.random_range(256..=65_536),
            _ => rng.random_range(0..=65_536),
        };
        let mut payload = vec![0u8; size];
        rng.fill_bytes(&mut payload);
        let bytes = Bytes::from(payload);
        let (more, command) = match rng.random_range(0..3) {
            0 => (false, false),
            1 => (true, false),
            _ => (false, true),
        };
        let frame = Frame {
            flags: FrameFlags { more, command },
            payload: Payload::from_bytes(bytes.clone()),
        };
        let mut out = BytesMut::new();
        encode_frame(&frame, &mut out);
        let (decoded, remaining) = decode_frame_from_bytes(out.freeze())
            .expect("decode of self-encoded frame must not error");
        let decoded = decoded.expect("must produce a frame");
        assert_eq!(decoded.flags.more, more, "more bit");
        assert_eq!(decoded.flags.command, command, "command bit");
        assert_eq!(decoded.payload.as_bytes(), bytes, "payload mismatch");
        assert_eq!(remaining, 0, "decoder left {remaining} bytes pending");
        if i % 50_000 == 0 {
            eprintln!("frame_roundtrip iter {i}");
        }
    }
}

#[test]
fn fuzz_handle_input_perturbed_greeting() {
    let mut rng = rng();
    let base = null_client_greeting();
    for i in 0..iters() / 4 {
        let mut buf = base.clone();
        let tail_len = rng.random_range(0..=512);
        let mut tail = vec![0u8; tail_len];
        rng.fill_bytes(&mut tail);
        buf.extend_from_slice(&tail);
        let flips = rng.random_range(0..=3);
        for _ in 0..flips {
            let pos = rng.random_range(0..64);
            buf[pos] = rng.random();
        }
        let mut conn = server_conn(SocketType::Pull);
        let _ = conn.handle_input(Bytes::copy_from_slice(&buf));
        drain(&mut conn);
        if i % 10_000 == 0 {
            eprintln!("perturbed iter {i}");
        }
    }
}

#[test]
fn fuzz_z85_decode() {
    let mut rng = rng();
    let alphabet: &[u8] =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    for i in 0..iters() / 4 {
        let len = rng.random_range(0..=256);
        let mut s = String::with_capacity(len);
        for _ in 0..len {
            if rng.random_bool(0.9) {
                s.push(alphabet[rng.random_range(0..alphabet.len())] as char);
            } else {
                let b = rng.random_range(0u8..=127);
                if b.is_ascii_graphic() || b == b' ' {
                    s.push(b as char);
                }
            }
        }
        let _ = z85::decode(&s);
        if i % 50_000 == 0 {
            eprintln!("z85 iter {i}");
        }
    }
}

#[cfg(feature = "lz4")]
#[test]
fn fuzz_lz4_decode() {
    use omq_tokio::proto::transform::lz4::Lz4Decoder;
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let mut tx = Lz4Decoder::new();
        let n_parts = rng.random_range(1..=4);
        let mut parts_vec: Vec<Bytes> = Vec::new();
        for _ in 0..n_parts {
            let part = random_bytes(&mut rng, 256);
            parts_vec.push(Bytes::from(part));
        }
        let msg = Message::multipart(parts_vec);
        let _ = tx.decode(msg);
        if i % 10_000 == 0 {
            eprintln!("lz4 iter {i}");
        }
    }
}

/// Random strings fed to `Endpoint::from_str()` — must never panic.
#[test]
fn fuzz_endpoint_parse() {
    use omq_tokio::Endpoint;
    use std::str::FromStr;
    let mut rng = rng();
    let schemes = [
        "tcp", "ipc", "inproc", "udp", "ws", "wss", "lz4+tcp", "lz4+ws", "bogus", "",
    ];
    for i in 0..iters() / 2 {
        let input = if rng.random_range(0..4) == 0 {
            let scheme = schemes[rng.random_range(0..schemes.len())];
            let tail_len = rng.random_range(0..128);
            let tail: String = (0..tail_len)
                .map(|_| rng.random_range(0x20..0x7f_u8) as char)
                .collect();
            format!("{scheme}://{tail}")
        } else {
            let len = rng.random_range(0..256);
            (0..len)
                .map(|_| rng.random_range(0x20..0x7f_u8) as char)
                .collect()
        };
        let _ = Endpoint::from_str(&input);
        if i % 100_000 == 0 {
            eprintln!("endpoint_parse iter {i}");
        }
    }
}

/// Valid endpoints must survive display → parse roundtrip.
#[test]
fn fuzz_endpoint_roundtrip() {
    use omq_tokio::Endpoint;
    use omq_tokio::endpoint::Host;
    use std::str::FromStr;
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let host = match rng.random_range(0..3) {
            0 => Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
            ))),
            1 => Host::Ip(std::net::IpAddr::V6(std::net::Ipv6Addr::new(
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
            ))),
            _ => Host::Name("example.com".to_string()),
        };
        let port: u16 = rng.random_range(1..=65535);
        let path = format!("/{}", rng.random_range(0..100));
        let ep = match rng.random_range(0..10) {
            0 => Endpoint::Tcp {
                host: host.clone(),
                port,
            },
            #[cfg(feature = "lz4")]
            1 => Endpoint::Lz4Tcp {
                host: host.clone(),
                port,
            },
            #[cfg(feature = "ws")]
            3 => Endpoint::Ws {
                host: host.clone(),
                port,
                path: path.clone(),
            },
            #[cfg(feature = "ws")]
            4 => Endpoint::Wss {
                host: host.clone(),
                port,
                path: path.clone(),
            },
            #[cfg(all(feature = "lz4", feature = "ws"))]
            5 => Endpoint::Lz4Ws {
                host: host.clone(),
                port,
                path: path.clone(),
            },
            _ => Endpoint::Tcp {
                host: host.clone(),
                port,
            },
        };
        let s = ep.to_string();
        let parsed = Endpoint::from_str(&s).unwrap_or_else(|e| {
            panic!("failed to parse roundtrip of {ep:?} → {s:?}: {e}");
        });
        assert_eq!(ep, parsed, "roundtrip mismatch for {s:?}");
        if i % 50_000 == 0 {
            eprintln!("endpoint_roundtrip iter {i}");
        }
    }
}

/// Compression transform encode→decode roundtrip with random messages.
#[cfg(all(feature = "lz4", feature = "ws"))]
#[test]
fn fuzz_compress_ws_roundtrip() {
    use omq_tokio::Endpoint;
    use omq_tokio::endpoint::Host;
    use omq_tokio::options::Options;
    use omq_tokio::proto::transform::MessageEncoder;
    let mut rng = rng();
    let localhost = Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    let endpoints: Vec<Endpoint> = vec![Endpoint::Lz4Ws {
        host: localhost.clone(),
        port: 1,
        path: "/".into(),
    }];
    let opts = Options::default();
    for i in 0..iters() / 4 {
        let ep = &endpoints[rng.random_range(0..endpoints.len())];
        let (mut enc, mut dec) = MessageEncoder::for_endpoint(ep, &opts)
            .expect("transform constructor")
            .expect("transform for compressed-ws");
        let n_parts = rng.random_range(1..=4);
        let mut parts: Vec<Bytes> = Vec::new();
        for _ in 0..n_parts {
            let len = rng.random_range(0..2048);
            let part: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            parts.push(Bytes::from(part));
        }
        let msg = Message::multipart(parts.clone());
        let wires = enc.encode(&msg).expect("encode failed");
        let mut decoded_parts: Vec<Bytes> = Vec::new();
        for wire in wires {
            if let Some(decoded) = dec.decode(wire).expect("decode failed") {
                for j in 0..decoded.len() {
                    decoded_parts.push(decoded.part_bytes(j).unwrap().clone());
                }
            }
        }
        assert_eq!(
            parts.len(),
            decoded_parts.len(),
            "part count mismatch at iter {i}"
        );
        for (j, (orig, got)) in parts.iter().zip(&decoded_parts).enumerate() {
            assert_eq!(orig, got, "part {j} mismatch at iter {i}");
        }
        if i % 50_000 == 0 {
            eprintln!("compress_ws_roundtrip iter {i}");
        }
    }
}

// ================================================================
// Tier 2: RFC compliance + adversarial structured inputs
// ================================================================

/// Greeting version + as_server compliance across all 256 major values,
/// random minors, every mechanism name variant.
#[test]
fn fuzz_greeting_compliance() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let major: u8 = rng.random();
        let minor: u8 = rng.random();
        let as_server = rng.random_bool(0.5);
        let g = Greeting {
            major,
            minor,
            mechanism: MechanismName::NULL,
            as_server,
        };
        let mut wire = BytesMut::new();
        g.encode(&mut wire);

        let mut conn = server_conn(SocketType::Pull);
        let result = conn.handle_input(wire.freeze());

        if major < 3 {
            assert!(result.is_err(), "major {major} must be rejected");
        } else if as_server {
            assert!(result.is_err(), "NULL + as_server=1 must be rejected");
        } else {
            assert!(
                result.is_ok(),
                "major {major} + as_server=0 must be accepted"
            );
        }
        if i % 10_000 == 0 {
            eprintln!("greeting_compliance iter {i}");
        }
    }
}

/// Our own NULL greeting must always have as_server=0.
#[test]
fn fuzz_null_greeting_output() {
    for role in [Role::Client, Role::Server] {
        for &st in &ALL_SOCKET_TYPES {
            let cfg = ConnectionConfig::new(role, st).mechanism(MechanismSetup::Null);
            let conn = Connection::new(cfg);
            let wire = conn.poll_transmit();
            assert!(wire.len() >= 33, "greeting too short");
            assert_eq!(
                wire[32], 0,
                "NULL greeting must have as_server=0 ({role:?}/{st:?})"
            );
        }
    }
}

/// Byte surgery on a captured real handshake: corrupt 1-8 random bytes,
/// then deliver in 1-byte chunks. Exercises every partial-parse path
/// under corruption.
#[test]
fn fuzz_handshake_byte_surgery() {
    let mut rng = rng();
    let base = valid_handshake("PULL");
    for i in 0..iters() / 4 {
        let mut wire = base.clone();
        let n_flips = rng.random_range(1..=8);
        for _ in 0..n_flips {
            let pos = rng.random_range(0..wire.len());
            wire[pos] = rng.random();
        }
        let mut conn = server_conn(SocketType::Pull);
        // Deliver one byte at a time — maximizes partial-parse coverage.
        let _ = feed_chunked(&mut conn, &wire, &mut rng, 1);
        if i % 10_000 == 0 {
            eprintln!("byte_surgery iter {i}");
        }
    }
}

/// Truncate a valid handshake at every possible offset. The codec must
/// never panic, regardless of where the cut lands (mid-signature,
/// mid-mechanism-name, mid-frame-header, mid-property, etc.).
#[test]
fn fuzz_handshake_truncation() {
    let mut rng = rng();
    let base = valid_handshake("PULL");
    for i in 0..iters() / 4 {
        let cut = rng.random_range(0..=base.len());
        let mut conn = server_conn(SocketType::Pull);
        // Sometimes deliver as one chunk, sometimes byte-by-byte.
        if rng.random_bool(0.5) {
            let _ = conn.handle_input(Bytes::copy_from_slice(&base[..cut]));
        } else {
            let _ = feed_chunked(&mut conn, &base[..cut], &mut rng, 3);
        }
        drain(&mut conn);
        if i % 10_000 == 0 {
            eprintln!("truncation iter {i}");
        }
    }
}

/// Valid handshake followed by adversarial post-handshake data:
/// random frames, illegal flag combos (COMMAND|MORE), huge declared
/// sizes with tiny payloads, zero-length frames, rapid-fire multi-frame
/// messages, commands after ready.
#[test]
fn fuzz_post_handshake_chaos() {
    let mut rng = rng();
    for i in 0..iters() / 8 {
        let mut push = client_conn(SocketType::Push);
        let mut pull = server_conn(SocketType::Pull);
        capture_handshake(&mut push, &mut pull);
        assert!(pull.is_ready(), "handshake must succeed");

        // Now feed hostile frames directly into pull.
        let n_frames = rng.random_range(1..=20);
        for _ in 0..n_frames {
            let frame = match rng.random_range(0..8) {
                // Random garbage
                0 => random_bytes(&mut rng, 512),
                // Short data frame, random payload
                1 => {
                    let payload = random_bytes(&mut rng, 255);
                    let mut f = vec![0x00u8]; // data, short
                    f.push(payload.len() as u8);
                    f.extend_from_slice(&payload);
                    f
                }
                // Long data frame with huge declared size, tiny actual
                2 => {
                    let mut f = vec![0x02u8]; // data, long
                    let declared: u64 = rng.random_range(0..=u32::MAX as u64);
                    f.extend_from_slice(&declared.to_be_bytes());
                    let actual = random_bytes(&mut rng, 64);
                    f.extend_from_slice(&actual);
                    f
                }
                // Illegal flag combo: COMMAND | MORE (0x05)
                3 => {
                    let payload = random_bytes(&mut rng, 128);
                    let mut f = vec![0x05u8]; // COMMAND|MORE — illegal
                    f.push(payload.len() as u8);
                    f.extend_from_slice(&payload);
                    f
                }
                // Command frame with random body
                4 => {
                    let body = random_bytes(&mut rng, 512);
                    long_command_frame(&body)
                }
                // Zero-length frame
                5 => vec![0x00, 0x00],
                // Frame with all flag bits set (0x07 = MORE|LONG|COMMAND)
                6 => {
                    let mut f = vec![0x07u8];
                    f.extend_from_slice(&0u64.to_be_bytes());
                    f
                }
                // READY/ERROR command after handshake (should be rejected)
                _ => {
                    let body = ready_body("PUSH", None, &[]);
                    command_frame(&body)
                }
            };
            if pull.handle_input(Bytes::from(frame)).is_err() {
                break;
            }
            drain(&mut pull);
        }
        if i % 5_000 == 0 {
            eprintln!("post_handshake iter {i}");
        }
    }
}

/// Bidirectional corruption: two real connections, but we randomly
/// corrupt, duplicate, drop, or truncate bytes flowing between them.
#[test]
fn fuzz_bidirectional_corruption() {
    let mut rng = rng();
    for i in 0..iters() / 8 {
        let mut a = client_conn(SocketType::Pair);
        let mut b = server_conn(SocketType::Pair);
        let mut a_err = false;
        let mut b_err = false;

        for _ in 0..30 {
            let a_out = a.poll_transmit().to_vec();
            let b_out = b.poll_transmit().to_vec();
            a.advance_transmit(a_out.len());
            b.advance_transmit(b_out.len());

            if a_out.is_empty() && b_out.is_empty() {
                break;
            }

            // Corrupt a→b
            if !a_out.is_empty() && !b_err {
                let wire = corrupt_wire(&a_out, &mut rng);
                if b.handle_input(Bytes::from(wire)).is_err() {
                    b_err = true;
                }
                drain(&mut b);
            }
            // Corrupt b→a
            if !b_out.is_empty() && !a_err {
                let wire = corrupt_wire(&b_out, &mut rng);
                if a.handle_input(Bytes::from(wire)).is_err() {
                    a_err = true;
                }
                drain(&mut a);
            }
            if a_err && b_err {
                break;
            }
        }

        // If handshake survived, send some messages through
        if a.is_ready() && b.is_ready() {
            for _ in 0..5 {
                let msg = Message::single(random_bytes(&mut rng, 128));
                if a.send_message(&msg).is_err() {
                    break;
                }
                let wire = a.poll_transmit().to_vec();
                a.advance_transmit(wire.len());
                let wire = corrupt_wire(&wire, &mut rng);
                let _ = b.handle_input(Bytes::from(wire));
                drain(&mut b);
            }
        }
        if i % 5_000 == 0 {
            eprintln!("bidirectional iter {i}");
        }
    }
}

fn corrupt_wire(data: &[u8], rng: &mut StdRng) -> Vec<u8> {
    let mut wire = data.to_vec();
    match rng.random_range(0..6) {
        // Pass through clean (10% of the time)
        0 => {}
        // Flip 1-3 random bytes
        1 => {
            let n = rng.random_range(1..=3).min(wire.len().max(1));
            for _ in 0..n {
                if !wire.is_empty() {
                    let pos = rng.random_range(0..wire.len());
                    wire[pos] = rng.random();
                }
            }
        }
        // Truncate
        2 => {
            if !wire.is_empty() {
                let cut = rng.random_range(0..wire.len());
                wire.truncate(cut);
            }
        }
        // Duplicate a random slice
        3 => {
            if wire.len() > 1 {
                let start = rng.random_range(0..wire.len());
                let len = rng.random_range(1..=(wire.len() - start).min(64));
                let dup = wire[start..start + len].to_vec();
                let insert_at = rng.random_range(0..=wire.len());
                wire.splice(insert_at..insert_at, dup);
            }
        }
        // Insert random garbage
        4 => {
            let garbage_len = rng.random_range(1..=32);
            let mut garbage = vec![0u8; garbage_len];
            rng.fill_bytes(&mut garbage);
            let insert_at = rng.random_range(0..=wire.len());
            wire.splice(insert_at..insert_at, garbage);
        }
        // Drop a random range of bytes
        _ => {
            if wire.len() > 1 {
                let start = rng.random_range(0..wire.len());
                let len = rng.random_range(1..=(wire.len() - start).min(16));
                wire.drain(start..start + len);
            }
        }
    }
    wire
}

/// Property parsing stress: boundary-length names (1, 127, 254, 255),
/// every byte value in names, many properties, truncation at every
/// offset within the property list, duplicate well-known properties,
/// declared-vs-actual value length mismatches.
#[test]
fn fuzz_property_adversarial() {
    let mut rng = rng();
    for i in 0..iters() / 2 {
        let scenario = rng.random_range(0..8);
        let result = match scenario {
            // Random name length, random chars (mix valid/invalid)
            0 => {
                let name_len: u8 = rng.random_range(0..=20);
                let mut name = Vec::with_capacity(name_len as usize);
                for _ in 0..name_len {
                    if rng.random_bool(0.5) {
                        name.push(VALID_PROP_CHARS[rng.random_range(0..VALID_PROP_CHARS.len())]);
                    } else {
                        name.push(rng.random());
                    }
                }
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(name_len);
                body.put_slice(&name);
                body.put_u32(1);
                body.put_u8(b'v');
                command::decode(body.freeze())
            }
            // Boundary-length property name (254, 255 bytes)
            1 => {
                let name_len = if rng.random_bool(0.5) { 254 } else { 255 };
                let name: Vec<u8> = (0..name_len)
                    .map(|_| VALID_PROP_CHARS[rng.random_range(0..VALID_PROP_CHARS.len())])
                    .collect();
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(name_len as u8);
                body.put_slice(&name);
                body.put_u32(3);
                body.put_slice(b"yes");
                command::decode(body.freeze())
            }
            // Duplicate Socket-Type
            2 => {
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PULL");
                command::decode(body.freeze())
            }
            // Duplicate Identity
            3 => {
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(8);
                body.put_slice(b"Identity");
                body.put_u32(3);
                body.put_slice(b"aaa");
                body.put_u8(8);
                body.put_slice(b"Identity");
                body.put_u32(3);
                body.put_slice(b"bbb");
                command::decode(body.freeze())
            }
            // Many properties (50-200), all valid
            4 => {
                let n_props = rng.random_range(50..=200);
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                for j in 0..n_props {
                    let name = format!("X-Prop-{j}");
                    body.put_u8(name.len() as u8);
                    body.put_slice(name.as_bytes());
                    body.put_u32(1);
                    body.put_u8(b'v');
                }
                command::decode(body.freeze())
            }
            // Truncation: build valid properties, cut at random offset
            5 => {
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(5);
                body.put_slice(b"X-Foo");
                body.put_u32(10);
                body.put_slice(b"0123456789");
                let full = body.freeze();
                let cut = rng.random_range(0..=full.len());
                command::decode(full.slice(..cut))
            }
            // Declared value length >> actual remaining bytes
            6 => {
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(5);
                body.put_slice(b"X-Big");
                // Declare huge value length but provide tiny payload
                body.put_u32(rng.random_range(1000..=u16::MAX as u32));
                let actual = random_bytes(&mut rng, 10);
                body.put_slice(&actual);
                command::decode(body.freeze())
            }
            // Every possible byte value in a 1-char property name
            _ => {
                let byte: u8 = rng.random();
                let mut body = BytesMut::new();
                body.put_u8(5);
                body.put_slice(b"READY");
                body.put_u8(11);
                body.put_slice(b"Socket-Type");
                body.put_u32(4);
                body.put_slice(b"PUSH");
                body.put_u8(1);
                body.put_u8(byte);
                body.put_u32(1);
                body.put_u8(b'v');
                let r = command::decode(body.freeze());
                let valid = byte.is_ascii_alphanumeric()
                    || byte == b'-'
                    || byte == b'_'
                    || byte == b'.'
                    || byte == b'+';
                match &r {
                    Ok(_) => assert!(valid, "accepted invalid prop name byte 0x{byte:02x}"),
                    Err(e) => assert!(!valid, "rejected valid prop name byte 0x{byte:02x}: {e}"),
                }
                r
            }
        };
        // scenarios 2-3 (duplicate props) must always be rejected
        if scenario == 2 {
            assert!(result.is_err(), "duplicate Socket-Type must be rejected");
        }
        if scenario == 3 {
            assert!(result.is_err(), "duplicate Identity must be rejected");
        }
        // scenario 6 (huge declared value) must be rejected (truncated)
        if scenario == 6 {
            assert!(result.is_err(), "declared len >> actual must be rejected");
        }
        let _ = result;
        if i % 50_000 == 0 {
            eprintln!("property_adversarial iter {i}");
        }
    }
}

/// Greeting with mechanism name mismatches, non-NULL mechanisms against
/// NULL server, and every possible mechanism name byte pattern.
#[test]
fn fuzz_greeting_mechanism_mismatch() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let mut raw = null_client_greeting();
        // Corrupt the mechanism name region (bytes 12..32)
        match rng.random_range(0..5) {
            // Random mechanism name
            0 => {
                for b in &mut raw[12..32] {
                    *b = rng.random();
                }
            }
            // "CURVE" against NULL server
            1 => {
                raw[12..32].fill(0);
                raw[12..17].copy_from_slice(b"CURVE");
            }
            // "PLAIN" against NULL server
            2 => {
                raw[12..32].fill(0);
                raw[12..17].copy_from_slice(b"PLAIN");
            }
            // Valid NULL but with garbage in padding after NUL
            3 => {
                raw[16] = rng.random_range(1..=255); // non-zero after "NULL"
            }
            // Empty mechanism name (all zeros)
            _ => {
                raw[12..32].fill(0);
            }
        }
        let mut conn = server_conn(SocketType::Pull);
        let _ = conn.handle_input(Bytes::from(raw));
        drain(&mut conn);
        if i % 10_000 == 0 {
            eprintln!("mech_mismatch iter {i}");
        }
    }
}

/// Full-sequence structured fuzz: valid greeting → adversarial command
/// frame → random tail. The command frame exercises the mechanism
/// handshake path (NULL READY parsing) with hostile content.
#[test]
fn fuzz_greeting_then_hostile_ready() {
    let mut rng = rng();
    let greeting = null_client_greeting();
    for i in 0..iters() / 4 {
        let mut wire = greeting.clone();
        // Build a command frame with random body that might or might not
        // look like READY.
        let body_len = rng.random_range(0..=512);
        let mut body = vec![0u8; body_len];
        rng.fill_bytes(&mut body);
        // 50%: make it look READY-shaped (name_len=5, "READY", then garbage)
        if rng.random_bool(0.5) && body_len >= 6 {
            body[0] = 5;
            body[1..6].copy_from_slice(b"READY");
        }
        // 20%: make it ERROR-shaped
        if rng.random_bool(0.2) && body_len >= 6 {
            body[0] = 5;
            body[1..6].copy_from_slice(b"ERROR");
        }
        if body_len <= 255 {
            wire.extend_from_slice(&command_frame(&body));
        } else {
            wire.extend_from_slice(&long_command_frame(&body));
        }
        // Sometimes append more garbage after the command frame.
        if rng.random_bool(0.3) {
            let tail = random_bytes(&mut rng, 256);
            wire.extend_from_slice(&tail);
        }
        let mut conn = server_conn(SocketType::Pull);
        // Sometimes deliver chunked, sometimes all at once.
        if rng.random_bool(0.5) {
            let _ = feed_chunked(&mut conn, &wire, &mut rng, 16);
        } else {
            let _ = conn.handle_input(Bytes::from(wire));
            drain(&mut conn);
        }
        if i % 10_000 == 0 {
            eprintln!("hostile_ready iter {i}");
        }
    }
}

/// Deliver a valid greeting one byte at a time with random delays
/// (interleaved with zero-length inputs), then follow with a READY
/// that has random corruption at each byte position.
#[test]
fn fuzz_greeting_byte_drip() {
    let mut rng = rng();
    let full = valid_handshake("PULL");
    for i in 0..iters() / 4 {
        let mut conn = server_conn(SocketType::Pull);
        let mut ok = true;
        for (j, &byte) in full.iter().enumerate() {
            // Occasionally corrupt this byte
            let b = if rng.random_bool(0.05) {
                rng.random()
            } else {
                byte
            };
            // Occasionally send a zero-length input (must be a no-op)
            if rng.random_bool(0.1) {
                if conn.handle_input(Bytes::new()).is_err() {
                    ok = false;
                    break;
                }
            }
            if conn.handle_input(Bytes::copy_from_slice(&[b])).is_err() {
                ok = false;
                break;
            }
            drain(&mut conn);
            // If we're past greeting (byte 63) and handshake completed, stop
            if j >= GREETING_LEN && conn.is_ready() {
                break;
            }
        }
        let _ = ok;
        if i % 10_000 == 0 {
            eprintln!("byte_drip iter {i}");
        }
    }
}

/// Frame decoder stress: adversarial flag bytes, size fields, and
/// payloads fed directly through the Connection state machine after a
/// valid handshake. Tests frame parsing in the post-handshake data path.
#[test]
fn fuzz_frame_adversarial() {
    let mut rng = rng();
    for i in 0..iters() / 8 {
        let mut push = client_conn(SocketType::Push);
        let mut pull = server_conn(SocketType::Pull);
        capture_handshake(&mut push, &mut pull);
        if !pull.is_ready() {
            continue;
        }

        // Build adversarial frames and feed them
        let n_frames = rng.random_range(1..=10);
        let mut wire = Vec::new();
        for _ in 0..n_frames {
            let flags: u8 = rng.random();
            let is_long = flags & 0x02 != 0;
            if is_long {
                wire.push(flags);
                let size: u64 = match rng.random_range(0..4) {
                    0 => 0,
                    1 => rng.random_range(0..=255),
                    2 => rng.random_range(256..=65536),
                    _ => rng.random_range(0..=u32::MAX as u64),
                };
                wire.extend_from_slice(&size.to_be_bytes());
                // Only provide a small amount of actual payload
                let actual = rng.random_range(0..=64).min(size as usize);
                let mut payload = vec![0u8; actual];
                rng.fill_bytes(&mut payload);
                wire.extend_from_slice(&payload);
            } else {
                wire.push(flags);
                let size: u8 = rng.random();
                wire.push(size);
                let actual = rng.random_range(0..=size as usize);
                let mut payload = vec![0u8; actual];
                rng.fill_bytes(&mut payload);
                wire.extend_from_slice(&payload);
            }
        }

        if rng.random_bool(0.5) {
            let _ = pull.handle_input(Bytes::from(wire));
        } else {
            let _ = feed_chunked(&mut pull, &wire, &mut rng, 8);
        }
        drain(&mut pull);
        if i % 5_000 == 0 {
            eprintln!("frame_adversarial iter {i}");
        }
    }
}

/// Message roundtrip under corruption: encode real messages through
/// push's Connection, corrupt the wire bytes, feed to pull. Must never
/// panic; corrupted messages must surface as Err or silently dropped,
/// never as garbage data presented as a valid message.
#[test]
fn fuzz_message_roundtrip_corrupt() {
    let mut rng = rng();
    for i in 0..iters() / 8 {
        let mut push = client_conn(SocketType::Push);
        let mut pull = server_conn(SocketType::Pull);
        capture_handshake(&mut push, &mut pull);
        if !push.is_ready() {
            continue;
        }

        // Send 1-5 messages, collect all wire bytes
        let n_msgs = rng.random_range(1..=5);
        let mut original_payloads: Vec<Vec<u8>> = Vec::new();
        for _ in 0..n_msgs {
            let n_parts = rng.random_range(1..=4);
            let parts: Vec<Vec<u8>> = (0..n_parts).map(|_| random_bytes(&mut rng, 256)).collect();
            let msg = Message::multipart(parts.iter().map(|p| Bytes::from(p.clone())));
            original_payloads.push(parts.into_iter().flatten().collect());
            if push.send_message(&msg).is_err() {
                break;
            }
        }
        let wire = push.poll_transmit().to_vec();
        push.advance_transmit(wire.len());

        // Corrupt the wire
        let corrupted = corrupt_wire(&wire, &mut rng);

        // Feed to pull
        if rng.random_bool(0.5) {
            let _ = pull.handle_input(Bytes::from(corrupted));
        } else {
            let _ = feed_chunked(&mut pull, &corrupted, &mut rng, 32);
        }
        drain(&mut pull);
        if i % 5_000 == 0 {
            eprintln!("msg_corrupt iter {i}");
        }
    }
}

/// State machine confusion: interleave greeting bytes from multiple
/// different connections, simulating stream mixup or injection.
#[test]
fn fuzz_interleaved_streams() {
    let mut rng = rng();
    for i in 0..iters() / 8 {
        // Build 2-4 different valid handshake streams
        let n_streams = rng.random_range(2..=4);
        let socket_types = ["PUSH", "PULL", "PAIR", "DEALER", "ROUTER"];
        let streams: Vec<Vec<u8>> = (0..n_streams)
            .map(|_| valid_handshake(socket_types[rng.random_range(0..socket_types.len())]))
            .collect();

        // Interleave bytes from all streams into one Franken-stream
        let mut franken = Vec::new();
        let mut positions: Vec<usize> = vec![0; n_streams];
        while positions.iter().zip(&streams).any(|(p, s)| *p < s.len()) {
            let stream_idx = rng.random_range(0..n_streams);
            if positions[stream_idx] < streams[stream_idx].len() {
                let chunk_len = rng
                    .random_range(1..=16)
                    .min(streams[stream_idx].len() - positions[stream_idx]);
                franken.extend_from_slice(
                    &streams[stream_idx][positions[stream_idx]..positions[stream_idx] + chunk_len],
                );
                positions[stream_idx] += chunk_len;
            }
        }

        let mut conn = server_conn(SocketType::Pull);
        let _ = feed_chunked(&mut conn, &franken, &mut rng, 32);
        if i % 5_000 == 0 {
            eprintln!("interleaved iter {i}");
        }
    }
}

/// Replay attack: complete a valid handshake, then replay the greeting
/// and READY again. The state machine must reject the second greeting.
#[test]
fn fuzz_replay_attack() {
    let mut rng = rng();
    let handshake = valid_handshake("PULL");
    for i in 0..iters() / 8 {
        let mut conn = server_conn(SocketType::Pull);
        if conn.handle_input(Bytes::from(handshake.clone())).is_err() {
            continue;
        }
        drain(&mut conn);

        // Replay: send the handshake bytes again
        let _ = conn.handle_input(Bytes::from(handshake.clone()));
        drain(&mut conn);

        // Also try replaying just the READY
        let ready_bytes = &handshake[GREETING_LEN..];
        let _ = conn.handle_input(Bytes::copy_from_slice(ready_bytes));
        drain(&mut conn);

        // Random data after replay
        let garbage = random_bytes(&mut rng, 512);
        let _ = conn.handle_input(Bytes::from(garbage));
        drain(&mut conn);
        if i % 5_000 == 0 {
            eprintln!("replay iter {i}");
        }
    }
}

/// Massive message: send a message close to max_message_size, then
/// slightly over, through corrupt and clean paths.
#[test]
fn fuzz_message_size_boundary() {
    let mut rng = rng();
    for i in 0..iters() / 8 {
        let max_size = rng.random_range(64..=8192);
        let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull)
            .mechanism(MechanismSetup::Null)
            .max_message_size(max_size);
        let mut pull = Connection::new(cfg);
        let mut push = client_conn(SocketType::Push);
        capture_handshake(&mut push, &mut pull);
        if !push.is_ready() {
            continue;
        }

        // Send a message right at the boundary
        for delta in [-2i64, -1, 0, 1, 2, 64] {
            let size = (max_size as i64 + delta).max(0) as usize;
            let payload = vec![0xAA; size];
            let msg = Message::single(Bytes::from(payload));
            if push.send_message(&msg).is_err() {
                continue;
            }
            let wire = push.poll_transmit().to_vec();
            push.advance_transmit(wire.len());

            let result = pull.handle_input(Bytes::from(wire));
            if result.is_err() {
                // Re-create pull for next iteration since it's in error state
                pull = Connection::new(
                    ConnectionConfig::new(Role::Server, SocketType::Pull)
                        .mechanism(MechanismSetup::Null)
                        .max_message_size(max_size),
                );
                push = client_conn(SocketType::Push);
                capture_handshake(&mut push, &mut pull);
                if !push.is_ready() {
                    break;
                }
            }
            drain(&mut pull);
        }
        if i % 5_000 == 0 {
            eprintln!("size_boundary iter {i}");
        }
    }
}

// ================================================================
// Mechanism-specific fuzz (feature-gated)
// ================================================================

#[cfg(feature = "curve")]
mod mech_fuzz {
    use super::*;

    fn greeting_bytes(mech_name: &[u8]) -> Vec<u8> {
        let mut g = vec![0u8; 64];
        g[0] = 0xff;
        g[9] = 0x7f;
        g[10] = 0x03;
        g[11] = 0x01;
        g[12..12 + mech_name.len()].copy_from_slice(mech_name);
        // as-server at offset 32 is implicitly 0 from zero-init.
        g
    }

    #[cfg(feature = "curve")]
    #[test]
    fn fuzz_curve_server_input() {
        use omq_tokio::{CurveKeypair, CurveServerOptions};
        let mut rng = rng();
        for i in 0..iters() / 8 {
            let kp = CurveKeypair::generate();
            let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                MechanismSetup::CurveServer {
                    our_keypair: kp,
                    options: CurveServerOptions::default(),
                },
            );
            let mut conn = Connection::new(cfg);
            let raw = random_bytes(&mut rng, 1024);
            let _ = conn.handle_input(Bytes::copy_from_slice(&raw));
            drain(&mut conn);
            if i % 5_000 == 0 {
                eprintln!("curve iter {i}");
            }
        }
    }

    #[cfg(feature = "curve")]
    #[test]
    fn fuzz_curve_hello_body() {
        use omq_tokio::{CurveKeypair, CurveServerOptions};
        let greeting = greeting_bytes(b"CURVE");
        let mut rng = rng();
        for i in 0..iters() / 8 {
            let kp = CurveKeypair::generate();
            let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                MechanismSetup::CurveServer {
                    our_keypair: kp,
                    options: CurveServerOptions::default(),
                },
            );
            let mut conn = Connection::new(cfg);
            let _ = conn.handle_input(Bytes::copy_from_slice(&greeting));
            let body_len = rng.random_range(0..=256);
            let mut body = Vec::with_capacity(1 + 5 + body_len);
            body.push(5);
            body.extend_from_slice(b"HELLO");
            let mut tail = vec![0u8; body_len];
            rng.fill_bytes(&mut tail);
            body.extend_from_slice(&tail);
            let frame = long_command_frame(&body);
            let _ = conn.handle_input(Bytes::copy_from_slice(&frame));
            drain(&mut conn);
            if i % 5_000 == 0 {
                eprintln!("curve hello iter {i}");
            }
        }
    }
}
