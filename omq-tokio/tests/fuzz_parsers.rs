#![cfg(feature = "fuzz")]
//! Hand-rolled fuzz tests for the wire parsers and the codec
//! state machine. Uses `rand` (already a dep) to generate
//! random byte sequences and asserts that decoders never panic
//! on hostile input - they must return `Result::Err`.
//!
//! This is coverage-blind (no libfuzzer / no cargo-fuzz on stable),
//! but at ~1 M iterations per target it still surfaces panics on
//! the cheap. Set `OMQ_FUZZ_SEED=<u64>` to reproduce a given run;
//! set `OMQ_FUZZ_ITERS=<N>` to tune the per-target budget.

use bytes::{Bytes, BytesMut};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use omq_tokio::proto::{
    SocketType, command,
    connection::{Connection, ConnectionConfig, Role},
    frame, greeting,
    mechanism::MechanismSetup,
    z85,
};

fn iters() -> usize {
    std::env::var("OMQ_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000)
}

fn rng() -> StdRng {
    let seed: u64 = std::env::var("OMQ_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            let mut s = [0u8; 8];
            rand::thread_rng().fill_bytes(&mut s);
            u64::from_le_bytes(s)
        });
    eprintln!("OMQ_FUZZ_SEED={seed}");
    StdRng::seed_from_u64(seed)
}

fn random_bytes(rng: &mut StdRng, max_len: usize) -> Vec<u8> {
    let len = rng.gen_range(0..=max_len);
    let mut v = vec![0u8; len];
    rng.fill_bytes(&mut v);
    v
}

#[test]
fn fuzz_greeting_decode() {
    let mut rng = rng();
    for i in 0..iters() {
        let raw = random_bytes(&mut rng, 96);
        let mut buf = BytesMut::from(&raw[..]);
        // Must not panic on any input.
        let _ = greeting::try_decode(&mut buf);
        if i % 50_000 == 0 {
            eprintln!("greeting iter {i}");
        }
    }
}

#[test]
fn fuzz_frame_decode() {
    let mut rng = rng();
    for i in 0..iters() {
        let raw = random_bytes(&mut rng, 4096);
        let mut buf = BytesMut::from(&raw[..]);
        let _ = frame::try_decode_frame(&mut buf);
        if i % 50_000 == 0 {
            eprintln!("frame iter {i}");
        }
    }
}

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

/// Feed a server-side codec a random byte stream as one big chunk and
/// drain events. Catches panics in the full state machine
/// (greeting → mechanism → ready) under hostile input.
#[test]
fn fuzz_handle_input_full_stream() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let cfg =
            ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(MechanismSetup::Null);
        let mut conn = Connection::new(cfg);
        let raw = random_bytes(&mut rng, 4096);
        let _ = conn.handle_input(&raw);
        // Drain whatever events the codec emitted.
        while conn.poll_event().is_some() {}
        if i % 10_000 == 0 {
            eprintln!("full_stream iter {i}");
        }
    }
}

/// Same as above but feeds the bytes in random-sized chunks. Exercises
/// partial-read paths in the state machine where the buffer holds an
/// incomplete header / frame between drives.
#[test]
fn fuzz_handle_input_chunked() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let cfg =
            ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(MechanismSetup::Null);
        let mut conn = Connection::new(cfg);
        let raw = random_bytes(&mut rng, 4096);
        let mut pos = 0;
        while pos < raw.len() {
            let chunk_len = rng.gen_range(1..=raw.len() - pos).min(64);
            if conn.handle_input(&raw[pos..pos + chunk_len]).is_err() {
                break; // codec rejected; further input would panic? no, but stop anyway
            }
            while conn.poll_event().is_some() {}
            pos += chunk_len;
        }
        if i % 10_000 == 0 {
            eprintln!("chunked iter {i}");
        }
    }
}

/// Exercise both client and server roles end-to-end: feed each side a
/// random stream and ensure neither panics. Doesn't require a valid
/// handshake - any rejected input must surface as Err.
#[test]
fn fuzz_handle_input_both_roles() {
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let role = if rng.gen_bool(0.5) {
            Role::Server
        } else {
            Role::Client
        };
        let st = match rng.gen_range(0..4) {
            0 => SocketType::Push,
            1 => SocketType::Pull,
            2 => SocketType::Pair,
            _ => SocketType::Dealer,
        };
        let cfg = ConnectionConfig::new(role, st).mechanism(MechanismSetup::Null);
        let mut conn = Connection::new(cfg);
        let raw = random_bytes(&mut rng, 2048);
        let _ = conn.handle_input(&raw);
        while conn.poll_event().is_some() {}
        if i % 10_000 == 0 {
            eprintln!("both_roles iter {i}");
        }
    }
}

/// Roundtrip property: encode an arbitrary frame, decode it, must
/// match. Catches asymmetries in the LONG / SHORT length encoding,
/// flag bit handling, or buffer-management bugs in either side.
#[test]
fn fuzz_frame_roundtrip() {
    use bytes::BytesMut;
    use omq_tokio::message::{Frame, FrameFlags, Payload};
    use omq_tokio::proto::frame::{encode_frame, try_decode_frame};
    let mut rng = rng();
    for i in 0..iters() / 2 {
        // Random size sweep covering both short (<=255) and long (>255)
        // header forms, plus boundary +/- 1.
        let size = match rng.gen_range(0..6) {
            0 => 0,
            1 => rng.gen_range(1..=255),
            2 => 255,
            3 => 256,
            4 => rng.gen_range(256..=65_536),
            _ => rng.gen_range(0..=65_536),
        };
        let mut payload = vec![0u8; size];
        rng.fill_bytes(&mut payload);
        let bytes = Bytes::from(payload);
        // command + more is illegal; flip them independently but
        // never both on.
        let (more, command) = match rng.gen_range(0..3) {
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
        let decoded =
            try_decode_frame(&mut out).expect("decode of self-encoded frame must not error");
        let decoded = decoded.expect("must produce a frame");
        assert_eq!(decoded.flags.more, more, "more bit");
        assert_eq!(decoded.flags.command, command, "command bit");
        assert_eq!(decoded.payload.coalesce(), bytes, "payload mismatch");
        assert!(out.is_empty(), "decoder left {} bytes pending", out.len());
        if i % 50_000 == 0 {
            eprintln!("frame_roundtrip iter {i}");
        }
    }
}

/// Structured-mutation fuzz: start with a valid client greeting prefix
/// and randomly perturb a small number of bytes. This drives the
/// state machine PAST the greeting check so the mechanism / frame
/// parsers see exercise (purely random bytes almost always fail at
/// the magic-byte check, leaving deeper paths unexplored).
#[test]
fn fuzz_handle_input_perturbed_greeting() {
    let mut rng = rng();
    // A valid 64-byte ZMTP 3.1 client greeting with NULL mechanism.
    // sig(10) + version(2) + mechanism(20) + as-server(1) + filler(31)
    let mut base: [u8; 64] = [0; 64];
    base[0] = 0xff;
    base[9] = 0x7f;
    base[10] = 0x03; // major
    base[11] = 0x01; // minor
    base[12..16].copy_from_slice(b"NULL");
    base[31] = 0; // as-server = false (client)
    for i in 0..iters() / 4 {
        let mut buf = base.to_vec();
        // Append a random tail of frame-or-junk bytes.
        let tail_len = rng.gen_range(0..=512);
        let mut tail = vec![0u8; tail_len];
        rng.fill_bytes(&mut tail);
        buf.extend_from_slice(&tail);
        // Flip 0..3 random bytes in the greeting prefix.
        let flips = rng.gen_range(0..=3);
        for _ in 0..flips {
            let pos = rng.gen_range(0..64);
            buf[pos] = rng.r#gen();
        }
        let cfg =
            ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(MechanismSetup::Null);
        let mut conn = Connection::new(cfg);
        let _ = conn.handle_input(&buf);
        while conn.poll_event().is_some() {}
        if i % 10_000 == 0 {
            eprintln!("perturbed iter {i}");
        }
    }
}

/// z85 is the ASCII armor used for CURVE keys - fed user-supplied
/// strings, must never panic on hostile input (incl. invalid chars,
/// lengths not divisible by 5, leading/trailing whitespace, etc.).
#[test]
fn fuzz_z85_decode() {
    let mut rng = rng();
    let alphabet: &[u8] =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    for i in 0..iters() {
        // Mix 90% z85-alphabet bytes with 10% arbitrary, length 0..256.
        let len = rng.gen_range(0..=256);
        let mut s = String::with_capacity(len);
        for _ in 0..len {
            if rng.gen_bool(0.9) {
                s.push(alphabet[rng.gen_range(0..alphabet.len())] as char);
            } else {
                let b = rng.gen_range(0u8..=127);
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

/// LZ4 transform decoder fuzz: random byte parts fed to the decoder,
/// must never panic (must reject as Err). Catches panics in the
/// SENTINEL parsing, declared-size handling, and decompress paths.
#[cfg(feature = "lz4")]
#[test]
fn fuzz_lz4_decode() {
    use omq_tokio::message::Message;
    use omq_tokio::proto::transform::lz4::Lz4Transform;
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let mut tx = Lz4Transform::new();
        // Build a random message with 1..=4 random parts.
        let n_parts = rng.gen_range(1..=4);
        let mut msg = Message::new();
        for _ in 0..n_parts {
            let part = random_bytes(&mut rng, 256);
            msg.push_part(omq_tokio::message::Payload::from_bytes(Bytes::from(part)));
        }
        let _ = tx.decode(msg);
        if i % 10_000 == 0 {
            eprintln!("lz4 iter {i}");
        }
    }
}

#[cfg(feature = "zstd")]
#[test]
fn fuzz_zstd_decode() {
    use omq_tokio::message::Message;
    use omq_tokio::proto::transform::zstd::ZstdTransform;
    let mut rng = rng();
    for i in 0..iters() / 4 {
        let mut tx = ZstdTransform::new();
        let n_parts = rng.gen_range(1..=4);
        let mut msg = Message::new();
        for _ in 0..n_parts {
            let part = random_bytes(&mut rng, 256);
            msg.push_part(omq_tokio::message::Payload::from_bytes(Bytes::from(part)));
        }
        let _ = tx.decode(msg);
        if i % 10_000 == 0 {
            eprintln!("zstd iter {i}");
        }
    }
}

#[cfg(any(feature = "curve", feature = "blake3zmq"))]
mod mech_fuzz {
    use super::*;
    use omq_tokio::proto::mechanism::MechanismSetup;

    /// Build a valid 64-byte client-side greeting for the given
    /// mechanism name (≤ 20 ASCII bytes, NUL-padded to 20). Fed into
    /// the mechanism-specific server fuzzers below so the random
    /// post-greeting bytes actually reach the mechanism handshake
    /// parsers.
    fn greeting_bytes(mech_name: &[u8]) -> Vec<u8> {
        let mut g = vec![0u8; 64];
        g[0] = 0xff;
        g[9] = 0x7f;
        g[10] = 0x03;
        g[11] = 0x01;
        g[12..12 + mech_name.len()].copy_from_slice(mech_name);
        g[31] = 0; // as-server = false (we're the client side of greeting)
        g
    }

    /// Wrap a body as a long-form COMMAND frame: flags=0x06 (CMD|LONG)
    /// + 8-byte size + body. Long form covers all command sizes
    /// without us guessing whether short form fits.
    fn long_command_frame(body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(9 + body.len());
        out.push(0b0000_0110); // COMMAND | LONG
        out.extend_from_slice(&(body.len() as u64).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// CURVE server-side fuzz, raw bytes only - most attempts fail at
    /// greeting; deeper paths exercised by the structured variant
    /// below.
    #[cfg(feature = "curve")]
    #[test]
    fn fuzz_curve_server_input() {
        use omq_tokio::CurveKeypair;
        let mut rng = rng();
        for i in 0..iters() / 8 {
            let kp = CurveKeypair::generate();
            let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                MechanismSetup::CurveServer {
                    keypair: kp,
                    authenticator: None,
                },
            );
            let mut conn = Connection::new(cfg);
            let raw = random_bytes(&mut rng, 1024);
            let _ = conn.handle_input(&raw);
            while conn.poll_event().is_some() {}
            if i % 5_000 == 0 {
                eprintln!("curve iter {i}");
            }
        }
    }

    /// CURVE server-side, structured: feed a valid greeting + a
    /// random HELLO-shaped command, so the random bytes actually
    /// land in `parse_hello`.
    #[cfg(feature = "curve")]
    #[test]
    fn fuzz_curve_hello_body() {
        use omq_tokio::CurveKeypair;
        let greeting = greeting_bytes(b"CURVE");
        let mut rng = rng();
        for i in 0..iters() / 8 {
            let kp = CurveKeypair::generate();
            let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                MechanismSetup::CurveServer {
                    keypair: kp,
                    authenticator: None,
                },
            );
            let mut conn = Connection::new(cfg);
            let _ = conn.handle_input(&greeting);
            // HELLO is 194-byte body for the well-formed case;
            // mutate length 0..256 to also hit the early-reject paths.
            let body_len = rng.gen_range(0..=256);
            // First byte of body is the command-name length; spelling
            // "HELLO" is what makes the dispatcher route here.
            let mut body = Vec::with_capacity(1 + 5 + body_len);
            body.push(5);
            body.extend_from_slice(b"HELLO");
            let mut tail = vec![0u8; body_len];
            rng.fill_bytes(&mut tail);
            body.extend_from_slice(&tail);
            let frame = long_command_frame(&body);
            let _ = conn.handle_input(&frame);
            while conn.poll_event().is_some() {}
            if i % 5_000 == 0 {
                eprintln!("curve hello iter {i}");
            }
        }
    }

    #[cfg(feature = "blake3zmq")]
    #[test]
    fn fuzz_blake3zmq_server_input() {
        use omq_tokio::Blake3ZmqKeypair;
        use omq_tokio::proto::mechanism::blake3zmq::CookieKeyring;
        use std::sync::Arc;
        let mut rng = rng();
        let keyring = Arc::new(CookieKeyring::new());
        for i in 0..iters() / 8 {
            let kp = Blake3ZmqKeypair::generate();
            let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                MechanismSetup::Blake3ZmqServer {
                    keypair: kp,
                    cookie_keyring: keyring.clone(),
                    authenticator: None,
                },
            );
            let mut conn = Connection::new(cfg);
            let raw = random_bytes(&mut rng, 1024);
            let _ = conn.handle_input(&raw);
            while conn.poll_event().is_some() {}
            if i % 5_000 == 0 {
                eprintln!("blake3zmq iter {i}");
            }
        }
    }

    /// BLAKE3ZMQ server-side, structured HELLO. Greeting carries the
    /// mechanism name "BLAKE3" (7 bytes), then a random HELLO body of
    /// the expected ~232-byte shape (or shorter, to exercise reject
    /// paths).
    #[cfg(feature = "blake3zmq")]
    #[test]
    fn fuzz_blake3zmq_hello_body() {
        use omq_tokio::Blake3ZmqKeypair;
        use omq_tokio::proto::mechanism::blake3zmq::CookieKeyring;
        use std::sync::Arc;
        let greeting = greeting_bytes(b"BLAKE3");
        let keyring = Arc::new(CookieKeyring::new());
        let mut rng = rng();
        for i in 0..iters() / 8 {
            let kp = Blake3ZmqKeypair::generate();
            let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                MechanismSetup::Blake3ZmqServer {
                    keypair: kp,
                    cookie_keyring: keyring.clone(),
                    authenticator: None,
                },
            );
            let mut conn = Connection::new(cfg);
            let _ = conn.handle_input(&greeting);
            let body_len = rng.gen_range(0..=256);
            let mut body = Vec::with_capacity(1 + 5 + body_len);
            body.push(5);
            body.extend_from_slice(b"HELLO");
            let mut tail = vec![0u8; body_len];
            rng.fill_bytes(&mut tail);
            body.extend_from_slice(&tail);
            let frame = long_command_frame(&body);
            let _ = conn.handle_input(&frame);
            while conn.poll_event().is_some() {}
            if i % 5_000 == 0 {
                eprintln!("blake3zmq hello iter {i}");
            }
        }
    }
}
