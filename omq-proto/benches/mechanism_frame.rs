//! Per-frame mechanism overhead microbench.
//!
//! Measures the raw cryptographic cost of sealing one ZMTP frame
//! payload under each mechanism. Wrapper overhead (nonce assembly,
//! counter increment, AAD construction) is sub-nanosecond and not
//! distinguished here - we go straight at the primitives:
//!
//! - **NULL**       baseline; no crypto, just `Bytes::copy_from_slice`.
//! - **CURVE**      one `crypto_box::SalsaBox` seal (XSalsa20 +
//!   Poly1305 16-byte tag, RFC 26).
//! - **BLAKE3ZMQ**  one `chacha20-blake3` AEAD seal (ChaCha20 + keyed
//!   BLAKE3 32-byte tag, RFC §10).
//!
//! Run: `cargo bench -p omq-proto --bench mechanism_frame --features 'curve blake3zmq'`
//!
//! omq-proto pins a fork of `chacha20-blake3` adding
//! `#[target_feature(enable = "avx2")]` to the AVX2/AVX512 functions,
//! so the SIMD paths light up under stock release flags. Without that
//! patch you also need `RUSTFLAGS='-C target-cpu=native'` to get
//! equivalent numbers; without either, BLAKE3ZMQ drops to ~50 MiB/s
//! at bulk sizes. `crypto_box` (CURVE) does not have a SIMD path for
//! salsa20 either way.

use std::hint::black_box;
use std::time::Instant;

use chacha20_blake3::ChaCha20Blake3;
use crypto_box::{aead::Aead, SalsaBox, SecretKey};

const SIZES: &[usize] = &[64, 256, 1024, 4096, 16384, 65536];

/// Target wall-time per cell. The bench picks an iteration count that
/// roughly hits this - large payloads run fewer iters, small ones run
/// more, so each cell takes ~the same time and the table doesn't
/// stretch out into 30-minute territory.
const TARGET_NS_PER_CELL: u64 = 200_000_000; // 200 ms

fn main() {
    println!("Mechanism per-frame microbench");
    println!(
        "primitives: NULL (memcpy) | CURVE (XSalsa20Poly1305) | BLAKE3ZMQ (ChaCha20-BLAKE3)"
    );
    println!("target wall-time per cell: ~{} ms\n", TARGET_NS_PER_CELL / 1_000_000);

    println!(
        "  {:>6} | {:>14} | {:>14} | {:>14}",
        "size", "NULL ns/op", "CURVE", "BLAKE3ZMQ"
    );
    println!("  {}", "-".repeat(64));

    // Defeat constant-folding by going through black_box for the key
    // and nonce material. Without this, LLVM hoists/precomputes parts
    // of the BLAKE3 KDF for the short-payload cases.
    let key_b3: [u8; 32] = black_box([0x42u8; 32]);
    let nonce_b3: [u8; 24] = black_box([0x11u8; 24]);
    let cipher_b3 = ChaCha20Blake3::new(key_b3);
    let aad: &[u8] = &[];

    let secret_a = SecretKey::generate(&mut rand::rngs::OsRng);
    let secret_b = SecretKey::generate(&mut rand::rngs::OsRng);
    let salsa = SalsaBox::new(&secret_b.public_key(), &secret_a);
    let nonce_curve = crypto_box::Nonce::from(black_box([0x22u8; 24]));

    let mut rows = Vec::with_capacity(SIZES.len());
    for &size in SIZES {
        let plain = vec![0xACu8; size];

        let null_ns = bench(|| {
            black_box(bytes::Bytes::copy_from_slice(black_box(&plain)));
        });
        let curve_ns = bench(|| {
            black_box(
                salsa
                    .encrypt(&nonce_curve, black_box(plain.as_slice()))
                    .unwrap(),
            );
        });
        let b3_ns = bench(|| {
            black_box(cipher_b3.encrypt(&nonce_b3, black_box(&plain), aad));
        });

        println!(
            "  {:>6} | {:>14} | {:>14} | {:>14}",
            size,
            format!("{null_ns:>5} ns"),
            format!("{curve_ns:>5} ns"),
            format!("{b3_ns:>5} ns"),
        );
        rows.push((size, null_ns, curve_ns, b3_ns));
    }

    println!();
    println!(
        "  {:>6} | {:>14} | {:>14} | {:>14}",
        "size", "NULL MiB/s", "CURVE", "BLAKE3ZMQ"
    );
    println!("  {}", "-".repeat(64));
    for (size, null_ns, curve_ns, b3_ns) in rows {
        println!(
            "  {:>6} | {:>14} | {:>14} | {:>14}",
            size,
            mibps(size, null_ns),
            mibps(size, curve_ns),
            mibps(size, b3_ns),
        );
    }
}

fn bench(mut f: impl FnMut()) -> u64 {
    // Warm up + size-up: keep doubling until one batch takes ~10 ms,
    // then run enough batches to hit TARGET_NS_PER_CELL total.
    let mut iters: u64 = 1;
    loop {
        let start = Instant::now();
        for _ in 0..iters {
            f();
        }
        let probe = start.elapsed().as_nanos() as u64;
        if probe > 10_000_000 || iters > 1 << 24 {
            let total_iters = ((TARGET_NS_PER_CELL / probe.max(1)).max(1) * iters).max(iters);
            let start = Instant::now();
            for _ in 0..total_iters {
                f();
            }
            let elapsed = start.elapsed().as_nanos() as u64;
            return elapsed / total_iters.max(1);
        }
        iters *= 2;
    }
}

fn mibps(size: usize, ns: u64) -> String {
    if ns == 0 {
        return "    -    ".into();
    }
    let bytes_per_sec = (size as f64) / (ns as f64) * 1e9;
    format!("{:>9.0}", bytes_per_sec / (1024.0 * 1024.0))
}
